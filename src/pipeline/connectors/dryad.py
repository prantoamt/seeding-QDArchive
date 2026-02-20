"""Dryad REST API v2 connector for public datasets."""

import logging
import re
import time
from pathlib import Path
from urllib.parse import quote

import httpx

from pipeline.connectors.base import BaseConnector, SearchResult

logger = logging.getLogger("pipeline")

# Timeout for API requests (seconds)
REQUEST_TIMEOUT = 30.0
DOWNLOAD_TIMEOUT = 120.0

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2.0  # seconds, doubles each retry

# Rate limiting: anonymous limit is 30 req/min, lower for downloads
MIN_REQUEST_INTERVAL = 2.0

BASE_URL = "https://datadryad.org/api/v2"
SITE_URL = "https://datadryad.org"


class DryadConnector(BaseConnector):
    """Connector for the Dryad open-data repository."""

    def __init__(self) -> None:
        self._last_request_time = 0.0

    @property
    def name(self) -> str:
        return "dryad"

    def _throttle(self) -> None:
        """Enforce minimum interval between API requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def search(self, query: str, file_type: str | None = None) -> list[SearchResult]:
        """Search Dryad datasets, with pagination."""
        results: list[SearchResult] = []
        page = 1
        per_page = 25

        while True:
            self._throttle()
            params: dict[str, str | int] = {
                "q": query,
                "per_page": per_page,
                "page": page,
            }
            resp = httpx.get(
                f"{BASE_URL}/search",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            total = data.get("total", 0)
            embedded = data.get("_embedded", {})
            items = embedded.get("stash:datasets", [])
            if not items:
                break

            for item in items:
                doi = item.get("identifier", "")
                title = item.get("title", "")
                abstract = _strip_html(item.get("abstract", ""))

                authors = item.get("authors", [])
                author_names = "; ".join(
                    f"{a.get('firstName', '')} {a.get('lastName', '')}".strip()
                    for a in authors
                    if a.get("firstName") or a.get("lastName")
                )

                keywords = item.get("keywords", []) or []

                source_url = f"{SITE_URL}/stash/dataset/{doi}" if doi else ""

                result = SearchResult(
                    source_name="dryad",
                    source_url=source_url,
                    title=title,
                    description=abstract,
                    authors=author_names,
                    date_published=item.get("publicationDate", ""),
                    keywords=keywords,
                    tags=keywords,
                )
                results.append(result)

            if page * per_page >= total:
                break
            page += 1

        logger.info("Search '%s' on dryad returned %d records", query, len(results))
        return results

    def get_metadata(self, record_url: str) -> SearchResult:
        """Fetch full metadata for a Dryad dataset.

        Accepts URLs like https://datadryad.org/stash/dataset/doi:10.5061/dryad.xxx
        or a bare DOI string.
        """
        doi = _extract_doi(record_url)

        # Fetch dataset metadata
        self._throttle()
        encoded_doi = _encode_doi(doi)
        resp = httpx.get(
            f"{BASE_URL}/datasets/{encoded_doi}",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        # Basic metadata
        title = data.get("title", "")
        description = _strip_html(data.get("abstract", ""))

        authors = data.get("authors", [])
        author_names = "; ".join(
            f"{a.get('firstName', '')} {a.get('lastName', '')}".strip()
            for a in authors
            if a.get("firstName") or a.get("lastName")
        )

        # License
        license_url = data.get("license", "")
        license_type = _parse_license(license_url)

        # Keywords (optional)
        keywords = data.get("keywords", []) or []

        # Geographic coverage from locations (optional)
        locations = data.get("locations", []) or []
        geo_coverage = [
            loc.get("place") for loc in locations if loc.get("place")
        ]

        # Funders → producer (optional)
        funders = data.get("funders", []) or []
        producers = [
            f.get("organization") for f in funders if f.get("organization")
        ]

        # Related works → publication (optional)
        related_works = data.get("relatedWorks", []) or []
        publications = []
        for rw in related_works:
            ident = rw.get("identifier", "")
            relationship = rw.get("relationship", "")
            if ident:
                publications.append(
                    f"{relationship}: {ident}" if relationship else ident
                )

        # Uploader = first author
        uploader_name = ""
        uploader_email = ""
        if authors:
            first = authors[0]
            uploader_name = (
                f"{first.get('firstName', '')} {first.get('lastName', '')}".strip()
            )
            uploader_email = first.get("email", "") or ""

        # Get version ID from _links to fetch files
        version_href = (
            data.get("_links", {})
            .get("stash:version", {})
            .get("href", "")
        )

        files = []
        if version_href:
            self._throttle()
            # version_href is a relative path like /api/v2/versions/{id}
            if version_href.startswith("/"):
                version_url = f"{SITE_URL}{version_href}"
            else:
                version_url = version_href
            files_resp = httpx.get(
                f"{version_url}/files",
                timeout=REQUEST_TIMEOUT,
            )
            files_resp.raise_for_status()
            files_data = files_resp.json()

            file_items = files_data.get("_embedded", {}).get("stash:files", [])
            doi_sanitized = _sanitize_doi(doi)

            for f in file_items:
                file_path = f.get("path", "")
                ext = Path(file_path).suffix.lstrip(".") if file_path else ""

                # API download endpoint requires auth; use public
                # file_stream URL instead: /stash/downloads/file_stream/{id}
                download_href = (
                    f.get("_links", {})
                    .get("stash:download", {})
                    .get("href", "")
                )
                file_id = _extract_file_id(download_href)
                download_url = (
                    f"{SITE_URL}/stash/downloads/file_stream/{file_id}"
                    if file_id
                    else ""
                )

                digest = f.get("digest", "")
                api_checksum = f"sha-256:{digest}" if digest else ""

                files.append({
                    "id": doi_sanitized,
                    "name": file_path,
                    "size": f.get("size", 0),
                    "download_url": download_url,
                    "api_checksum": api_checksum,
                    "restricted": False,
                    "content_type": f.get("mimeType", ""),
                    "friendly_type": ext,
                })

        return SearchResult(
            source_name="dryad",
            source_url=record_url,
            title=title,
            description=description,
            authors=author_names,
            license_type=license_type,
            license_url=license_url,
            date_published=data.get("publicationDate", ""),
            keywords=keywords,
            tags=keywords,
            kind_of_data=[],
            language=[],
            software=[],
            geographic_coverage=geo_coverage,
            depositor="",
            producer=producers,
            publication=publications,
            uploader_name=uploader_name,
            uploader_email=uploader_email,
            files=files,
        )

    def download(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Download a file from Dryad. Returns local file path.

        Retries up to MAX_RETRIES times on connection errors with exponential backoff.
        """
        self._throttle()
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with httpx.stream(
                    "GET", url, timeout=DOWNLOAD_TIMEOUT, follow_redirects=True
                ) as resp:
                    resp.raise_for_status()

                    if not filename:
                        filename = url.rstrip("/").split("/")[-1]

                    file_path = dest / filename
                    with open(file_path, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=8192):
                            f.write(chunk)

                logger.info("Downloaded %s -> %s", url, file_path)
                return str(file_path)
            except (httpx.ConnectError, httpx.ReadError, ConnectionError) as e:
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    logger.warning(
                        "Download attempt %d/%d failed for %s: %s. Retrying in %.0fs...",
                        attempt, MAX_RETRIES, url, e, delay,
                    )
                    time.sleep(delay)
                else:
                    raise


def _strip_html(text: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    clean = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", clean).strip()


def _extract_doi(url: str) -> str:
    """Extract DOI from a Dryad dataset URL or bare DOI string.

    Handles URLs like:
        https://datadryad.org/stash/dataset/doi:10.5061/dryad.xxx
        doi:10.5061/dryad.xxx
    """
    # Match doi:... pattern in the URL
    match = re.search(r"(doi:\S+)", url)
    if match:
        return match.group(1).rstrip("/")
    # Already a bare DOI
    stripped = url.strip().rstrip("/")
    if stripped.startswith("doi:"):
        return stripped
    # Last path segment as fallback
    return stripped.split("/")[-1]


def _encode_doi(doi: str) -> str:
    """URL-encode a DOI for use in API paths.

    Turns 'doi:10.5061/dryad.xxx' into 'doi%3A10.5061%2Fdryad.xxx'.
    """
    return quote(doi, safe="")


def _extract_file_id(download_href: str) -> str:
    """Extract numeric file ID from a Dryad API download href.

    E.g. '/api/v2/files/4443803/download' → '4443803'
    """
    match = re.search(r"/files/(\d+)/download", download_href)
    return match.group(1) if match else ""


def _sanitize_doi(doi: str) -> str:
    """Sanitize a DOI string for use as a filesystem-safe ID.

    Turns 'doi:10.5061/dryad.xxx' into 'doi_10.5061_dryad.xxx'.
    """
    return doi.replace(":", "_").replace("/", "_")


def _parse_license(spdx_url: str) -> str:
    """Extract license identifier from an SPDX URL.

    E.g. 'https://creativecommons.org/publicdomain/zero/1.0/' → 'CC0-1.0'
    """
    if not spdx_url:
        return ""
    # Common Dryad license: CC0
    if "publicdomain/zero/1.0" in spdx_url or "CC0" in spdx_url:
        return "CC0-1.0"
    # Try to extract from creativecommons.org URL pattern
    match = re.search(r"creativecommons\.org/licenses/([^/]+)/([^/]+)", spdx_url)
    if match:
        license_id = match.group(1).upper()
        version = match.group(2)
        return f"CC-{license_id}-{version}"
    # Fallback: return the URL itself
    return spdx_url
