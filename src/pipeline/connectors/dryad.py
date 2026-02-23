"""Dryad REST API v2 connector for public datasets."""

import html
import logging
import re
import time
from pathlib import Path
from urllib.parse import quote

import httpx

from pipeline.connectors.base import BaseConnector, SearchResult

logger = logging.getLogger("pipeline")

BASE_URL = "https://datadryad.org/api/v2"

# Timeout for API requests (seconds)
REQUEST_TIMEOUT = 30.0
DOWNLOAD_TIMEOUT = 120.0

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2.0  # seconds, doubles each retry

# Rate limiting: 2 seconds between API calls
MIN_REQUEST_INTERVAL = 2.0

# Maximum results to fetch per search query
MAX_SEARCH_RESULTS = 500


class DryadConnector(BaseConnector):
    """Connector for the Dryad Digital Repository."""

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
        per_page = 100

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

            items = data.get("_embedded", {}).get("stash:datasets", [])
            if not items:
                break

            for item in items:
                title = item.get("title", "")
                abstract = _strip_html(item.get("abstract", ""))

                authors_list = item.get("authors", [])
                author_names = "; ".join(
                    f"{a.get('firstName', '')} {a.get('lastName', '')}".strip()
                    for a in authors_list
                    if a.get("firstName") or a.get("lastName")
                )

                keywords = item.get("keywords", []) or []
                identifier = item.get("identifier", "")
                source_url = (
                    f"https://datadryad.org/stash/dataset/{identifier}"
                    if identifier
                    else ""
                )

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

            total = data.get("total", 0)
            if page * per_page >= total:
                break
            if len(results) >= MAX_SEARCH_RESULTS:
                logger.info(
                    "Search '%s' capped at %d results (total available: %d)",
                    query,
                    len(results),
                    total,
                )
                break
            page += 1

        logger.info("Search '%s' on dryad returned %d records", query, len(results))
        return results

    def get_metadata(self, record_url: str) -> SearchResult:
        """Fetch full metadata for a Dryad dataset.

        Accepts URLs like https://datadryad.org/stash/dataset/doi:10.5061/dryad.xxx
        or a bare DOI like doi:10.5061/dryad.xxx.
        """
        doi = _extract_doi(record_url)
        encoded_doi = _encode_doi(doi)

        # Fetch dataset metadata
        self._throttle()
        resp = httpx.get(
            f"{BASE_URL}/datasets/{encoded_doi}",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        # Basic metadata
        title = data.get("title", "")
        abstract = _strip_html(data.get("abstract", ""))
        methods = _strip_html(data.get("methods", ""))
        description = abstract
        if methods:
            description = f"{abstract}\n\nMethods: {methods}" if abstract else methods

        # Authors
        authors_list = data.get("authors", [])
        author_names = "; ".join(
            f"{a.get('firstName', '')} {a.get('lastName', '')}".strip()
            for a in authors_list
            if a.get("firstName") or a.get("lastName")
        )

        # License — Dryad is always CC0
        license_info = data.get("license", "")
        license_type = "CC0-1.0"
        license_url = "https://creativecommons.org/publicdomain/zero/1.0/"
        if isinstance(license_info, str) and "creativecommons" in license_info:
            license_url = license_info

        # Keywords
        keywords = data.get("keywords", []) or []

        # Field of science → kind_of_data
        field_of_science = data.get("fieldOfScience", "")
        kind_of_data = [field_of_science] if field_of_science else []

        # Geographic coverage
        locations = data.get("locations", []) or []
        geographic_coverage = [
            loc.get("place", "")
            for loc in locations
            if loc.get("place")
        ]

        # Related works → publications
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
        if authors_list:
            first = authors_list[0]
            uploader_name = (
                f"{first.get('firstName', '')} {first.get('lastName', '')}".strip()
            )
            uploader_email = first.get("email", "") or ""

        # Get version info to fetch files
        version_href = (
            data.get("_links", {}).get("stash:version", {}).get("href", "")
        )
        if not version_href:
            logger.warning("No version link found for %s", record_url)
            return SearchResult(
                source_name="dryad",
                source_url=record_url,
                title=title,
                description=description,
                authors=author_names,
            )

        # Extract version URL (may be relative)
        if version_href.startswith("/"):
            version_url = f"https://datadryad.org{version_href}"
        elif version_href.startswith("http"):
            version_url = version_href
        else:
            version_url = f"{BASE_URL}/{version_href}"

        # Fetch files for this version (paginated)
        files = self._fetch_version_files(version_url)

        identifier = data.get("identifier", "")
        source_url = (
            f"https://datadryad.org/stash/dataset/{identifier}"
            if identifier
            else record_url
        )

        return SearchResult(
            source_name="dryad",
            source_url=source_url,
            title=title,
            description=description,
            authors=author_names,
            license_type=license_type,
            license_url=license_url,
            date_published=data.get("publicationDate", ""),
            keywords=keywords,
            tags=keywords,
            kind_of_data=kind_of_data,
            language=[],
            geographic_coverage=geographic_coverage,
            software=[],
            depositor="",
            producer=[],
            publication=publications,
            uploader_name=uploader_name,
            uploader_email=uploader_email,
            files=files,
        )

    def _fetch_version_files(self, version_url: str) -> list[dict]:
        """Fetch all files for a dataset version, with pagination."""
        files: list[dict] = []
        page = 1

        while True:
            self._throttle()
            resp = httpx.get(
                f"{version_url}/files",
                params={"per_page": 100, "page": page},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            items = data.get("_embedded", {}).get("stash:files", [])
            if not items:
                break

            for f in items:
                file_name = f.get("path", "")
                ext = Path(file_name).suffix.lstrip(".") if file_name else ""

                # Build download URL from the file's self link
                download_href = (
                    f.get("_links", {}).get("stash:download", {}).get("href", "")
                )
                if download_href.startswith("/"):
                    download_url = f"https://datadryad.org{download_href}"
                elif download_href.startswith("http"):
                    download_url = download_href
                else:
                    download_url = f"{BASE_URL}/{download_href}" if download_href else ""

                # Checksum
                digest_type = f.get("digestType", "")
                digest = f.get("digest", "")
                api_checksum = (
                    f"{digest_type}:{digest}" if digest_type and digest else ""
                )

                # File ID from self link
                self_href = f.get("_links", {}).get("self", {}).get("href", "")
                file_id = self_href.rstrip("/").split("/")[-1] if self_href else ""

                files.append({
                    "id": file_id,
                    "name": file_name,
                    "size": f.get("size", 0),
                    "download_url": download_url,
                    "api_checksum": api_checksum,
                    "restricted": False,
                    "content_type": f.get("mimeType", ""),
                    "friendly_type": ext,
                })

            total = data.get("total", 0)
            if page * 100 >= total:
                break
            page += 1

        return files

    def download(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Download a file from Dryad. Returns local file path.

        Dryad downloads redirect (302) to presigned AWS Lambda URLs.
        Retries up to MAX_RETRIES times on connection errors with exponential backoff.
        """
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
                        "Download attempt %d/%d failed for %s: %s. "
                        "Retrying in %.0fs...",
                        attempt,
                        MAX_RETRIES,
                        url,
                        e,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    raise


def _strip_html(text: str) -> str:
    """Remove HTML tags, decode entities, and collapse whitespace."""
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", " ", text)
    clean = html.unescape(clean)
    return re.sub(r"\s+", " ", clean).strip()


def _extract_doi(url: str) -> str:
    """Extract DOI from a Dryad URL or bare DOI string.

    Handles:
    - https://datadryad.org/stash/dataset/doi:10.5061/dryad.xxx
    - doi:10.5061/dryad.xxx
    - https://doi.org/10.5061/dryad.xxx
    """
    # Match doi: prefix in URL path
    match = re.search(r"(doi:\S+)", url)
    if match:
        return match.group(1).rstrip("/")

    # Match https://doi.org/10.xxxx pattern
    match = re.search(r"doi\.org/(10\.\S+)", url)
    if match:
        return f"doi:{match.group(1).rstrip('/')}"

    # Bare DOI without prefix
    stripped = url.strip().rstrip("/")
    if stripped.startswith("10."):
        return f"doi:{stripped}"

    return stripped


def _encode_doi(doi: str) -> str:
    """URL-encode a DOI for use in API paths.

    doi:10.5061/dryad.xxx → doi%3A10.5061%2Fdryad.xxx
    """
    return quote(doi, safe="")
