"""Zenodo REST API connector for public records."""

import logging
import re
import time
from pathlib import Path

import httpx

from pipeline.connectors.base import BaseConnector, SearchResult

logger = logging.getLogger("pipeline")

# Timeout for API requests (seconds)
REQUEST_TIMEOUT = 30.0
DOWNLOAD_TIMEOUT = 120.0

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2.0  # seconds, doubles each retry

# Rate limiting: 2 seconds between API calls (well within 30 req/min)
MIN_REQUEST_INTERVAL = 2.0


class ZenodoConnector(BaseConnector):
    """Connector for the Zenodo open-access repository."""

    BASE_URL = "https://zenodo.org/api"

    def __init__(self) -> None:
        self._last_request_time = 0.0

    @property
    def name(self) -> str:
        return "zenodo"

    def _throttle(self) -> None:
        """Enforce minimum interval between API requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def search(self, query: str, file_type: str | None = None) -> list[SearchResult]:
        """Search Zenodo records, with pagination."""
        results: list[SearchResult] = []
        page = 1
        per_page = 25

        while True:
            self._throttle()
            params: dict[str, str | int] = {
                "q": query,
                "size": per_page,
                "page": page,
            }
            resp = httpx.get(
                f"{self.BASE_URL}/records",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            hits = data.get("hits", {})
            items = hits.get("hits", [])
            if not items:
                break

            for item in items:
                meta = item.get("metadata", {})

                # Client-side file_type filtering
                if file_type:
                    ext = file_type if file_type.startswith(".") else f".{file_type}"
                    file_entries = item.get("files", [])
                    has_match = any(f.get("key", "").endswith(ext) for f in file_entries)
                    if not has_match:
                        continue

                record_id = item.get("id", "")
                creators = meta.get("creators", [])
                author_names = "; ".join(c.get("name", "") for c in creators if c.get("name"))

                result = SearchResult(
                    source_name="zenodo",
                    source_url=f"https://zenodo.org/records/{record_id}",
                    title=meta.get("title", ""),
                    description=_strip_html(meta.get("description", "")),
                    authors=author_names,
                    date_published=meta.get("publication_date", ""),
                    keywords=meta.get("keywords", []),
                    tags=meta.get("keywords", []),
                )
                results.append(result)

            total = hits.get("total", 0)
            if page * per_page >= total:
                break
            page += 1

        logger.info("Search '%s' on zenodo returned %d records", query, len(results))
        return results

    def get_metadata(self, record_url: str) -> SearchResult:
        """Fetch full metadata for a Zenodo record.

        Accepts URLs like https://zenodo.org/records/12345 or just the numeric ID.
        """
        record_id = _extract_record_id(record_url)

        self._throttle()
        resp = httpx.get(
            f"{self.BASE_URL}/records/{record_id}",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        meta = data.get("metadata", {})

        # Basic metadata
        title = meta.get("title", "")
        description = _strip_html(meta.get("description", ""))

        creators = meta.get("creators", [])
        author_names = "; ".join(c.get("name", "") for c in creators if c.get("name"))

        # License
        license_info = meta.get("license", {})
        license_type = license_info.get("id", "") if isinstance(license_info, dict) else ""

        # Keywords
        keywords = meta.get("keywords", [])

        # Language (single value → list)
        lang = meta.get("language")
        language = [lang] if lang else []

        # Resource type → kind_of_data
        resource_type = meta.get("resource_type", {})
        rtype = resource_type.get("type", "") if isinstance(resource_type, dict) else ""
        kind_of_data = [rtype] if rtype else []

        # Contributors → producer
        contributors = meta.get("contributors", [])
        producers = [c.get("name", "") for c in contributors if c.get("name")]

        # Related identifiers → publication
        related = meta.get("related_identifiers", [])
        publications = []
        for ri in related:
            ident = ri.get("identifier", "")
            relation = ri.get("relation", "")
            if ident:
                publications.append(f"{relation}: {ident}" if relation else ident)

        # Uploader = first creator
        uploader_name = creators[0].get("name", "") if creators else ""

        # Access right: if not "open", all files are restricted
        access_right = meta.get("access_right", "open")
        is_restricted = access_right != "open"

        # Files
        files = []
        for f in data.get("files", []):
            key = f.get("key", "")
            # Derive friendly_type from file extension (API has no type field)
            ext = Path(key).suffix.lstrip(".") if key else ""
            files.append({
                "id": record_id,
                "name": key,
                "size": f.get("size", 0),
                "download_url": f.get("links", {}).get("self", ""),
                "api_checksum": f.get("checksum", ""),
                "restricted": is_restricted,
                "content_type": "",
                "friendly_type": ext,
            })

        return SearchResult(
            source_name="zenodo",
            source_url=record_url,
            title=title,
            description=description,
            authors=author_names,
            license_type=license_type,
            date_published=meta.get("publication_date", ""),
            keywords=keywords,
            tags=keywords,
            kind_of_data=kind_of_data,
            language=language,
            geographic_coverage=[],
            software=[],
            depositor="",
            producer=producers,
            publication=publications,
            uploader_name=uploader_name,
            uploader_email="",
            files=files,
        )

    def download(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Download a file from Zenodo. Returns local file path.

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


def _extract_record_id(url: str) -> str:
    """Extract numeric record ID from a Zenodo URL or bare ID string."""
    # Handle URLs like https://zenodo.org/records/12345 or /record/12345
    match = re.search(r"/records?/(\d+)", url)
    if match:
        return match.group(1)
    # Bare numeric ID
    stripped = url.strip().rstrip("/")
    if stripped.isdigit():
        return stripped
    # Last path segment as fallback
    return stripped.split("/")[-1]
