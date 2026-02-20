"""UK Data Service (ReShare) connector via EPrints JSON export."""

import logging
import re
import time
from pathlib import Path

import httpx

from pipeline.connectors.base import BaseConnector, SearchResult

logger = logging.getLogger("pipeline")

# Timeout for API requests (seconds)
REQUEST_TIMEOUT = 60.0  # large JSON exports can be slow
DOWNLOAD_TIMEOUT = 120.0

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2.0  # seconds, doubles each retry

# Rate limiting: polite delay between requests
MIN_REQUEST_INTERVAL = 2.0

BASE_URL = "https://reshare.ukdataservice.ac.uk"

# EPrints auto-generated files to skip
_SKIP_FILENAMES = {
    "lightbox.jpg", "preview.jpg", "medium.jpg", "small.jpg",
    "indexcodes.txt",
}

# License mapping from EPrints identifiers to standard names
_LICENSE_MAP = {
    "cc_by": "CC-BY-4.0",
    "cc_by_sa": "CC-BY-SA-4.0",
    "cc_by_nc": "CC-BY-NC-4.0",
    "cc_by_nc_sa": "CC-BY-NC-SA-4.0",
    "cc_by_nd": "CC-BY-ND-4.0",
    "cc_by_nc_nd": "CC-BY-NC-ND-4.0",
    "cc_public_domain": "CC0-1.0",
}


class UKDataServiceConnector(BaseConnector):
    """Connector for the UK Data Service ReShare repository."""

    def __init__(self) -> None:
        self._last_request_time = 0.0

    @property
    def name(self) -> str:
        return "ukds"

    def _throttle(self) -> None:
        """Enforce minimum interval between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def search(self, query: str, file_type: str | None = None) -> list[SearchResult]:
        """Search ReShare datasets via JSON export."""
        self._throttle()
        params = {
            "output": "JSON",
            "q": query,
            "_action_export": "1",
            "_action_export_redir": "1",
            "_order": "bytitle",
            "basic_srchtype": "ALL",
            "_satisfyall": "ALL",
            "_action_search": "Search",
        }
        resp = httpx.get(
            f"{BASE_URL}/cgi/search/simple/export_reshare_JSON.js",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        results: list[SearchResult] = []
        for item in data:
            eprint_id = item.get("eprintid", "")

            # Client-side file_type filtering
            if file_type:
                ext = file_type if file_type.startswith(".") else f".{file_type}"
                has_match = any(
                    f.get("filename", "").endswith(ext)
                    for doc in item.get("documents", [])
                    for f in doc.get("files", [])
                    if f.get("filename") not in _SKIP_FILENAMES
                )
                if not has_match:
                    continue

            creators = item.get("creators", [])
            author_names = "; ".join(
                _format_creator(c) for c in creators if _format_creator(c)
            )

            keywords = item.get("keywords", []) or []

            result = SearchResult(
                source_name="ukds",
                source_url=f"{BASE_URL}/{eprint_id}/",
                title=item.get("title", ""),
                description=_strip_html(item.get("abstract", "")),
                authors=author_names,
                date_published=item.get("date", ""),
                keywords=keywords,
                tags=keywords,
            )
            results.append(result)

        logger.info("Search '%s' on ukds returned %d records", query, len(results))
        return results

    def get_metadata(self, record_url: str) -> SearchResult:
        """Fetch full metadata for a ReShare record.

        Accepts URLs like https://reshare.ukdataservice.ac.uk/857166/
        or https://reshare.ukdataservice.ac.uk/id/eprint/857166.
        """
        eprint_id = _extract_eprint_id(record_url)

        self._throttle()
        resp = httpx.get(
            f"{BASE_URL}/cgi/export/eprint/{eprint_id}"
            f"/JSON/reshare-eprint-{eprint_id}.js",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        # Single record can be dict or list with one element
        if isinstance(data, list):
            data = data[0] if data else {}

        # Basic metadata
        title = data.get("title", "")
        description = _strip_html(data.get("abstract", ""))

        creators = data.get("creators", []) or []
        author_names = "; ".join(
            _format_creator(c) for c in creators if _format_creator(c)
        )

        # Keywords
        keywords = data.get("keywords", []) or []

        # Language
        language = data.get("language", []) or []

        # Kind of data
        kind_of_data = data.get("data_kind", []) or []

        # Geographic coverage
        country = data.get("country", []) or []
        geo_cover = data.get("geographic_cover", "")
        geo_coverage = country.copy()
        if geo_cover and geo_cover not in geo_coverage:
            geo_coverage.append(geo_cover)

        # Funders → producer
        producers = data.get("award_funders", []) or []

        # DOI → publication
        doi = data.get("doi", "")
        publications = [f"https://doi.org/{doi}"] if doi else []

        # Uploader / contact
        uploader_name = ""
        uploader_email = ""
        if creators:
            uploader_name = _format_creator(creators[0])
            uploader_email = creators[0].get("id", "") or ""

        depositor = ""
        contacts = data.get("contact_details", []) or []
        if contacts:
            depositor = _format_creator(contacts[0])

        # Date of collection
        collection_dates = data.get("collection_dates", {}) or {}
        date_from = str(collection_dates.get("date_from", ""))
        date_to = str(collection_dates.get("date_to", ""))
        date_of_collection = ""
        if date_from or date_to:
            if date_from and date_to:
                date_of_collection = f"{date_from} to {date_to}"
            else:
                date_of_collection = date_from or date_to

        # License — pick the most common open license across documents
        all_licenses = [
            doc.get("license", "")
            for doc in data.get("documents", [])
            if doc.get("license")
        ]
        license_type = _pick_license(all_licenses)
        license_url = _license_url(license_type)

        # Files
        files = _build_file_list(
            eprint_id, data.get("documents", [])
        )

        return SearchResult(
            source_name="ukds",
            source_url=record_url,
            title=title,
            description=description,
            authors=author_names,
            license_type=license_type,
            license_url=license_url,
            date_published=data.get("date", ""),
            keywords=keywords,
            tags=keywords,
            kind_of_data=kind_of_data,
            language=language,
            software=[],
            geographic_coverage=geo_coverage,
            depositor=depositor,
            producer=producers,
            publication=publications,
            date_of_collection=date_of_collection,
            time_period_covered="",
            uploader_name=uploader_name,
            uploader_email=uploader_email,
            files=files,
        )

    def download(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Download a file from ReShare. Returns local file path.

        Retries up to MAX_RETRIES times on connection errors with
        exponential backoff.
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
                        "Download attempt %d/%d failed for %s: %s. "
                        "Retrying in %.0fs...",
                        attempt, MAX_RETRIES, url, e, delay,
                    )
                    time.sleep(delay)
                else:
                    raise


def _build_file_list(eprint_id: int | str, documents: list) -> list[dict]:
    """Build file dicts from EPrints documents, skipping thumbnails."""
    files = []
    for doc in documents:
        security = doc.get("security", "")
        license_str = doc.get("license", "")
        is_restricted = security != "public"

        # Skip non-open licenses for restricted files
        is_open = _is_open_license(license_str)

        # Extract document ID from URI for reliable download URLs.
        # The `placement` field does NOT match the URL path number;
        # the document URI (/id/document/{doc_id}) redirects correctly.
        doc_id = _extract_doc_id(doc.get("uri", ""))

        for f in doc.get("files", []):
            fname = f.get("filename", "")

            # Skip EPrints auto-generated thumbnails/index files
            if fname in _SKIP_FILENAMES:
                continue

            ext = Path(fname).suffix.lstrip(".") if fname else ""

            download_url = (
                f"{BASE_URL}/id/document/{doc_id}"
                if doc_id
                else ""
            )

            files.append({
                "id": str(eprint_id),
                "name": fname,
                "size": f.get("filesize", 0),
                "download_url": download_url,
                "api_checksum": "",
                "restricted": is_restricted or not is_open,
                "content_type": f.get("mime_type", ""),
                "friendly_type": ext,
            })
    return files


def _strip_html(text: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    clean = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", clean).strip()


def _extract_eprint_id(url: str) -> str:
    """Extract eprint ID from a ReShare URL or bare ID.

    Handles:
        https://reshare.ukdataservice.ac.uk/857166/
        https://reshare.ukdataservice.ac.uk/id/eprint/857166
        857166
    """
    # /id/eprint/{id} pattern
    match = re.search(r"/id/eprint/(\d+)", url)
    if match:
        return match.group(1)
    # /{id}/ pattern (numeric path segment)
    match = re.search(r"/(\d+)/?$", url)
    if match:
        return match.group(1)
    # Bare numeric ID
    stripped = url.strip().rstrip("/")
    if stripped.isdigit():
        return stripped
    return stripped.split("/")[-1]


def _extract_doc_id(uri: str) -> str:
    """Extract document ID from an EPrints document URI.

    E.g. 'http://reshare.ukdataservice.ac.uk/id/document/3744469' → '3744469'
    """
    match = re.search(r"/id/document/(\d+)", uri)
    return match.group(1) if match else ""


def _format_creator(creator: dict) -> str:
    """Format a creator/contact dict into 'Given Family' string."""
    name = creator.get("name", {}) or {}
    given = name.get("given", "") or ""
    family = name.get("family", "") or ""
    return f"{given} {family}".strip()


def _is_open_license(license_str: str) -> bool:
    """Check if an EPrints license string is an open license."""
    return license_str in _LICENSE_MAP


def _map_license(license_str: str) -> str:
    """Map EPrints license identifier to standard format."""
    return _LICENSE_MAP.get(license_str, license_str)


def _pick_license(licenses: list[str]) -> str:
    """Pick the best license from a list of EPrints license strings.

    Prefers open licenses. Returns mapped standard format.
    """
    if not licenses:
        return ""
    # Prefer open licenses
    for lic in licenses:
        if lic in _LICENSE_MAP:
            return _LICENSE_MAP[lic]
    # Fallback to first
    return _map_license(licenses[0])


def _license_url(license_type: str) -> str:
    """Return a URL for a standard license type."""
    urls = {
        "CC-BY-4.0": "https://creativecommons.org/licenses/by/4.0/",
        "CC-BY-SA-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
        "CC-BY-NC-4.0": "https://creativecommons.org/licenses/by-nc/4.0/",
        "CC-BY-NC-SA-4.0": "https://creativecommons.org/licenses/by-nc-sa/4.0/",
        "CC-BY-ND-4.0": "https://creativecommons.org/licenses/by-nd/4.0/",
        "CC-BY-NC-ND-4.0": "https://creativecommons.org/licenses/by-nc-nd/4.0/",
        "CC0-1.0": "https://creativecommons.org/publicdomain/zero/1.0/",
    }
    return urls.get(license_type, "")
