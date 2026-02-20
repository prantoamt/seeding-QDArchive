"""QualidataNet connector via Elasticsearch API.

QualidataNet is a federated metadata portal for qualitative research data,
operated by KonsortSWD / RDC Qualiservice (University of Bremen).  It
aggregates metadata from 6 German research data centers but does **not**
host files — actual data lives at partner institutions.

The public Elasticsearch endpoint requires no authentication:
    POST https://www.qualidatanet.com/es/qualidatanet/dataset/_search
"""

import logging
import re
import time

import httpx

from pipeline.connectors.base import BaseConnector, SearchResult

logger = logging.getLogger("pipeline")

BASE_URL = "https://www.qualidatanet.com"
ES_ENDPOINT = f"{BASE_URL}/es/qualidatanet/dataset/_search"

REQUEST_TIMEOUT = 30.0
MIN_REQUEST_INTERVAL = 1.0
PAGE_SIZE = 50


class QualidataNetConnector(BaseConnector):
    """Connector for the QualidataNet federated metadata portal."""

    def __init__(self) -> None:
        self._last_request_time = 0.0
        # Cache search results so get_metadata doesn't need another API call.
        # The ES `metadatalink` field is analyzed text, so exact term queries
        # fail.  Cache keyed by source_url (the metadatalink DOI URL).
        self._cache: dict[str, SearchResult] = {}

    @property
    def name(self) -> str:
        return "qualidatanet"

    def _throttle(self) -> None:
        """Enforce minimum interval between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def search(
        self, query: str, file_type: str | None = None
    ) -> list[SearchResult]:
        """Search QualidataNet datasets via Elasticsearch."""
        results: list[SearchResult] = []
        offset = 0

        while True:
            self._throttle()
            body: dict = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "citation_title",
                            "description",
                            "keyword",
                        ],
                    }
                },
                "size": PAGE_SIZE,
                "from": offset,
            }
            resp = httpx.post(
                ES_ENDPOINT,
                json=body,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            hits = data.get("hits", {})
            hit_list = hits.get("hits", [])
            total = _extract_total(hits)

            if not hit_list:
                break

            for hit in hit_list:
                src = hit.get("_source", {})

                # Client-side file_type filtering on format field
                if file_type:
                    formats = _as_list(src.get("format", []))
                    ext = file_type.lstrip(".")
                    if not any(ext in fmt for fmt in formats):
                        continue

                result = _hit_to_search_result(hit)
                results.append(result)
                if result.source_url:
                    self._cache[result.source_url] = result

            offset += PAGE_SIZE
            if offset >= total:
                break

        logger.info(
            "Search '%s' on qualidatanet returned %d records",
            query,
            len(results),
        )
        return results

    def get_metadata(self, record_url: str) -> SearchResult:
        """Fetch full metadata for a QualidataNet record.

        Returns cached result from search() when available.  Falls back
        to an Elasticsearch match_phrase query on metadatalink.
        """
        # Return from cache if available (populated by search())
        if record_url in self._cache:
            return self._cache[record_url]

        self._throttle()
        body: dict = {
            "query": {
                "match_phrase": {"metadatalink": record_url},
            },
            "size": 1,
        }
        resp = httpx.post(
            ES_ENDPOINT,
            json=body,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            raise ValueError(
                f"No QualidataNet record found for URL: {record_url}"
            )

        return _hit_to_search_result(hits[0])

    def download(
        self, url: str, dest_dir: str, filename: str | None = None
    ) -> str:
        """Not supported — QualidataNet is a metadata-only portal."""
        raise NotImplementedError(
            "QualidataNet does not host files. "
            "Data must be downloaded from partner institutions."
        )


def _hit_to_search_result(hit: dict) -> SearchResult:
    """Convert an Elasticsearch hit to a SearchResult."""
    src = hit.get("_source", {})

    title = src.get("citation_title", "")
    description = _normalize_text(src.get("description", ""))
    authors = _normalize_authors(src.get("citation_authors", ""))
    date_published = str(src.get("citation_date", ""))
    keywords = _as_list(src.get("keyword", []))
    location = _as_list(src.get("location", []))
    data_center = src.get("dataCenter", "")
    metadatalink = src.get("metadatalink", "")
    license_raw = _as_list(src.get("license", []))
    license_type = license_raw[0] if license_raw else ""
    kind_of_data = _as_list(src.get("type", []))

    # Use metadatalink (DOI URL) as the source_url
    source_url = metadatalink or ""

    return SearchResult(
        source_name="qualidatanet",
        source_url=source_url,
        title=title,
        description=description,
        authors=authors,
        license_type=license_type,
        date_published=date_published,
        keywords=keywords,
        tags=keywords,
        kind_of_data=kind_of_data,
        geographic_coverage=location,
        producer=[data_center] if data_center else [],
        publication=[metadatalink] if metadatalink else [],
        files=[],  # metadata-only portal
    )


def _normalize_text(value: str | list) -> str:
    """Join list values and strip HTML."""
    if isinstance(value, list):
        value = "\n".join(str(v) for v in value)
    return _strip_html(str(value))


def _normalize_authors(value: str | list) -> str:
    """Normalize author field — may be a string or list."""
    if isinstance(value, list):
        return "; ".join(str(a) for a in value if a)
    return str(value) if value else ""


def _as_list(value: str | list | None) -> list[str]:
    """Ensure a value is a list of strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v]
    return [str(value)] if value else []


def _strip_html(text: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    clean = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", clean).strip()


def _extract_total(hits: dict) -> int:
    """Extract total hit count (ES 5.x returns int, 7.x returns dict)."""
    total = hits.get("total", 0)
    if isinstance(total, dict):
        return total.get("value", 0)
    return int(total)
