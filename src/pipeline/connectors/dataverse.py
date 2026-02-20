"""Dataverse API connector — works for QDR, DANS, DataverseNO."""

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


class DataverseConnector(BaseConnector):
    """Connector for Dataverse-based repositories (QDR, DANS, DataverseNO)."""

    def __init__(self, base_url: str, instance_name: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._instance_name = instance_name

    @property
    def name(self) -> str:
        return self._instance_name

    def search(self, query: str, file_type: str | None = None) -> list[SearchResult]:
        """Search datasets via the Dataverse Search API, with pagination."""
        results: list[SearchResult] = []
        per_page = 100
        start = 0

        while True:
            params: dict[str, str | int] = {
                "q": query,
                "type": "dataset",
                "per_page": per_page,
                "start": start,
            }

            resp = httpx.get(
                f"{self._base_url}/api/search",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})

            items = data.get("items", [])
            if not items:
                break

            for item in items:
                result = SearchResult(
                    source_name=self._instance_name,
                    source_url=item.get("url", ""),
                    title=item.get("name", ""),
                    description=item.get("description", ""),
                    authors="; ".join(item.get("authors", [])),
                    date_published=item.get("published_at", ""),
                    tags=item.get("subjects", []),
                )
                # Extract global_id for later metadata fetch
                global_id = item.get("global_id", "")
                if global_id:
                    result.source_url = (
                        f"{self._base_url}/dataset.xhtml?persistentId={global_id}"
                    )
                results.append(result)

            total_count = data.get("total_count", 0)
            start += per_page
            if start >= total_count:
                break

        logger.info(
            "Search '%s' on %s returned %d datasets", query, self._instance_name, len(results)
        )
        return results

    def get_metadata(self, record_url: str) -> SearchResult:
        """Fetch full dataset metadata including file list.

        Accepts either a persistent ID URL (?persistentId=...) or a numeric dataset ID.
        """
        persistent_id = self._extract_persistent_id(record_url)

        if persistent_id:
            resp = httpx.get(
                f"{self._base_url}/api/datasets/:persistentId",
                params={"persistentId": persistent_id},
                timeout=REQUEST_TIMEOUT,
            )
        else:
            # Try treating the URL tail as a numeric ID
            dataset_id = record_url.rstrip("/").split("/")[-1]
            resp = httpx.get(
                f"{self._base_url}/api/datasets/{dataset_id}",
                timeout=REQUEST_TIMEOUT,
            )

        resp.raise_for_status()
        data = resp.json().get("data", {})
        version = data.get("latestVersion", {})
        metadata_blocks = version.get("metadataBlocks", {})
        citation = metadata_blocks.get("citation", {})
        fields = {f["typeName"]: f for f in citation.get("fields", [])}

        # Extract basic metadata
        title = _get_field_value(fields, "title", "")
        description_list = _get_field_value(fields, "dsDescription", [])
        description = ""
        if isinstance(description_list, list) and description_list:
            raw = description_list[0].get("dsDescriptionValue", {}).get("value", "")
            description = _strip_html(raw)

        authors_list = _get_field_value(fields, "author", [])
        author_names = []
        if isinstance(authors_list, list):
            for a in authors_list:
                name = a.get("authorName", {}).get("value", "")
                if name:
                    author_names.append(name)

        subject_list = _get_field_value(fields, "subject", [])

        # Extended metadata fields
        keyword_list = _get_field_value(fields, "keyword", [])
        keywords = []
        if isinstance(keyword_list, list):
            for kw in keyword_list:
                val = kw.get("keywordValue", {}).get("value", "")
                if val:
                    keywords.append(val)

        kind_of_data = _get_field_value(fields, "kindOfData", [])
        if not isinstance(kind_of_data, list):
            kind_of_data = []

        language_list = _get_field_value(fields, "language", [])
        if not isinstance(language_list, list):
            language_list = []

        software_list = _get_field_value(fields, "software", [])
        software = []
        if isinstance(software_list, list):
            for sw in software_list:
                val = sw.get("softwareName", {}).get("value", "")
                if val:
                    software.append(val)

        geo_list = _get_field_value(fields, "geographicCoverage", [])
        geo_coverage = []
        if isinstance(geo_list, list):
            for geo in geo_list:
                country = geo.get("country", {}).get("value", "")
                if country:
                    geo_coverage.append(country)

        # Provenance fields (mainly DataverseNO)
        depositor = _get_field_value(fields, "depositor", "")
        if not isinstance(depositor, str):
            depositor = ""

        producer_list = _get_field_value(fields, "producer", [])
        producers = []
        if isinstance(producer_list, list):
            for p in producer_list:
                name = p.get("producerName", {}).get("value", "")
                if name:
                    producers.append(name)

        pub_list = _get_field_value(fields, "publication", [])
        publications = []
        if isinstance(pub_list, list):
            for pub in pub_list:
                citation_text = pub.get("publicationCitation", {}).get("value", "")
                pub_url = pub.get("publicationURL", {}).get("value", "")
                entry = citation_text or pub_url
                if entry:
                    publications.append(entry)

        collection_list = _get_field_value(fields, "dateOfCollection", [])
        date_of_collection = ""
        if isinstance(collection_list, list) and collection_list:
            start = collection_list[0].get("dateOfCollectionStart", {}).get("value", "")
            end = collection_list[0].get("dateOfCollectionEnd", {}).get("value", "")
            if start or end:
                date_of_collection = f"{start} to {end}" if start and end else (start or end)

        tp_list = _get_field_value(fields, "timePeriodCovered", [])
        time_period_covered = ""
        if isinstance(tp_list, list) and tp_list:
            start = tp_list[0].get("timePeriodCoveredStart", {}).get("value", "")
            end = tp_list[0].get("timePeriodCoveredEnd", {}).get("value", "")
            if start or end:
                time_period_covered = f"{start} to {end}" if start and end else (start or end)

        # Contact / uploader info
        contact_list = _get_field_value(fields, "datasetContact", [])
        uploader_name = ""
        uploader_email = ""
        if isinstance(contact_list, list) and contact_list:
            uploader_name = contact_list[0].get("datasetContactName", {}).get("value", "")
            uploader_email = contact_list[0].get("datasetContactEmail", {}).get("value", "")

        # License info
        license_info = version.get("license", {})
        license_name = license_info.get("name", "") if isinstance(license_info, dict) else ""
        license_uri = license_info.get("uri", "") if isinstance(license_info, dict) else ""
        terms = version.get("termsOfAccess", "")
        if not license_name and terms:
            license_name = terms

        # Files
        files = []
        for fentry in version.get("files", []):
            df = fentry.get("dataFile", {})
            checksum = df.get("checksum", {})
            api_checksum = ""
            if checksum:
                api_checksum = f"{checksum.get('type', '')}:{checksum.get('value', '')}"
            files.append({
                "id": df.get("id"),
                "name": df.get("filename", ""),
                "size": df.get("filesize", 0),
                "content_type": df.get("contentType", ""),
                "friendly_type": df.get("friendlyType", ""),
                "download_url": f"{self._base_url}/api/access/datafile/{df.get('id')}",
                "restricted": fentry.get("restricted", False),
                "api_checksum": api_checksum,
            })

        result = SearchResult(
            source_name=self._instance_name,
            source_url=record_url,
            title=title,
            description=description,
            authors="; ".join(author_names),
            license_type=license_name,
            license_url=license_uri,
            date_published=version.get("releaseTime", ""),
            tags=subject_list if isinstance(subject_list, list) else [],
            keywords=keywords,
            kind_of_data=kind_of_data,
            language=language_list,
            software=software,
            geographic_coverage=geo_coverage,
            depositor=depositor,
            producer=producers,
            publication=publications,
            date_of_collection=date_of_collection,
            time_period_covered=time_period_covered,
            uploader_name=uploader_name,
            uploader_email=uploader_email,
            files=files,
        )
        return result

    def download(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Download a file from the Dataverse access API. Returns local file path.

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
                        filename = _filename_from_headers(resp.headers)
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

    @staticmethod
    def _extract_persistent_id(url: str) -> str | None:
        """Extract persistentId from a Dataverse dataset URL."""
        if "persistentId=" in url:
            return url.split("persistentId=", 1)[1].split("&")[0]
        if url.startswith("doi:") or url.startswith("hdl:"):
            return url
        return None


def _strip_html(text: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    clean = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", clean).strip()


def _get_field_value(fields: dict, type_name: str, default=None):
    """Extract the value from a Dataverse metadata field dict."""
    field = fields.get(type_name)
    if field is None:
        return default
    return field.get("value", default)


def _filename_from_headers(headers: httpx.Headers) -> str | None:
    """Extract filename from Content-Disposition header."""
    cd = headers.get("content-disposition", "")
    if "filename=" in cd:
        # Handle both filename="name" and filename=name
        part = cd.split("filename=", 1)[1]
        return part.strip('"').strip("'").split(";")[0].strip()
    return None
