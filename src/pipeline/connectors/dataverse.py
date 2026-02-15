"""Dataverse API connector — works for QDR, DANS, DataverseNO."""

import logging
from pathlib import Path

import httpx

from pipeline.connectors.base import BaseConnector, SearchResult

logger = logging.getLogger("pipeline")

# Timeout for API requests (seconds)
REQUEST_TIMEOUT = 30.0
DOWNLOAD_TIMEOUT = 120.0


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
            description = description_list[0].get("dsDescriptionValue", {}).get("value", "")

        authors_list = _get_field_value(fields, "author", [])
        author_names = []
        if isinstance(authors_list, list):
            for a in authors_list:
                name = a.get("authorName", {}).get("value", "")
                if name:
                    author_names.append(name)

        subject_list = _get_field_value(fields, "subject", [])

        # License info
        license_info = version.get("license", {})
        license_name = license_info.get("name", "") if isinstance(license_info, dict) else ""
        license_uri = license_info.get("uri", "") if isinstance(license_info, dict) else ""
        terms = version.get("termsOfUse", "")
        if not license_name and terms:
            license_name = terms

        # Files
        files = []
        for fentry in version.get("files", []):
            df = fentry.get("dataFile", {})
            files.append({
                "id": df.get("id"),
                "name": df.get("filename", ""),
                "size": df.get("filesize", 0),
                "content_type": df.get("contentType", ""),
                "download_url": f"{self._base_url}/api/access/datafile/{df.get('id')}",
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
            files=files,
        )
        return result

    def download(self, url: str, dest_dir: str) -> str:
        """Download a file from the Dataverse access API. Returns local file path."""
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)

        with httpx.stream("GET", url, timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as resp:
            resp.raise_for_status()

            # Try to get filename from Content-Disposition header
            filename = _filename_from_headers(resp.headers)
            if not filename:
                # Fallback: use the file ID from URL
                filename = url.rstrip("/").split("/")[-1]

            file_path = dest / filename
            with open(file_path, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=8192):
                    f.write(chunk)

        logger.info("Downloaded %s -> %s", url, file_path)
        return str(file_path)

    @staticmethod
    def _extract_persistent_id(url: str) -> str | None:
        """Extract persistentId from a Dataverse dataset URL."""
        if "persistentId=" in url:
            return url.split("persistentId=", 1)[1].split("&")[0]
        if url.startswith("doi:") or url.startswith("hdl:"):
            return url
        return None


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
