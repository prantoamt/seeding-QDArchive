"""Tests for the Dataverse connector with mocked HTTP responses."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from pipeline.connectors.base import BaseConnector
from pipeline.connectors.dataverse import DataverseConnector, _filename_from_headers


@pytest.fixture
def connector():
    return DataverseConnector("https://data.qdr.syr.edu", "qdr")


# -- Interface compliance --


def test_implements_base_connector(connector):
    assert isinstance(connector, BaseConnector)


def test_name_property(connector):
    assert connector.name == "qdr"


# -- Search --

SEARCH_RESPONSE = {
    "status": "OK",
    "data": {
        "q": "qualitative",
        "total_count": 2,
        "start": 0,
        "items": [
            {
                "name": "Interview Dataset A",
                "type": "dataset",
                "url": "https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/F6ABC123",
                "global_id": "doi:10.5064/F6ABC123",
                "description": "A qualitative interview dataset",
                "authors": ["Smith, J.", "Doe, A."],
                "published_at": "2023-06-15",
                "subjects": ["Social Sciences"],
            },
            {
                "name": "Focus Group Data",
                "type": "dataset",
                "url": "https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/F6XYZ789",
                "global_id": "doi:10.5064/F6XYZ789",
                "description": "Focus group transcripts",
                "authors": ["Lee, B."],
                "published_at": "2024-01-20",
                "subjects": ["Education"],
            },
        ],
    },
}


def test_search_parses_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        results = connector.search("qualitative")

    assert len(results) == 2
    assert results[0].title == "Interview Dataset A"
    assert results[0].source_name == "qdr"
    assert "doi:10.5064/F6ABC123" in results[0].source_url
    assert results[0].authors == "Smith, J.; Doe, A."
    assert results[1].title == "Focus Group Data"

    # Verify correct URL was called
    mock_get.assert_called_once()
    call_args = mock_get.call_args
    assert "/api/search" in call_args[0][0]
    assert call_args[1]["params"]["q"] == "qualitative"
    assert call_args[1]["params"]["type"] == "dataset"


def test_search_empty_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"status": "OK", "data": {"items": [], "total_count": 0}}
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        results = connector.search("nonexistent")

    assert results == []


# -- Get metadata --

DATASET_RESPONSE = {
    "status": "OK",
    "data": {
        "latestVersion": {
            "releaseTime": "2023-06-15T00:00:00Z",
            "license": {"name": "CC0 1.0", "uri": "https://creativecommons.org/publicdomain/zero/1.0/"},
            "termsOfAccess": "Freely available",
            "metadataBlocks": {
                "citation": {
                    "fields": [
                        {"typeName": "title", "value": "Interview Dataset A"},
                        {
                            "typeName": "dsDescription",
                            "value": [
                                {"dsDescriptionValue": {"value": "A qualitative interview dataset"}}
                            ],
                        },
                        {
                            "typeName": "author",
                            "value": [
                                {"authorName": {"value": "Smith, J."}},
                                {"authorName": {"value": "Doe, A."}},
                            ],
                        },
                        {"typeName": "subject", "value": ["Social Sciences"]},
                        {
                            "typeName": "keyword",
                            "value": [
                                {"keywordValue": {"value": "qualitative research"}},
                                {"keywordValue": {"value": "interviews"}},
                            ],
                        },
                        {
                            "typeName": "kindOfData",
                            "value": ["interview transcripts", "coded qualitative data"],
                        },
                        {"typeName": "language", "value": ["English"]},
                        {
                            "typeName": "software",
                            "value": [
                                {"softwareName": {"value": "NVivo 12"}},
                            ],
                        },
                        {
                            "typeName": "geographicCoverage",
                            "value": [
                                {"country": {"value": "United States"}},
                                {"country": {"value": "Canada"}},
                            ],
                        },
                        {
                            "typeName": "datasetContact",
                            "value": [
                                {
                                    "datasetContactName": {"value": "Smith, J."},
                                    "datasetContactEmail": {"value": "smith@example.edu"},
                                },
                            ],
                        },
                        {"typeName": "depositor", "value": "Doe, A."},
                        {
                            "typeName": "producer",
                            "value": [
                                {"producerName": {"value": "University of Testing"}},
                            ],
                        },
                        {
                            "typeName": "publication",
                            "value": [
                                {
                                    "publicationCitation": {
                                        "value": "Smith (2023) Qualitative Study",
                                    },
                                    "publicationURL": {"value": "https://doi.org/10.1234/test"},
                                },
                            ],
                        },
                        {
                            "typeName": "dateOfCollection",
                            "value": [
                                {
                                    "dateOfCollectionStart": {"value": "2022-01-01"},
                                    "dateOfCollectionEnd": {"value": "2022-12-31"},
                                },
                            ],
                        },
                        {
                            "typeName": "timePeriodCovered",
                            "value": [
                                {
                                    "timePeriodCoveredStart": {"value": "2020-01-01"},
                                    "timePeriodCoveredEnd": {"value": "2022-06-30"},
                                },
                            ],
                        },
                    ]
                }
            },
            "files": [
                {
                    "restricted": False,
                    "dataFile": {
                        "id": 12345,
                        "filename": "interviews.qdpx",
                        "filesize": 204800,
                        "contentType": "application/x-zip-refiqda",
                        "friendlyType": "REFI-QDA-Project",
                        "checksum": {"type": "SHA-1", "value": "abc123def456"},
                    },
                },
                {
                    "restricted": True,
                    "dataFile": {
                        "id": 12346,
                        "filename": "codebook.pdf",
                        "filesize": 51200,
                        "contentType": "application/pdf",
                        "friendlyType": "Adobe PDF",
                        "checksum": {"type": "MD5", "value": "deadbeef"},
                    },
                },
            ],
        }
    },
}


def test_get_metadata_with_persistent_id(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = DATASET_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    url = "https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/F6ABC123"

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        result = connector.get_metadata(url)

    assert result.title == "Interview Dataset A"
    assert result.license_type == "CC0 1.0"
    assert result.authors == "Smith, J.; Doe, A."
    assert len(result.files) == 2
    assert result.files[0]["name"] == "interviews.qdpx"
    assert result.files[0]["id"] == 12345
    assert "/api/access/datafile/12345" in result.files[0]["download_url"]

    # New metadata fields
    assert result.keywords == ["qualitative research", "interviews"]
    assert result.kind_of_data == ["interview transcripts", "coded qualitative data"]
    assert result.language == ["English"]
    assert result.software == ["NVivo 12"]
    assert result.geographic_coverage == ["United States", "Canada"]

    # Uploader / contact info
    assert result.uploader_name == "Smith, J."
    assert result.uploader_email == "smith@example.edu"

    # Provenance fields
    assert result.depositor == "Doe, A."
    assert result.producer == ["University of Testing"]
    assert result.publication == ["Smith (2023) Qualitative Study"]
    assert result.date_of_collection == "2022-01-01 to 2022-12-31"
    assert result.time_period_covered == "2020-01-01 to 2022-06-30"

    # File-level fields
    assert result.files[0]["restricted"] is False
    assert result.files[0]["friendly_type"] == "REFI-QDA-Project"
    assert result.files[0]["content_type"] == "application/x-zip-refiqda"
    assert result.files[0]["api_checksum"] == "SHA-1:abc123def456"
    assert result.files[1]["restricted"] is True
    assert result.files[1]["api_checksum"] == "MD5:deadbeef"

    # Should have used persistentId endpoint
    call_args = mock_get.call_args
    assert ":persistentId" in call_args[0][0]
    assert call_args[1]["params"]["persistentId"] == "doi:10.5064/F6ABC123"


def test_get_metadata_terms_of_access_fallback(connector):
    """When no license block exists, termsOfAccess should be used as license_type."""
    response = {
        "status": "OK",
        "data": {
            "latestVersion": {
                "releaseTime": "2023-01-01T00:00:00Z",
                "termsOfAccess": "QDR Standard Access",
                "metadataBlocks": {
                    "citation": {
                        "fields": [
                            {"typeName": "title", "value": "No License Dataset"},
                        ]
                    }
                },
                "files": [],
            }
        },
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    url = "https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/F6TEST"
    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata(url)

    assert result.license_type == "QDR Standard Access"


def test_get_metadata_missing_optional_fields(connector):
    """Fields like depositor, producer, etc. default to empty when absent (e.g. DANS)."""
    response = {
        "status": "OK",
        "data": {
            "latestVersion": {
                "releaseTime": "2023-01-01T00:00:00Z",
                "license": {"name": "DANS Licence", "uri": "https://example.com"},
                "metadataBlocks": {
                    "citation": {
                        "fields": [
                            {"typeName": "title", "value": "Minimal Dataset"},
                            {
                                "typeName": "datasetContact",
                                "value": [
                                    {"datasetContactName": {"value": "Contact Person"}},
                                ],
                            },
                        ]
                    }
                },
                "files": [],
            }
        },
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    url = "https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/MINIMAL"
    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata(url)

    assert result.uploader_name == "Contact Person"
    assert result.uploader_email == ""
    assert result.depositor == ""
    assert result.producer == []
    assert result.publication == []
    assert result.date_of_collection == ""
    assert result.time_period_covered == ""


def test_get_metadata_with_numeric_id(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = DATASET_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        connector.get_metadata("https://data.qdr.syr.edu/dataset/42")

    # Should have used numeric ID endpoint
    call_args = mock_get.call_args
    assert "/api/datasets/42" in call_args[0][0]


# -- Download --


def test_download_creates_file(connector, tmp_path):
    content = b"fake file content for testing"

    mock_response = MagicMock()
    mock_response.headers = httpx.Headers(
        {"content-disposition": 'attachment; filename="test_data.qdpx"'}
    )
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_bytes = MagicMock(return_value=iter([content]))
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_response):
        path = connector.download(
            "https://data.qdr.syr.edu/api/access/datafile/12345",
            str(tmp_path),
        )

    assert path == str(tmp_path / "test_data.qdpx")
    assert (tmp_path / "test_data.qdpx").read_bytes() == content


def test_download_fallback_filename(connector, tmp_path):
    content = b"data"

    mock_response = MagicMock()
    mock_response.headers = httpx.Headers({})  # No Content-Disposition
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_bytes = MagicMock(return_value=iter([content]))
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_response):
        path = connector.download(
            "https://data.qdr.syr.edu/api/access/datafile/99999",
            str(tmp_path),
        )

    # Falls back to using the ID from URL
    assert path == str(tmp_path / "99999")


# -- Helpers --


def test_extract_persistent_id():
    url = "https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/F6ABC123"
    assert DataverseConnector._extract_persistent_id(url) == "doi:10.5064/F6ABC123"


def test_extract_persistent_id_bare_doi():
    pid = "doi:10.5064/F6ABC123"
    assert DataverseConnector._extract_persistent_id(pid) == pid


def test_extract_persistent_id_none():
    assert DataverseConnector._extract_persistent_id("https://example.com/dataset/42") is None


def test_filename_from_headers():
    headers = httpx.Headers({"content-disposition": 'attachment; filename="data.csv"'})
    assert _filename_from_headers(headers) == "data.csv"


def test_filename_from_headers_missing():
    headers = httpx.Headers({})
    assert _filename_from_headers(headers) is None


# -- Connector registry --


def test_connector_registry():
    from pipeline.connectors import CONNECTORS

    assert "qdr" in CONNECTORS
    assert isinstance(CONNECTORS["qdr"], DataverseConnector)
    assert "dans" in CONNECTORS
    assert isinstance(CONNECTORS["dans"], DataverseConnector)
    assert "dataverseno" in CONNECTORS
    assert isinstance(CONNECTORS["dataverseno"], DataverseConnector)
