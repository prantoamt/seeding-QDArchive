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
                    ]
                }
            },
            "files": [
                {
                    "dataFile": {
                        "id": 12345,
                        "filename": "interviews.qdpx",
                        "filesize": 204800,
                        "contentType": "application/octet-stream",
                    }
                },
                {
                    "dataFile": {
                        "id": 12346,
                        "filename": "codebook.pdf",
                        "filesize": 51200,
                        "contentType": "application/pdf",
                    }
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

    # Should have used persistentId endpoint
    call_args = mock_get.call_args
    assert ":persistentId" in call_args[0][0]
    assert call_args[1]["params"]["persistentId"] == "doi:10.5064/F6ABC123"


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
