"""Tests for the Dryad connector with mocked HTTP responses."""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.connectors.base import BaseConnector
from pipeline.connectors.dryad import (
    DryadConnector,
    _encode_doi,
    _extract_doi,
    _parse_license,
    _sanitize_doi,
    _strip_html,
)


@pytest.fixture
def connector():
    c = DryadConnector()
    # Disable throttling in tests
    c._last_request_time = 0.0
    return c


# -- Interface compliance --


def test_implements_base_connector(connector):
    assert isinstance(connector, BaseConnector)


def test_name_property(connector):
    assert connector.name == "dryad"


# -- Search --

SEARCH_RESPONSE = {
    "total": 2,
    "_embedded": {
        "stash:datasets": [
            {
                "identifier": "doi:10.5061/dryad.abc123",
                "title": "Qualitative Interview Data",
                "abstract": "A set of <b>qualitative</b> interviews",
                "authors": [
                    {"firstName": "Jane", "lastName": "Smith"},
                    {"firstName": "John", "lastName": "Doe"},
                ],
                "publicationDate": "2023-06-15",
                "keywords": ["qualitative", "interviews"],
            },
            {
                "identifier": "doi:10.5061/dryad.def456",
                "title": "Focus Group Transcripts",
                "abstract": "Focus group data",
                "authors": [{"firstName": "Bob", "lastName": "Lee"}],
                "publicationDate": "2024-01-20",
                "keywords": ["focus groups"],
            },
        ]
    },
}


def test_search_parses_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        results = connector.search("qualitative interviews")

    assert len(results) == 2
    assert results[0].title == "Qualitative Interview Data"
    assert results[0].source_name == "dryad"
    assert results[0].source_url == "https://datadryad.org/stash/dataset/doi:10.5061/dryad.abc123"
    assert results[0].authors == "Jane Smith; John Doe"
    assert results[0].description == "A set of qualitative interviews"  # HTML stripped
    assert results[0].keywords == ["qualitative", "interviews"]
    assert results[0].date_published == "2023-06-15"
    assert results[1].title == "Focus Group Transcripts"
    assert results[1].authors == "Bob Lee"

    mock_get.assert_called_once()
    call_args = mock_get.call_args
    assert "/api/v2/search" in call_args[0][0]
    assert call_args[1]["params"]["q"] == "qualitative interviews"


def test_search_empty_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"total": 0, "_embedded": {"stash:datasets": []}}
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        results = connector.search("nonexistent")

    assert results == []


def test_search_pagination(connector):
    page1 = {
        "total": 30,
        "_embedded": {
            "stash:datasets": [
                {
                    "identifier": f"doi:10.5061/dryad.item{i}",
                    "title": f"Item {i}",
                    "abstract": "",
                    "authors": [],
                    "publicationDate": "2024-01-01",
                    "keywords": [],
                }
                for i in range(25)
            ]
        },
    }
    page2 = {
        "total": 30,
        "_embedded": {
            "stash:datasets": [
                {
                    "identifier": f"doi:10.5061/dryad.item{i}",
                    "title": f"Item {i}",
                    "abstract": "",
                    "authors": [],
                    "publicationDate": "2024-01-01",
                    "keywords": [],
                }
                for i in range(25, 30)
            ]
        },
    }

    mock_resp1 = MagicMock()
    mock_resp1.json.return_value = page1
    mock_resp1.raise_for_status = MagicMock()

    mock_resp2 = MagicMock()
    mock_resp2.json.return_value = page2
    mock_resp2.raise_for_status = MagicMock()

    with patch("httpx.get", side_effect=[mock_resp1, mock_resp2]):
        results = connector.search("test")

    assert len(results) == 30


# -- Get metadata --

DATASET_RESPONSE = {
    "identifier": "doi:10.5061/dryad.abc123",
    "title": "Qualitative Interview Data",
    "abstract": "A set of <b>qualitative</b> interviews about <i>health</i>.",
    "authors": [
        {"firstName": "Jane", "lastName": "Smith", "email": "jane@example.com"},
        {"firstName": "John", "lastName": "Doe"},
    ],
    "license": "https://creativecommons.org/publicdomain/zero/1.0/",
    "publicationDate": "2023-06-15",
    "keywords": ["qualitative research", "interviews"],
    "locations": [
        {"place": "Berlin, Germany"},
        {"place": "London, UK"},
    ],
    "funders": [
        {"organization": "National Science Foundation"},
        {"organization": "DFG"},
    ],
    "relatedWorks": [
        {"identifier": "10.1234/test", "relationship": "IsCitedBy"},
    ],
    "_links": {
        "stash:version": {
            "href": "/api/v2/versions/12345",
        },
    },
}

FILES_RESPONSE = {
    "_embedded": {
        "stash:files": [
            {
                "path": "interviews.qdpx",
                "size": 204800,
                "mimeType": "application/zip",
                "digest": "abc123def456",
                "_links": {
                    "stash:download": {
                        "href": "/api/v2/files/111/download",
                    },
                },
            },
            {
                "path": "codebook.pdf",
                "size": 51200,
                "mimeType": "application/pdf",
                "digest": "deadbeef0000",
                "_links": {
                    "stash:download": {
                        "href": "/api/v2/files/222/download",
                    },
                },
            },
        ]
    }
}


def test_get_metadata_full(connector):
    mock_dataset_resp = MagicMock()
    mock_dataset_resp.json.return_value = DATASET_RESPONSE
    mock_dataset_resp.raise_for_status = MagicMock()

    mock_files_resp = MagicMock()
    mock_files_resp.json.return_value = FILES_RESPONSE
    mock_files_resp.raise_for_status = MagicMock()

    url = "https://datadryad.org/stash/dataset/doi:10.5061/dryad.abc123"
    with patch("httpx.get", side_effect=[mock_dataset_resp, mock_files_resp]) as mock_get:
        result = connector.get_metadata(url)

    # Basic fields
    assert result.title == "Qualitative Interview Data"
    assert result.source_name == "dryad"
    assert result.source_url == url
    assert result.authors == "Jane Smith; John Doe"
    assert result.license_type == "CC0-1.0"
    assert result.license_url == "https://creativecommons.org/publicdomain/zero/1.0/"
    assert result.date_published == "2023-06-15"

    # HTML stripped from description
    assert "<b>" not in result.description
    assert "qualitative" in result.description

    # Extended metadata
    assert result.keywords == ["qualitative research", "interviews"]
    assert result.tags == ["qualitative research", "interviews"]
    assert result.geographic_coverage == ["Berlin, Germany", "London, UK"]
    assert result.producer == ["National Science Foundation", "DFG"]
    assert result.publication == ["IsCitedBy: 10.1234/test"]
    assert result.uploader_name == "Jane Smith"
    assert result.uploader_email == "jane@example.com"

    # Empty fields (not available in Dryad)
    assert result.language == []
    assert result.software == []
    assert result.kind_of_data == []
    assert result.depositor == ""

    # Files
    assert len(result.files) == 2
    f0 = result.files[0]
    assert f0["name"] == "interviews.qdpx"
    assert f0["id"] == "doi_10.5061_dryad.abc123"
    assert f0["size"] == 204800
    assert f0["download_url"] == "https://datadryad.org/api/v2/files/111/download"
    assert f0["api_checksum"] == "sha-256:abc123def456"
    assert f0["restricted"] is False
    assert f0["content_type"] == "application/zip"
    assert f0["friendly_type"] == "qdpx"

    f1 = result.files[1]
    assert f1["name"] == "codebook.pdf"
    assert f1["friendly_type"] == "pdf"
    assert f1["content_type"] == "application/pdf"

    # Correct API endpoints called
    calls = mock_get.call_args_list
    assert "datasets/" in calls[0][0][0]
    assert "/files" in calls[1][0][0]


def test_get_metadata_missing_optional_fields(connector):
    """Optional fields default to empty when absent."""
    response = {
        "identifier": "doi:10.5061/dryad.minimal",
        "title": "Minimal Record",
        "authors": [{"firstName": "Author", "lastName": "One"}],
        "publicationDate": "2024-01-01",
        "_links": {},
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata("https://datadryad.org/stash/dataset/doi:10.5061/dryad.minimal")

    assert result.title == "Minimal Record"
    assert result.description == ""
    assert result.license_type == ""
    assert result.keywords == []
    assert result.geographic_coverage == []
    assert result.producer == []
    assert result.publication == []
    assert result.uploader_name == "Author One"
    assert result.uploader_email == ""
    assert result.files == []


def test_get_metadata_html_stripping(connector):
    response = {
        "identifier": "doi:10.5061/dryad.htmltest",
        "title": "HTML Test",
        "abstract": "<p>This is <strong>bold</strong> and <em>italic</em>.</p>",
        "authors": [],
        "publicationDate": "2024-01-01",
        "_links": {},
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata("https://datadryad.org/stash/dataset/doi:10.5061/dryad.htmltest")

    assert result.description == "This is bold and italic ."
    assert "<" not in result.description


# -- Download --


def test_download_creates_file(connector, tmp_path):
    content = b"fake dryad file content"

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_bytes = MagicMock(return_value=iter([content]))
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_response):
        path = connector.download(
            "https://datadryad.org/api/v2/files/111/download",
            str(tmp_path),
        )

    assert path == str(tmp_path / "download")
    assert (tmp_path / "download").read_bytes() == content


def test_download_explicit_filename(connector, tmp_path):
    content = b"data"

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_bytes = MagicMock(return_value=iter([content]))
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_response):
        path = connector.download(
            "https://datadryad.org/api/v2/files/111/download",
            str(tmp_path),
            filename="interviews.qdpx",
        )

    assert path == str(tmp_path / "interviews.qdpx")
    assert (tmp_path / "interviews.qdpx").read_bytes() == content


# -- Helpers --


def test_extract_doi_from_url():
    url = "https://datadryad.org/stash/dataset/doi:10.5061/dryad.abc123"
    assert _extract_doi(url) == "doi:10.5061/dryad.abc123"


def test_extract_doi_bare():
    assert _extract_doi("doi:10.5061/dryad.abc123") == "doi:10.5061/dryad.abc123"


def test_extract_doi_with_trailing_slash():
    url = "https://datadryad.org/stash/dataset/doi:10.5061/dryad.abc123/"
    assert _extract_doi(url) == "doi:10.5061/dryad.abc123"


def test_encode_doi():
    assert _encode_doi("doi:10.5061/dryad.abc123") == "doi%3A10.5061%2Fdryad.abc123"


def test_sanitize_doi():
    assert _sanitize_doi("doi:10.5061/dryad.abc123") == "doi_10.5061_dryad.abc123"


def test_parse_license_cc0():
    assert _parse_license("https://creativecommons.org/publicdomain/zero/1.0/") == "CC0-1.0"


def test_parse_license_cc_by():
    assert _parse_license("https://creativecommons.org/licenses/by/4.0/") == "CC-BY-4.0"


def test_parse_license_empty():
    assert _parse_license("") == ""


def test_strip_html():
    assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"
    assert _strip_html("no tags") == "no tags"
    assert _strip_html("") == ""


# -- Connector registry --


def test_connector_registry():
    from pipeline.connectors import CONNECTORS

    assert "dryad" in CONNECTORS
    assert isinstance(CONNECTORS["dryad"], DryadConnector)
