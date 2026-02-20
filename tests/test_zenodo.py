"""Tests for the Zenodo connector with mocked HTTP responses."""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.connectors.base import BaseConnector
from pipeline.connectors.zenodo import ZenodoConnector, _extract_record_id, _strip_html


@pytest.fixture
def connector():
    c = ZenodoConnector()
    # Disable throttling in tests
    c._last_request_time = 0.0
    return c


# -- Interface compliance --


def test_implements_base_connector(connector):
    assert isinstance(connector, BaseConnector)


def test_name_property(connector):
    assert connector.name == "zenodo"


# -- Search --

SEARCH_RESPONSE = {
    "hits": {
        "total": 2,
        "hits": [
            {
                "id": 12345,
                "metadata": {
                    "title": "Qualitative Interview Study",
                    "description": "A set of <b>qualitative</b> interviews",
                    "creators": [
                        {"name": "Smith, J."},
                        {"name": "Doe, A."},
                    ],
                    "publication_date": "2023-06-15",
                    "keywords": ["qualitative", "interviews"],
                },
                "files": [
                    {"key": "interviews.qdpx", "size": 204800},
                    {"key": "codebook.pdf", "size": 51200},
                ],
            },
            {
                "id": 67890,
                "metadata": {
                    "title": "Focus Group Transcripts",
                    "description": "Focus group data",
                    "creators": [{"name": "Lee, B."}],
                    "publication_date": "2024-01-20",
                    "keywords": ["focus groups"],
                },
                "files": [
                    {"key": "transcripts.docx", "size": 102400},
                ],
            },
        ],
    }
}


def test_search_parses_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        results = connector.search("qualitative interviews")

    assert len(results) == 2
    assert results[0].title == "Qualitative Interview Study"
    assert results[0].source_name == "zenodo"
    assert results[0].source_url == "https://zenodo.org/records/12345"
    assert results[0].authors == "Smith, J.; Doe, A."
    assert results[0].description == "A set of qualitative interviews"  # HTML stripped
    assert results[0].keywords == ["qualitative", "interviews"]
    assert results[1].title == "Focus Group Transcripts"

    mock_get.assert_called_once()
    call_args = mock_get.call_args
    assert "/api/records" in call_args[0][0]
    assert call_args[1]["params"]["q"] == "qualitative interviews"


def test_search_empty_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"hits": {"total": 0, "hits": []}}
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        results = connector.search("nonexistent")

    assert results == []


def test_search_file_type_filtering(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        results = connector.search("qualitative", file_type="qdpx")

    # Only the first record has a .qdpx file
    assert len(results) == 1
    assert results[0].title == "Qualitative Interview Study"


def test_search_file_type_filtering_with_dot(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        results = connector.search("qualitative", file_type=".docx")

    assert len(results) == 1
    assert results[0].title == "Focus Group Transcripts"


# -- Get metadata --

RECORD_RESPONSE = {
    "id": 12345,
    "metadata": {
        "title": "Qualitative Interview Study",
        "description": "A set of <b>qualitative</b> interviews about <i>health</i>.",
        "creators": [
            {"name": "Smith, J."},
            {"name": "Doe, A."},
        ],
        "license": {"id": "cc-by-4.0"},
        "publication_date": "2023-06-15",
        "keywords": ["qualitative research", "interviews"],
        "language": "eng",
        "resource_type": {"type": "dataset"},
        "contributors": [
            {"name": "University of Testing"},
        ],
        "related_identifiers": [
            {"identifier": "10.1234/test", "relation": "isSupplementTo"},
        ],
        "access_right": "open",
    },
    "files": [
        {
            "id": "a1b2c3d4-uuid",
            "key": "interviews.qdpx",
            "size": 204800,
            "checksum": "md5:abc123def456",
            "links": {"self": "https://zenodo.org/api/records/12345/files/interviews.qdpx/content"},
        },
        {
            "id": "e5f6g7h8-uuid",
            "key": "codebook.pdf",
            "size": 51200,
            "checksum": "md5:deadbeef",
            "links": {"self": "https://zenodo.org/api/records/12345/files/codebook.pdf/content"},
        },
    ],
}


def test_get_metadata_full(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = RECORD_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    url = "https://zenodo.org/records/12345"
    with patch("httpx.get", return_value=mock_resp) as mock_get:
        result = connector.get_metadata(url)

    # Basic fields
    assert result.title == "Qualitative Interview Study"
    assert result.source_name == "zenodo"
    assert result.source_url == url
    assert result.authors == "Smith, J.; Doe, A."
    assert result.license_type == "cc-by-4.0"
    assert result.date_published == "2023-06-15"

    # HTML stripped from description
    assert "<b>" not in result.description
    assert "qualitative" in result.description

    # Extended metadata
    assert result.keywords == ["qualitative research", "interviews"]
    assert result.tags == ["qualitative research", "interviews"]
    assert result.language == ["eng"]
    assert result.kind_of_data == ["dataset"]
    assert result.producer == ["University of Testing"]
    assert result.publication == ["isSupplementTo: 10.1234/test"]
    assert result.uploader_name == "Smith, J."
    assert result.uploader_email == ""

    # Empty fields (not available in Zenodo)
    assert result.software == []
    assert result.geographic_coverage == []
    assert result.depositor == ""

    # Files
    assert len(result.files) == 2
    assert result.files[0]["name"] == "interviews.qdpx"
    assert result.files[0]["id"] == "12345"
    assert result.files[0]["size"] == 204800
    assert result.files[0]["download_url"] == "https://zenodo.org/api/records/12345/files/interviews.qdpx/content"
    assert result.files[0]["api_checksum"] == "md5:abc123def456"
    assert result.files[0]["restricted"] is False
    assert result.files[0]["friendly_type"] == "qdpx"  # derived from extension
    assert result.files[0]["content_type"] == ""
    assert result.files[1]["name"] == "codebook.pdf"
    assert result.files[1]["friendly_type"] == "pdf"  # derived from extension

    # Correct API endpoint
    call_args = mock_get.call_args
    assert "/api/records/12345" in call_args[0][0]


def test_get_metadata_restricted_record(connector):
    """When access_right is not 'open', all files should be marked restricted."""
    response = dict(RECORD_RESPONSE)
    response = {**RECORD_RESPONSE}
    response["metadata"] = {**RECORD_RESPONSE["metadata"], "access_right": "restricted"}

    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata("https://zenodo.org/records/12345")

    assert all(f["restricted"] is True for f in result.files)


def test_get_metadata_missing_optional_fields(connector):
    """Optional fields default to empty when absent."""
    response = {
        "id": 99999,
        "metadata": {
            "title": "Minimal Record",
            "creators": [{"name": "Author"}],
            "access_right": "open",
        },
        "files": [],
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata("https://zenodo.org/records/99999")

    assert result.title == "Minimal Record"
    assert result.description == ""
    assert result.license_type == ""
    assert result.keywords == []
    assert result.language == []
    assert result.kind_of_data == []
    assert result.producer == []
    assert result.publication == []
    assert result.uploader_name == "Author"
    assert result.files == []


def test_get_metadata_html_stripping(connector):
    response = {
        "id": 11111,
        "metadata": {
            "title": "HTML Test",
            "description": "<p>This is <strong>bold</strong> and <em>italic</em>.</p>",
            "creators": [],
            "access_right": "open",
        },
        "files": [],
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata("https://zenodo.org/records/11111")

    assert result.description == "This is bold and italic ."
    assert "<" not in result.description


# -- Download --


def test_download_creates_file(connector, tmp_path):
    content = b"fake zenodo file content"

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_bytes = MagicMock(return_value=iter([content]))
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_response):
        path = connector.download(
            "https://zenodo.org/api/files/bucket1/interviews.qdpx",
            str(tmp_path),
        )

    assert path == str(tmp_path / "interviews.qdpx")
    assert (tmp_path / "interviews.qdpx").read_bytes() == content


def test_download_explicit_filename(connector, tmp_path):
    content = b"data"

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_bytes = MagicMock(return_value=iter([content]))
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_response):
        path = connector.download(
            "https://zenodo.org/api/files/bucket1/interviews.qdpx",
            str(tmp_path),
            filename="custom_name.qdpx",
        )

    assert path == str(tmp_path / "custom_name.qdpx")
    assert (tmp_path / "custom_name.qdpx").read_bytes() == content


# -- Helpers --


def test_extract_record_id_from_url():
    assert _extract_record_id("https://zenodo.org/records/12345") == "12345"


def test_extract_record_id_from_record_url():
    assert _extract_record_id("https://zenodo.org/record/12345") == "12345"


def test_extract_record_id_bare_number():
    assert _extract_record_id("12345") == "12345"


def test_strip_html():
    assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"
    assert _strip_html("no tags") == "no tags"
    assert _strip_html("") == ""


# -- Connector registry --


def test_connector_registry():
    from pipeline.connectors import CONNECTORS

    assert "zenodo" in CONNECTORS
    assert isinstance(CONNECTORS["zenodo"], ZenodoConnector)
