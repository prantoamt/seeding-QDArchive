"""Tests for the UK Data Service (ReShare) connector with mocked HTTP."""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.connectors.base import BaseConnector
from pipeline.connectors.ukds import (
    UKDataServiceConnector,
    _extract_doc_id,
    _extract_eprint_id,
    _format_creator,
    _is_open_license,
    _map_license,
    _pick_license,
    _strip_html,
)


@pytest.fixture
def connector():
    c = UKDataServiceConnector()
    c._last_request_time = 0.0
    return c


# -- Interface compliance --


def test_implements_base_connector(connector):
    assert isinstance(connector, BaseConnector)


def test_name_property(connector):
    assert connector.name == "ukds"


# -- Search --

SEARCH_RESPONSE = [
    {
        "eprintid": 857166,
        "title": "Transcript Qualitative Interview Data",
        "abstract": "A set of <b>qualitative</b> interviews",
        "creators": [
            {"name": {"given": "Thomas", "family": "Wells"}},
            {"name": {"given": "Jane", "family": "Doe"}},
        ],
        "date": "2024-09-02",
        "keywords": ["CRIMINAL JUSTICE", "SOCIAL POLICY"],
        "documents": [
            {
                "placement": 1,
                "license": "cc_by_nc_sa",
                "security": "public",
                "files": [{"filename": "857166_documentation.zip", "filesize": 132076}],
            },
        ],
    },
    {
        "eprintid": 853130,
        "title": "Focus Group Transcripts",
        "abstract": "Focus group data",
        "creators": [{"name": {"given": "Bob", "family": "Lee"}}],
        "date": "2023-05-10",
        "keywords": ["PEDAGOGY"],
        "documents": [],
    },
]


def test_search_parses_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        results = connector.search("qualitative interview")

    assert len(results) == 2
    assert results[0].title == "Transcript Qualitative Interview Data"
    assert results[0].source_name == "ukds"
    assert "857166" in results[0].source_url
    assert results[0].authors == "Thomas Wells; Jane Doe"
    assert "qualitative" in results[0].description
    assert "<b>" not in results[0].description
    assert results[0].keywords == ["CRIMINAL JUSTICE", "SOCIAL POLICY"]
    assert results[1].title == "Focus Group Transcripts"

    mock_get.assert_called_once()
    call_args = mock_get.call_args
    assert "export_reshare_JSON" in call_args[0][0]


def test_search_empty_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = []
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        results = connector.search("nonexistent")

    assert results == []


def test_search_file_type_filtering(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        results = connector.search("qualitative", file_type="zip")

    # Only first record has a .zip file
    assert len(results) == 1
    assert results[0].title == "Transcript Qualitative Interview Data"


# -- Get metadata --

RECORD_RESPONSE = {
    "eprintid": 857166,
    "title": "Transcript Qualitative Interview Data",
    "abstract": "A <b>qualitative</b> study of prison residents.",
    "creators": [
        {
            "name": {"given": "Thomas", "family": "Wells"},
            "id": "thomas@example.com",
        },
        {"name": {"given": "Jane", "family": "Doe"}},
    ],
    "date": "2024-09-02",
    "doi": "10.5255/UKDA-SN-857166",
    "keywords": ["CRIMINAL JUSTICE", "SOCIAL POLICY"],
    "language": ["English"],
    "data_kind": ["Text"],
    "country": ["United Kingdom"],
    "geographic_cover": "Northern England",
    "award_funders": ["ESRC"],
    "contact_details": [
        {"name": {"given": "Thomas", "family": "Wells"}},
    ],
    "collection_dates": {
        "date_from": "2022-08-03",
        "date_to": "2022-09-29",
    },
    "documents": [
        {
            "uri": "http://reshare.ukdataservice.ac.uk/id/document/3760839",
            "placement": 1,
            "license": "cc_by_nc_sa",
            "security": "public",
            "files": [
                {
                    "filename": "857166_documentation.zip",
                    "filesize": 132076,
                    "mime_type": "application/zip",
                },
            ],
        },
        {
            "uri": "http://reshare.ukdataservice.ac.uk/id/document/3760840",
            "placement": 2,
            "license": "ukda_eul",
            "security": "staffonly",
            "files": [
                {
                    "filename": "857166_data.zip",
                    "filesize": 1588674,
                    "mime_type": "application/zip",
                },
            ],
        },
        {
            "uri": "http://reshare.ukdataservice.ac.uk/id/document/3760841",
            "placement": 3,
            "license": "cc_by_sa",
            "security": "public",
            "files": [
                {
                    "filename": "857166_readme.docx",
                    "filesize": 42489,
                    "mime_type": (
                        "application/vnd.openxmlformats"
                        "-officedocument.wordprocessingml.document"
                    ),
                },
            ],
        },
        {
            "uri": "http://reshare.ukdataservice.ac.uk/id/document/3760842",
            "placement": 10,
            "license": "cc_public_domain",
            "security": "public",
            "files": [
                {"filename": "lightbox.jpg", "filesize": 806, "mime_type": "image/png"},
                {"filename": "indexcodes.txt", "filesize": 350, "mime_type": "text/plain"},
            ],
        },
    ],
}


def test_get_metadata_full(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = RECORD_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    url = "https://reshare.ukdataservice.ac.uk/857166/"
    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata(url)

    # Basic fields
    assert result.title == "Transcript Qualitative Interview Data"
    assert result.source_name == "ukds"
    assert result.source_url == url
    assert result.authors == "Thomas Wells; Jane Doe"
    assert result.license_type == "CC-BY-NC-SA-4.0"
    assert "creativecommons.org" in result.license_url
    assert result.date_published == "2024-09-02"

    # HTML stripped
    assert "<b>" not in result.description
    assert "qualitative" in result.description

    # Extended metadata
    assert result.keywords == ["CRIMINAL JUSTICE", "SOCIAL POLICY"]
    assert result.language == ["English"]
    assert result.kind_of_data == ["Text"]
    assert "United Kingdom" in result.geographic_coverage
    assert "Northern England" in result.geographic_coverage
    assert result.producer == ["ESRC"]
    assert "10.5255/UKDA-SN-857166" in result.publication[0]
    assert result.uploader_name == "Thomas Wells"
    assert result.uploader_email == "thomas@example.com"
    assert result.depositor == "Thomas Wells"
    assert result.date_of_collection == "2022-08-03 to 2022-09-29"

    # Empty fields
    assert result.software == []
    assert result.time_period_covered == ""

    # Files — 3 real files (thumbnails/index filtered out)
    assert len(result.files) == 3

    # Public file
    f0 = result.files[0]
    assert f0["name"] == "857166_documentation.zip"
    assert f0["id"] == "857166"
    assert f0["restricted"] is False
    assert f0["download_url"] == (
        "https://reshare.ukdataservice.ac.uk/id/document/3760839"
    )
    assert f0["content_type"] == "application/zip"
    assert f0["friendly_type"] == "zip"

    # Restricted file (staffonly + non-open license)
    f1 = result.files[1]
    assert f1["name"] == "857166_data.zip"
    assert f1["restricted"] is True

    # Another public file
    f2 = result.files[2]
    assert f2["name"] == "857166_readme.docx"
    assert f2["restricted"] is False


def test_get_metadata_list_response(connector):
    """Single record endpoint may return a list."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = [RECORD_RESPONSE]
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata(
            "https://reshare.ukdataservice.ac.uk/857166/"
        )

    assert result.title == "Transcript Qualitative Interview Data"


def test_get_metadata_missing_optional_fields(connector):
    response = {
        "eprintid": 99999,
        "title": "Minimal Record",
        "creators": [{"name": {"given": "Author", "family": "One"}}],
        "date": "2024-01-01",
        "documents": [],
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata(
            "https://reshare.ukdataservice.ac.uk/99999/"
        )

    assert result.title == "Minimal Record"
    assert result.description == ""
    assert result.license_type == ""
    assert result.keywords == []
    assert result.language == []
    assert result.kind_of_data == []
    assert result.geographic_coverage == []
    assert result.producer == []
    assert result.publication == []
    assert result.uploader_name == "Author One"
    assert result.files == []


def test_get_metadata_html_stripping(connector):
    response = {
        "eprintid": 11111,
        "title": "HTML Test",
        "abstract": "<p>This is <strong>bold</strong> and <em>italic</em>.</p>",
        "creators": [],
        "date": "2024-01-01",
        "documents": [],
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.get", return_value=mock_resp):
        result = connector.get_metadata(
            "https://reshare.ukdataservice.ac.uk/11111/"
        )

    assert result.description == "This is bold and italic ."
    assert "<" not in result.description


# -- Download --


def test_download_creates_file(connector, tmp_path):
    content = b"fake reshare file content"

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_bytes = MagicMock(return_value=iter([content]))
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_response):
        path = connector.download(
            "https://reshare.ukdataservice.ac.uk/857166/1/857166_documentation.zip",
            str(tmp_path),
        )

    assert path == str(tmp_path / "857166_documentation.zip")
    assert (tmp_path / "857166_documentation.zip").read_bytes() == content


def test_download_explicit_filename(connector, tmp_path):
    content = b"data"

    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.iter_bytes = MagicMock(return_value=iter([content]))
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("httpx.stream", return_value=mock_response):
        path = connector.download(
            "https://reshare.ukdataservice.ac.uk/857166/1/857166_documentation.zip",
            str(tmp_path),
            filename="custom.zip",
        )

    assert path == str(tmp_path / "custom.zip")


# -- Helpers --


def test_extract_eprint_id_from_url():
    url = "https://reshare.ukdataservice.ac.uk/857166/"
    assert _extract_eprint_id(url) == "857166"


def test_extract_eprint_id_from_eprint_url():
    url = "https://reshare.ukdataservice.ac.uk/id/eprint/857166"
    assert _extract_eprint_id(url) == "857166"


def test_extract_eprint_id_bare_number():
    assert _extract_eprint_id("857166") == "857166"


def test_extract_doc_id():
    uri = "http://reshare.ukdataservice.ac.uk/id/document/3744469"
    assert _extract_doc_id(uri) == "3744469"


def test_extract_doc_id_empty():
    assert _extract_doc_id("") == ""


def test_format_creator():
    c = {"name": {"given": "Thomas", "family": "Wells"}}
    assert _format_creator(c) == "Thomas Wells"


def test_format_creator_missing_given():
    c = {"name": {"given": None, "family": "Wells"}}
    assert _format_creator(c) == "Wells"


def test_is_open_license():
    assert _is_open_license("cc_by") is True
    assert _is_open_license("cc_by_sa") is True
    assert _is_open_license("cc_public_domain") is True
    assert _is_open_license("ukda_eul") is False
    assert _is_open_license("unknown") is False


def test_map_license():
    assert _map_license("cc_by") == "CC-BY-4.0"
    assert _map_license("cc_by_nc_sa") == "CC-BY-NC-SA-4.0"
    assert _map_license("cc_public_domain") == "CC0-1.0"
    assert _map_license("ukda_eul") == "ukda_eul"


def test_pick_license_prefers_open():
    assert _pick_license(["ukda_eul", "cc_by_sa"]) == "CC-BY-SA-4.0"


def test_pick_license_empty():
    assert _pick_license([]) == ""


def test_strip_html():
    assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"
    assert _strip_html("no tags") == "no tags"
    assert _strip_html("") == ""


# -- Connector registry --


def test_connector_registry():
    from pipeline.connectors import CONNECTORS

    assert "ukds" in CONNECTORS
    assert isinstance(CONNECTORS["ukds"], UKDataServiceConnector)
