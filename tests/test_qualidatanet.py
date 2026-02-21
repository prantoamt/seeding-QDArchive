"""Tests for the QualidataNet connector with mocked HTTP."""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.connectors.base import BaseConnector
from pipeline.connectors.qualidatanet import (
    QualidataNetConnector,
    _as_list,
    _extract_total,
    _normalize_authors,
    _normalize_text,
    _strip_html,
)


@pytest.fixture
def connector():
    c = QualidataNetConnector()
    c._last_request_time = 0.0
    return c


# -- Interface compliance --


def test_implements_base_connector(connector):
    assert isinstance(connector, BaseConnector)


def test_name_property(connector):
    assert connector.name == "qualidatanet"


# -- Search --

ES_SEARCH_RESPONSE = {
    "hits": {
        "total": 2,
        "hits": [
            {
                "_id": "oai:pangaea.de:doi:10.1594/PANGAEA.929747",
                "_source": {
                    "citation_title": "Qualitative Interview Data on Migration",
                    "description": [
                        "A <b>qualitative</b> study on migration patterns.",
                        "Interviews conducted in Berlin.",
                    ],
                    "citation_authors": ["Mueller, Anna", "Schmidt, Jan"],
                    "citation_date": "2021",
                    "keyword": ["migration", "qualitative research"],
                    "license": ["CC-BY-4.0"],
                    "dataCenter": "FDZ Qualiservice",
                    "metadatalink": "https://doi.pangaea.de/10.1594/PANGAEA.929747",
                    "type": ["Editorial Publication of Datasets"],
                    "location": ["Berlin, Germany"],
                    "format": ["application/zip", "3 datasets"],
                },
            },
            {
                "_id": "oai:dipf:doi:10.7477/42:1:1",
                "_source": {
                    "citation_title": "Focus Group Transcripts on Education",
                    "description": "Focus group data from German schools.",
                    "citation_authors": "Weber, Lisa",
                    "citation_date": "2020",
                    "keyword": ["education", "focus group"],
                    "license": ["All rights reserved"],
                    "dataCenter": "FDZ Bildung",
                    "metadatalink": "https://doi.org/10.7477/42:1:1",
                    "format": [],
                },
            },
        ],
    }
}


def test_search_parses_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = ES_SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.post", return_value=mock_resp) as mock_post:
        results = connector.search("qualitative interview")

    assert len(results) == 2

    r0 = results[0]
    assert r0.title == "Qualitative Interview Data on Migration"
    assert r0.source_name == "qualidatanet"
    assert r0.source_url == (
        "https://doi.pangaea.de/10.1594/PANGAEA.929747"
    )
    assert r0.authors == "Mueller, Anna; Schmidt, Jan"
    assert "qualitative" in r0.description
    assert "<b>" not in r0.description
    assert r0.keywords == ["migration", "qualitative research"]
    assert r0.license_type == "CC-BY-4.0"
    assert r0.producer == ["FDZ Qualiservice"]
    assert r0.geographic_coverage == ["Berlin, Germany"]
    assert r0.date_published == "2021"
    assert r0.files == []

    r1 = results[1]
    assert r1.title == "Focus Group Transcripts on Education"
    assert r1.authors == "Weber, Lisa"

    mock_post.assert_called_once()
    call_body = mock_post.call_args[1]["json"]
    assert call_body["query"]["multi_match"]["query"] == (
        "qualitative interview"
    )


def test_search_empty_results(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"hits": {"total": 0, "hits": []}}
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.post", return_value=mock_resp):
        results = connector.search("nonexistent")

    assert results == []


def test_search_file_type_filtering(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = ES_SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.post", return_value=mock_resp):
        results = connector.search("qualitative", file_type="zip")

    # Only first record has "application/zip" in format
    assert len(results) == 1
    assert results[0].title == "Qualitative Interview Data on Migration"


def test_search_pagination(connector):
    page1 = {
        "hits": {
            "total": 2,
            "hits": [
                {
                    "_id": "rec1",
                    "_source": {
                        "citation_title": "Record 1",
                        "metadatalink": "https://doi.org/rec1",
                    },
                },
            ],
        }
    }
    page2 = {
        "hits": {
            "total": 2,
            "hits": [
                {
                    "_id": "rec2",
                    "_source": {
                        "citation_title": "Record 2",
                        "metadatalink": "https://doi.org/rec2",
                    },
                },
            ],
        }
    }
    page3 = {"hits": {"total": 2, "hits": []}}

    mock_resp1 = MagicMock()
    mock_resp1.json.return_value = page1
    mock_resp1.raise_for_status = MagicMock()

    mock_resp2 = MagicMock()
    mock_resp2.json.return_value = page2
    mock_resp2.raise_for_status = MagicMock()

    mock_resp3 = MagicMock()
    mock_resp3.json.return_value = page3
    mock_resp3.raise_for_status = MagicMock()

    with patch(
        "httpx.post", side_effect=[mock_resp1, mock_resp2, mock_resp3]
    ):
        # Use small page size to trigger pagination
        import pipeline.connectors.qualidatanet as mod

        orig = mod.PAGE_SIZE
        mod.PAGE_SIZE = 1
        try:
            results = connector.search("interview")
        finally:
            mod.PAGE_SIZE = orig

    assert len(results) == 2
    assert results[0].title == "Record 1"
    assert results[1].title == "Record 2"


# -- Get metadata --


def test_get_metadata_from_cache(connector):
    """get_metadata returns cached result from search() without API call."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = ES_SEARCH_RESPONSE
    mock_resp.raise_for_status = MagicMock()

    # Run search first to populate cache
    with patch("httpx.post", return_value=mock_resp):
        connector.search("qualitative")

    url = "https://doi.pangaea.de/10.1594/PANGAEA.929747"
    # No httpx.post mock needed — should come from cache
    result = connector.get_metadata(url)

    assert result.title == "Qualitative Interview Data on Migration"
    assert result.source_name == "qualidatanet"
    assert result.source_url == url
    assert result.authors == "Mueller, Anna; Schmidt, Jan"
    assert result.license_type == "CC-BY-4.0"
    assert result.date_published == "2021"
    assert result.keywords == ["migration", "qualitative research"]
    assert result.geographic_coverage == ["Berlin, Germany"]
    assert result.producer == ["FDZ Qualiservice"]
    assert result.publication == [url]
    assert result.kind_of_data == ["Editorial Publication of Datasets"]
    assert result.files == []

    # HTML stripped from description
    assert "<b>" not in result.description
    assert "qualitative" in result.description


def test_get_metadata_fallback_api(connector):
    """get_metadata falls back to ES query when not in cache."""
    es_resp = {
        "hits": {
            "total": 1,
            "hits": [ES_SEARCH_RESPONSE["hits"]["hits"][0]],
        }
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = es_resp
    mock_resp.raise_for_status = MagicMock()

    url = "https://doi.pangaea.de/10.1594/PANGAEA.929747"
    with patch("httpx.post", return_value=mock_resp) as mock_post:
        result = connector.get_metadata(url)

    assert result.title == "Qualitative Interview Data on Migration"
    # Verify match_phrase query was used
    call_body = mock_post.call_args[1]["json"]
    assert "match_phrase" in call_body["query"]


def test_get_metadata_not_found(connector):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"hits": {"total": 0, "hits": []}}
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.post", return_value=mock_resp):
        with pytest.raises(ValueError, match="No QualidataNet record"):
            connector.get_metadata("https://doi.org/nonexistent")


def test_get_metadata_missing_optional_fields(connector):
    es_resp = {
        "hits": {
            "total": 1,
            "hits": [
                {
                    "_id": "minimal",
                    "_source": {
                        "citation_title": "Minimal Record",
                        "metadatalink": "https://doi.org/minimal",
                    },
                }
            ],
        }
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = es_resp
    mock_resp.raise_for_status = MagicMock()

    with patch("httpx.post", return_value=mock_resp):
        result = connector.get_metadata("https://doi.org/minimal")

    assert result.title == "Minimal Record"
    assert result.description == ""
    assert result.authors == ""
    assert result.license_type == ""
    assert result.keywords == []
    assert result.geographic_coverage == []
    assert result.producer == []
    assert result.files == []


# -- Download --


def test_download_raises(connector):
    with pytest.raises(NotImplementedError, match="does not host files"):
        connector.download("https://example.com", "/tmp")


# -- Helpers --


def test_normalize_text_list():
    assert _normalize_text(["Hello", "<b>world</b>"]) == "Hello world"


def test_normalize_text_string():
    assert _normalize_text("<p>Simple</p>") == "Simple"


def test_normalize_text_empty():
    assert _normalize_text("") == ""


def test_normalize_authors_list():
    assert _normalize_authors(["Alice", "Bob"]) == "Alice; Bob"


def test_normalize_authors_string():
    assert _normalize_authors("Alice") == "Alice"


def test_normalize_authors_empty():
    assert _normalize_authors("") == ""


def test_as_list_none():
    assert _as_list(None) == []


def test_as_list_string():
    assert _as_list("hello") == ["hello"]


def test_as_list_list():
    assert _as_list(["a", "b"]) == ["a", "b"]


def test_strip_html():
    assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"
    assert _strip_html("no tags") == "no tags"
    assert _strip_html("") == ""


def test_extract_total_int():
    assert _extract_total({"total": 42}) == 42


def test_extract_total_dict():
    assert _extract_total({"total": {"value": 42}}) == 42


def test_extract_total_missing():
    assert _extract_total({}) == 0


# -- Connector registry --


def test_connector_registry():
    """QualidataNet is metadata-only and not in the active connector registry."""
    from pipeline.connectors import CONNECTORS

    assert "qualidatanet" not in CONNECTORS
