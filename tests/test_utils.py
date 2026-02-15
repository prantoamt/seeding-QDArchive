"""Tests for utility functions."""

from pipeline.connectors.dataverse import _strip_html
from pipeline.utils.license import is_open_license


def test_strip_html_basic():
    assert _strip_html("<p>Hello <b>world</b></p>") == "Hello world"


def test_strip_html_nested():
    raw = "<h3>Overview</h3><p>Some <em>qualitative</em> data.</p>"
    assert _strip_html(raw) == "Overview Some qualitative data."


def test_strip_html_whitespace():
    raw = "<p>\n  Multiple\n  lines\n</p>"
    result = _strip_html(raw)
    assert "  " not in result  # no double spaces
    assert "\n" not in result


def test_strip_html_empty():
    assert _strip_html("") == ""


def test_strip_html_no_tags():
    assert _strip_html("plain text") == "plain text"


def test_standard_access_license():
    assert is_open_license("Standard Access")


def test_standard_access_case_insensitive():
    assert is_open_license("standard access")
    assert is_open_license("STANDARD ACCESS")
