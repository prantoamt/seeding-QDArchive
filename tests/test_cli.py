"""Tests for CLI commands using Click's test runner."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipeline.cli import cli
from pipeline.db.connection import get_session
from pipeline.db.models import File


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Use a temporary DB and data dir for every test."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("pipeline.config.DB_PATH", db_path)
    monkeypatch.setattr("pipeline.config.DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr("pipeline.config.DATA_DIR", tmp_path / "data")
    monkeypatch.setattr("pipeline.config.EXPORTS_DIR", tmp_path / "exports")
    monkeypatch.setattr("pipeline.config.LOG_FILE", tmp_path / "pipeline.log")

    # Re-initialize engine with new DB_URL
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    session_local = sessionmaker(bind=engine)
    monkeypatch.setattr("pipeline.db.connection.engine", engine)
    monkeypatch.setattr("pipeline.db.connection.SessionLocal", session_local)

    from pipeline.db.models import Base

    Base.metadata.create_all(engine)


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def sample_records():
    """Insert sample records into the DB."""
    session = get_session()
    session.add(File(
        source_name="qdr", file_name="analysis.qdpx", file_type=".qdpx",
        title="Test Dataset", authors="Smith, J.", is_qda_file=True,
        file_size_bytes=1024, notes="access restricted (403)",
    ))
    session.add(File(
        source_name="qdr", file_name="transcript.pdf", file_type=".pdf",
        title="Test Dataset", authors="Smith, J.", is_qda_file=False,
        file_size_bytes=2048, local_path="/tmp/transcript.pdf", file_hash="abc123",
    ))
    session.commit()
    session.close()


def test_status_empty(runner):
    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "Total records:" in result.output
    assert "0" in result.output


def test_status_with_records(runner, sample_records):
    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "2" in result.output  # total
    assert "qdr" in result.output


def test_list_sources(runner):
    result = runner.invoke(cli, ["list-sources"])
    assert result.exit_code == 0
    assert "qdr" in result.output
    assert "ready" in result.output


def test_db_empty(runner):
    result = runner.invoke(cli, ["db"])
    assert result.exit_code == 0
    assert "No records found" in result.output


def test_db_with_records(runner, sample_records):
    result = runner.invoke(cli, ["db"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.p" in result.output  # may be truncated in table


def test_db_qda_only(runner, sample_records):
    result = runner.invoke(cli, ["db", "--qda-only"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.pdf" not in result.output


def test_db_restricted_only(runner, sample_records):
    result = runner.invoke(cli, ["db", "--restricted-only"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.pdf" not in result.output


def test_show_record(runner, sample_records):
    result = runner.invoke(cli, ["show", "1"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "Smith, J." in result.output
    assert "restricted" in result.output


def test_show_multiple(runner, sample_records):
    result = runner.invoke(cli, ["show", "1", "2"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.pdf" in result.output


def test_show_not_found(runner):
    result = runner.invoke(cli, ["show", "999"])
    assert result.exit_code == 0
    assert "not found" in result.output


def test_export(runner, sample_records, tmp_path):
    output = str(tmp_path / "exports" / "metadata.csv")
    result = runner.invoke(cli, ["export", "-o", output])
    assert result.exit_code == 0
    assert "Exported 2 records" in result.output


def test_reset_confirmed(runner, sample_records):
    result = runner.invoke(cli, ["reset", "-y"])
    assert result.exit_code == 0
    assert "Reset complete" in result.output
    assert "Deleted" in result.output


def test_reset_aborted(runner):
    result = runner.invoke(cli, ["reset"], input="n\n")
    assert result.exit_code == 0
    assert "Aborted" in result.output


def test_search_unknown_source(runner):
    result = runner.invoke(cli, ["search", "nonexistent"])
    assert result.exit_code == 1
    assert "Unknown source" in result.output


def test_search_with_connector(runner):
    mock_results = [
        MagicMock(
            title="Test Dataset",
            authors="Doe, A.",
            date_published="2024-01-01",
            source_url="https://example.com/1",
        )
    ]
    mock_connector = MagicMock()
    mock_connector.search.return_value = mock_results

    with patch("pipeline.cli.CONNECTORS", {"test": mock_connector}):
        result = runner.invoke(cli, ["search", "test", "-q", "qualitative"])

    assert result.exit_code == 0
    assert "Test Dataset" in result.output
