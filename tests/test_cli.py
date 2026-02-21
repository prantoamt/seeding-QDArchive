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
        source_url="https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/F6ABC123",
        download_url="https://data.qdr.syr.edu/api/access/datafile/12345",
        title="Test Dataset", authors="Smith, J.", is_qda_file=True,
        file_size_bytes=1024, notes="access restricted (403)",
        keywords="qualitative research; interviews", language="English",
        software="NVivo 12", restricted=True,
        uploader_name="Smith, J.", uploader_email="smith@example.edu",
        local_directory="test-dataset-doi_10.5064_F6ABC123",
        depositor="Doe, A.", producer="University of Testing",
        publication="Smith (2023) Qualitative Study",
        date_of_collection="2022-01-01 to 2022-12-31",
        time_period_covered="2020-01-01 to 2022-06-30",
    ))
    session.add(File(
        source_name="qdr", file_name="transcript.pdf", file_type=".pdf",
        source_url="https://data.qdr.syr.edu/dataset.xhtml?persistentId=doi:10.5064/F6ABC123",
        download_url="https://data.qdr.syr.edu/api/access/datafile/12346",
        title="Test Dataset", authors="Smith, J.", is_qda_file=False,
        file_size_bytes=2048, local_path="/tmp/transcript.pdf", file_hash="abc123",
        keywords="focus groups", language="German",
        software=None, restricted=False,
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


def test_show_new_fields(runner, sample_records):
    """Verify show command displays uploader, provenance, and local_directory fields."""
    result = runner.invoke(cli, ["show", "1"])
    assert result.exit_code == 0
    assert "smith@example.edu" in result.output
    assert "Smith, J." in result.output
    assert "test-dataset-doi_10.5064_F6ABC123" in result.output
    assert "Doe, A." in result.output
    assert "University of Testing" in result.output
    assert "Smith (2023) Qualitative Study" in result.output
    assert "2022-01-01 to 2022-12-31" in result.output
    assert "2020-01-01 to 2022-06-30" in result.output


def test_show_empty_new_fields(runner, sample_records):
    """Record 2 has no new fields — show should display dashes."""
    result = runner.invoke(cli, ["show", "2"])
    assert result.exit_code == 0
    # The new fields should show '—' for record 2
    assert "Uploader:" in result.output
    assert "Depositor:" in result.output


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


def test_scrape_skips_already_cataloged(runner, sample_records):
    """Files already in DB by download_url are skipped without downloading."""
    # Give the existing record a download_url to match against
    session = get_session()
    rec = session.query(File).filter_by(file_name="transcript.pdf").first()
    rec.download_url = "https://example.com/api/files/99/download"
    session.commit()
    session.close()

    mock_result = MagicMock(
        title="Test Dataset",
        source_url="https://example.com/dataset/1",
    )
    mock_metadata = MagicMock(
        license_type="CC BY 4.0",
        license_url="https://creativecommons.org/licenses/by/4.0/",
        title="Test Dataset",
        description="qualitative interview transcripts",
        authors="Doe",
        date_published="2024-01-01",
        tags=[],
        keywords=[],
        kind_of_data=[],
        language=[],
        software=[],
        geographic_coverage=[],
        depositor="",
        producer=[],
        publication=[],
        date_of_collection="",
        time_period_covered="",
        uploader_name="",
        uploader_email="",
        files=[{
            "name": "transcript.pdf",
            "download_url": "https://example.com/api/files/99/download",
            "id": 99,
            "size": 2048,
            "restricted": False,
        }],
    )
    mock_connector = MagicMock()
    mock_connector.search.return_value = [mock_result]
    mock_connector.get_metadata.return_value = mock_metadata

    with patch("pipeline.cli.CONNECTORS", {"qdr": mock_connector}):
        result = runner.invoke(cli, ["scrape", "qdr", "-q", "test"])

    assert result.exit_code == 0
    assert "Already cataloged" in result.output
    mock_connector.download.assert_not_called()


def test_db_search(runner, sample_records):
    result = runner.invoke(cli, ["db", "--search", "interview"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.pdf" not in result.output


def test_db_language(runner, sample_records):
    result = runner.invoke(cli, ["db", "--language", "english"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.pdf" not in result.output


def test_db_software(runner, sample_records):
    result = runner.invoke(cli, ["db", "--software", "nvivo"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.pdf" not in result.output


def test_db_file_type(runner, sample_records):
    result = runner.invoke(cli, ["db", "--file-type", ".pdf"])
    assert result.exit_code == 0
    assert "transcript.p" in result.output
    assert "analysis.qdpx" not in result.output


def test_db_file_type_auto_dot(runner, sample_records):
    result = runner.invoke(cli, ["db", "--file-type", "pdf"])
    assert result.exit_code == 0
    assert "transcript.p" in result.output
    assert "analysis.qdpx" not in result.output


def test_db_has_software(runner, sample_records):
    result = runner.invoke(cli, ["db", "--has-software"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.pdf" not in result.output


def test_db_has_keywords(runner, sample_records):
    result = runner.invoke(cli, ["db", "--has-keywords"])
    assert result.exit_code == 0
    assert "analysis.qdpx" in result.output
    assert "transcript.p" in result.output


def test_db_restricted_only_uses_column(runner, sample_records):
    """Verify --restricted-only filters by the restricted column, not local_path."""
    result = runner.invoke(cli, ["db", "--restricted-only"])
    assert result.exit_code == 0
    # Record 1 has restricted=True, record 2 has restricted=False
    assert "analysis.qdpx" in result.output
    assert "transcript.pdf" not in result.output


def test_status_extended(runner, sample_records):
    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "Restricted:" in result.output
    assert "1" in result.output  # one restricted record
    assert "By language:" in result.output
    assert "English" in result.output
    assert "German" in result.output
    assert "By software:" in result.output
    assert "NVivo 12" in result.output
