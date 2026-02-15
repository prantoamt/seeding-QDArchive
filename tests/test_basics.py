"""Smoke tests for the pipeline scaffolding."""

from pathlib import Path

from pipeline.config import QDA_EXTENSIONS, ensure_dirs
from pipeline.db.connection import init_db
from pipeline.db.models import Base, File
from pipeline.storage.file_manager import compute_sha256
from pipeline.utils.license import is_open_license


def test_ensure_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.config.DATA_DIR", tmp_path / "data")
    monkeypatch.setattr("pipeline.config.EXPORTS_DIR", tmp_path / "exports")
    ensure_dirs()
    assert (tmp_path / "data").is_dir()
    assert (tmp_path / "exports").is_dir()


def test_file_model_repr():
    f = File(id=1, source_name="zenodo", file_name="test.qdpx")
    assert "zenodo" in repr(f)


def test_qda_extensions():
    assert ".qdpx" in QDA_EXTENSIONS
    assert ".pdf" not in QDA_EXTENSIONS


def test_open_license():
    assert is_open_license("CC-BY-4.0")
    assert is_open_license("cc0")
    assert is_open_license("CC BY SA")
    assert not is_open_license(None)
    assert not is_open_license("")
    assert not is_open_license("all-rights-reserved")


def test_sha256(tmp_path):
    p = tmp_path / "hello.txt"
    p.write_text("hello world")
    h = compute_sha256(p)
    assert len(h) == 64
    assert h == compute_sha256(p)  # deterministic
