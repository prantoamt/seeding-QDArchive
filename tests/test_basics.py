"""Smoke tests for the pipeline scaffolding."""


from pipeline.config import QDA_EXTENSIONS, ensure_dirs
from pipeline.db.models import File
from pipeline.storage.file_manager import compute_sha256, get_storage_path, slugify
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


# --- slugify tests ---


def test_slugify_basic():
    assert slugify("Qualitative Nursing Study") == "qualitative-nursing-study"


def test_slugify_special_chars():
    assert slugify("Data & Analysis: Results (2024)") == "data-analysis-results-2024"


def test_slugify_unicode():
    assert slugify("Ärzte und Übersetzung") == "arzte-und-ubersetzung"


def test_slugify_truncation():
    long = "a-very-long-title-that-exceeds-the-maximum-length-we-want-for-directory-names"
    result = slugify(long, max_length=40)
    assert len(result) <= 40
    assert not result.endswith("-")


def test_slugify_empty():
    assert slugify("") == ""


def test_slugify_only_special_chars():
    assert slugify("@#$%^&*!") == ""


def test_slugify_hyphen_collapsing():
    assert slugify("foo---bar   baz") == "foo-bar-baz"


# --- get_storage_path with title ---


def test_storage_path_with_title(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.storage.file_manager.DATA_DIR", tmp_path)
    path = get_storage_path("qdr", "doi_10.5064_F60Z715Z", "file.pdf", title="Nursing Study")
    assert path.parent.name == "nursing-study-doi_10.5064_F60Z715Z"
    assert path.name == "file.pdf"
    assert path.parent.exists()


def test_storage_path_without_title(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.storage.file_manager.DATA_DIR", tmp_path)
    path = get_storage_path("qdr", "doi_10.5064_F60Z715Z", "file.pdf")
    assert path.parent.name == "doi_10.5064_F60Z715Z"


def test_storage_path_with_empty_title(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.storage.file_manager.DATA_DIR", tmp_path)
    path = get_storage_path("qdr", "doi_10.5064_F60Z715Z", "file.pdf", title="")
    assert path.parent.name == "doi_10.5064_F60Z715Z"
