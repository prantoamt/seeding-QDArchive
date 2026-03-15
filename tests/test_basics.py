"""Smoke tests for the pipeline scaffolding."""


from pipeline.config import QDA_EXTENSIONS, ensure_dirs, normalize_language
from pipeline.db.models import Keyword, PersonRole, PersonRoleType, Project, ProjectFile
from pipeline.storage.file_manager import compute_sha256, get_storage_path
from pipeline.utils.license import is_open_license


def test_ensure_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.config.DATA_DIR", tmp_path / "data")
    monkeypatch.setattr("pipeline.config.EXPORTS_DIR", tmp_path / "exports")
    ensure_dirs()
    assert (tmp_path / "data").is_dir()
    assert (tmp_path / "exports").is_dir()


def test_project_model_repr():
    p = Project(
        id=1, title="Test Project", repository_id=1,
        repository_url="https://zenodo.org",
        project_url="https://zenodo.org/records/123",
        download_repository_folder="zenodo",
        download_project_folder="123",
    )
    assert "Test Project" in repr(p)


def test_project_file_model_repr():
    f = ProjectFile(id=1, file_name="test.qdpx", file_type=".qdpx", project_id=1)
    assert "test.qdpx" in repr(f)


def test_keyword_model_repr():
    kw = Keyword(id=1, project_id=1, keyword="qualitative")
    assert "qualitative" in repr(kw)


def test_person_role_model_repr():
    pr = PersonRole(id=1, project_id=1, name="Smith, J.", role=PersonRoleType.AUTHOR)
    assert "Smith" in repr(pr)
    assert "AUTHOR" in repr(pr)


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


# --- get_storage_path tests ---


def test_storage_path_basic(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.storage.file_manager.DATA_DIR", tmp_path)
    path = get_storage_path("zenodo", "12345", "file.pdf")
    assert path.parent.name == "12345"
    assert path.name == "file.pdf"
    assert path.parent.exists()


def test_storage_path_with_version(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.storage.file_manager.DATA_DIR", tmp_path)
    path = get_storage_path("zenodo", "12345", "file.pdf", version_folder="v1")
    assert "v1" in str(path)
    assert path.name == "file.pdf"


def test_storage_path_collision(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.storage.file_manager.DATA_DIR", tmp_path)
    path1 = get_storage_path("zenodo", "12345", "file.pdf")
    path1.write_text("first")
    path2 = get_storage_path("zenodo", "12345", "file.pdf")
    assert path2.name == "file_2.pdf"


# --- normalize_language tests ---


def test_normalize_language_english():
    assert normalize_language("English") == "en"


def test_normalize_language_code():
    assert normalize_language("en") == "en"


def test_normalize_language_empty():
    assert normalize_language("") is None
    assert normalize_language(None) is None
