"""SQLAlchemy models for the pipeline metadata database."""

import enum
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class DownloadMethod(str, enum.Enum):
    API_CALL = "API-CALL"
    SCRAPING = "SCRAPING"


class PersonRoleType(str, enum.Enum):
    AUTHOR = "AUTHOR"
    CONTRIBUTOR = "CONTRIBUTOR"
    EDITOR = "EDITOR"
    UNKNOWN = "UNKNOWN"


class Project(Base):
    """A dataset/record from a repository (one project has many files)."""

    __tablename__ = "projects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Required by course schema
    query_string: Mapped[str | None] = mapped_column(Text)
    repository_id: Mapped[int] = mapped_column(Integer, nullable=False)
    repository_url: Mapped[str] = mapped_column(Text, nullable=False)
    project_url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    version: Mapped[str | None] = mapped_column(String(100))
    title: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    language: Mapped[str | None] = mapped_column(String(10))
    doi: Mapped[str | None] = mapped_column(Text)
    upload_date: Mapped[str | None] = mapped_column(String(50))
    download_date: Mapped[datetime | None] = mapped_column(DateTime)
    download_repository_folder: Mapped[str] = mapped_column(String(100), nullable=False)
    download_project_folder: Mapped[str] = mapped_column(String(500), nullable=False)
    download_version_folder: Mapped[str | None] = mapped_column(String(100))
    download_method: Mapped[DownloadMethod] = mapped_column(
        Enum(DownloadMethod), nullable=False, default=DownloadMethod.API_CALL
    )

    # Extra columns (beyond course schema)
    license_type: Mapped[str | None] = mapped_column(String(100))
    license_url: Mapped[str | None] = mapped_column(Text)
    tags: Mapped[str | None] = mapped_column(Text)
    kind_of_data: Mapped[str | None] = mapped_column(Text)
    software: Mapped[str | None] = mapped_column(Text)
    geographic_coverage: Mapped[str | None] = mapped_column(Text)
    restricted: Mapped[bool | None] = mapped_column(Boolean)
    depositor: Mapped[str | None] = mapped_column(Text)
    producer: Mapped[str | None] = mapped_column(Text)
    publication: Mapped[str | None] = mapped_column(Text)
    date_of_collection: Mapped[str | None] = mapped_column(Text)
    time_period_covered: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    files: Mapped[list["ProjectFile"]] = relationship(
        back_populates="project", cascade="all, delete-orphan"
    )
    keywords: Mapped[list["Keyword"]] = relationship(
        back_populates="project", cascade="all, delete-orphan"
    )
    persons: Mapped[list["PersonRole"]] = relationship(
        back_populates="project", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Project(id={self.id}, title='{self.title[:40]}')>"


class ProjectFile(Base):
    """A single file within a project."""

    __tablename__ = "files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(Integer, ForeignKey("projects.id"), nullable=False)
    file_name: Mapped[str] = mapped_column(String(500), nullable=False)
    file_type: Mapped[str] = mapped_column(String(50), nullable=False)

    # Extra columns (beyond course schema)
    file_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    file_size_bytes: Mapped[int | None] = mapped_column(Integer)
    content_type: Mapped[str | None] = mapped_column(String(200))
    friendly_type: Mapped[str | None] = mapped_column(String(200))
    api_checksum: Mapped[str | None] = mapped_column(String(150))
    is_qda_file: Mapped[bool] = mapped_column(Boolean, default=False)
    download_url: Mapped[str | None] = mapped_column(Text)
    local_path: Mapped[str | None] = mapped_column(Text)
    downloaded_at: Mapped[datetime | None] = mapped_column(DateTime)

    # Relationship
    project: Mapped["Project"] = relationship(back_populates="files")

    def __repr__(self) -> str:
        return f"<ProjectFile(id={self.id}, name='{self.file_name}')>"


class Keyword(Base):
    """A keyword associated with a project."""

    __tablename__ = "keywords"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(Integer, ForeignKey("projects.id"), nullable=False)
    keyword: Mapped[str] = mapped_column(String(500), nullable=False)

    # Relationship
    project: Mapped["Project"] = relationship(back_populates="keywords")

    def __repr__(self) -> str:
        return f"<Keyword(id={self.id}, keyword='{self.keyword}')>"


class PersonRole(Base):
    """A person associated with a project and their role."""

    __tablename__ = "person_roles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(Integer, ForeignKey("projects.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    role: Mapped[PersonRoleType] = mapped_column(
        Enum(PersonRoleType), nullable=False, default=PersonRoleType.UNKNOWN
    )

    # Relationship
    project: Mapped["Project"] = relationship(back_populates="persons")

    def __repr__(self) -> str:
        return f"<PersonRole(id={self.id}, name='{self.name}', role={self.role})>"
