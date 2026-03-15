"""Abstract base class for data source connectors."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class SearchResult:
    """A single search result from a data source."""

    source_name: str
    source_url: str
    title: str
    description: str = ""

    # Persons with roles (replaces flat 'authors' string)
    persons: list[dict] = field(default_factory=list)
    # Each dict: {"name": "Last, First", "role": "AUTHOR"|"CONTRIBUTOR"|"EDITOR"|"UNKNOWN"}

    # Identifiers
    doi: str = ""
    version: str = ""
    project_id_on_source: str = ""  # ID as it appears on the source website

    # License
    license_type: str = ""
    license_url: str = ""

    # Dates
    date_published: str = ""

    # Lists that map to child tables
    keywords: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)

    # Extended metadata (kept as extra columns)
    kind_of_data: list[str] = field(default_factory=list)
    language: list[str] = field(default_factory=list)
    software: list[str] = field(default_factory=list)
    geographic_coverage: list[str] = field(default_factory=list)
    depositor: str = ""
    producer: list[str] = field(default_factory=list)
    publication: list[str] = field(default_factory=list)
    date_of_collection: str = ""
    time_period_covered: str = ""

    # Files
    files: list[dict] = field(default_factory=list)

    # Download info
    download_method: str = "API-CALL"


class BaseConnector(ABC):
    """Interface that every data source connector must implement."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name of the data source."""

    @abstractmethod
    def search(self, query: str, file_type: str | None = None) -> list[SearchResult]:
        """Search the data source and return matching records."""

    @abstractmethod
    def get_metadata(self, record_url: str) -> SearchResult:
        """Fetch full metadata for a specific record."""

    @abstractmethod
    def download(self, url: str, dest_dir: str, filename: str | None = None) -> str:
        """Download a file and return the local path."""
