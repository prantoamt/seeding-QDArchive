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
    authors: str = ""
    license_type: str = ""
    license_url: str = ""
    date_published: str = ""
    tags: list[str] = field(default_factory=list)
    files: list[dict] = field(default_factory=list)


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
    def download(self, url: str, dest_dir: str) -> str:
        """Download a file and return the local path."""
