"""Connector registry — maps source names to connector instances."""

from pipeline.connectors.base import BaseConnector, SearchResult
from pipeline.connectors.dataverse import DataverseConnector

CONNECTORS: dict[str, BaseConnector] = {
    "qdr": DataverseConnector("https://data.qdr.syr.edu", "qdr"),
    "dans": DataverseConnector("https://ssh.datastations.nl", "dans"),
    "dataverseno": DataverseConnector("https://dataverse.no", "dataverseno"),
}

__all__ = ["CONNECTORS", "BaseConnector", "DataverseConnector", "SearchResult"]
