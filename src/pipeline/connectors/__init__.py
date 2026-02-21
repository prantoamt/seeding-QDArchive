"""Connector registry — maps source names to connector instances."""

from pipeline.connectors.base import BaseConnector, SearchResult
from pipeline.connectors.dataverse import DataverseConnector
from pipeline.connectors.dryad import DryadConnector
from pipeline.connectors.qualidatanet import QualidataNetConnector
from pipeline.connectors.ukds import UKDataServiceConnector
from pipeline.connectors.zenodo import ZenodoConnector

CONNECTORS: dict[str, BaseConnector] = {
    "qdr": DataverseConnector("https://data.qdr.syr.edu", "qdr"),
    "dans": DataverseConnector("https://ssh.datastations.nl", "dans"),
    "dataverseno": DataverseConnector("https://dataverse.no", "dataverseno"),
    "harvard": DataverseConnector("https://dataverse.harvard.edu", "harvard"),
    "zenodo": ZenodoConnector(),
    "dryad": DryadConnector(),
    "ukds": UKDataServiceConnector(),
    "qualidatanet": QualidataNetConnector(),
}

__all__ = [
    "CONNECTORS", "BaseConnector", "DataverseConnector",
    "DryadConnector", "QualidataNetConnector", "SearchResult",
    "UKDataServiceConnector", "ZenodoConnector",
]
