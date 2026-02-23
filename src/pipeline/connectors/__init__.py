"""Connector registry — maps source names to connector instances."""

from pipeline.connectors.base import BaseConnector, SearchResult
from pipeline.connectors.dataverse import DataverseConnector
from pipeline.connectors.dryad import DryadConnector
from pipeline.connectors.ukds import UKDataServiceConnector
from pipeline.connectors.zenodo import ZenodoConnector

CONNECTORS: dict[str, BaseConnector] = {
    "qdr": DataverseConnector("https://data.qdr.syr.edu", "qdr"),
    "dans": DataverseConnector("https://ssh.datastations.nl", "dans"),
    "dataverseno": DataverseConnector("https://dataverse.no", "dataverseno"),
    "harvard": DataverseConnector("https://dataverse.harvard.edu", "harvard"),
    "sodha": DataverseConnector("https://www.sodha.be", "sodha"),
    "acss": DataverseConnector("https://dataverse.theacss.org", "acss"),
    "kuleuven": DataverseConnector("https://rdr.kuleuven.be", "kuleuven"),
    "uclouvain": DataverseConnector("https://dataverse.uclouvain.be", "uclouvain"),
    "repod": DataverseConnector("https://repod.icm.edu.pl", "repod"),
    "heidata": DataverseConnector("https://heidata.uni-heidelberg.de", "heidata"),
    "bonndata": DataverseConnector("https://bonndata.uni-bonn.de", "bonndata"),
    "dataverselv": DataverseConnector("https://dv.dataverse.lv", "dataverselv"),
    "crossda": DataverseConnector("https://data.crossda.hr", "crossda"),
    "darus": DataverseConnector("https://darus.uni-stuttgart.de", "darus"),
    "rsu": DataverseConnector("https://dataverse.rsu.lv", "rsu"),
    "uva": DataverseConnector("https://dataverse.lib.virginia.edu", "uva"),
    "nycu": DataverseConnector("https://dataverse.lib.nycu.edu.tw", "nycu"),
    "pucp": DataverseConnector("https://datos.pucp.edu.pe", "pucp"),
    "zenodo": ZenodoConnector(),
    "ukds": UKDataServiceConnector(),
    "dryad": DryadConnector(),
}

__all__ = [
    "CONNECTORS", "BaseConnector", "DataverseConnector", "DryadConnector",
    "SearchResult", "UKDataServiceConnector", "ZenodoConnector",
]
