"""What Fabric offers for each kind of file store an ADF dataset can sit on, from Microsoft's pages (read 2026-10-26).

Used by the storage map, the dependency inventory and the reports, so a dataset on S3, SFTP or a file share is never just "not mapped":
it says whether a OneLake shortcut exists for it, whether Copy can read or write it, whether a gateway is needed, and what to do.

Sources: OneLake shortcuts <https://learn.microsoft.com/fabric/onelake/onelake-shortcuts>, shortcuts to on-premises data
<https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut>, Fabric connector overview
<https://learn.microsoft.com/fabric/data-factory/connector-overview>, Folder connector
<https://learn.microsoft.com/fabric/data-factory/connector-folder-overview>.
Only ADLS Gen2 shortcuts are created by this project (live-verified). Everything else here is advice, never an action.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

VERIFIED_ON = "2026-10-26"
SOURCES = (
    "https://learn.microsoft.com/fabric/onelake/onelake-shortcuts",
    "https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut",
    "https://learn.microsoft.com/fabric/data-factory/connector-overview",
)


@dataclass(frozen=True)
class Store:
    kind: str
    label: str
    shortcut: str  # yes | gateway | no   (gateway: a shortcut exists, and a gateway reaches a firewalled or on-premises endpoint)
    copy: str  # read_write | read_only | none  (Fabric pipeline Copy activity)
    gateway: str  # a note when Fabric reaches it through an on-premises data gateway, else ""
    alternatives: tuple[str, ...]

    def summary(self) -> str:
        sc = {"yes": "a OneLake shortcut exists", "gateway": "a OneLake shortcut exists (a gateway reaches a firewalled or on-premises endpoint)",
              "no": "there is no OneLake shortcut"}[self.shortcut]
        cp = {"read_write": "Copy can read and write it", "read_only": "Copy can read it but not write it", "none": "Copy has no connector for it"}[self.copy]
        return f"{self.label}: {sc}; {cp}."


_GW = "A private or on-premises endpoint is reached through an on-premises data gateway."
STORES: dict[str, Store] = {s.kind: s for s in (
    Store("adls_gen2", "Azure Data Lake Storage Gen2", "yes", "read_write", "", (
        "Mapped by this tool: a OneLake shortcut (`create-shortcut`), then `convert --lakehouse` to read it through the lakehouse.",)),
    Store("blob", "Azure Blob Storage", "yes", "read_write", "", (
        "Create a OneLake shortcut to the Blob container in Fabric (this tool does not create Blob shortcuts yet), then read it through the lakehouse.",
        "Or keep the Copy activity on its Azure Blob connection (it converts as it is).",
        "Or copy the files into the lakehouse Files area with a Copy activity.")),
    Store("s3", "Amazon S3", "gateway", "read_write", _GW, (
        "Create an Amazon S3 shortcut in Fabric (a connection with AWS keys, or a Microsoft Entra service principal); shortcut caching can cut egress cost.",
        "For a bucket behind a firewall or VPC, select an on-premises data gateway when creating the shortcut.",
        "Or keep the Copy activity on its Amazon S3 connection, or copy the files into the lakehouse Files area.")),
    Store("s3_compatible", "Amazon S3 compatible storage", "gateway", "read_write", _GW, (
        "Create an S3-compatible shortcut in Fabric; for an on-premises endpoint select an on-premises data gateway.",
        "Or keep the Copy activity on its Amazon S3 Compatible connection, or copy the files into the lakehouse Files area.")),
    Store("gcs", "Google Cloud Storage", "gateway", "read_write", _GW, (
        "Create a Google Cloud Storage shortcut in Fabric; for a bucket behind a firewall select an on-premises data gateway.",
        "Or keep the Copy activity on its Google Cloud Storage connection, or copy the files into the lakehouse Files area.")),
    Store("azure_files", "Azure Files", "no", "read_write", "", (
        "Keep the Copy activity on its Azure Files connection and land the files in the lakehouse Files area.",
        "Or move the share's data to ADLS Gen2 or Blob Storage, then use a shortcut.")),
    Store("sftp", "SFTP", "no", "read_write", _GW, (
        "Keep the Copy activity on its SFTP connection and land the files in the lakehouse Files area on the pipeline's schedule.",
        "A server that is not reachable from the internet needs an on-premises data gateway.")),
    Store("ftp", "FTP", "no", "read_only", _GW, (
        "Keep the Copy activity on its FTP connection as a source and land the files in the lakehouse Files area (Fabric Copy cannot write to FTP).",
        "Consider moving the exchange to SFTP.")),
    Store("file_server", "File server / network share", "no", "read_write", "The Folder connector runs through an on-premises data gateway with Windows authentication.", (
        "Install an on-premises data gateway that can reach the share and keep the Copy activity on the Folder connection, landing the files in the lakehouse Files area.",
        "Or move the data to ADLS Gen2 or Blob Storage first, then use a shortcut.")),
    Store("hdfs", "Hadoop Distributed File System (HDFS)", "no", "read_only", _GW, (
        "Keep the Copy activity on its HDFS connection as a source and land the files in the lakehouse Files area (Fabric Copy cannot write to HDFS).",
        "Or move the data to ADLS Gen2 first, then use a shortcut.")),
    Store("http", "HTTP server", "no", "read_only", "", (
        "Keep the Copy activity on its HTTP connection as a source and land the files in the lakehouse Files area.",)),
    Store("oracle_cloud_storage", "Oracle Cloud Storage", "no", "read_only", "", (
        "Keep the Copy activity on its Oracle Cloud Storage connection as a source and land the files in the lakehouse Files area.",
        "Or move the data to an S3-compatible endpoint or ADLS Gen2, then use a shortcut.")),
    Store("adls_gen1", "Azure Data Lake Storage Gen1", "no", "none", "", (
        "Gen1 is retired and Fabric has no connector for it: move the data to ADLS Gen2 first, then use a shortcut.",)),
)}

# ADF file-location type -> store kind
LOCATIONS: dict[str, str] = {
    "AzureBlobFSLocation": "adls_gen2", "AzureBlobStorageLocation": "blob", "AmazonS3Location": "s3", "AmazonS3CompatibleLocation": "s3_compatible",
    "GoogleCloudStorageLocation": "gcs", "AzureFileStorageLocation": "azure_files", "SftpLocation": "sftp", "FtpServerLocation": "ftp",
    "FileServerLocation": "file_server", "HdfsLocation": "hdfs", "HttpServerLocation": "http", "OracleCloudStorageLocation": "oracle_cloud_storage",
    "AzureDataLakeStoreLocation": "adls_gen1",
}


def for_location(location_type: object) -> Optional[Store]:
    kind = LOCATIONS.get(str(location_type))
    return STORES.get(kind) if kind else None
