"""Which ADF linked-service types have a documented Fabric counterpart.

Derived from Microsoft's Fabric Data Factory documentation (the data-store tables of the
Lookup, Script and Stored procedure activity articles, and the ADF-vs-Fabric comparison). The
verdicts are deliberately three-valued — a connector the documentation does not mention is
"unlisted", not "unsupported": absence from this table is a reason to VERIFY, not to refuse.
"""

from __future__ import annotations

from typing import Optional

# ADF linked-service ``type`` (lowercase) -> Fabric connector name, for connectors the Fabric
# documentation lists as data stores for pipeline activities.
SUPPORTED: dict[str, str] = {k.lower(): v for k, v in {
    "AzureBlobStorage": "Azure Blob Storage", "AzureBlobFS": "Azure Data Lake Storage Gen2",
    "AzureFileStorage": "Azure Files", "AzureTableStorage": "Azure Table Storage",
    "AzureSqlDatabase": "Azure SQL Database", "AzureSqlMI": "Azure SQL Managed Instance",
    "AzureSqlDW": "Azure Synapse Analytics", "SqlServer": "SQL Server database",
    "AmazonRdsForSqlServer": "Amazon RDS for SQL Server", "AmazonRdsForOracle": "Amazon RDS for Oracle",
    "Oracle": "Oracle database", "PostgreSql": "PostgreSQL database",
    "AzurePostgreSql": "Azure Database for PostgreSQL", "MySql": "MySQL database",
    "AzureMySql": "Azure Database for MySQL", "MariaDB": "MariaDB", "Snowflake": "Snowflake",
    "CosmosDb": "Azure Cosmos DB for NoSQL", "AzureDataExplorer": "Azure Data Explorer",
    "AzureDatabricksDeltaLake": "Azure Databricks", "AmazonRedshift": "Amazon Redshift", "Db2": "IBM Db2 database",
    "GoogleBigQuery": "Google BigQuery", "Greenplum": "Greenplum for Pipeline", "Informix": "Informix For Pipeline",
    "MicrosoftAccess": "Microsoft Access", "Presto": "Presto", "SapHana": "SAP HANA database",
    "SapTable": "SAP Table", "SapBw": "SAP BW Open Hub", "Teradata": "Teradata database", "Vertica": "Vertica",
    "AmazonS3": "Amazon S3", "AmazonS3Compatible": "Amazon S3 Compatible", "FileServer": "Folder",
    "FtpServer": "FTP", "Sftp": "SFTP", "GoogleCloudStorage": "Google Cloud Storage", "Hdfs": "Hdfs for Pipeline",
    "OracleCloudStorage": "Oracle Cloud Storage", "HttpServer": "HTTP", "OData": "OData", "Odbc": "ODBC",
    "RestService": "REST", "Cassandra": "Cassandra", "CommonDataServiceForApps": "Dataverse",
    "Dynamics": "Dynamics 365", "DynamicsAX": "Dynamics AX", "DynamicsCrm": "Dynamics CRM",
    "Salesforce": "Salesforce objects", "SalesforceServiceCloud": "Salesforce Service Cloud",
    "ServiceNow": "ServiceNow", "SharePointOnlineList": "SharePoint Online List",
}.items()}

# Known to have no Fabric counterpart (retired or not offered).
UNSUPPORTED: dict[str, str] = {k.lower(): v for k, v in {
    "AzureDataLakeStore": "Azure Data Lake Storage Gen1 is retired; migrate the data to ADLS Gen2 first",
    "AzureDataLakeAnalytics": "Azure Data Lake Analytics is retired",
}.items()}

# Stores the Stored procedure activity supports (activity article).
STORED_PROCEDURE_STORES = frozenset(s.lower() for s in (
    "AzureSqlDatabase", "AzureSqlMI", "AzureSqlDW", "SqlServer", "AmazonRdsForSqlServer"))

# Stores the Script activity supports (activity article).
SCRIPT_STORES = frozenset(s.lower() for s in (
    "AzureSqlDatabase", "AzureSqlMI", "AzureSqlDW", "SqlServer", "AzurePostgreSql", "Oracle", "Snowflake"))


def verdict(ls_type: Optional[str]) -> tuple[str, str]:
    """('supported'|'unlisted'|'unsupported', explanation)."""
    key = (ls_type or "").lower()
    if not key:
        return "unlisted", "the linked service type is unknown"
    if key in UNSUPPORTED:
        return "unsupported", UNSUPPORTED[key]
    if key in SUPPORTED:
        return "supported", SUPPORTED[key]
    return "unlisted", f"'{ls_type}' is not named in the Fabric connector documentation consulted"
