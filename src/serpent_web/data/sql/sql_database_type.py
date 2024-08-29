from enum import Enum


class DatabaseType(Enum):
    SQLITE = "sqlite"
    DATABRICKS = "databricks"
    POSTGRES = "postgres"
    AZURESQL = "azuresql"
