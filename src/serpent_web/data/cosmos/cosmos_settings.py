class CosmosSettings:
    """
    Represents settings used to configure Azure Cosmos DB Client
    """
    account_uri: str
    account_key: str
    database_name: str
    container_name: str
    use_rbac: bool = True
