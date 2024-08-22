from azure.cosmos import CosmosClient, DatabaseProxy, ContainerProxy
from typing import Optional, Any
from azure.identity import DefaultAzureCredential

from src.api.shared.settings import CosmosSettingsModel

class CosmosClientContext:
    _settings: CosmosSettingsModel
    _client: CosmosClient
    _database: DatabaseProxy
    _container: ContainerProxy

    def __init__(self, settings: CosmosSettingsModel, client: CosmosClient = None ):
        self._settings = settings
        self._client = client
    
    def _get_client(self) -> CosmosClient:
        if self._client is None:
            if self._settings.use_rbac:
                rbac_credentials = DefaultAzureCredential()
                self._client = CosmosClient(url = self._settings.account_uri, credential=rbac_credentials)
            else:
                self._client = CosmosClient(url = self._settings.account_uri, credential= self._settings.account_key)

        return self._client
    
    def _get_container(self, container_name: str) -> ContainerProxy:
        client = self._get_client()
        database = client.get_database_client(database=self._settings.database_name)
        container = database.get_container_client(container=container_name)
        return container
    
    def _get_database(self) -> DatabaseProxy:
        if self._database is None:
            self._database = self._get_client().get_database_client(database=self._settings.database_name)

        return self._database

    def delete_item(self, item_id: str, partition_key_val: str, container_name: str):
        container = self._get_container(container_name)
        container.delete_item(item = item_id, partition_key=partition_key_val)
    
    def add_item(self, item: dict, container_name: str):
        """
            Insert the item in Cosmos DB.
        """
        container = self._get_container(container_name)
        container.create_item(item)


    def update_item(self, item: dict, container_name: str):
        """
            Update the item in Cosmos DB.
        """
        container = self._get_container(container_name)
        container.upsert_item(item)
        

    
    def get_item(self, item_id: str, partition_key_val: str, container_name: str) -> dict[str, Any]:
        container = self._get_container(container_name)
        response = container.read_item(item=item_id, partition_key=partition_key_val)
        return response
    
    def query_items(
            self,
            container_name: str,
            query: str,
            partition_key_val: str,
            parameters: Optional[list[dict[str, object]]] = None,
            max_item_count: int = -1,
            token: str = None ) -> list[dict[str, Any]]:
        
        if not container_name:
            raise ValueError('container_name must be specified')
        if not partition_key_val:
            raise ValueError('partition_key_val must be specified')
        if not query:
            raise ValueError('query must be specified')
        
        container = self._get_container(container_name)
        query_iterable = container.query_items(
            query=query,
            partition_key=partition_key_val,
            parameters=parameters,
            max_item_count=max_item_count)
        
        items = list(query_iterable)
        
        return items
