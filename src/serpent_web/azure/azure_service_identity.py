import logging

from cachetools import TTLCache, cached
from azure.identity import DefaultAzureCredential

from src.api.settings import EnvironmentSettings

_logger = logging.getLogger()
environment_settings = EnvironmentSettings()

# 5 minute cache
cache = TTLCache(maxsize=100, ttl=300)


@cached(cache)
def get_access_token(scope: str) -> str:
    try:
        _logger.info(
            f"Requesting managed identity access for scope: {scope} and with identity: {environment_settings.azure_client_id}")
        credential = DefaultAzureCredential()
        token = credential.get_token(scope)
        access_token = token.token
        _logger.info(f"Managed identity access token received")
        return access_token
    except Exception as e:
        _logger.exception("error retrieving managed identity access token", exc_info=e)
        raise e
