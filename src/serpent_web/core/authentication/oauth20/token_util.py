import logging

from cachetools import TTLCache, cached
import requests
from jose import jwt
from starlette.exceptions import HTTPException
from jsonpath_ng import parse

_logger = logging.getLogger(__name__)

RSAA = "RS256"

# Cache for JWKS keys
cache = TTLCache(maxsize=100, ttl=600)


@cached(cache)
def get_jwks_keys(jwks_url):
    """Fetch JWKS keys from the JWKS endpoint."""
    try:
        _logger.info(f"fetching JWKS keys for cache from: {jwks_url}")
        resp = requests.get(jwks_url)
        resp.raise_for_status()
        jwks = resp.json()
        return {key["kid"]: key for key in jwks["keys"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch JWKS keys.") from e


def get_public_key(token, jwks_url):
    """Retrieve the public key for a given JWT token from the JWKS endpoint."""
    try:
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")
        keys = get_jwks_keys(jwks_url)
        if kid not in keys:
            raise HTTPException(status_code=403, detail="Public key not found for the given token.")
        return keys.get(kid)
    except jwt.JWTError as e:
        raise HTTPException(status_code=403, detail="Invalid token headers.") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error processing token.") from e


def verify_token(token, jwks_url: str, audience: str):
    """Verify the JWT token."""
    if not token:
        raise HTTPException(status_code=403, detail="Access Token is missing")

    try:
        public_key = get_public_key(token=token, jwks_url=jwks_url)
        data = jwt.decode(token, public_key, algorithms=[RSAA], audience=audience)
        # Token is valid

        return data

    except jwt.JWTClaimsError:
        raise HTTPException(status_code=403, detail="Claims verification failed.")
    except jwt.ExpiredSignatureError:
        new_token = refresh_token(token)
        return verify_token(token=new_token, jwks_url=jwks_url, audience=audience)
    except Exception as e:
        raise HTTPException(status_code=403, detail="Token is invalid.") from e


def get_claim_from_payload(payload: dict, claim_json_path: str) -> str | None:
    """
    Extract a specified claim from a JWT payload using JSONPath.

    :param payload: The JWT payload (decoded token).
    :param claim_json_path: The JSONPath string specifying the claim to extract. Ex: "uid" or for a nested claim "user.name"
    :return: The extracted claim value.
    """
    jsonpath_expression = parse(claim_json_path)

    matches = jsonpath_expression.find(payload)
    return matches[0].value


# TODO - implement refresh for oauth 2.0
def refresh_token(expired_token):
    """Refresh an access token using a valid refresh token."""
    try:
        user_id = "extracted_user_id"  # Extract the user identity from the refresh token
        new_access_token = "newly_issued_access_token"  # Issue a new access token using your existing logic

        return {"access_token": new_access_token}
    except Exception as e:
        raise HTTPException(status_code=403, detail="Token has expired") from e