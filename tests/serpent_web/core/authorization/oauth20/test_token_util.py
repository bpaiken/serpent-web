import pytest
import requests
from jose import jwt
from starlette.exceptions import HTTPException

from serpent_web.core.authentication.oauth20.token_util import get_jwks_keys, get_claim_from_payload, verify_token


@pytest.mark.unit
def test_get_jwks_keys_should_return_the_public_jwks_keys(monkeypatch):
    # arrange
    scope = DefaultScope()
    def mock_get(_url):
        return TestResponse(200, scope.test_jwks)

    monkeypatch.setattr(requests, "get", mock_get)

    expected_result = {
        "test_key_id": {
            "kid": "test_key_id",
            "kty": "RSA",
            "alg": "RS256",
            "use": "sig",
            "n": "test_n",
            "e": "test_e"
        }
    }

    # act
    result = get_jwks_keys(scope.jwks_url)

    # assert
    assert result == expected_result

@pytest.mark.unit
def test_get_jwks_keys_should_raise_exception_when_response_is_not_successful(monkeypatch):
    # arrange
    scope = DefaultScope()
    def bad_get(_url):
        raise requests.RequestException("Network error")

    monkeypatch.setattr(requests, "get", bad_get)

    # act
    with pytest.raises(HTTPException) as exc_info:
        get_jwks_keys(scope.jwks_url)

    # assert
    assert exc_info.value.status_code == 500


@pytest.mark.unit
def test_get_claim_from_payload_should_return_the_claim_value_for_a_given_json_path():
    # arrange
    payload = {
        "claims": {
            "email": "foo@bar.com",
        }
    }

    claim_json_path = "claims.email"

    # act
    result = get_claim_from_payload(payload, claim_json_path)

    # assert
    assert result == "foo@bar.com"

@pytest.mark.unit
def test_get_claim_from_payload_should_return_none_when_claim_not_found():
    # arrange
    payload = {}

    # act
    result = get_claim_from_payload(payload, "claims.email")

    # assert
    assert result is None


# ----- verify_token ------------------------------------------------
def test_verify_token_should_return_data(monkeypatch):
    # arrange
    scope = DefaultScope()
    token = scope.token
    jwks_url = "https://example.com/jwks"
    audience = scope.audience
    expected_data = scope.token_payload

    def mock_get_public_key(token, jwks_url):
        return "public_key"

    def mock_jwt_decode(token, public_key, algorithms, audience):
        return expected_data

    monkeypatch.setattr("serpent_web.core.authentication.oauth20.token_util.get_public_key", mock_get_public_key)
    monkeypatch.setattr("serpent_web.core.authentication.oauth20.token_util.jwt.decode", mock_jwt_decode)

    # act
    result = verify_token(token, jwks_url, audience)

    # assert
    assert result == expected_data


def test_verify_token_should_raise_exception_when_token_is_missing():
    # arrange
    scope = DefaultScope()
    token = ""
    jwks_url = scope.jwks_url
    audience = scope.audience

    # act & assert
    with pytest.raises(HTTPException) as exc_info:
        verify_token(token, jwks_url, audience)

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == "Access Token is missing"

def test_verify_token_should_raise_exception_when_claims_verification_fails(monkeypatch):
    # arrange
    scope= DefaultScope()
    token = "invalid.token.value"
    jwks_url = scope.jwks_url
    audience = scope.audience

    def mock_get_public_key(token, jwks_url):
        return "public_key"

    def mock_jwt_decode(token, public_key, algorithms, audience):
        raise jwt.JWTClaimsError

    monkeypatch.setattr("serpent_web.core.authentication.oauth20.token_util.get_public_key", mock_get_public_key)
    monkeypatch.setattr("serpent_web.core.authentication.oauth20.token_util.jwt.decode", mock_jwt_decode)

    # act & assert
    with pytest.raises(HTTPException) as exc_info:
        verify_token(token, jwks_url, audience)

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == "Claims verification failed."

def test_verify_token_should_raise_exception_when_token_is_invalid(monkeypatch):
    # arrange
    scope = DefaultScope()
    token = "invalid.token.value"
    jwks_url = scope.jwks_url
    audience = scope.audience

    def mock_get_public_key(token, jwks_url):
        return "public_key"

    def mock_jwt_decode(token, public_key, algorithms, audience):
        raise Exception("Invalid token")

    monkeypatch.setattr("serpent_web.core.authentication.oauth20.token_util.get_public_key", mock_get_public_key)
    monkeypatch.setattr("serpent_web.core.authentication.oauth20.token_util.jwt.decode", mock_jwt_decode)

    # act & assert
    with pytest.raises(HTTPException) as exc_info:
        verify_token(token, jwks_url, audience)

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == "Token is invalid."

class DefaultScope:
    def __init__(self):
        # clear get_jwks_keys cache
        get_jwks_keys.cache_clear()

        self.jwks_url = "https://example.com/jwks"
        self.client_id = "test_client_id"
        self.token = "fake.token.value"
        self.token_payload = {"sub": "1234567890", "name": "John Doe", "iat": 1516239022}
        self.audience= "test_audience"
        self.test_jwks = {
            "keys": [
                {
                    "kid": "test_key_id",
                    "kty": "RSA",
                    "alg": "RS256",
                    "use": "sig",
                    "n": "test_n",
                    "e": "test_e"
                }
            ]
        }


class TestResponse:
    def __init__(self, status_code, json_data):
        self.status_code = status_code
        self.json_data = json_data

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.HTTPError(f"Status code: {self.status_code}")

    def json(self):
        return self.json_data
