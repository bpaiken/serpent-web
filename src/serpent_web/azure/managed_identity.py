import requests

from serpent_web.azure.azure_service_identity import get_access_token


# def get_identity_request_session():
#     session = requests.Session()
#
#     session.headers.update({
#         "Content-Type": "application/json"
#     })
#
#     access_token =
#

class RequestSessionService:
    def __init__(self, scope: str, bypass_token: bool = False, custom_headers: dict = None):
        self.scope = scope
        self.bypass_token = bypass_token
        self.session = requests.Session()
        self._set_headers(custom_headers)

    def _set_headers(self, custom_headers: dict | None):
        custom_headers = custom_headers or {}
        if custom_headers.get("Content-Type", False):
            self.session.headers.update({
                "Content-Type": "application/json"
            })

        if not self.bypass_token:
            access_token = get_access_token(self.scope)
            self.session.headers.update({
                "Authorization": f"Bearer {access_token}"
            })
