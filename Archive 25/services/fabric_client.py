import os
import threading
import webbrowser
import requests
import msal
from flask import Flask, request as flask_request
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

FABRIC_SCOPE = ["https://api.fabric.microsoft.com/.default"]
REDIRECT_URI = "http://localhost:5000/getAToken"
BASE = "https://api.fabric.microsoft.com/v1"


class FabricAuthError(Exception):
    pass


class FabricAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")


class InteractiveAuth:
    def __init__(
        self,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        self.tenant_id = tenant_id or os.environ["FABRIC_TENANT_ID"]
        self.client_id = client_id or os.environ["FABRIC_CLIENT_ID"]
        self.client_secret = client_secret or os.environ.get("FABRIC_CLIENT_SECRET")
        self._authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self._app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=self._authority,
        )
        self._auth_code: Optional[str] = None
        self._flask_app = Flask(__name__)
        self._flask_app.secret_key = os.urandom(24)

    def _start_flask(self) -> None:
        @self._flask_app.route("/getAToken")
        def get_token():
            code = flask_request.args.get("code")
            error = flask_request.args.get("error")
            if error:
                return f"<h3>Login failed: {error}</h3>", 400
            if not code:
                return "<h3>No auth code received.</h3>", 400
            self._auth_code = code
            shutdown = flask_request.environ.get("werkzeug.server.shutdown")
            if shutdown:
                shutdown()
            return "<h3>Login successful. You can close this tab and return to your terminal.</h3>"

        self._flask_app.run(port=5000, use_reloader=False, threaded=True)

    def _launch_browser_flow(self) -> None:
        auth_url = self._app.get_authorization_request_url(
            scopes=FABRIC_SCOPE,
            redirect_uri=REDIRECT_URI,
        )
        t = threading.Thread(target=self._start_flask, daemon=True)
        t.start()
        print(f"\nOpening browser for Microsoft login...\n{auth_url}\n", flush=True)
        webbrowser.open(auth_url)
        t.join(timeout=120)
        if not self._auth_code:
            raise FabricAuthError("Timed out waiting for browser login.")

    def get_token(self) -> str:
        accounts = self._app.get_accounts()
        if accounts:
            result = self._app.acquire_token_silent(FABRIC_SCOPE, account=accounts[0])
            if result and "access_token" in result:
                return result["access_token"]

        self._launch_browser_flow()

        result = self._app.acquire_token_by_authorization_code(
            code=self._auth_code,
            scopes=FABRIC_SCOPE,
            redirect_uri=REDIRECT_URI,
        )
        if "access_token" not in result:
            error = result.get("error", "unknown")
            desc = result.get("error_description", "")
            raise FabricAuthError(f"Token exchange failed [{error}]: {desc}")
        return result["access_token"]

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json",
        }


class FabricClient:
    def __init__(self, auth: Optional[InteractiveAuth] = None):
        self.auth = auth or InteractiveAuth()
        self._session = requests.Session()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", 60)
        headers = self.auth.headers
        print(f">>> {method} {url}", flush=True)
        resp = self._session.request(method, url, headers=headers, **kwargs)
        print(f"<<< {resp.status_code} {url}", flush=True)
        if not resp.ok:
            print(f"    ERROR BODY: {resp.text}", flush=True)
            raise FabricAPIError(resp.status_code, resp.text)
        return resp

    # ---------------------------------------------------------------- pipelines

    def get_pipeline_definition(self, workspace_id: str, pipeline_id: str) -> dict:
        url = f"{BASE}/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/getDefinition"
        return self._request("POST", url).json()

    def get_pipeline(self, workspace_id: str, pipeline_id: str) -> dict:
        url = f"{BASE}/workspaces/{workspace_id}/dataPipelines/{pipeline_id}"
        return self._request("GET", url).json()

    def create_pipeline(self, workspace_id: str, display_name: str, definition: dict) -> dict:
        url = f"{BASE}/workspaces/{workspace_id}/items"
        body = {
            "displayName": display_name,
            "type": "DataPipeline",
            "definition": definition,
        }
        resp = self._request("POST", url, json=body)
        return resp.json() if resp.content else {}

    def update_pipeline_definition(self, workspace_id: str, item_id: str, definition: dict) -> dict:
        url = f"{BASE}/workspaces/{workspace_id}/items/{item_id}/updateDefinition"
        resp = self._request("POST", url, json={"definition": definition})
        return resp.json() if resp.content else {}

    # ---------------------------------------------------------------- connections

    def create_rest_connection(
        self,
        display_name: str,
        base_url: str,
        privacy_level: str = "Organizational",
        skip_test: bool = True,
    ) -> dict:
        """
        Create a real Fabric ShareableCloud REST (Web) connection.

        Uses:
          connectionDetails.type       = "Web"
          connectionDetails.creationMethod = "Web"
          credentialType               = "Anonymous"

        Returns the full connection object including the UUID `id` field.

        Requires the delegated scope: Connection.ReadWrite.All
        The signed-in user must consent to this scope on first run.
        """
        url = f"{BASE}/connections"
        body = {
            "connectivityType": "ShareableCloud",
            "displayName": display_name,
            "connectionDetails": {
                "type": "Web",
                "creationMethod": "Web",
                "parameters": [
                    {
                        "dataType": "Text",
                        "name": "url",
                        "value": base_url,
                    }
                ],
            },
            "privacyLevel": privacy_level,
            "credentialDetails": {
                "singleSignOnType": "None",
                "connectionEncryption": "NotEncrypted",
                "skipTestConnection": skip_test,
                "credentials": {
                    "credentialType": "Anonymous",
                },
            },
        }
        resp = self._request("POST", url, json=body)
        return resp.json()