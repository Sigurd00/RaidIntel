from __future__ import annotations
import time, logging
from typing import Any, Dict, Optional
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

log = logging.getLogger(__name__)
OAUTH_URL = "https://www.warcraftlogs.com/oauth/token"

class WCLClient:
    """Client for Warcraft Logs v2 GraphQL"""

    def __init__(self, site: str, client_id: str, client_secret: str, timeout: int = 90) -> None:
        self.base_gql = f"https://{site}.warcraftlogs.com/api/v2/client"
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._session = requests.Session()

    def _need_token(self) -> bool:
        return not self._token or time.time() >= self._token_expiry

    def _refresh_token(self) -> None:
        r = self._session.post(
            OAUTH_URL,
            data={"grant_type":"client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=self.timeout
        )
        r.raise_for_status()
        data = r.json()
        self._token = data["access_token"]
        self._token_expiry = time.time() + float(data.get("expires_in", 3600)) * 0.9
        log.debug("Obtained new OAuth token; expires in ~%ss", data.get("expires_in"))

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
    )
    def gql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        if self._need_token():
            self._refresh_token()
        headers = {"Authorization": f"Bearer {self._token}", "Accept":"application/json", "Content-Type":"application/json"}
        resp = self._session.post(self.base_gql, json={"query": query, "variables": variables}, timeout=self.timeout, headers=headers)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            log.warning("HTTP error %s: %s", resp.status_code, getattr(e, "response", None))
            raise
        payload = resp.json()
        if "errors" in payload:
            raise RuntimeError(f"GraphQL error: {payload['errors']}")
        return payload["data"]
