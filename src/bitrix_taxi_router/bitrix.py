from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib import error, parse, request

from .contracts import PortalAuth


class BitrixApiError(RuntimeError):
    pass


@dataclass
class TokenRefreshResult:
    access_token: str
    refresh_token: str
    expires_at: datetime | None
    client_endpoint: str | None
    server_endpoint: str | None
    status: str | None


class BitrixClient:
    def __init__(
        self,
        auth: PortalAuth,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        self.auth = auth
        self.client_id = client_id
        self.client_secret = client_secret

    def call(self, method_name: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            return self._call_once(method_name, params or {})
        except BitrixApiError as exc:
            error_text = str(exc).lower()
            if "expired_token" not in error_text and "invalid_token" not in error_text:
                raise
            if not (self.auth.refresh_token and self.client_id and self.client_secret):
                raise
            refreshed = self.refresh_tokens()
            self.auth.access_token = refreshed.access_token
            self.auth.refresh_token = refreshed.refresh_token
            self.auth.client_endpoint = refreshed.client_endpoint or self.auth.client_endpoint
            self.auth.server_endpoint = refreshed.server_endpoint or self.auth.server_endpoint
            self.auth.status = refreshed.status or self.auth.status
            return self._call_once(method_name, params or {})

    def get_lead(self, lead_id: int) -> dict[str, Any]:
        response = self.call("crm.lead.get", {"id": int(lead_id)})
        return dict(response.get("result", {}))

    def update_lead(self, lead_id: int, fields: dict[str, Any]) -> dict[str, Any]:
        return self.call("crm.lead.update", {"id": int(lead_id), "fields": fields})

    def list_users(self) -> list[dict[str, Any]]:
        response = self.call(
            "user.get",
            {
                "filter": {"ACTIVE": True},
                "select": ["ID", "NAME", "LAST_NAME", "SECOND_NAME", "WORK_POSITION", "ACTIVE"],
            },
        )
        result = response.get("result", [])
        return list(result if isinstance(result, list) else [])

    def bind_event(self, event_name: str, handler_url: str) -> dict[str, Any]:
        return self.call("event.bind", {"event": event_name, "handler": handler_url})

    def refresh_tokens(self) -> TokenRefreshResult:
        if not (self.auth.refresh_token and self.client_id and self.client_secret):
            raise BitrixApiError("refresh token flow is not configured")

        query = parse.urlencode(
            {
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.auth.refresh_token,
            }
        )
        url = f"https://oauth.bitrix24.tech/oauth/token/?{query}"
        req = request.Request(url=url, method="GET")
        try:
            with request.urlopen(req, timeout=30) as response:
                body = json.loads(response.read().decode("utf-8", errors="replace"))
        except error.URLError as exc:
            raise BitrixApiError(f"token refresh failed: {exc}") from exc

        if "error" in body:
            raise BitrixApiError(f"token refresh failed: {body['error']}: {body.get('error_description', '')}")

        expires_in = _safe_int(body.get("expires_in"))
        expires_at = (
            datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in)
            if expires_in is not None
            else None
        )
        return TokenRefreshResult(
            access_token=str(body.get("access_token") or ""),
            refresh_token=str(body.get("refresh_token") or ""),
            expires_at=expires_at,
            client_endpoint=_as_optional_str(body.get("client_endpoint")),
            server_endpoint=_as_optional_str(body.get("server_endpoint")),
            status=_as_optional_str(body.get("status")),
        )

    def _call_once(self, method_name: str, params: dict[str, Any]) -> dict[str, Any]:
        if not self.auth.client_endpoint:
            raise BitrixApiError("client_endpoint is not configured")
        if not self.auth.access_token:
            raise BitrixApiError("access_token is not configured")

        endpoint = self.auth.client_endpoint.rstrip("/")
        url = f"{endpoint}/{method_name}.json"
        payload = dict(params)
        payload["auth"] = self.auth.access_token
        req = request.Request(
            url=url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=30) as response:
                body = json.loads(response.read().decode("utf-8", errors="replace"))
        except error.URLError as exc:
            raise BitrixApiError(f"{method_name} failed: {exc}") from exc

        if "error" in body:
            raise BitrixApiError(f"{method_name} failed: {body['error']}: {body.get('error_description', '')}")
        return body


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
