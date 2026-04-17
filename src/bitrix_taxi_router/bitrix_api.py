from __future__ import annotations

import json
from typing import Any
from urllib import error, request

from .contracts import PortalAuth


class BitrixApiError(RuntimeError):
    pass


class BitrixClient:
    def __init__(self, portal: PortalAuth) -> None:
        self.portal = portal

    def call(self, method: str, params: dict[str, object] | None = None) -> dict[str, Any]:
        endpoint = (self.portal.client_endpoint or "").strip()
        access_token = (self.portal.access_token or "").strip()
        if not endpoint:
            raise BitrixApiError("Portal client endpoint is missing")
        if not access_token:
            raise BitrixApiError("Portal access token is missing")

        url = f"{endpoint.rstrip('/')}/{method}"
        payload = json.dumps({"auth": access_token, **(params or {})}, ensure_ascii=False).encode("utf-8")
        http_request = request.Request(
            url,
            data=payload,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with request.urlopen(http_request, timeout=20) as response:
                body = response.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise BitrixApiError(f"Bitrix API HTTP error {exc.code}: {detail or exc.reason}") from exc
        except error.URLError as exc:
            raise BitrixApiError(f"Bitrix API is unavailable: {exc.reason}") from exc

        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise BitrixApiError("Bitrix API returned invalid JSON") from exc

        if not isinstance(data, dict):
            raise BitrixApiError("Bitrix API returned an unexpected payload")
        if data.get("error"):
            message = str(data.get("error_description") or data["error"])
            raise BitrixApiError(message)
        return data

    def call_list(self, method: str, params: dict[str, object] | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        next_start: int | None = 0

        while next_start is not None:
            page_params = dict(params or {})
            if next_start:
                page_params["start"] = next_start

            payload = self.call(method, page_params)
            result = payload.get("result")
            if isinstance(result, list):
                page_items = result
            elif isinstance(result, dict) and isinstance(result.get("items"), list):
                page_items = result["items"]
            else:
                raise BitrixApiError(f"Bitrix API method {method} did not return a list")

            for item in page_items:
                if isinstance(item, dict):
                    items.append(item)

            raw_next = payload.get("next")
            if raw_next in (None, False):
                next_start = None
            else:
                next_start = int(raw_next)

        return items
