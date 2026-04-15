from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from .bitrix_api import BitrixApiError, BitrixClient
from .contracts import PortalAuth
from .database import Database


class BitrixListClient(Protocol):
    def call(self, method: str, params: dict[str, object] | None = None) -> dict[str, Any]:
        ...

    def call_list(self, method: str, params: dict[str, object] | None = None) -> list[dict[str, Any]]:
        ...


class PortalService:
    def __init__(
        self,
        database: Database,
        *,
        bitrix_client_factory: Callable[[PortalAuth], BitrixListClient] | None = None,
    ) -> None:
        self.database = database
        self.bitrix_client_factory = bitrix_client_factory or BitrixClient

    def install_portal(self, payload: dict[str, Any]) -> dict[str, Any]:
        auth = self._extract_auth_payload(payload)
        portal = PortalAuth(
            member_id=str(auth["member_id"]),
            domain=str(auth["domain"]),
            access_token=_as_optional_str(auth.get("access_token")),
            refresh_token=_as_optional_str(auth.get("refresh_token")),
            client_endpoint=_as_optional_str(auth.get("client_endpoint")),
            server_endpoint=_as_optional_str(auth.get("server_endpoint")),
            application_token=_as_optional_str(auth.get("application_token")),
            status=_as_optional_str(auth.get("status")),
        )
        self._upsert_portal(portal)
        return {
            "member_id": portal.member_id,
            "domain": portal.domain,
            "saved": True,
        }

    def get_portal(self, portal_member_id: str) -> PortalAuth:
        row = self.database.fetch_one("SELECT * FROM portals WHERE member_id = ?", (portal_member_id,))
        if row is None:
            raise ValueError(f"Portal {portal_member_id} is not installed")
        return PortalAuth(
            member_id=str(row["member_id"]),
            domain=str(row["domain"]),
            access_token=_as_optional_str(row["access_token"]),
            refresh_token=_as_optional_str(row["refresh_token"]),
            client_endpoint=_as_optional_str(row["client_endpoint"]),
            server_endpoint=_as_optional_str(row["server_endpoint"]),
            application_token=_as_optional_str(row["application_token"]),
            status=_as_optional_str(row["status"]),
        )

    def get_reference_data(self, portal_member_id: str) -> dict[str, list[dict[str, object]]]:
        client = self._get_bitrix_client(portal_member_id)
        return {
            "users": self._normalize_users(client.call_list("user.get")),
            "stages": self._normalize_stages(
                client.call_list("crm.status.list", {"filter": {"ENTITY_ID": "DEAL_STAGE"}})
            ),
            "responsible_fields": self._normalize_responsible_fields(
                client.call("crm.item.fields", {"entityTypeId": 2, "useOriginalUfNames": "Y"})
            ),
        }

    def list_portal_users(self, portal_member_id: str) -> list[dict[str, object]]:
        client = self._get_bitrix_client(portal_member_id)
        return self._normalize_users(client.call_list("user.get"))

    def list_deal_stages(self, portal_member_id: str) -> list[dict[str, object]]:
        client = self._get_bitrix_client(portal_member_id)
        return self._normalize_stages(client.call_list("crm.status.list", {"filter": {"ENTITY_ID": "DEAL_STAGE"}}))

    def list_responsible_fields(self, portal_member_id: str) -> list[dict[str, object]]:
        client = self._get_bitrix_client(portal_member_id)
        return self._normalize_responsible_fields(
            client.call("crm.item.fields", {"entityTypeId": 2, "useOriginalUfNames": "Y"})
        )

    def _extract_auth_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if "member_id" in payload and "domain" in payload:
            return payload
        auth = payload.get("auth")
        if isinstance(auth, dict) and "member_id" in auth and "domain" in auth:
            return auth
        raise ValueError("Bitrix auth payload is missing member_id/domain")

    def _upsert_portal(self, portal: PortalAuth) -> None:
        now = _iso_now()
        existing = self.database.fetch_one("SELECT * FROM portals WHERE member_id = ?", (portal.member_id,))
        if existing:
            merged = PortalAuth(
                member_id=portal.member_id,
                domain=portal.domain or str(existing["domain"]),
                access_token=portal.access_token or _as_optional_str(existing["access_token"]),
                refresh_token=portal.refresh_token or _as_optional_str(existing["refresh_token"]),
                client_endpoint=portal.client_endpoint or _as_optional_str(existing["client_endpoint"]),
                server_endpoint=portal.server_endpoint or _as_optional_str(existing["server_endpoint"]),
                application_token=portal.application_token or _as_optional_str(existing["application_token"]),
                status=portal.status or _as_optional_str(existing["status"]),
            )
            self.database.execute(
                """
                UPDATE portals
                SET domain = ?, application_token = ?, access_token = ?, refresh_token = ?,
                    client_endpoint = ?, server_endpoint = ?, status = ?, updated_at = ?
                WHERE member_id = ?
                """,
                (
                    merged.domain,
                    merged.application_token,
                    merged.access_token,
                    merged.refresh_token,
                    merged.client_endpoint,
                    merged.server_endpoint,
                    merged.status,
                    now,
                    merged.member_id,
                ),
            )
            return

        self.database.execute(
            """
            INSERT INTO portals (
                member_id, domain, application_token, access_token, refresh_token,
                client_endpoint, server_endpoint, status, installed_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                portal.member_id,
                portal.domain,
                portal.application_token,
                portal.access_token,
                portal.refresh_token,
                portal.client_endpoint,
                portal.server_endpoint,
                portal.status,
                now,
                now,
            ),
        )

    def _get_bitrix_client(self, portal_member_id: str) -> BitrixListClient:
        portal = self.get_portal(portal_member_id)
        return self.bitrix_client_factory(portal)

    def _normalize_users(self, rows: list[dict[str, Any]]) -> list[dict[str, object]]:
        users: list[dict[str, object]] = []
        for row in rows:
            user_id = str(row.get("ID") or "").strip()
            if not user_id:
                continue

            full_name = " ".join(
                part for part in (str(row.get("NAME") or "").strip(), str(row.get("LAST_NAME") or "").strip()) if part
            )
            display_name = full_name or str(row.get("EMAIL") or "").strip() or f"Пользователь {user_id}"

            users.append(
                {
                    "id": user_id,
                    "name": display_name,
                    "is_active": str(row.get("ACTIVE") or "").upper() != "N",
                    "source": "bitrix:user.get",
                }
            )

        users.sort(key=lambda item: (str(item["name"]).casefold(), str(item["id"])))
        return users

    def _normalize_stages(self, rows: list[dict[str, Any]]) -> list[dict[str, object]]:
        stages: list[dict[str, object]] = []
        for row in rows:
            stage_id = str(row.get("STATUS_ID") or "").strip()
            name = str(row.get("NAME") or "").strip()
            if not stage_id or not name:
                continue

            raw_sort = row.get("SORT")
            try:
                sort_order = int(raw_sort) if raw_sort is not None else 999999
            except (TypeError, ValueError):
                sort_order = 999999

            stages.append(
                {
                    "id": stage_id,
                    "name": name,
                    "sort": sort_order,
                    "source": "bitrix:crm.status.list",
                }
            )

        stages.sort(key=lambda item: (int(item["sort"]), str(item["name"]).casefold(), str(item["id"])))
        return stages

    def _normalize_responsible_fields(self, payload: dict[str, Any]) -> list[dict[str, object]]:
        result = payload.get("result")
        fields_map: dict[str, Any] | None = None
        if isinstance(result, dict):
            nested_fields = result.get("fields")
            if isinstance(nested_fields, dict):
                fields_map = nested_fields
            else:
                fields_map = result
        if fields_map is None:
            raise BitrixApiError("Bitrix API field metadata response has an unexpected format")

        fields: list[dict[str, object]] = []
        for field_key, meta in fields_map.items():
            if not isinstance(meta, dict):
                continue

            field_id = str(meta.get("upperName") or field_key).strip()
            field_name = str(meta.get("title") or meta.get("formLabel") or meta.get("listLabel") or field_id).strip()
            normalized_type = str(meta.get("type") or meta.get("userType") or "").strip().lower()
            is_default = field_id == "ASSIGNED_BY_ID"
            is_read_only = bool(meta.get("isReadOnly"))
            is_multiple = bool(meta.get("isMultiple"))
            if normalized_type not in {"employee", "user"}:
                continue
            if is_read_only or is_multiple:
                continue

            fields.append(
                {
                    "id": field_id,
                    "name": field_name,
                    "is_default": is_default,
                    "source": "bitrix:crm.item.fields",
                }
            )

        fields.sort(key=lambda item: (not bool(item["is_default"]), str(item["name"]).casefold(), str(item["id"])))
        return fields


def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _as_optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
