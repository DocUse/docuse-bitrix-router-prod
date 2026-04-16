from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from .bitrix_api import BitrixApiError, BitrixClient
from .contracts import PortalAuth
from .database import Database, to_json

DEAL_ENTITY_TYPE_ID = 2
BITRIX_EVENT_DEAL_CREATED = "ONCRMDEALADD"
BITRIX_EVENT_APP_TEST = "ONAPPTEST"
DISTRIBUTION_EVENT_DEAL_CREATED = "deal_created"
logger = logging.getLogger(__name__)


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

    def get_distribution_group(self, portal_member_id: str) -> dict[str, object] | None:
        self.get_portal(portal_member_id)
        row = self.database.fetch_one(
            "SELECT * FROM distribution_group_configs WHERE portal_member_id = ?",
            (portal_member_id,),
        )
        if row is None:
            return None

        members = _parse_json_list(row["members_json"], "distribution group members")
        load_stage_ids = _parse_json_list(row["load_stage_ids_json"], "distribution load stages")
        return {
            "portal_member_id": str(row["portal_member_id"]),
            "name": str(row["name"]),
            "distribution_type": str(row["distribution_type"]),
            "event_type": str(row["event_type"]),
            "distribution_stage_id": str(row["distribution_stage_id"]),
            "responsible_field_id": str(row["responsible_field_id"]),
            "wait_seconds": int(row["wait_seconds"]),
            "retry_interval_seconds": int(row["retry_interval_seconds"]),
            "is_active": bool(int(row["is_active"])),
            "members": members,
            "load_stage_ids": load_stage_ids,
        }

    def get_distribution_statistics(self, portal_member_id: str) -> dict[str, object]:
        self.get_portal(portal_member_id)
        config = self.get_distribution_group(portal_member_id)
        configured_members = config.get("members") if isinstance(config, dict) else []
        member_names = {
            str(member.get("user_id")): str(member.get("user_id"))
            for member in configured_members
            if isinstance(member, dict) and str(member.get("user_id") or "").strip()
        }

        deal_rows = self.database.fetch_all(
            """
            SELECT *
            FROM distribution_deal_runtime
            WHERE portal_member_id = ?
            ORDER BY updated_at DESC, created_at DESC, deal_id DESC
            LIMIT 100
            """,
            (portal_member_id,),
        )
        member_rows = self.database.fetch_all(
            """
            SELECT *
            FROM distribution_member_runtime
            WHERE portal_member_id = ?
            ORDER BY updated_at DESC, user_id ASC
            """,
            (portal_member_id,),
        )
        diagnostic_rows = self.database.fetch_all(
            """
            SELECT *
            FROM diagnostic_logs
            WHERE portal_member_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 200
            """,
            (portal_member_id,),
        )

        journal = [
            {
                "deal_id": str(row["deal_id"]),
                "event_type": str(row["event_type"]),
                "status": str(row["status"]),
                "assigned_user_id": _as_optional_str(row["assigned_user_id"]),
                "assigned_user_name": member_names.get(str(row["assigned_user_id"])) if row["assigned_user_id"] else None,
                "assigned_field_id": _as_optional_str(row["assigned_field_id"]),
                "note": _as_optional_str(row["note"]),
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"]),
            }
            for row in deal_rows
        ]
        member_runtime = [
            {
                "user_id": str(row["user_id"]),
                "user_name": member_names.get(str(row["user_id"])) or str(row["user_id"]),
                "last_assigned_deal_id": _as_optional_str(row["last_assigned_deal_id"]),
                "last_assigned_at": _as_optional_str(row["last_assigned_at"]),
                "updated_at": str(row["updated_at"]),
            }
            for row in member_rows
        ]
        diagnostics = [
            {
                "id": int(row["id"]),
                "portal_member_id": _as_optional_str(row["portal_member_id"]),
                "deal_id": _as_optional_str(row["deal_id"]),
                "level": str(row["level"]),
                "source": str(row["source"]),
                "message": str(row["message"]),
                "details": _parse_json_object(row["details_json"], "diagnostic log details"),
                "created_at": str(row["created_at"]),
            }
            for row in diagnostic_rows
        ]
        summary = {
            "journal_count": len(journal),
            "member_runtime_count": len(member_runtime),
            "diagnostic_count": len(diagnostics),
            "assigned_count": sum(1 for item in journal if item["status"] == "assigned"),
            "waiting_count": sum(1 for item in journal if item["status"] == "waiting"),
            "ignored_count": sum(1 for item in journal if item["status"] == "ignored"),
        }
        return {
            "summary": summary,
            "journal": journal,
            "members": member_runtime,
            "diagnostics": diagnostics,
        }

    def record_diagnostic_log(
        self,
        *,
        source: str,
        message: str,
        level: str = "info",
        portal_member_id: str | None = None,
        deal_id: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        normalized_portal_member_id = _as_optional_str(portal_member_id)
        if normalized_portal_member_id:
            existing_portal = self.database.fetch_one(
                "SELECT member_id FROM portals WHERE member_id = ?",
                (normalized_portal_member_id,),
            )
            if existing_portal is None:
                normalized_portal_member_id = None
        self.database.execute(
            """
            INSERT INTO diagnostic_logs (
                portal_member_id, deal_id, level, source, message, details_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_portal_member_id,
                _as_optional_str(deal_id),
                level.strip().lower() or "info",
                source.strip(),
                message.strip(),
                to_json(details or {}),
                _iso_now(),
            ),
        )

    def save_distribution_group(self, portal_member_id: str, payload: dict[str, Any]) -> dict[str, object]:
        self.get_portal(portal_member_id)
        normalized = self._normalize_distribution_group_payload(payload)
        now = _iso_now()

        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO distribution_group_configs (
                    portal_member_id, name, distribution_type, event_type, distribution_stage_id,
                    responsible_field_id, wait_seconds, retry_interval_seconds, is_active,
                    members_json, load_stage_ids_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(portal_member_id) DO UPDATE SET
                    name = excluded.name,
                    distribution_type = excluded.distribution_type,
                    event_type = excluded.event_type,
                    distribution_stage_id = excluded.distribution_stage_id,
                    responsible_field_id = excluded.responsible_field_id,
                    wait_seconds = excluded.wait_seconds,
                    retry_interval_seconds = excluded.retry_interval_seconds,
                    is_active = excluded.is_active,
                    members_json = excluded.members_json,
                    load_stage_ids_json = excluded.load_stage_ids_json,
                    updated_at = excluded.updated_at
                """,
                (
                    portal_member_id,
                    normalized["name"],
                    normalized["distribution_type"],
                    normalized["event_type"],
                    normalized["distribution_stage_id"],
                    normalized["responsible_field_id"],
                    normalized["wait_seconds"],
                    normalized["retry_interval_seconds"],
                    1 if bool(normalized["is_active"]) else 0,
                    to_json(normalized["members"]),
                    to_json(normalized["load_stage_ids"]),
                    now,
                    now,
                ),
            )

        saved = self.get_distribution_group(portal_member_id)
        if saved is None:
            raise ValueError("Distribution group was not saved")
        return saved

    def ensure_deal_created_event_binding(self, portal_member_id: str, handler_url: str) -> dict[str, object]:
        return self._ensure_event_binding(
            portal_member_id,
            handler_url,
            event_name=BITRIX_EVENT_DEAL_CREATED,
            ensure_message="Ensuring ONCRMDEALADD binding.",
            already_exists_message="ONCRMDEALADD binding already exists.",
            created_message="Created ONCRMDEALADD binding.",
        )

    def run_event_delivery_check(self, portal_member_id: str, handler_url: str) -> dict[str, object]:
        binding = self._ensure_event_binding(
            portal_member_id,
            handler_url,
            event_name=BITRIX_EVENT_APP_TEST,
            ensure_message="Ensuring ONAPPTEST binding.",
            already_exists_message="ONAPPTEST binding already exists.",
            created_message="Created ONAPPTEST binding.",
        )
        check_id = f"delivery-check-{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}"
        client = self._get_bitrix_client(portal_member_id)
        client.call(
            "event.test",
            {
                "check_id": check_id,
                "portal_member_id": portal_member_id,
                "handler_url": handler_url.strip(),
            },
        )
        self.record_diagnostic_log(
            source="event_delivery_check",
            message="Triggered Bitrix event delivery self-test.",
            portal_member_id=portal_member_id,
            details={"event": BITRIX_EVENT_APP_TEST, "check_id": check_id, "handler": handler_url.strip()},
        )
        return {
            "event": BITRIX_EVENT_APP_TEST,
            "check_id": check_id,
            "handler": handler_url.strip(),
            "binding": binding,
            "triggered": True,
        }

    def _ensure_event_binding(
        self,
        portal_member_id: str,
        handler_url: str,
        *,
        event_name: str,
        ensure_message: str,
        already_exists_message: str,
        created_message: str,
    ) -> dict[str, object]:
        normalized_handler_url = handler_url.strip()
        if not normalized_handler_url:
            raise ValueError("Event handler URL is required")

        logger.info(
            "Ensuring %s binding for portal=%s handler=%s",
            event_name,
            portal_member_id,
            normalized_handler_url,
        )
        self.record_diagnostic_log(
            source="event_binding",
            message=ensure_message,
            portal_member_id=portal_member_id,
            details={"handler": normalized_handler_url, "event": event_name},
        )
        client = self._get_bitrix_client(portal_member_id)
        existing = self._normalize_event_handlers(client.call("event.get"))
        for binding in existing:
            if binding["event"] != event_name:
                continue
            if _normalize_handler_url(str(binding["handler"])) != _normalize_handler_url(normalized_handler_url):
                continue
            logger.info(
                "%s binding already exists for portal=%s handler=%s",
                event_name,
                portal_member_id,
                normalized_handler_url,
            )
            self.record_diagnostic_log(
                source="event_binding",
                message=already_exists_message,
                portal_member_id=portal_member_id,
                details={"handler": normalized_handler_url, "event": event_name},
            )
            return {
                "event": event_name,
                "handler": normalized_handler_url,
                "already_bound": True,
                "bound": False,
            }

        client.call(
            "event.bind",
            {
                "event": event_name,
                "handler": normalized_handler_url,
            },
        )
        logger.info(
            "Created %s binding for portal=%s handler=%s",
            event_name,
            portal_member_id,
            normalized_handler_url,
        )
        self.record_diagnostic_log(
            source="event_binding",
            message=created_message,
            portal_member_id=portal_member_id,
            details={"handler": normalized_handler_url, "event": event_name},
        )
        return {
            "event": event_name,
            "handler": normalized_handler_url,
            "already_bound": False,
            "bound": True,
        }

    def ensure_configured_deal_created_event_binding(
        self,
        portal_member_id: str,
        handler_url: str,
    ) -> dict[str, object]:
        config = self.get_distribution_group(portal_member_id)
        if config is None:
            logger.info("Skipping ONCRMDEALADD binding for portal=%s: no distribution config", portal_member_id)
            self.record_diagnostic_log(
                source="event_binding",
                message="Skipped ONCRMDEALADD binding because distribution group is not configured.",
                portal_member_id=portal_member_id,
            )
            return {
                "event": BITRIX_EVENT_DEAL_CREATED,
                "handler": handler_url.strip(),
                "configured": False,
                "bound": False,
                "already_bound": False,
                "reason": "group_not_configured",
            }
        if str(config.get("event_type") or "") != DISTRIBUTION_EVENT_DEAL_CREATED:
            logger.info(
                "Skipping ONCRMDEALADD binding for portal=%s: unsupported config event_type=%s",
                portal_member_id,
                config.get("event_type"),
            )
            self.record_diagnostic_log(
                source="event_binding",
                message="Skipped ONCRMDEALADD binding because config event_type is unsupported.",
                portal_member_id=portal_member_id,
                details={"event_type": str(config.get("event_type") or "")},
            )
            return {
                "event": BITRIX_EVENT_DEAL_CREATED,
                "handler": handler_url.strip(),
                "configured": True,
                "bound": False,
                "already_bound": False,
                "reason": "unsupported_event_type",
            }
        binding = self.ensure_deal_created_event_binding(portal_member_id, handler_url)
        binding["configured"] = True
        return binding

    def handle_bitrix_event(self, payload: dict[str, Any]) -> dict[str, object]:
        event_name = str(payload.get("event") or "").strip().upper()
        logger.info("Received Bitrix event event=%s payload_keys=%s", event_name or "<empty>", sorted(payload.keys()))
        portal_member_id = _as_optional_str(_safe_extract_event_member_id(payload))
        self.record_diagnostic_log(
            source="bitrix_event",
            message="Received Bitrix event payload.",
            portal_member_id=portal_member_id,
            details={"event": event_name or None, "payload_keys": sorted(payload.keys())},
        )
        if event_name != BITRIX_EVENT_DEAL_CREATED:
            self.record_diagnostic_log(
                source="bitrix_event",
                message="Ignored unsupported Bitrix event.",
                portal_member_id=portal_member_id,
                details={"event": event_name or None},
            )
            return {
                "status": "ignored",
                "reason": "unsupported_event",
                "event": event_name or None,
            }

        if self._can_install_from_payload(payload):
            self.install_portal(payload)

        portal_member_id = self._extract_event_member_id(payload)
        deal_id = self._extract_event_deal_id(payload)
        logger.info(
            "Processing ONCRMDEALADD portal=%s deal_id=%s",
            portal_member_id,
            deal_id,
        )
        self.record_diagnostic_log(
            source="bitrix_event",
            message="Processing ONCRMDEALADD.",
            portal_member_id=portal_member_id,
            deal_id=deal_id,
            details={"event": event_name},
        )
        return self._handle_deal_created_event(portal_member_id, deal_id)

    def _handle_deal_created_event(self, portal_member_id: str, deal_id: str) -> dict[str, object]:
        self.get_portal(portal_member_id)

        existing_runtime = self._get_deal_runtime(portal_member_id, deal_id, DISTRIBUTION_EVENT_DEAL_CREATED)
        if existing_runtime is not None:
            result = dict(existing_runtime)
            result["reason"] = "duplicate_event"
            logger.info(
                "Ignoring duplicate ONCRMDEALADD portal=%s deal_id=%s status=%s",
                portal_member_id,
                deal_id,
                existing_runtime.get("status"),
            )
            self.record_diagnostic_log(
                source="deal_assignment",
                message="Ignored duplicate ONCRMDEALADD event.",
                portal_member_id=portal_member_id,
                deal_id=deal_id,
                details={"status": str(existing_runtime.get("status") or "")},
            )
            return result

        config = self.get_distribution_group(portal_member_id)
        if config is None:
            logger.info("Ignoring deal assignment portal=%s deal_id=%s: no config", portal_member_id, deal_id)
            self.record_diagnostic_log(
                source="deal_assignment",
                message="Ignored deal because distribution group is not configured.",
                portal_member_id=portal_member_id,
                deal_id=deal_id,
            )
            return self._record_and_return_deal_runtime(
                portal_member_id,
                deal_id,
                status="ignored",
                note="Distribution group is not configured",
            )
        if not bool(config.get("is_active")):
            logger.info("Ignoring deal assignment portal=%s deal_id=%s: config inactive", portal_member_id, deal_id)
            self.record_diagnostic_log(
                source="deal_assignment",
                message="Ignored deal because distribution group is inactive.",
                portal_member_id=portal_member_id,
                deal_id=deal_id,
            )
            return self._record_and_return_deal_runtime(
                portal_member_id,
                deal_id,
                status="ignored",
                note="Distribution group is inactive",
            )
        if str(config.get("event_type") or "") != DISTRIBUTION_EVENT_DEAL_CREATED:
            logger.info(
                "Ignoring deal assignment portal=%s deal_id=%s: event_type=%s",
                portal_member_id,
                deal_id,
                config.get("event_type"),
            )
            self.record_diagnostic_log(
                source="deal_assignment",
                message="Ignored deal because distribution config does not handle deal creation.",
                portal_member_id=portal_member_id,
                deal_id=deal_id,
                details={"event_type": str(config.get("event_type") or "")},
            )
            return self._record_and_return_deal_runtime(
                portal_member_id,
                deal_id,
                status="ignored",
                note="Distribution group does not handle deal creation events",
            )

        members = config.get("members")
        if not isinstance(members, list) or not members:
            logger.info("Ignoring deal assignment portal=%s deal_id=%s: no members", portal_member_id, deal_id)
            self.record_diagnostic_log(
                source="deal_assignment",
                message="Ignored deal because distribution group has no members.",
                portal_member_id=portal_member_id,
                deal_id=deal_id,
            )
            return self._record_and_return_deal_runtime(
                portal_member_id,
                deal_id,
                status="ignored",
                note="Distribution group has no members",
            )

        client = self._get_bitrix_client(portal_member_id)
        deal = self._get_deal_item(client, deal_id)
        current_stage_id = self._extract_deal_stage_id(deal)
        if current_stage_id != str(config["distribution_stage_id"]):
            logger.info(
                "Ignoring deal assignment portal=%s deal_id=%s: stage=%s expected=%s",
                portal_member_id,
                deal_id,
                current_stage_id or "<empty>",
                config["distribution_stage_id"],
            )
            self.record_diagnostic_log(
                source="deal_assignment",
                message="Ignored deal because current stage does not match distribution stage.",
                portal_member_id=portal_member_id,
                deal_id=deal_id,
                details={
                    "current_stage_id": current_stage_id or "",
                    "expected_stage_id": str(config["distribution_stage_id"]),
                },
            )
            return self._record_and_return_deal_runtime(
                portal_member_id,
                deal_id,
                status="ignored",
                note=f"Deal stage {current_stage_id or '<empty>'} does not match distribution stage",
            )

        selection = self._select_distribution_candidate(portal_member_id, client, config)
        if selection["selected_member"] is None:
            logger.info(
                "No available members for portal=%s deal_id=%s loads=%s",
                portal_member_id,
                deal_id,
                selection["loads"],
            )
            self.record_diagnostic_log(
                source="deal_assignment",
                message="No available distribution members: all limits are reached.",
                portal_member_id=portal_member_id,
                deal_id=deal_id,
                details={"loads": selection["loads"]},
            )
            return self._record_and_return_deal_runtime(
                portal_member_id,
                deal_id,
                status="waiting",
                note="All distribution members reached their limits",
                extra={"loads": selection["loads"]},
            )

        selected_member = selection["selected_member"]
        selected_user_id = str(selected_member["user_id"])
        responsible_field_id = str(config["responsible_field_id"])

        self._assign_deal_to_member(client, deal_id, responsible_field_id, selected_user_id)
        self._touch_member_runtime(portal_member_id, selected_user_id, deal_id)
        logger.info(
            "Assigned deal portal=%s deal_id=%s user_id=%s field=%s loads=%s",
            portal_member_id,
            deal_id,
            selected_user_id,
            responsible_field_id,
            selection["loads"],
        )
        self.record_diagnostic_log(
            source="deal_assignment",
            message="Assigned deal to distribution member.",
            portal_member_id=portal_member_id,
            deal_id=deal_id,
            details={
                "user_id": selected_user_id,
                "responsible_field_id": responsible_field_id,
                "loads": selection["loads"],
            },
        )
        return self._record_and_return_deal_runtime(
            portal_member_id,
            deal_id,
            status="assigned",
            assigned_user_id=selected_user_id,
            assigned_field_id=responsible_field_id,
            note="Deal assigned after load calculation",
            extra={"loads": selection["loads"]},
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

    def _can_install_from_payload(self, payload: dict[str, Any]) -> bool:
        auth = payload.get("auth")
        if not isinstance(auth, dict):
            return False
        return bool(str(auth.get("member_id") or "").strip() and str(auth.get("domain") or "").strip())

    def _extract_event_member_id(self, payload: dict[str, Any]) -> str:
        auth = payload.get("auth")
        if isinstance(auth, dict):
            member_id = str(auth.get("member_id") or "").strip()
            if member_id:
                return member_id

        member_id = str(payload.get("member_id") or payload.get("MEMBER_ID") or "").strip()
        if member_id:
            return member_id
        raise ValueError("Bitrix event payload is missing member_id")

    def _extract_event_deal_id(self, payload: dict[str, Any]) -> str:
        data = payload.get("data")
        if isinstance(data, dict):
            fields = data.get("FIELDS")
            if isinstance(fields, dict):
                deal_id = str(fields.get("ID") or "").strip()
                if deal_id:
                    return deal_id
        raise ValueError("Bitrix deal event payload is missing deal ID")

    def _get_deal_item(self, client: BitrixListClient, deal_id: str) -> dict[str, Any]:
        payload = client.call(
            "crm.item.get",
            {
                "entityTypeId": DEAL_ENTITY_TYPE_ID,
                "id": _maybe_int(deal_id),
                "useOriginalUfNames": "Y",
            },
        )
        result = payload.get("result")
        if not isinstance(result, dict):
            raise BitrixApiError("Bitrix deal payload has an unexpected format")
        item = result.get("item")
        if not isinstance(item, dict):
            raise BitrixApiError("Bitrix deal payload is missing item data")
        return item

    def _extract_deal_stage_id(self, deal: dict[str, Any]) -> str:
        return str(deal.get("stageId") or deal.get("STAGE_ID") or "").strip()

    def _select_distribution_candidate(
        self,
        portal_member_id: str,
        client: BitrixListClient,
        config: dict[str, object],
    ) -> dict[str, object]:
        members = config.get("members")
        if not isinstance(members, list):
            raise ValueError("Stored distribution members payload must be a list")

        responsible_field_id = str(config["responsible_field_id"])
        load_stage_ids = config.get("load_stage_ids")
        if not isinstance(load_stage_ids, list):
            raise ValueError("Stored distribution load stages payload must be a list")

        last_assigned_map = self._get_member_last_assigned_map(portal_member_id)
        loads: list[dict[str, object]] = []
        available_members: list[dict[str, object]] = []
        for raw_member in members:
            if not isinstance(raw_member, dict):
                continue
            user_id = str(raw_member.get("user_id") or "").strip()
            if not user_id:
                continue
            limit_value = _coerce_int(raw_member.get("limit"), field_name=f"Member limit for {user_id}")
            sort_order = _coerce_int(raw_member.get("sort_order"), field_name=f"Member sort order for {user_id}")
            current_load = self._count_member_load(client, responsible_field_id, load_stage_ids, user_id)
            last_assigned_at = last_assigned_map.get(user_id)
            descriptor = {
                "user_id": user_id,
                "limit": limit_value,
                "sort_order": sort_order,
                "current_load": current_load,
                "last_assigned_at": last_assigned_at,
            }
            loads.append(descriptor)
            if current_load < limit_value:
                available_members.append(descriptor)

        available_members.sort(
            key=lambda member: (
                int(member["current_load"]),
                str(member.get("last_assigned_at") or ""),
                int(member["sort_order"]),
            )
        )
        selected_member = available_members[0] if available_members else None
        return {"selected_member": selected_member, "loads": loads}

    def _count_member_load(
        self,
        client: BitrixListClient,
        responsible_field_id: str,
        load_stage_ids: list[object],
        user_id: str,
    ) -> int:
        filter_field = _resolve_responsible_field_api_name(responsible_field_id)
        items = client.call_list(
            "crm.item.list",
            {
                "entityTypeId": DEAL_ENTITY_TYPE_ID,
                "select": ["id"],
                "filter": {
                    "@stageId": [str(stage_id) for stage_id in load_stage_ids],
                    f"@{filter_field}": [_maybe_int(user_id)],
                },
                "useOriginalUfNames": "Y",
            },
        )
        return len(items)

    def _assign_deal_to_member(
        self,
        client: BitrixListClient,
        deal_id: str,
        responsible_field_id: str,
        user_id: str,
    ) -> None:
        field_name = _resolve_responsible_field_api_name(responsible_field_id)
        client.call(
            "crm.item.update",
            {
                "entityTypeId": DEAL_ENTITY_TYPE_ID,
                "id": _maybe_int(deal_id),
                "fields": {field_name: _maybe_int(user_id)},
                "useOriginalUfNames": "Y",
            },
        )

    def _get_member_last_assigned_map(self, portal_member_id: str) -> dict[str, str | None]:
        rows = self.database.fetch_all(
            """
            SELECT user_id, last_assigned_at
            FROM distribution_member_runtime
            WHERE portal_member_id = ?
            """,
            (portal_member_id,),
        )
        return {str(row["user_id"]): _as_optional_str(row["last_assigned_at"]) for row in rows}

    def _touch_member_runtime(self, portal_member_id: str, user_id: str, deal_id: str) -> None:
        now = _iso_now()
        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO distribution_member_runtime (
                    portal_member_id, user_id, last_assigned_deal_id, last_assigned_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(portal_member_id, user_id) DO UPDATE SET
                    last_assigned_deal_id = excluded.last_assigned_deal_id,
                    last_assigned_at = excluded.last_assigned_at,
                    updated_at = excluded.updated_at
                """,
                (portal_member_id, user_id, deal_id, now, now),
            )

    def _get_deal_runtime(
        self,
        portal_member_id: str,
        deal_id: str,
        event_type: str,
    ) -> dict[str, object] | None:
        row = self.database.fetch_one(
            """
            SELECT *
            FROM distribution_deal_runtime
            WHERE portal_member_id = ? AND deal_id = ? AND event_type = ?
            """,
            (portal_member_id, deal_id, event_type),
        )
        if row is None:
            return None
        return {
            "portal_member_id": str(row["portal_member_id"]),
            "deal_id": str(row["deal_id"]),
            "event_type": str(row["event_type"]),
            "status": str(row["status"]),
            "assigned_user_id": _as_optional_str(row["assigned_user_id"]),
            "assigned_field_id": _as_optional_str(row["assigned_field_id"]),
            "note": _as_optional_str(row["note"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    def _record_and_return_deal_runtime(
        self,
        portal_member_id: str,
        deal_id: str,
        *,
        status: str,
        note: str,
        assigned_user_id: str | None = None,
        assigned_field_id: str | None = None,
        extra: dict[str, object] | None = None,
    ) -> dict[str, object]:
        now = _iso_now()
        existing = self._get_deal_runtime(portal_member_id, deal_id, DISTRIBUTION_EVENT_DEAL_CREATED)
        created_at = str(existing["created_at"]) if existing is not None else now

        with self.database.connection() as connection:
            connection.execute(
                """
                INSERT INTO distribution_deal_runtime (
                    portal_member_id, deal_id, event_type, status, assigned_user_id,
                    assigned_field_id, note, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(portal_member_id, deal_id, event_type) DO UPDATE SET
                    status = excluded.status,
                    assigned_user_id = excluded.assigned_user_id,
                    assigned_field_id = excluded.assigned_field_id,
                    note = excluded.note,
                    updated_at = excluded.updated_at
                """,
                (
                    portal_member_id,
                    deal_id,
                    DISTRIBUTION_EVENT_DEAL_CREATED,
                    status,
                    assigned_user_id,
                    assigned_field_id,
                    note,
                    created_at,
                    now,
                ),
            )

        result = self._get_deal_runtime(portal_member_id, deal_id, DISTRIBUTION_EVENT_DEAL_CREATED)
        if result is None:
            raise ValueError("Distribution deal runtime was not saved")
        if extra:
            result.update(extra)
        return result

    def _normalize_event_handlers(self, payload: dict[str, Any]) -> list[dict[str, object]]:
        result = payload.get("result")
        if not isinstance(result, list):
            raise BitrixApiError("Bitrix event handler payload has an unexpected format")

        handlers: list[dict[str, object]] = []
        for item in result:
            if not isinstance(item, dict):
                continue
            event_name = str(item.get("event") or "").strip().upper()
            handler = str(item.get("handler") or "").strip()
            if not event_name or not handler:
                continue
            handlers.append({"event": event_name, "handler": handler})
        return handlers

    def _normalize_distribution_group_payload(self, payload: dict[str, Any]) -> dict[str, object]:
        name = str(payload.get("name") or "").strip()
        if not name:
            raise ValueError("Distribution group name is required")

        distribution_type = str(payload.get("distribution_type") or "").strip()
        if distribution_type != "round_robin_load_time":
            raise ValueError("Unsupported distribution type")

        event_type = str(payload.get("event_type") or "").strip()
        if event_type != "deal_created":
            raise ValueError("Unsupported event type")

        distribution_stage_id = str(payload.get("distribution_stage_id") or "").strip()
        if not distribution_stage_id:
            raise ValueError("Distribution stage is required")

        responsible_field_id = str(payload.get("responsible_field_id") or "").strip()
        if not responsible_field_id:
            raise ValueError("Responsible field is required")

        wait_seconds = _coerce_int(payload.get("wait_seconds"), field_name="Wait time")
        retry_interval_seconds = _coerce_int(
            payload.get("retry_interval_seconds"),
            field_name="Retry interval",
        )
        if wait_seconds < 1:
            raise ValueError("Wait time must be at least 1 second")
        if retry_interval_seconds < 1:
            raise ValueError("Retry interval must be at least 1 second")

        raw_load_stage_ids = payload.get("load_stage_ids")
        if not isinstance(raw_load_stage_ids, list):
            raise ValueError("Load stages must be a list")
        load_stage_ids = _normalize_string_list(raw_load_stage_ids)
        if not load_stage_ids:
            raise ValueError("At least one load stage is required")

        raw_members = payload.get("members")
        if not isinstance(raw_members, list):
            raise ValueError("Distribution members must be a list")

        members: list[dict[str, object]] = []
        seen_user_ids: set[str] = set()
        for index, member in enumerate(raw_members):
            if not isinstance(member, dict):
                raise ValueError("Each distribution member must be an object")
            user_id = str(member.get("user_id") or "").strip()
            if not user_id:
                raise ValueError("Each selected member must have a user_id")
            if user_id in seen_user_ids:
                raise ValueError("Duplicate distribution member detected")
            limit_value = _coerce_int(member.get("limit"), field_name=f"Member limit for {user_id}")
            if limit_value < 0:
                raise ValueError("Member limit cannot be negative")
            members.append({"user_id": user_id, "limit": limit_value, "sort_order": index})
            seen_user_ids.add(user_id)

        if not members:
            raise ValueError("At least one distribution member is required")

        return {
            "name": name,
            "distribution_type": distribution_type,
            "event_type": event_type,
            "distribution_stage_id": distribution_stage_id,
            "responsible_field_id": responsible_field_id,
            "wait_seconds": wait_seconds,
            "retry_interval_seconds": retry_interval_seconds,
            "is_active": bool(payload.get("is_active", True)),
            "members": members,
            "load_stage_ids": load_stage_ids,
        }

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


def _coerce_int(value: Any, *, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc


def _normalize_string_list(values: list[object]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        result.append(text)
        seen.add(text)
    return result


def _parse_json_list(raw_value: Any, field_name: str) -> list[dict[str, object]] | list[str]:
    try:
        parsed = json.loads(str(raw_value or "[]"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Stored {field_name} payload is invalid") from exc
    if not isinstance(parsed, list):
        raise ValueError(f"Stored {field_name} payload must be a list")
    return parsed


def _parse_json_object(raw_value: Any, field_name: str) -> dict[str, object]:
    try:
        parsed = json.loads(str(raw_value or "{}"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Stored {field_name} payload is invalid") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"Stored {field_name} payload must be an object")
    return parsed


def _resolve_responsible_field_api_name(field_id: str) -> str:
    normalized = field_id.strip().upper()
    if normalized == "ASSIGNED_BY_ID":
        return "assignedById"
    return field_id.strip()


def _maybe_int(value: Any) -> int | str:
    text = str(value or "").strip()
    if text.isdigit():
        return int(text)
    return text


def _normalize_handler_url(value: str) -> str:
    return value.strip().rstrip("/")


def _safe_extract_event_member_id(payload: dict[str, Any]) -> str | None:
    auth = payload.get("auth")
    if isinstance(auth, dict):
        member_id = str(auth.get("member_id") or "").strip()
        if member_id:
            return member_id
    member_id = str(payload.get("member_id") or payload.get("MEMBER_ID") or "").strip()
    return member_id or None
