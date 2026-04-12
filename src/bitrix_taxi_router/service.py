from __future__ import annotations

import sqlite3
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .bitrix import BitrixClient
from .contracts import DistributionGroupInput, GroupMemberInput, PortalAuth
from .database import Database, to_json
from .settings import Settings


class AssignmentService:
    SUPPORTED_EVENTS = {"ONCRMLEADADD", "ONCRMLEADUPDATE"}

    def __init__(
        self,
        database: Database,
        settings: Settings,
        *,
        now_fn: Callable[[], datetime] | None = None,
        client_factory: Callable[[PortalAuth], Any] | None = None,
    ) -> None:
        self.database = database
        self.settings = settings
        self.now_fn = now_fn or (lambda: datetime.now(tz=timezone.utc))
        self.client_factory = client_factory or self._default_client_factory

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

    def register_default_event_handlers(self, portal_member_id: str) -> list[str]:
        if not self.settings.app_base_url:
            return []
        portal = self._get_portal(portal_member_id)
        client = self._make_client(portal)
        handler_url = f"{self.settings.app_base_url}/events/bitrix"
        registered: list[str] = []
        for event_name in sorted(self.SUPPORTED_EVENTS):
            client.bind_event(event_name, handler_url)
            registered.append(event_name)
        self._persist_portal_tokens(client.auth)
        return registered

    def save_group(self, group_input: DistributionGroupInput) -> int:
        now = self._iso_now()
        existing = None
        if group_input.group_id is not None:
            existing = self.database.fetch_one(
                "SELECT id FROM distribution_groups WHERE id = ? AND portal_member_id = ?",
                (group_input.group_id, group_input.portal_member_id),
            )

        if existing:
            group_id = int(existing["id"])
            self.database.execute(
                """
                UPDATE distribution_groups
                SET name = ?, initial_stage_id = ?, timeout_seconds = ?, priority = ?,
                    event_on_add = ?, event_on_update = ?, is_active = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    group_input.name,
                    group_input.initial_stage_id,
                    int(group_input.timeout_seconds),
                    int(group_input.priority),
                    1 if group_input.event_on_add else 0,
                    1 if group_input.event_on_update else 0,
                    1 if group_input.is_active else 0,
                    now,
                    group_id,
                ),
            )
            self.database.execute("DELETE FROM group_members WHERE group_id = ?", (group_id,))
        else:
            group_id = self.database.execute(
                """
                INSERT INTO distribution_groups (
                    portal_member_id, name, entity_type, initial_stage_id, timeout_seconds,
                    priority, event_on_add, event_on_update, is_active, created_at, updated_at
                )
                VALUES (?, ?, 'lead', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    group_input.portal_member_id,
                    group_input.name,
                    group_input.initial_stage_id,
                    int(group_input.timeout_seconds),
                    int(group_input.priority),
                    1 if group_input.event_on_add else 0,
                    1 if group_input.event_on_update else 0,
                    1 if group_input.is_active else 0,
                    now,
                    now,
                ),
            )

        rows = [
            (
                group_id,
                int(member.user_id),
                int(member.sort_order),
                1 if member.is_active else 0,
                now,
                now,
            )
            for member in sorted(group_input.members, key=lambda item: (item.sort_order, item.user_id))
        ]
        if rows:
            self.database.executemany(
                """
                INSERT INTO group_members (
                    group_id, bitrix_user_id, sort_order, is_active, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return group_id

    def list_groups(self, portal_member_id: str) -> list[dict[str, Any]]:
        rows = self.database.fetch_all(
            """
            SELECT id, portal_member_id, name, initial_stage_id, timeout_seconds,
                   priority, event_on_add, event_on_update, is_active
            FROM distribution_groups
            WHERE portal_member_id = ?
            ORDER BY priority ASC, id ASC
            """,
            (portal_member_id,),
        )
        return [
            {
                **dict(row),
                "members": self._list_group_members(int(row["id"])),
            }
            for row in rows
        ]

    def delete_group(self, portal_member_id: str, group_id: int) -> bool:
        existing = self.database.fetch_one(
            "SELECT id FROM distribution_groups WHERE id = ? AND portal_member_id = ?",
            (int(group_id), portal_member_id),
        )
        if existing is None:
            return False
        self.database.execute("DELETE FROM group_members WHERE group_id = ?", (int(group_id),))
        self.database.execute("DELETE FROM distribution_groups WHERE id = ? AND portal_member_id = ?", (int(group_id), portal_member_id))
        self.database.execute(
            """
            UPDATE lead_assignments
            SET status = 'completed', completion_reason = 'group_deleted', completed_at = ?, updated_at = ?
            WHERE group_id = ? AND portal_member_id = ? AND status = 'waiting'
            """,
            (self._iso_now(), self._iso_now(), int(group_id), portal_member_id),
        )
        return True

    def list_portal_employees(self, portal_member_id: str) -> list[dict[str, Any]]:
        portal = self._get_portal(portal_member_id)
        client = self._make_client(portal)
        users = client.list_users()
        self._persist_portal_tokens(client.auth)
        normalized: list[dict[str, Any]] = []
        for user in users:
            normalized.append(
                {
                    "id": _safe_int(user.get("ID")),
                    "name": " ".join(
                        part
                        for part in [
                            str(user.get("NAME") or "").strip(),
                            str(user.get("LAST_NAME") or "").strip(),
                        ]
                        if part
                    ).strip(),
                    "work_position": str(user.get("WORK_POSITION") or "").strip(),
                    "active": str(user.get("ACTIVE") or "Y") == "Y",
                }
            )
        return normalized

    def process_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        event_name = str(payload.get("event") or "").upper().strip()
        if event_name not in self.SUPPORTED_EVENTS:
            return {"status": "ignored", "reason": "unsupported_event"}

        portal = self._resolve_portal_for_payload(payload)
        lead_id = _extract_lead_id(payload)
        self.database.execute(
            """
            INSERT INTO event_log (portal_member_id, event_type, lead_id, payload_raw, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (portal.member_id, event_name, lead_id, to_json(payload), self._iso_now()),
        )
        client = self._make_client(portal)
        lead = client.get_lead(lead_id)
        self._persist_portal_tokens(client.auth)

        completed = self._complete_if_taken_in_work(portal.member_id, lead_id, str(lead.get("STATUS_ID") or ""))

        assigned_group_id: int | None = None
        if str(lead.get("STATUS_ID") or ""):
            for group in self._list_candidate_groups(portal.member_id, event_name):
                if str(group["initial_stage_id"]) != str(lead.get("STATUS_ID")):
                    continue
                if self._get_open_assignment(portal.member_id, int(group["id"]), lead_id) is not None:
                    assigned_group_id = int(group["id"])
                    break
                assigned = self._assign_first_member(
                    portal=portal,
                    client=client,
                    group=group,
                    lead_id=lead_id,
                    lead_status_id=str(lead.get("STATUS_ID") or ""),
                )
                if assigned is not None:
                    assigned_group_id = assigned
                    break

        return {
            "status": "ok",
            "event": event_name,
            "lead_id": lead_id,
            "completed_assignments": completed,
            "assigned_group_id": assigned_group_id,
        }

    def process_due_reassignments(self) -> dict[str, Any]:
        due_rows = self.database.fetch_all(
            """
            SELECT *
            FROM lead_assignments
            WHERE status = 'waiting' AND due_at <= ?
            ORDER BY due_at ASC, id ASC
            """,
            (self._iso_now(),),
        )

        processed = 0
        reassigned = 0
        completed = 0
        for assignment in due_rows:
            processed += 1
            portal = self._get_portal(str(assignment["portal_member_id"]))
            client = self._make_client(portal)
            lead = client.get_lead(int(assignment["lead_id"]))
            lead_status_id = str(lead.get("STATUS_ID") or "")

            if lead_status_id != str(assignment["initial_stage_id"]):
                self._complete_assignment(int(assignment["id"]), "taken_in_work", lead_status_id)
                completed += 1
                self._persist_portal_tokens(client.auth)
                continue

            group = self.database.fetch_one(
                """
                SELECT *
                FROM distribution_groups
                WHERE id = ? AND is_active = 1
                """,
                (int(assignment["group_id"]),),
            )
            members = self._list_group_members(int(assignment["group_id"]))
            next_index = int(assignment["current_member_index"]) + 1
            if group is None or next_index >= len(members):
                self._complete_assignment(int(assignment["id"]), "exhausted", lead_status_id)
                completed += 1
                self._persist_portal_tokens(client.auth)
                continue

            next_member = members[next_index]
            client.update_lead(int(assignment["lead_id"]), {"ASSIGNED_BY_ID": int(next_member["bitrix_user_id"])})
            self._persist_portal_tokens(client.auth)
            due_at = self.now_fn() + timedelta(seconds=int(group["timeout_seconds"]))
            self.database.execute(
                """
                UPDATE lead_assignments
                SET current_member_index = ?, current_user_id = ?, due_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    next_index,
                    int(next_member["bitrix_user_id"]),
                    _iso(due_at),
                    self._iso_now(),
                    int(assignment["id"]),
                ),
            )
            self._record_history(
                assignment_id=int(assignment["id"]),
                action="reassigned",
                from_user_id=int(assignment["current_user_id"]),
                to_user_id=int(next_member["bitrix_user_id"]),
                lead_status_id=lead_status_id,
                payload={"source": "timeout_worker"},
            )
            reassigned += 1

        return {
            "processed": processed,
            "reassigned": reassigned,
            "completed": completed,
        }

    def _assign_first_member(
        self,
        *,
        portal: PortalAuth,
        client: Any,
        group: sqlite3.Row,
        lead_id: int,
        lead_status_id: str,
    ) -> int | None:
        members = self._list_group_members(int(group["id"]))
        if not members:
            return None
        first_member = members[0]
        client.update_lead(lead_id, {"ASSIGNED_BY_ID": int(first_member["bitrix_user_id"])})
        self._persist_portal_tokens(client.auth)

        now = self.now_fn()
        assignment_id = self.database.execute(
            """
            INSERT INTO lead_assignments (
                portal_member_id, group_id, lead_id, status, current_member_index,
                current_user_id, initial_stage_id, due_at, created_at, updated_at
            )
            VALUES (?, ?, ?, 'waiting', 0, ?, ?, ?, ?, ?)
            """,
            (
                portal.member_id,
                int(group["id"]),
                int(lead_id),
                int(first_member["bitrix_user_id"]),
                str(group["initial_stage_id"]),
                _iso(now + timedelta(seconds=int(group["timeout_seconds"]))),
                _iso(now),
                _iso(now),
            ),
        )
        self._record_history(
            assignment_id=assignment_id,
            action="assigned",
            from_user_id=None,
            to_user_id=int(first_member["bitrix_user_id"]),
            lead_status_id=lead_status_id,
            payload={"group_id": int(group["id"])},
        )
        return int(group["id"])

    def _complete_if_taken_in_work(self, portal_member_id: str, lead_id: int, lead_status_id: str) -> int:
        if not lead_status_id:
            return 0
        rows = self.database.fetch_all(
            """
            SELECT id, initial_stage_id
            FROM lead_assignments
            WHERE portal_member_id = ? AND lead_id = ? AND status = 'waiting'
            """,
            (portal_member_id, int(lead_id)),
        )
        completed = 0
        for row in rows:
            if lead_status_id == str(row["initial_stage_id"]):
                continue
            self._complete_assignment(int(row["id"]), "taken_in_work", lead_status_id)
            completed += 1
        return completed

    def _complete_assignment(self, assignment_id: int, reason: str, lead_status_id: str) -> None:
        now = self._iso_now()
        self.database.execute(
            """
            UPDATE lead_assignments
            SET status = 'completed', completion_reason = ?, completed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (reason, now, now, assignment_id),
        )
        self._record_history(
            assignment_id=assignment_id,
            action="completed",
            from_user_id=None,
            to_user_id=None,
            lead_status_id=lead_status_id,
            payload={"reason": reason},
        )

    def _record_history(
        self,
        *,
        assignment_id: int,
        action: str,
        from_user_id: int | None,
        to_user_id: int | None,
        lead_status_id: str | None,
        payload: dict[str, Any],
    ) -> None:
        self.database.execute(
            """
            INSERT INTO assignment_history (
                assignment_id, action, from_user_id, to_user_id, lead_status_id, payload_raw, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                assignment_id,
                action,
                from_user_id,
                to_user_id,
                lead_status_id,
                to_json(payload),
                self._iso_now(),
            ),
        )

    def _list_candidate_groups(self, portal_member_id: str, event_name: str) -> list[sqlite3.Row]:
        if event_name == "ONCRMLEADADD":
            clause = "event_on_add = 1"
        else:
            clause = "event_on_update = 1"
        return self.database.fetch_all(
            f"""
            SELECT *
            FROM distribution_groups
            WHERE portal_member_id = ? AND is_active = 1 AND {clause}
            ORDER BY priority ASC, id ASC
            """,
            (portal_member_id,),
        )

    def _list_group_members(self, group_id: int) -> list[dict[str, Any]]:
        rows = self.database.fetch_all(
            """
            SELECT bitrix_user_id, sort_order, is_active
            FROM group_members
            WHERE group_id = ? AND is_active = 1
            ORDER BY sort_order ASC, id ASC
            """,
            (group_id,),
        )
        return [dict(row) for row in rows]

    def _get_open_assignment(self, portal_member_id: str, group_id: int, lead_id: int) -> sqlite3.Row | None:
        return self.database.fetch_one(
            """
            SELECT *
            FROM lead_assignments
            WHERE portal_member_id = ? AND group_id = ? AND lead_id = ? AND status = 'waiting'
            """,
            (portal_member_id, group_id, lead_id),
        )

    def _extract_auth_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if "member_id" in payload and "domain" in payload:
            return payload
        auth = payload.get("auth")
        if isinstance(auth, dict) and "member_id" in auth and "domain" in auth:
            return auth
        raise ValueError("Bitrix auth payload is missing member_id/domain")

    def _resolve_portal_for_payload(self, payload: dict[str, Any]) -> PortalAuth:
        auth = payload.get("auth")
        if isinstance(auth, dict) and auth.get("member_id") and auth.get("domain"):
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
            return self._get_portal(portal.member_id)

        member_id = str(payload.get("member_id") or "").strip()
        if member_id:
            return self._get_portal(member_id)
        raise ValueError("Unable to resolve portal for Bitrix payload")

    def _upsert_portal(self, portal: PortalAuth) -> None:
        now = self._iso_now()
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
                client_endpoint, server_endpoint, status, expires_at, installed_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                None,
                now,
                now,
            ),
        )

    def _persist_portal_tokens(self, portal: PortalAuth) -> None:
        self.database.execute(
            """
            UPDATE portals
            SET access_token = ?, refresh_token = ?, client_endpoint = ?, server_endpoint = ?, status = ?, updated_at = ?
            WHERE member_id = ?
            """,
            (
                portal.access_token,
                portal.refresh_token,
                portal.client_endpoint,
                portal.server_endpoint,
                portal.status,
                self._iso_now(),
                portal.member_id,
            ),
        )

    def _get_portal(self, portal_member_id: str) -> PortalAuth:
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

    def _make_client(self, portal: PortalAuth) -> Any:
        return self.client_factory(portal)

    def _default_client_factory(self, portal: PortalAuth) -> BitrixClient:
        return BitrixClient(
            portal,
            client_id=self.settings.bitrix_client_id,
            client_secret=self.settings.bitrix_client_secret,
        )

    def _iso_now(self) -> str:
        return _iso(self.now_fn())


def _extract_lead_id(payload: dict[str, Any]) -> int:
    lead_id = (((payload.get("data") or {}).get("FIELDS") or {}).get("ID"))
    parsed = _safe_int(lead_id)
    if parsed is None:
        raise ValueError("Bitrix event payload does not contain lead ID")
    return parsed


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def group_input_from_payload(payload: dict[str, Any]) -> DistributionGroupInput:
    members = [
        GroupMemberInput(
            user_id=int(item["user_id"]),
            sort_order=int(item.get("sort_order", index + 1)),
            is_active=bool(item.get("is_active", True)),
        )
        for index, item in enumerate(payload.get("members", []))
    ]
    return DistributionGroupInput(
        group_id=_safe_int(payload.get("group_id")),
        portal_member_id=str(payload["portal_member_id"]),
        name=str(payload["name"]),
        initial_stage_id=str(payload["initial_stage_id"]),
        timeout_seconds=int(payload["timeout_seconds"]),
        priority=int(payload.get("priority", 1)),
        event_on_add=bool(payload.get("event_on_add", True)),
        event_on_update=bool(payload.get("event_on_update", True)),
        is_active=bool(payload.get("is_active", True)),
        members=members,
    )


def group_to_payload(group: DistributionGroupInput) -> dict[str, Any]:
    return {
        **asdict(group),
        "members": [asdict(member) for member in group.members],
    }
