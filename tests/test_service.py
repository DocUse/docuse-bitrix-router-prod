from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bitrix_taxi_router.contracts import PortalAuth
from bitrix_taxi_router.database import Database
from bitrix_taxi_router.service import AssignmentService, group_input_from_payload
from bitrix_taxi_router.settings import Settings


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.current = start

    def now(self) -> datetime:
        return self.current

    def advance(self, *, minutes: int = 0, seconds: int = 0) -> None:
        self.current += timedelta(minutes=minutes, seconds=seconds)


class FakeBitrixClient:
    def __init__(self, auth: PortalAuth, leads: dict[int, dict[str, str]]) -> None:
        self.auth = auth
        self.leads = leads
        self.updated: list[tuple[int, dict[str, object]]] = []

    def get_lead(self, lead_id: int) -> dict[str, str]:
        return dict(self.leads[lead_id])

    def update_lead(self, lead_id: int, fields: dict[str, object]) -> dict[str, object]:
        self.updated.append((lead_id, dict(fields)))
        lead = self.leads[lead_id]
        for key, value in fields.items():
            lead[key] = str(value)
        return {"result": True}

    def list_users(self) -> list[dict[str, object]]:
        return [
            {"ID": "101", "NAME": "Ирина", "LAST_NAME": "Иванова", "ACTIVE": "Y"},
            {"ID": "102", "NAME": "Ольга", "LAST_NAME": "Петрова", "ACTIVE": "Y"},
        ]

    def bind_event(self, event_name: str, handler_url: str) -> dict[str, object]:
        return {"result": {"event": event_name, "handler": handler_url}}


class AssignmentServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.settings = Settings(
            app_env="test",
            app_base_url="https://example.test",
            app_host="127.0.0.1",
            app_port=8000,
            db_path=self.root / "data.sqlite3",
            bitrix_client_id="app-id",
            bitrix_client_secret="app-secret",
        )
        self.database = Database(self.settings.db_path)
        self.database.init_schema()
        self.clock = FakeClock(datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc))
        self.leads: dict[int, dict[str, str]] = {}
        self.service = AssignmentService(
            self.database,
            self.settings,
            now_fn=self.clock.now,
            client_factory=lambda auth: FakeBitrixClient(auth, self.leads),
        )
        self.service.install_portal(
            {
                "member_id": "portal-1",
                "domain": "portal.example.bitrix24.ru",
                "access_token": "token-1",
                "refresh_token": "refresh-1",
                "client_endpoint": "https://portal.example.bitrix24.ru/rest",
                "server_endpoint": "https://oauth.bitrix24.tech/rest",
                "application_token": "app-token",
                "status": "S",
            }
        )
        self.group_id = self.service.save_group(
            group_input_from_payload(
                {
                    "portal_member_id": "portal-1",
                    "name": "Рекрутеры",
                    "initial_stage_id": "NEW",
                    "timeout_seconds": 60,
                    "members": [
                        {"user_id": 101, "sort_order": 1},
                        {"user_id": 102, "sort_order": 2},
                    ],
                }
            )
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_assigns_first_recruiter_on_new_lead(self) -> None:
        self.leads[5001] = {"ID": "5001", "STATUS_ID": "NEW", "ASSIGNED_BY_ID": "999"}

        result = self.service.process_event(
            {
                "event": "ONCRMLEADADD",
                "data": {"FIELDS": {"ID": "5001"}},
                "auth": {"member_id": "portal-1", "domain": "portal.example.bitrix24.ru"},
            }
        )

        self.assertEqual("ok", result["status"])
        self.assertEqual(self.group_id, result["assigned_group_id"])
        self.assertEqual("101", self.leads[5001]["ASSIGNED_BY_ID"])

        row = self.database.fetch_one(
            "SELECT status, current_member_index, current_user_id FROM lead_assignments WHERE lead_id = ?",
            (5001,),
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("waiting", row["status"])
        self.assertEqual(0, row["current_member_index"])
        self.assertEqual(101, row["current_user_id"])

    def test_completes_assignment_when_lead_moves_from_initial_stage(self) -> None:
        self.leads[5002] = {"ID": "5002", "STATUS_ID": "NEW", "ASSIGNED_BY_ID": "999"}
        self.service.process_event(
            {
                "event": "ONCRMLEADADD",
                "data": {"FIELDS": {"ID": "5002"}},
                "auth": {"member_id": "portal-1", "domain": "portal.example.bitrix24.ru"},
            }
        )

        self.leads[5002]["STATUS_ID"] = "IN_WORK"
        result = self.service.process_event(
            {
                "event": "ONCRMLEADUPDATE",
                "data": {"FIELDS": {"ID": "5002"}},
                "auth": {"member_id": "portal-1", "domain": "portal.example.bitrix24.ru"},
            }
        )

        self.assertEqual(1, result["completed_assignments"])
        row = self.database.fetch_one(
            "SELECT status, completion_reason FROM lead_assignments WHERE lead_id = ?",
            (5002,),
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("completed", row["status"])
        self.assertEqual("taken_in_work", row["completion_reason"])

    def test_reassigns_to_next_recruiter_after_timeout(self) -> None:
        self.leads[5003] = {"ID": "5003", "STATUS_ID": "NEW", "ASSIGNED_BY_ID": "999"}
        self.service.process_event(
            {
                "event": "ONCRMLEADADD",
                "data": {"FIELDS": {"ID": "5003"}},
                "auth": {"member_id": "portal-1", "domain": "portal.example.bitrix24.ru"},
            }
        )

        self.clock.advance(minutes=2)
        result = self.service.process_due_reassignments()

        self.assertEqual(1, result["processed"])
        self.assertEqual(1, result["reassigned"])
        self.assertEqual("102", self.leads[5003]["ASSIGNED_BY_ID"])

        row = self.database.fetch_one(
            "SELECT status, current_member_index, current_user_id FROM lead_assignments WHERE lead_id = ?",
            (5003,),
        )
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("waiting", row["status"])
        self.assertEqual(1, row["current_member_index"])
        self.assertEqual(102, row["current_user_id"])

    def test_delete_group_marks_open_assignments_completed(self) -> None:
        self.leads[5004] = {"ID": "5004", "STATUS_ID": "NEW", "ASSIGNED_BY_ID": "999"}
        self.service.process_event(
            {
                "event": "ONCRMLEADADD",
                "data": {"FIELDS": {"ID": "5004"}},
                "auth": {"member_id": "portal-1", "domain": "portal.example.bitrix24.ru"},
            }
        )

        deleted = self.service.delete_group("portal-1", self.group_id)

        self.assertTrue(deleted)
        group_row = self.database.fetch_one("SELECT id FROM distribution_groups WHERE id = ?", (self.group_id,))
        self.assertIsNone(group_row)
        assignment_row = self.database.fetch_one(
            "SELECT status, completion_reason FROM lead_assignments WHERE lead_id = ?",
            (5004,),
        )
        self.assertIsNotNone(assignment_row)
        assert assignment_row is not None
        self.assertEqual("completed", assignment_row["status"])
        self.assertEqual("group_deleted", assignment_row["completion_reason"])

    def test_event_without_tokens_does_not_erase_saved_portal_tokens(self) -> None:
        self.leads[5005] = {"ID": "5005", "STATUS_ID": "NEW", "ASSIGNED_BY_ID": "999"}

        self.service.process_event(
            {
                "event": "ONCRMLEADADD",
                "data": {"FIELDS": {"ID": "5005"}},
                "auth": {"member_id": "portal-1", "domain": "portal.example.bitrix24.ru"},
            }
        )

        portal_row = self.database.fetch_one(
            "SELECT access_token, refresh_token, client_endpoint FROM portals WHERE member_id = ?",
            ("portal-1",),
        )
        self.assertIsNotNone(portal_row)
        assert portal_row is not None
        self.assertEqual("token-1", portal_row["access_token"])
        self.assertEqual("refresh-1", portal_row["refresh_token"])
        self.assertEqual("https://portal.example.bitrix24.ru/rest", portal_row["client_endpoint"])


if __name__ == "__main__":
    unittest.main()
