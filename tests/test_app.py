from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fastapi.testclient import TestClient

from bitrix_taxi_router.app import create_app
from bitrix_taxi_router.settings import Settings


class FakeBitrixClient:
    def __init__(self, responses: dict[str, object]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, object] | None]] = []

    def call(self, method: str, params: dict[str, object] | None = None) -> dict[str, object]:
        self.calls.append(("call", method, params))
        response = self.responses.get(method)
        if callable(response):
            response = response(params)
        if not isinstance(response, dict):
            raise AssertionError(f"Expected dict response for {method}")
        return response

    def call_list(self, method: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
        self.calls.append(("call_list", method, params))
        response = self.responses.get(method)
        if callable(response):
            response = response(params)
        if not isinstance(response, list):
            raise AssertionError(f"Expected list response for {method}")
        return response


class AppUiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.settings = Settings(
            app_env="test",
            app_host="127.0.0.1",
            app_port=8000,
            db_path=Path(self.temp_dir.name) / "data.sqlite3",
        )
        self.client = TestClient(create_app(self.settings))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_groups_ui_get_embeds_member_id_from_query_string(self) -> None:
        response = self.client.get("/ui/groups?member_id=portal-123")

        self.assertEqual(200, response.status_code)
        self.assertIn('const initialDistributionMemberId = "portal-123";', response.text)

    def test_groups_ui_post_embeds_member_id_from_bitrix_payload(self) -> None:
        response = self.client.post(
            "/ui/groups",
            data={
                "AUTH_ID": "token-1",
                "REFRESH_ID": "refresh-1",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "MEMBER_ID": "portal-456",
            },
        )

        self.assertEqual(200, response.status_code)
        self.assertIn('const initialDistributionMemberId = "portal-456";', response.text)

    def test_groups_ui_contains_bitrix_auth_fallback_and_hidden_grid_rule(self) -> None:
        response = self.client.get("/ui/groups")

        self.assertEqual(200, response.status_code)
        self.assertIn("Распределение сделок", response.text)
        self.assertIn('id="statsPanel"', response.text)
        self.assertIn('id="statsDiagnosticsList"', response.text)
        self.assertIn('id="statsJournalList"', response.text)
        self.assertIn('id="distributionGroupsPanel"', response.text)
        self.assertIn('id="createDistributionGroupButton"', response.text)
        self.assertIn('id="distributionForm"', response.text)
        self.assertIn('id="participantsList"', response.text)
        self.assertIn('id="loadStagesList"', response.text)
        self.assertIn("distribution-scroll-box--participants", response.text)
        self.assertIn("distribution-scroll-box--stages", response.text)
        self.assertIn("load-stages-layout", response.text)
        self.assertIn("BX24.getAuth", response.text)
        self.assertIn("BX24.init", response.text)
        self.assertIn("/api/ui/groups/portal-context", response.text)
        self.assertIn("/api/ui/groups/config", response.text)

    def test_portal_context_endpoint_saves_portal_from_bitrix_auth_payload(self) -> None:
        response = self.client.post(
            "/api/ui/groups/portal-context",
            json={
                "AUTH_ID": "token-1",
                "REFRESH_ID": "refresh-1",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "member_id": "portal-789",
            },
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "status": "ok",
                "portal": {
                    "member_id": "portal-789",
                    "domain": "portal.example.bitrix24.ru",
                    "saved": True,
                },
                "event_binding": None,
            },
            response.json(),
        )

        stored = self.client.app.state.portal_service.get_portal("portal-789")
        self.assertEqual("portal.example.bitrix24.ru", stored.domain)
        self.assertEqual("token-1", stored.access_token)
        self.assertEqual("refresh-1", stored.refresh_token)
        self.assertEqual("https://portal.example.bitrix24.ru/rest/", stored.client_endpoint)

    def test_distribution_group_config_endpoint_saves_and_returns_config(self) -> None:
        self.client.post(
            "/api/ui/groups/portal-context",
            json={
                "AUTH_ID": "token-1",
                "REFRESH_ID": "refresh-1",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "member_id": "portal-789",
            },
        )

        save_response = self.client.post(
            "/api/ui/groups/config?member_id=portal-789",
            json={
                "name": "Основная группа",
                "distribution_type": "round_robin_load_time",
                "event_type": "deal_created",
                "distribution_stage_id": "NEW",
                "load_stage_ids": ["NEW", "IN_PROGRESS"],
                "responsible_field_id": "ASSIGNED_BY_ID",
                "wait_seconds": 120,
                "retry_interval_seconds": 30,
                "is_active": True,
                "members": [
                    {"user_id": "10", "limit": 3},
                    {"user_id": "20", "limit": 5},
                ],
            },
        )

        self.assertEqual(200, save_response.status_code)
        self.assertEqual("ok", save_response.json()["status"])
        self.assertEqual("Основная группа", save_response.json()["config"]["name"])

        get_response = self.client.get("/api/ui/groups/config?member_id=portal-789")
        self.assertEqual(200, get_response.status_code)
        self.assertEqual("Основная группа", get_response.json()["config"]["name"])
        self.assertEqual(
            [
                {"user_id": "10", "limit": 3, "sort_order": 0},
                {"user_id": "20", "limit": 5, "sort_order": 1},
            ],
            get_response.json()["config"]["members"],
        )

    def test_stats_endpoint_returns_distribution_runtime_journal(self) -> None:
        self.client.post(
            "/api/ui/groups/portal-context",
            json={
                "AUTH_ID": "token-1",
                "REFRESH_ID": "refresh-1",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "member_id": "portal-789",
            },
        )
        self.client.app.state.portal_service.save_distribution_group(
            "portal-789",
            {
                "name": "Основная группа",
                "distribution_type": "round_robin_load_time",
                "event_type": "deal_created",
                "distribution_stage_id": "NEW",
                "load_stage_ids": ["NEW"],
                "responsible_field_id": "ASSIGNED_BY_ID",
                "wait_seconds": 120,
                "retry_interval_seconds": 30,
                "is_active": True,
                "members": [{"user_id": "10", "limit": 3}],
            },
        )
        database = self.client.app.state.database
        database.execute(
            """
            INSERT INTO distribution_deal_runtime (
                portal_member_id, deal_id, event_type, status, assigned_user_id,
                assigned_field_id, note, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "portal-789",
                "700",
                "deal_created",
                "assigned",
                "10",
                "ASSIGNED_BY_ID",
                "Deal assigned after load calculation",
                "2026-04-16T09:00:00+00:00",
                "2026-04-16T09:01:00+00:00",
            ),
        )
        database.execute(
            """
            INSERT INTO distribution_member_runtime (
                portal_member_id, user_id, last_assigned_deal_id, last_assigned_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "portal-789",
                "10",
                "700",
                "2026-04-16T09:01:00+00:00",
                "2026-04-16T09:01:00+00:00",
            ),
        )
        database.execute(
            """
            INSERT INTO diagnostic_logs (
                portal_member_id, deal_id, level, source, message, details_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "portal-789",
                "700",
                "info",
                "bitrix_event_endpoint",
                "Received POST /api/bitrix/events hit.",
                '{"content_type":"application/x-www-form-urlencoded"}',
                "2026-04-16T09:00:30+00:00",
            ),
        )

        response = self.client.get("/api/ui/stats?member_id=portal-789")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(1, payload["summary"]["journal_count"])
        self.assertGreaterEqual(payload["summary"]["diagnostic_count"], 1)
        self.assertEqual(1, payload["summary"]["assigned_count"])
        self.assertEqual("700", payload["journal"][0]["deal_id"])
        self.assertEqual("10", payload["journal"][0]["assigned_user_id"])
        self.assertEqual("10", payload["journal"][0]["assigned_user_name"])
        self.assertEqual("700", payload["members"][0]["last_assigned_deal_id"])
        self.assertIn(
            "Received POST /api/bitrix/events hit.",
            [item["message"] for item in payload["diagnostics"]],
        )

    def test_distribution_group_config_endpoint_binds_event_for_public_https_request(self) -> None:
        fake_client = FakeBitrixClient(
            {
                "event.get": {"result": []},
                "event.bind": {"result": True},
            }
        )
        self.client.app.state.portal_service.bitrix_client_factory = lambda portal: fake_client
        self.client.post(
            "/api/ui/groups/portal-context",
            json={
                "AUTH_ID": "token-1",
                "REFRESH_ID": "refresh-1",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "member_id": "portal-789",
            },
        )

        response = self.client.post(
            "/api/ui/groups/config?member_id=portal-789",
            headers={
                "x-forwarded-proto": "https",
                "x-forwarded-host": "router.example.com",
            },
            json={
                "name": "Основная группа",
                "distribution_type": "round_robin_load_time",
                "event_type": "deal_created",
                "distribution_stage_id": "NEW",
                "load_stage_ids": ["NEW"],
                "responsible_field_id": "ASSIGNED_BY_ID",
                "wait_seconds": 120,
                "retry_interval_seconds": 30,
                "is_active": True,
                "members": [{"user_id": "10", "limit": 3}],
            },
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "event": "ONCRMDEALADD",
                "handler": "https://router.example.com/api/bitrix/events",
                "already_bound": False,
                "bound": True,
                "configured": True,
            },
            response.json()["event_binding"],
        )

    def test_distribution_group_config_endpoint_uses_public_base_url_for_binding(self) -> None:
        self.settings.public_base_url = "https://router.example.com/base/"
        fake_client = FakeBitrixClient(
            {
                "event.get": {"result": []},
                "event.bind": {"result": True},
            }
        )
        self.client.app.state.portal_service.bitrix_client_factory = lambda portal: fake_client
        self.client.post(
            "/api/ui/groups/portal-context",
            json={
                "AUTH_ID": "token-1",
                "REFRESH_ID": "refresh-1",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "member_id": "portal-789",
            },
        )

        response = self.client.post(
            "/api/ui/groups/config?member_id=portal-789",
            json={
                "name": "Основная группа",
                "distribution_type": "round_robin_load_time",
                "event_type": "deal_created",
                "distribution_stage_id": "NEW",
                "load_stage_ids": ["NEW"],
                "responsible_field_id": "ASSIGNED_BY_ID",
                "wait_seconds": 120,
                "retry_interval_seconds": 30,
                "is_active": True,
                "members": [{"user_id": "10", "limit": 3}],
            },
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "event": "ONCRMDEALADD",
                "handler": "https://router.example.com/base/api/bitrix/events",
                "already_bound": False,
                "bound": True,
                "configured": True,
            },
            response.json()["event_binding"],
        )

    def test_portal_context_endpoint_rebinds_existing_configured_group(self) -> None:
        self.settings.public_base_url = "https://router.example.com"
        fake_client = FakeBitrixClient(
            {
                "event.get": {"result": []},
                "event.bind": {"result": True},
            }
        )
        self.client.app.state.portal_service.bitrix_client_factory = lambda portal: fake_client
        self.client.post(
            "/api/ui/groups/portal-context",
            json={
                "AUTH_ID": "token-1",
                "REFRESH_ID": "refresh-1",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "member_id": "portal-789",
            },
        )
        self.client.app.state.portal_service.save_distribution_group(
            "portal-789",
            {
                "name": "Основная группа",
                "distribution_type": "round_robin_load_time",
                "event_type": "deal_created",
                "distribution_stage_id": "NEW",
                "load_stage_ids": ["NEW"],
                "responsible_field_id": "ASSIGNED_BY_ID",
                "wait_seconds": 120,
                "retry_interval_seconds": 30,
                "is_active": True,
                "members": [{"user_id": "10", "limit": 3}],
            },
        )
        fake_client.calls.clear()

        response = self.client.post(
            "/api/ui/groups/portal-context",
            json={
                "AUTH_ID": "token-2",
                "REFRESH_ID": "refresh-2",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "member_id": "portal-789",
            },
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "event": "ONCRMDEALADD",
                "handler": "https://router.example.com/api/bitrix/events",
                "already_bound": False,
                "bound": True,
                "configured": True,
            },
            response.json()["event_binding"],
        )
        self.assertEqual(
            [
                ("call", "event.get", None),
                (
                    "call",
                    "event.bind",
                    {
                        "event": "ONCRMDEALADD",
                        "handler": "https://router.example.com/api/bitrix/events",
                    },
                ),
            ],
            fake_client.calls,
        )

    def test_bitrix_event_endpoint_assigns_deal(self) -> None:
        fake_client = FakeBitrixClient(
            {
                "crm.item.get": {"result": {"item": {"id": 700, "stageId": "NEW"}}},
                "crm.item.list": [],
                "crm.item.update": {"result": {"item": {"id": 700}}},
            }
        )
        self.client.app.state.portal_service.bitrix_client_factory = lambda portal: fake_client
        self.client.post(
            "/api/ui/groups/portal-context",
            json={
                "AUTH_ID": "token-1",
                "REFRESH_ID": "refresh-1",
                "DOMAIN": "portal.example.bitrix24.ru",
                "PROTOCOL": "1",
                "member_id": "portal-789",
            },
        )
        self.client.post(
            "/api/ui/groups/config?member_id=portal-789",
            json={
                "name": "Основная группа",
                "distribution_type": "round_robin_load_time",
                "event_type": "deal_created",
                "distribution_stage_id": "NEW",
                "load_stage_ids": ["NEW"],
                "responsible_field_id": "ASSIGNED_BY_ID",
                "wait_seconds": 120,
                "retry_interval_seconds": 30,
                "is_active": True,
                "members": [{"user_id": "10", "limit": 3}],
            },
        )

        response = self.client.post(
            "/api/bitrix/events",
            data={
                "event": "ONCRMDEALADD",
                "data[FIELDS][ID]": "700",
                "auth[member_id]": "portal-789",
                "auth[domain]": "portal.example.bitrix24.ru",
            },
        )

        self.assertEqual(200, response.status_code)
        self.assertEqual("assigned", response.json()["result"]["status"])
        self.assertEqual("10", response.json()["result"]["assigned_user_id"])

    def test_bitrix_event_probe_is_available_via_get(self) -> None:
        response = self.client.get("/api/bitrix/events")

        self.assertEqual(200, response.status_code)
        self.assertEqual({"status": "ready"}, response.json())

    def test_config_endpoint_works_with_legacy_distribution_groups_table_present(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        try:
            db_path = Path(temp_dir.name) / "legacy.sqlite3"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE portals (
                    member_id TEXT PRIMARY KEY,
                    domain TEXT NOT NULL,
                    application_token TEXT,
                    access_token TEXT,
                    refresh_token TEXT,
                    client_endpoint TEXT,
                    server_endpoint TEXT,
                    status TEXT,
                    installed_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE distribution_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portal_member_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    initial_stage_id TEXT NOT NULL,
                    timeout_seconds INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    event_on_add INTEGER NOT NULL,
                    event_on_update INTEGER NOT NULL,
                    is_active INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO portals (
                    member_id, domain, application_token, access_token, refresh_token,
                    client_endpoint, server_endpoint, status, installed_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "portal-legacy",
                    "portal.example.bitrix24.ru",
                    None,
                    "token-1",
                    "refresh-1",
                    "https://portal.example.bitrix24.ru/rest/",
                    "https://oauth.bitrix.info/rest/",
                    "T",
                    "now",
                    "now",
                ),
            )
            connection.commit()
            connection.close()

            settings = Settings(
                app_env="test",
                app_host="127.0.0.1",
                app_port=8000,
                db_path=db_path,
            )
            client = TestClient(create_app(settings))

            response = client.get("/api/ui/groups/config?member_id=portal-legacy")
            self.assertEqual(200, response.status_code)
            self.assertEqual({"config": None}, response.json())
        finally:
            temp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
