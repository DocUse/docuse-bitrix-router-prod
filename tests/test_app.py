from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
