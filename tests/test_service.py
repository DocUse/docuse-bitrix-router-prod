from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bitrix_taxi_router.bitrix_api import BitrixApiError
from bitrix_taxi_router.bitrix_api import BitrixClient
from bitrix_taxi_router.database import Database
from bitrix_taxi_router.service import PortalService
from bitrix_taxi_router.services.assignment import count_member_load


class FakeBitrixClient:
    def __init__(self, responses: dict[str, object]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, object] | None]] = []

    def _resolve(self, method: str, params: dict[str, object] | None) -> object:
        self.calls.append(("call", method, params))
        response = self.responses.get(method)
        if callable(response):
            return response(params)
        return response

    def call(self, method: str, params: dict[str, object] | None = None) -> dict[str, object]:
        response = self._resolve(method, params)
        if isinstance(response, Exception):
            raise response
        if not isinstance(response, dict):
            raise AssertionError(f"Expected dict response for {method}")
        return response

    def call_list(self, method: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
        self.calls.append(("call_list", method, params))
        response = self.responses.get(method)
        if callable(response):
            response = response(params)
        if isinstance(response, Exception):
            raise response
        if not isinstance(response, list):
            raise AssertionError(f"Expected list response for {method}")
        return response


class PortalServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.temp_dir.name) / "data.sqlite3")
        self.database.init_schema()
        self.service = PortalService(self.database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_bitrix_client_call_list_accepts_items_nested_under_result(self) -> None:
        client = BitrixClient.__new__(BitrixClient)
        responses = [
            {"result": {"items": [{"id": 1}, {"id": 2}]}, "next": 50},
            {"result": {"items": [{"id": 3}]}}
        ]

        def fake_call(method: str, params: dict[str, object] | None = None) -> dict[str, object]:
            self.assertEqual("crm.item.list", method)
            if params and "start" in params:
                self.assertEqual(50, params["start"])
            return responses.pop(0)

        client.call = fake_call  # type: ignore[method-assign]

        result = client.call_list("crm.item.list", {"entityTypeId": 2})

        self.assertEqual([{"id": 1}, {"id": 2}, {"id": 3}], result)

    def test_install_portal_saves_new_portal(self) -> None:
        result = self.service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "refresh_token": "refresh-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                    "server_endpoint": "https://oauth.bitrix24.tech/rest/",
                    "status": "F",
                }
            }
        )

        self.assertEqual(
            {"member_id": "portal-1", "domain": "portal.example.bitrix24.ru", "saved": True},
            result,
        )

        portal = self.service.get_portal("portal-1")
        self.assertEqual("portal.example.bitrix24.ru", portal.domain)
        self.assertEqual("token-1", portal.access_token)
        self.assertEqual("refresh-1", portal.refresh_token)

    def test_install_portal_updates_existing_portal_without_erasing_tokens(self) -> None:
        self.service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "refresh_token": "refresh-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                }
            }
        )

        self.service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                }
            }
        )

        portal = self.service.get_portal("portal-1")
        self.assertEqual("token-1", portal.access_token)
        self.assertEqual("refresh-1", portal.refresh_token)
        self.assertEqual("https://portal.example.bitrix24.ru/rest/", portal.client_endpoint)

    def test_get_reference_data_normalizes_users_stages_and_responsible_fields(self) -> None:
        service = PortalService(
            self.database,
            bitrix_client_factory=lambda portal: FakeBitrixClient(
                {
                    "user.get": [
                        {"ID": "20", "NAME": "Иван", "LAST_NAME": "Петров", "ACTIVE": "Y"},
                        {"ID": "30", "EMAIL": "solo@example.com", "ACTIVE": "N"},
                    ],
                    "crm.status.list": [
                        {"STATUS_ID": "NEW", "NAME": "Новая", "SORT": "20"},
                        {"STATUS_ID": "PREPAYMENT_INVOICE", "NAME": "Счет", "SORT": "40"},
                    ],
                    "crm.item.fields": {
                        "result": {
                            "fields": {
                                "assignedById": {
                                    "upperName": "ASSIGNED_BY_ID",
                                    "title": "Ответственный",
                                    "type": "user",
                                    "isReadOnly": False,
                                    "isMultiple": False,
                                },
                                "ufCrmManager": {
                                    "upperName": "UF_CRM_MANAGER",
                                    "title": "Менеджер",
                                    "type": "user",
                                    "isReadOnly": False,
                                    "isMultiple": False,
                                },
                                "createdBy": {
                                    "upperName": "CREATED_BY",
                                    "title": "Создал",
                                    "type": "user",
                                    "isReadOnly": True,
                                    "isMultiple": False,
                                },
                                "observers": {
                                    "upperName": "OBSERVERS",
                                    "title": "Наблюдатели",
                                    "type": "user",
                                    "isReadOnly": False,
                                    "isMultiple": True,
                                },
                                "title": {"upperName": "TITLE", "title": "Название", "type": "string"},
                            }
                        }
                    },
                }
            ),
        )
        service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                }
            }
        )

        payload = service.get_reference_data("portal-1")

        self.assertEqual(
            [
                {"id": "30", "name": "solo@example.com", "is_active": False, "source": "bitrix:user.get"},
                {"id": "20", "name": "Иван Петров", "is_active": True, "source": "bitrix:user.get"},
            ],
            payload["users"],
        )
        self.assertEqual(
            [
                {"id": "NEW", "name": "Новая", "sort": 20, "source": "bitrix:crm.status.list"},
                {
                    "id": "PREPAYMENT_INVOICE",
                    "name": "Счет",
                    "sort": 40,
                    "source": "bitrix:crm.status.list",
                },
            ],
            payload["stages"],
        )
        self.assertEqual(
            [
                {
                    "id": "ASSIGNED_BY_ID",
                    "name": "Ответственный",
                    "is_default": True,
                    "source": "bitrix:crm.item.fields",
                },
                {
                    "id": "UF_CRM_MANAGER",
                    "name": "Менеджер",
                    "is_default": False,
                    "source": "bitrix:crm.item.fields",
                },
            ],
            payload["responsible_fields"],
        )

    def test_list_responsible_fields_raises_when_bitrix_payload_is_invalid(self) -> None:
        service = PortalService(
            self.database,
            bitrix_client_factory=lambda portal: FakeBitrixClient({"crm.item.fields": {"result": []}}),
        )
        service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                }
            }
        )

        with self.assertRaises(BitrixApiError):
            service.list_responsible_fields("portal-1")

    def test_save_and_get_distribution_group_round_trip(self) -> None:
        self.service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                }
            }
        )

        saved = self.service.save_distribution_group(
            "portal-1",
            {
                "name": "Основная группа",
                "distribution_type": "round_robin_load_time",
                "event_type": "deal_created",
                "distribution_stage_id": "NEW",
                "load_stage_ids": ["NEW", "PREPAYMENT_INVOICE"],
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

        self.assertEqual("Основная группа", saved["name"])
        self.assertEqual("round_robin_load_time", saved["distribution_type"])
        self.assertEqual(["NEW", "PREPAYMENT_INVOICE"], saved["load_stage_ids"])
        self.assertEqual(
            [
                {"user_id": "10", "limit": 3, "sort_order": 0},
                {"user_id": "20", "limit": 5, "sort_order": 1},
            ],
            saved["members"],
        )

        loaded = self.service.get_distribution_group("portal-1")
        self.assertEqual(saved, loaded)

    def test_save_distribution_group_requires_selected_members_and_load_stages(self) -> None:
        self.service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                }
            }
        )

        with self.assertRaisesRegex(ValueError, "At least one distribution member is required"):
            self.service.save_distribution_group(
                "portal-1",
                {
                    "name": "Пустая группа",
                    "distribution_type": "round_robin_load_time",
                    "event_type": "deal_created",
                    "distribution_stage_id": "NEW",
                    "load_stage_ids": ["NEW"],
                    "responsible_field_id": "ASSIGNED_BY_ID",
                    "wait_seconds": 120,
                    "retry_interval_seconds": 30,
                    "is_active": True,
                    "members": [],
                },
            )

        with self.assertRaisesRegex(ValueError, "At least one load stage is required"):
            self.service.save_distribution_group(
                "portal-1",
                {
                    "name": "Без нагрузки",
                    "distribution_type": "round_robin_load_time",
                    "event_type": "deal_created",
                    "distribution_stage_id": "NEW",
                    "load_stage_ids": [],
                    "responsible_field_id": "ASSIGNED_BY_ID",
                    "wait_seconds": 120,
                    "retry_interval_seconds": 30,
                    "is_active": True,
                    "members": [{"user_id": "10", "limit": 1}],
                },
            )

    def test_ensure_deal_created_event_binding_skips_duplicate_handler(self) -> None:
        fake_client = FakeBitrixClient(
            {
                "event.get": {
                    "result": [
                        {
                            "event": "ONCRMDEALADD",
                            "handler": "https://app.example.com/api/bitrix/events/",
                        }
                    ]
                },
                "event.bind": {"result": True},
            }
        )
        service = PortalService(self.database, bitrix_client_factory=lambda portal: fake_client)
        service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                }
            }
        )

        result = service.ensure_deal_created_event_binding("portal-1", "https://app.example.com/api/bitrix/events")

        self.assertEqual(
            {
                "event": "ONCRMDEALADD",
                "handler": "https://app.example.com/api/bitrix/events",
                "already_bound": True,
                "bound": False,
            },
            result,
        )
        self.assertEqual([("call", "event.get", None)], fake_client.calls)

    def test_handle_bitrix_event_assigns_member_with_oldest_turn_when_load_is_equal(self) -> None:
        fake_client = FakeBitrixClient(
            {
                "crm.item.get": {"result": {"item": {"id": 501, "stageId": "NEW"}}},
                "crm.item.list": [
                    {"id": 1, "stageId": "NEW", "assignedById": 10},
                    {"id": 2, "stageId": "IN_PROGRESS", "assignedById": 10},
                    {"id": 3, "stageId": "NEW", "assignedById": 20},
                    {"id": 4, "stageId": "IN_PROGRESS", "assignedById": 20},
                ],
                "crm.item.update": {"result": {"item": {"id": 501}}},
            }
        )
        service = PortalService(self.database, bitrix_client_factory=lambda portal: fake_client)
        service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                }
            }
        )
        service.save_distribution_group(
            "portal-1",
            {
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
                    {"user_id": "10", "limit": 5},
                    {"user_id": "20", "limit": 5},
                ],
            },
        )
        self.database.execute(
            """
            INSERT INTO distribution_member_runtime (
                portal_member_id, user_id, last_assigned_deal_id, last_assigned_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            ("portal-1", "10", "400", "2026-04-15T09:00:00+00:00", "2026-04-15T09:00:00+00:00"),
        )
        self.database.execute(
            """
            INSERT INTO distribution_member_runtime (
                portal_member_id, user_id, last_assigned_deal_id, last_assigned_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            ("portal-1", "20", "300", "2026-04-15T08:00:00+00:00", "2026-04-15T08:00:00+00:00"),
        )

        result = service.handle_bitrix_event(
            {
                "event": "ONCRMDEALADD",
                "data": {"FIELDS": {"ID": "501"}},
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                },
            }
        )

        self.assertEqual("assigned", result["status"])
        self.assertEqual("20", result["assigned_user_id"])
        update_calls = [call for call in fake_client.calls if call[1] == "crm.item.update"]
        self.assertEqual(1, len(update_calls))
        self.assertEqual(
            {
                "entityTypeId": 2,
                "id": 501,
                "fields": {"assignedById": 20},
                "useOriginalUfNames": "Y",
            },
            update_calls[0][2],
        )

    def test_handle_bitrix_event_marks_deal_waiting_when_everyone_hit_limit(self) -> None:
        fake_client = FakeBitrixClient(
            {
                "crm.item.get": {"result": {"item": {"id": 700, "stageId": "NEW"}}},
                "crm.item.list": [
                    {"id": 1, "stageId": "NEW", "assignedById": 10},
                    {"id": 2, "stageId": "NEW", "assignedById": 10},
                ],
                "crm.item.update": {"result": {"item": {"id": 700}}},
            }
        )
        service = PortalService(self.database, bitrix_client_factory=lambda portal: fake_client)
        service.install_portal(
            {
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                    "access_token": "token-1",
                    "client_endpoint": "https://portal.example.bitrix24.ru/rest/",
                }
            }
        )
        service.save_distribution_group(
            "portal-1",
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
                "members": [
                    {"user_id": "10", "limit": 2},
                ],
            },
        )

        result = service.handle_bitrix_event(
            {
                "event": "ONCRMDEALADD",
                "data": {"FIELDS": {"ID": "700"}},
                "auth": {
                    "member_id": "portal-1",
                    "domain": "portal.example.bitrix24.ru",
                },
            }
        )

        self.assertEqual("waiting", result["status"])
        self.assertIsNone(result["assigned_user_id"])
        self.assertEqual([], [call for call in fake_client.calls if call[1] == "crm.item.update"])

    def test_count_member_load_accepts_stage_ids_with_funnel_prefix(self) -> None:
        fake_client = FakeBitrixClient(
            {
                "crm.item.list": [
                    {"id": 1, "stageId": "C10:NEW", "assignedById": 6496},
                    {"id": 2, "stageId": "C10:EXECUTING", "assignedById": 6496},
                    {"id": 3, "stageId": "C10:WON", "assignedById": 6496},
                ]
            }
        )

        result = count_member_load(fake_client, "ASSIGNED_BY_ID", ["NEW", "EXECUTING"], "6496")

        self.assertEqual(2, result)

    def test_count_member_load_uses_local_responsible_matching(self) -> None:
        fake_client = FakeBitrixClient(
            {
                "crm.item.list": [
                    {"id": 1, "stageId": "NEW", "assignedById": 6496},
                    {"id": 2, "stageId": "IN_PROGRESS", "assignedById": 6496},
                    {"id": 3, "stageId": "NEW", "assignedById": 9362},
                    {"id": 4, "stageId": "WON", "assignedById": 6496},
                ]
            }
        )

        result = count_member_load(fake_client, "ASSIGNED_BY_ID", ["NEW", "IN_PROGRESS"], "6496")

        self.assertEqual(2, result)


if __name__ == "__main__":
    unittest.main()
