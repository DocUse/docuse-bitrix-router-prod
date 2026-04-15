from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from bitrix_taxi_router.bitrix_api import BitrixApiError
from bitrix_taxi_router.database import Database
from bitrix_taxi_router.service import PortalService


class FakeBitrixClient:
    def __init__(self, responses: dict[str, object]) -> None:
        self.responses = responses

    def call(self, method: str, params: dict[str, object] | None = None) -> dict[str, object]:
        _ = params
        response = self.responses.get(method)
        if isinstance(response, Exception):
            raise response
        if not isinstance(response, dict):
            raise AssertionError(f"Expected dict response for {method}")
        return response

    def call_list(self, method: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
        _ = params
        response = self.responses.get(method)
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


if __name__ == "__main__":
    unittest.main()
