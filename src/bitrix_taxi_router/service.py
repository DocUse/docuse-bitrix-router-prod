from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from .bitrix_api import BitrixClient
from .bitrix.normalizers import normalize_responsible_fields
from .bitrix.normalizers import normalize_stages
from .bitrix.normalizers import normalize_users
from .contracts import PortalAuth
from .database import Database
from .services.config_store import get_distribution_group as load_distribution_group
from .services.config_store import delete_distribution_group as remove_distribution_group
from .services.config_store import save_distribution_group as persist_distribution_group
from .services.diagnostic_store import record_diagnostic_log as persist_diagnostic_log
from .services.event_binding import ensure_configured_deal_created_event_binding as ensure_configured_binding
from .services.event_binding import ensure_event_binding
from .services.event_binding import run_event_delivery_check as run_delivery_check
from .services.event_dispatch import handle_bitrix_event as process_bitrix_event
from .services.portal_store import get_portal as load_portal
from .services.portal_store import install_portal as persist_portal_install
from .services.statistics import get_distribution_statistics as load_distribution_statistics

BITRIX_EVENT_DEAL_CREATED = "ONCRMDEALADD"
BITRIX_EVENT_APP_TEST = "ONAPPTEST"
DISTRIBUTION_EVENT_DEAL_CREATED = "deal_created"


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
        return persist_portal_install(self.database, payload, now_factory=_iso_now)

    def get_portal(self, portal_member_id: str) -> PortalAuth:
        return load_portal(self.database, portal_member_id)

    def get_reference_data(self, portal_member_id: str) -> dict[str, list[dict[str, object]]]:
        client = self._get_bitrix_client(portal_member_id)
        return {
            "users": normalize_users(client.call_list("user.get")),
            "stages": normalize_stages(
                client.call_list("crm.status.list", {"filter": {"ENTITY_ID": "DEAL_STAGE"}})
            ),
            "responsible_fields": normalize_responsible_fields(
                client.call("crm.item.fields", {"entityTypeId": 2, "useOriginalUfNames": "Y"})
            ),
        }

    def list_portal_users(self, portal_member_id: str) -> list[dict[str, object]]:
        client = self._get_bitrix_client(portal_member_id)
        return normalize_users(client.call_list("user.get"))

    def list_deal_stages(self, portal_member_id: str) -> list[dict[str, object]]:
        client = self._get_bitrix_client(portal_member_id)
        return normalize_stages(client.call_list("crm.status.list", {"filter": {"ENTITY_ID": "DEAL_STAGE"}}))

    def list_responsible_fields(self, portal_member_id: str) -> list[dict[str, object]]:
        client = self._get_bitrix_client(portal_member_id)
        return normalize_responsible_fields(
            client.call("crm.item.fields", {"entityTypeId": 2, "useOriginalUfNames": "Y"})
        )

    def get_distribution_group(self, portal_member_id: str) -> dict[str, object] | None:
        return load_distribution_group(
            self.database,
            portal_member_id,
            ensure_portal_exists=self.get_portal,
        )

    def get_distribution_statistics(self, portal_member_id: str) -> dict[str, object]:
        return load_distribution_statistics(
            self.database,
            portal_member_id,
            ensure_portal_exists=self.get_portal,
            load_distribution_group=self.get_distribution_group,
            get_bitrix_client=self._get_bitrix_client,
        )

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
        persist_diagnostic_log(
            self.database,
            source=source,
            message=message,
            level=level,
            portal_member_id=portal_member_id,
            deal_id=deal_id,
            details=details,
            now_factory=_iso_now,
        )

    def save_distribution_group(self, portal_member_id: str, payload: dict[str, Any]) -> dict[str, object]:
        return persist_distribution_group(
            self.database,
            portal_member_id,
            payload,
            ensure_portal_exists=self.get_portal,
            load_distribution_group=self.get_distribution_group,
            now_factory=_iso_now,
        )

    def delete_distribution_group(self, portal_member_id: str) -> bool:
        return remove_distribution_group(
            self.database,
            portal_member_id,
            ensure_portal_exists=self.get_portal,
        )

    def ensure_deal_created_event_binding(self, portal_member_id: str, handler_url: str) -> dict[str, object]:
        return ensure_event_binding(
            portal_member_id,
            handler_url,
            event_name=BITRIX_EVENT_DEAL_CREATED,
            ensure_message="Ensuring ONCRMDEALADD binding.",
            already_exists_message="ONCRMDEALADD binding already exists.",
            created_message="Created ONCRMDEALADD binding.",
            get_bitrix_client=self._get_bitrix_client,
            record_diagnostic_log=self.record_diagnostic_log,
        )

    def run_event_delivery_check(self, portal_member_id: str, handler_url: str) -> dict[str, object]:
        return run_delivery_check(
            portal_member_id,
            handler_url,
            event_name=BITRIX_EVENT_APP_TEST,
            ensure_message="Ensuring ONAPPTEST binding.",
            already_exists_message="ONAPPTEST binding already exists.",
            created_message="Created ONAPPTEST binding.",
            get_bitrix_client=self._get_bitrix_client,
            record_diagnostic_log=self.record_diagnostic_log,
        )

    def ensure_configured_deal_created_event_binding(
        self,
        portal_member_id: str,
        handler_url: str,
    ) -> dict[str, object]:
        return ensure_configured_binding(
            portal_member_id,
            handler_url,
            config=self.get_distribution_group(portal_member_id),
            bitrix_event_deal_created=BITRIX_EVENT_DEAL_CREATED,
            distribution_event_deal_created=DISTRIBUTION_EVENT_DEAL_CREATED,
            ensure_deal_created_event_binding=self.ensure_deal_created_event_binding,
            record_diagnostic_log=self.record_diagnostic_log,
        )

    def handle_bitrix_event(self, payload: dict[str, Any]) -> dict[str, object]:
        return process_bitrix_event(
            payload,
            database=self.database,
            install_portal=self.install_portal,
            get_portal=self.get_portal,
            get_distribution_group=self.get_distribution_group,
            get_bitrix_client=self._get_bitrix_client,
            record_diagnostic_log=self.record_diagnostic_log,
            now_factory=_iso_now,
            bitrix_event_deal_created=BITRIX_EVENT_DEAL_CREATED,
            distribution_event_deal_created=DISTRIBUTION_EVENT_DEAL_CREATED,
        )

    def _get_bitrix_client(self, portal_member_id: str) -> BitrixListClient:
        portal = self.get_portal(portal_member_id)
        return self.bitrix_client_factory(portal)


def _iso_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
