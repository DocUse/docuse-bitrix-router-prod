from __future__ import annotations

from typing import Any
from typing import Protocol

from .common import coerce_int
from .common import maybe_int


DEAL_ENTITY_TYPE_ID = 2


class BitrixListClient(Protocol):
    def call(self, method: str, params: dict[str, object] | None = None) -> dict[str, Any]:
        ...

    def call_list(self, method: str, params: dict[str, object] | None = None) -> list[dict[str, Any]]:
        ...


def select_distribution_candidate(
    portal_member_id: str,
    client: BitrixListClient,
    config: dict[str, object],
    *,
    last_assigned_map: dict[str, str | None],
) -> dict[str, object]:
    members = config.get("members")
    if not isinstance(members, list):
        raise ValueError("Stored distribution members payload must be a list")

    responsible_field_id = str(config["responsible_field_id"])
    load_stage_ids = config.get("load_stage_ids")
    if not isinstance(load_stage_ids, list):
        raise ValueError("Stored distribution load stages payload must be a list")

    member_ids = [
        str(raw_member.get("user_id") or "").strip()
        for raw_member in members
        if isinstance(raw_member, dict) and str(raw_member.get("user_id") or "").strip()
    ]
    load_by_member = count_member_loads(client, responsible_field_id, load_stage_ids, member_ids)

    loads: list[dict[str, object]] = []
    available_members: list[dict[str, object]] = []
    for raw_member in members:
        if not isinstance(raw_member, dict):
            continue
        user_id = str(raw_member.get("user_id") or "").strip()
        if not user_id:
            continue
        limit_value = coerce_int(raw_member.get("limit"), field_name=f"Member limit for {user_id}")
        sort_order = coerce_int(raw_member.get("sort_order"), field_name=f"Member sort order for {user_id}")
        current_load = load_by_member.get(user_id, 0)
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


def count_member_load(
    client: BitrixListClient,
    responsible_field_id: str,
    load_stage_ids: list[object],
    user_id: str,
) -> int:
    return count_member_loads(client, responsible_field_id, load_stage_ids, [user_id]).get(user_id, 0)


def count_member_loads(
    client: BitrixListClient,
    responsible_field_id: str,
    load_stage_ids: list[object],
    user_ids: list[str],
) -> dict[str, int]:
    filter_field = resolve_responsible_field_api_name(responsible_field_id)
    normalized_user_ids = [user_id for user_id in user_ids if user_id]
    if not normalized_user_ids:
        return {}

    items = client.call_list(
        "crm.item.list",
        {
            "entityTypeId": DEAL_ENTITY_TYPE_ID,
            "select": ["id", "stageId", filter_field],
            "useOriginalUfNames": "Y",
        },
    )
    configured_stage_ids = [str(stage_id) for stage_id in load_stage_ids]
    loads = {user_id: 0 for user_id in normalized_user_ids}
    for item in items:
        if not item_matches_load_stage(item, configured_stage_ids):
            continue
        assigned_user_id = extract_item_responsible_user_id(item, responsible_field_id)
        if assigned_user_id not in loads:
            continue
        loads[assigned_user_id] += 1
    return loads


def assign_deal_to_member(
    client: BitrixListClient,
    deal_id: str,
    responsible_field_id: str,
    user_id: str,
) -> None:
    field_name = resolve_responsible_field_api_name(responsible_field_id)
    client.call(
        "crm.item.update",
        {
            "entityTypeId": DEAL_ENTITY_TYPE_ID,
            "id": maybe_int(deal_id),
            "fields": {field_name: maybe_int(user_id)},
            "useOriginalUfNames": "Y",
        },
    )
def resolve_responsible_field_api_name(field_id: str) -> str:
    normalized = field_id.strip().upper()
    if normalized == "ASSIGNED_BY_ID":
        return "assignedById"
    return field_id.strip()


def item_matches_load_stage(item: dict[str, object], configured_stage_ids: list[str]) -> bool:
    stage_id = str(item.get("stageId") or item.get("STAGE_ID") or "").strip()
    if not stage_id:
        return False

    stage_suffix = stage_id.split(":", 1)[-1]
    return any(configured_stage_id == stage_id or configured_stage_id == stage_suffix for configured_stage_id in configured_stage_ids)


def extract_item_responsible_user_id(item: dict[str, object], responsible_field_id: str) -> str | None:
    field_name = resolve_responsible_field_api_name(responsible_field_id)
    candidate_values = [
        item.get(field_name),
        item.get(field_name[:1].upper() + field_name[1:]),
        item.get(responsible_field_id),
        item.get(responsible_field_id.upper()),
    ]
    for value in candidate_values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None
