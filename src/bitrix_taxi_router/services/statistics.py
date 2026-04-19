from __future__ import annotations

from collections import Counter
from collections.abc import Callable

from ..database import Database
from .common import as_optional_str
from .config_store import parse_json_object

JOURNAL_LIMIT = 100
DIAGNOSTIC_LIMIT = 200


def get_distribution_statistics(
    database: Database,
    portal_member_id: str,
    *,
    ensure_portal_exists: Callable[[str], object],
    load_distribution_group: Callable[[str], dict[str, object] | None],
) -> dict[str, object]:
    ensure_portal_exists(portal_member_id)
    config = load_distribution_group(portal_member_id)
    configured_members = config.get("members") if isinstance(config, dict) else []
    member_names = {
        str(member.get("user_id")): str(member.get("user_id"))
        for member in configured_members
        if isinstance(member, dict) and str(member.get("user_id") or "").strip()
    }

    deal_rows = database.fetch_all(
        """
        SELECT *
        FROM distribution_deal_runtime
        WHERE portal_member_id = ?
        ORDER BY updated_at DESC, created_at DESC, deal_id DESC
        LIMIT ?
        """,
        (portal_member_id, JOURNAL_LIMIT),
    )
    member_rows = database.fetch_all(
        """
        SELECT *
        FROM distribution_member_runtime
        WHERE portal_member_id = ?
        ORDER BY updated_at DESC, user_id ASC
        """,
        (portal_member_id,),
    )
    diagnostic_rows = database.fetch_all(
        """
        SELECT *
        FROM diagnostic_logs
        WHERE portal_member_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (portal_member_id, DIAGNOSTIC_LIMIT),
    )

    journal = [
        {
            "deal_id": str(row["deal_id"]),
            "event_type": str(row["event_type"]),
            "status": str(row["status"]),
            "assigned_user_id": as_optional_str(row["assigned_user_id"]),
            "assigned_user_name": member_names.get(str(row["assigned_user_id"])) if row["assigned_user_id"] else None,
            "assigned_field_id": as_optional_str(row["assigned_field_id"]),
            "note": as_optional_str(row["note"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }
        for row in deal_rows
    ]
    member_runtime = [
        {
            "user_id": str(row["user_id"]),
            "user_name": member_names.get(str(row["user_id"])) or str(row["user_id"]),
            "last_assigned_deal_id": as_optional_str(row["last_assigned_deal_id"]),
            "last_assigned_at": as_optional_str(row["last_assigned_at"]),
            "updated_at": str(row["updated_at"]),
        }
        for row in member_rows
    ]
    member_runtime_by_user_id = {
        str(item["user_id"]): item
        for item in member_runtime
    }
    diagnostics = [
        {
            "id": int(row["id"]),
            "portal_member_id": as_optional_str(row["portal_member_id"]),
            "deal_id": as_optional_str(row["deal_id"]),
            "level": str(row["level"]),
            "source": str(row["source"]),
            "message": str(row["message"]),
            "details": parse_json_object(row["details_json"], "diagnostic log details"),
            "created_at": str(row["created_at"]),
        }
        for row in diagnostic_rows
    ]
    assigned_counter = Counter(
        str(item["assigned_user_id"])
        for item in journal
        if item["status"] == "assigned" and item["assigned_user_id"]
    )
    distribution_user_ids: list[str] = []
    for raw_member in configured_members:
        if not isinstance(raw_member, dict):
            continue
        user_id = str(raw_member.get("user_id") or "").strip()
        if user_id:
            distribution_user_ids.append(user_id)
    for user_id in assigned_counter:
        if user_id not in distribution_user_ids:
            distribution_user_ids.append(user_id)
    for user_id in member_runtime_by_user_id:
        if user_id not in distribution_user_ids:
            distribution_user_ids.append(user_id)
    distribution_items = [
        {
            "user_id": user_id,
            "user_name": member_names.get(user_id) or user_id,
            "group_name": str(config.get("name") or "").strip() if isinstance(config, dict) else "",
            "assigned_count": int(assigned_counter.get(user_id, 0)),
            "last_assigned_deal_id": member_runtime_by_user_id.get(user_id, {}).get("last_assigned_deal_id"),
            "last_assigned_at": member_runtime_by_user_id.get(user_id, {}).get("last_assigned_at"),
            "updated_at": member_runtime_by_user_id.get(user_id, {}).get("updated_at"),
            "is_group_member": user_id in member_names,
        }
        for user_id in distribution_user_ids
    ]
    summary = {
        "journal_count": len(journal),
        "member_runtime_count": len(member_runtime),
        "diagnostic_count": len(diagnostics),
        "assigned_count": sum(1 for item in journal if item["status"] == "assigned"),
        "waiting_count": sum(1 for item in journal if item["status"] == "waiting"),
        "ignored_count": sum(1 for item in journal if item["status"] == "ignored"),
        "journal_limit": JOURNAL_LIMIT,
        "diagnostic_limit": DIAGNOSTIC_LIMIT,
    }
    return {
        "summary": summary,
        "journal": journal,
        "members": member_runtime,
        "diagnostics": diagnostics,
        "distribution": {
            "group_name": str(config.get("name") or "").strip() if isinstance(config, dict) else "",
            "group_active": bool(config.get("is_active")) if isinstance(config, dict) else False,
            "assigned_total": int(sum(item["assigned_count"] for item in distribution_items)),
            "items": distribution_items,
        },
    }
