from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class GroupMemberInput:
    user_id: int
    sort_order: int
    is_active: bool = True


@dataclass
class DistributionGroupInput:
    portal_member_id: str
    name: str
    initial_stage_id: str
    timeout_seconds: int
    priority: int = 1
    event_on_add: bool = True
    event_on_update: bool = True
    is_active: bool = True
    members: list[GroupMemberInput] = field(default_factory=list)
    group_id: int | None = None


@dataclass
class PortalAuth:
    member_id: str
    domain: str
    access_token: str | None
    refresh_token: str | None
    client_endpoint: str | None
    server_endpoint: str | None
    application_token: str | None
    status: str | None
