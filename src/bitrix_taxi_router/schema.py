from __future__ import annotations


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS portals (
    member_id TEXT PRIMARY KEY,
    domain TEXT NOT NULL,
    application_token TEXT,
    access_token TEXT,
    refresh_token TEXT,
    client_endpoint TEXT,
    server_endpoint TEXT,
    status TEXT,
    expires_at TEXT,
    installed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS distribution_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portal_member_id TEXT NOT NULL,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT 'lead',
    initial_stage_id TEXT NOT NULL,
    timeout_seconds INTEGER NOT NULL,
    priority INTEGER NOT NULL DEFAULT 1,
    event_on_add INTEGER NOT NULL DEFAULT 1,
    event_on_update INTEGER NOT NULL DEFAULT 1,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(portal_member_id) REFERENCES portals(member_id)
);

CREATE INDEX IF NOT EXISTS idx_distribution_groups_portal
    ON distribution_groups(portal_member_id, is_active, priority, id);

CREATE TABLE IF NOT EXISTS group_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL,
    bitrix_user_id INTEGER NOT NULL,
    sort_order INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(group_id) REFERENCES distribution_groups(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_group_members_group
    ON group_members(group_id, is_active, sort_order, id);

CREATE TABLE IF NOT EXISTS lead_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portal_member_id TEXT NOT NULL,
    group_id INTEGER NOT NULL,
    lead_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    current_member_index INTEGER NOT NULL DEFAULT 0,
    current_user_id INTEGER NOT NULL,
    initial_stage_id TEXT NOT NULL,
    due_at TEXT NOT NULL,
    completion_reason TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(portal_member_id) REFERENCES portals(member_id),
    FOREIGN KEY(group_id) REFERENCES distribution_groups(id)
);

CREATE INDEX IF NOT EXISTS idx_lead_assignments_due
    ON lead_assignments(status, due_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_assignments_open_unique
    ON lead_assignments(portal_member_id, group_id, lead_id)
    WHERE status = 'waiting';

CREATE TABLE IF NOT EXISTS assignment_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    assignment_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    from_user_id INTEGER,
    to_user_id INTEGER,
    lead_status_id TEXT,
    payload_raw TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(assignment_id) REFERENCES lead_assignments(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portal_member_id TEXT,
    event_type TEXT NOT NULL,
    lead_id INTEGER,
    payload_raw TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""
