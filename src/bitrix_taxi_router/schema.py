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
    installed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS distribution_group_configs (
    portal_member_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    distribution_type TEXT NOT NULL,
    event_type TEXT NOT NULL,
    distribution_stage_id TEXT NOT NULL,
    responsible_field_id TEXT NOT NULL,
    wait_seconds INTEGER NOT NULL,
    retry_interval_seconds INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    members_json TEXT NOT NULL,
    load_stage_ids_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (portal_member_id) REFERENCES portals(member_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS distribution_member_runtime (
    portal_member_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    last_assigned_deal_id TEXT,
    last_assigned_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (portal_member_id, user_id),
    FOREIGN KEY (portal_member_id) REFERENCES portals(member_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS distribution_deal_runtime (
    portal_member_id TEXT NOT NULL,
    deal_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    assigned_user_id TEXT,
    assigned_field_id TEXT,
    note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (portal_member_id, deal_id, event_type),
    FOREIGN KEY (portal_member_id) REFERENCES portals(member_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS diagnostic_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portal_member_id TEXT,
    deal_id TEXT,
    level TEXT NOT NULL,
    source TEXT NOT NULL,
    message TEXT NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (portal_member_id) REFERENCES portals(member_id) ON DELETE CASCADE
);
"""
