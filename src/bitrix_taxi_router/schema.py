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

CREATE TABLE IF NOT EXISTS distribution_groups (
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
"""
