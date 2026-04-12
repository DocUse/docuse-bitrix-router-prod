from __future__ import annotations

import json
from urllib.parse import parse_qs

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from .database import Database
from .service import AssignmentService, group_input_from_payload
from .settings import Settings
from .ui import render_settings_page


def create_app(settings: Settings | None = None) -> FastAPI:
    effective_settings = settings or Settings.load()
    effective_settings.ensure_runtime_dirs()

    database = Database(effective_settings.db_path)
    database.init_schema()
    service = AssignmentService(database, effective_settings)

    app = FastAPI(title="Bitrix Taxi Router")
    app.state.settings = effective_settings
    app.state.database = database
    app.state.assignment_service = service

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return """
        <html>
          <head><title>Bitrix Taxi Router</title></head>
          <body>
            <h1>Bitrix Taxi Router</h1>
            <p>Сервер запущен. Используйте API для установки портала, настройки групп и запуска перераспределения.</p>
            <ul>
              <li><a href="/ui/groups">/ui/groups</a> — экран настройки групп</li>
              <li>POST /install/callback</li>
              <li>POST /events/bitrix</li>
              <li>GET /api/groups?member_id=...</li>
              <li>POST /api/groups</li>
              <li>GET /api/employees?member_id=...</li>
              <li>POST /api/jobs/reassign</li>
            </ul>
          </body>
        </html>
        """

    @app.get("/ui/groups", response_class=HTMLResponse)
    async def groups_ui_get(request: Request) -> str:
        member_id = (request.query_params.get("member_id") or "").strip() or None
        return render_settings_page(initial_member_id=member_id)

    @app.head("/ui/groups")
    async def groups_ui_head() -> dict[str, str]:
        return {}

    @app.post("/ui/groups", response_class=HTMLResponse)
    async def groups_ui_post(request: Request) -> str:
        payload = _normalize_bitrix_payload(await _read_bitrix_payload(request))
        member_id = _extract_member_id_from_context(payload)
        message = None
        if member_id and _payload_contains_installable_auth(payload):
            try:
                saved = service.install_portal(payload)
                message = f"Контекст портала сохранен: {saved['domain']}"
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        return render_settings_page(initial_member_id=member_id, portal_message=message)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/install/callback", response_class=HTMLResponse)
    async def install_callback_get() -> str:
        return "Bitrix install callback is ready."

    @app.head("/install/callback")
    async def install_callback_head() -> dict[str, str]:
        return {}

    @app.post("/install/callback")
    async def install_callback(request: Request) -> dict[str, object]:
        payload = _normalize_bitrix_payload(await _read_bitrix_payload(request))
        try:
            saved = service.install_portal(payload)
            registered = service.register_default_event_handlers(saved["member_id"])
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "ok", "portal": saved, "registered_events": registered}

    @app.post("/events/bitrix")
    async def bitrix_event(request: Request) -> dict[str, object]:
        payload = await _read_bitrix_payload(request)
        try:
            return service.process_event(payload)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/groups")
    async def list_groups(member_id: str) -> list[dict[str, object]]:
        try:
            return service.list_groups(member_id)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/groups")
    async def save_group(request: Request) -> dict[str, object]:
        payload = await request.json()
        try:
            group_id = service.save_group(group_input_from_payload(payload))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "ok", "group_id": group_id}

    @app.delete("/api/groups/{group_id}")
    async def delete_group(group_id: int, member_id: str) -> dict[str, object]:
        try:
            deleted = service.delete_group(member_id, group_id)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not deleted:
            raise HTTPException(status_code=404, detail="Group not found")
        return {"status": "ok", "group_id": group_id}

    @app.get("/api/employees")
    async def list_employees(member_id: str) -> list[dict[str, object]]:
        try:
            return service.list_portal_employees(member_id)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/jobs/reassign")
    async def run_reassign_job() -> dict[str, object]:
        try:
            return service.process_due_reassignments()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app


async def _read_bitrix_payload(request: Request) -> dict[str, object]:
    body = await request.body()
    if not body:
        return {}

    text = body.decode("utf-8", errors="replace").strip()
    content_type = (request.headers.get("content-type") or "").lower()
    if "application/json" in content_type or text.startswith("{"):
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        return payload

    return _parse_form_encoded_payload(text)


def _normalize_bitrix_payload(payload: dict[str, object]) -> dict[str, object]:
    normalized = dict(payload)
    if "auth" not in normalized and any(key in normalized for key in ("AUTH_ID", "REFRESH_ID", "DOMAIN")):
        protocol_raw = str(normalized.get("PROTOCOL") or "").strip().lower()
        scheme = "https" if protocol_raw in {"1", "https"} else "http"
        domain = str(normalized.get("DOMAIN") or "").strip()
        auth: dict[str, object] = {
            "member_id": normalized.get("member_id") or normalized.get("MEMBER_ID") or "",
            "domain": domain,
            "access_token": normalized.get("AUTH_ID") or "",
            "refresh_token": normalized.get("REFRESH_ID") or "",
            "client_endpoint": f"{scheme}://{domain}/rest/" if domain else "",
            "server_endpoint": "https://oauth.bitrix24.tech/rest/",
            "status": normalized.get("APP_STATUS") or normalized.get("status") or "",
        }
        normalized["auth"] = auth
    return normalized


def _parse_form_encoded_payload(text: str) -> dict[str, object]:
    flat = parse_qs(text, keep_blank_values=True)
    nested: dict[str, object] = {}
    for raw_key, values in flat.items():
        value: object = values[-1] if values else ""
        tokens = raw_key.replace("]", "").split("[")
        cursor = nested
        for token in tokens[:-1]:
            existing = cursor.get(token)
            if not isinstance(existing, dict):
                existing = {}
                cursor[token] = existing
            cursor = existing
        cursor[tokens[-1]] = value
    return nested


def _extract_member_id_from_context(payload: dict[str, object]) -> str | None:
    auth = payload.get("auth")
    if isinstance(auth, dict):
        member_id = str(auth.get("member_id") or "").strip()
        if member_id:
            return member_id
    member_id = str(payload.get("member_id") or payload.get("MEMBER_ID") or "").strip()
    return member_id or None


def _payload_contains_installable_auth(payload: dict[str, object]) -> bool:
    auth = payload.get("auth")
    if not isinstance(auth, dict):
        return False
    return bool(str(auth.get("member_id") or "").strip() and str(auth.get("domain") or "").strip())
