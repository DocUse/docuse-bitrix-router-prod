from __future__ import annotations

import json
from collections.abc import Callable
from urllib.parse import parse_qs

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from .bitrix_api import BitrixApiError
from .database import Database
from .service import PortalService
from .settings import Settings
from .ui import render_blank_page, render_install_page


def create_app(settings: Settings | None = None) -> FastAPI:
    effective_settings = settings or Settings.load()
    effective_settings.ensure_runtime_dirs()

    database = Database(effective_settings.db_path)
    database.init_schema()
    service = PortalService(database)

    app = FastAPI(title="Bitrix Taxi Router")
    app.state.settings = effective_settings
    app.state.database = database
    app.state.portal_service = service

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return render_blank_page()

    @app.get("/ui/groups", response_class=HTMLResponse)
    async def groups_ui_get(request: Request) -> str:
        _ = request
        return render_blank_page()

    @app.head("/ui/groups")
    async def groups_ui_head() -> dict[str, str]:
        return {}

    @app.post("/ui/groups", response_class=HTMLResponse)
    async def groups_ui_post(request: Request) -> str:
        payload = _normalize_bitrix_payload(await _read_bitrix_payload(request))
        if _payload_contains_installable_auth(payload):
            try:
                service.install_portal(payload)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        return render_blank_page()

    @app.get("/api/ui/groups/reference-data")
    async def groups_reference_data(request: Request) -> dict[str, object]:
        member_id = _require_member_id(request)
        return _load_reference_data(service.get_reference_data, member_id)

    @app.get("/api/ui/groups/reference-data/users")
    async def groups_reference_users(request: Request) -> dict[str, object]:
        member_id = _require_member_id(request)
        return {"items": _load_reference_data(service.list_portal_users, member_id)}

    @app.get("/api/ui/groups/reference-data/stages")
    async def groups_reference_stages(request: Request) -> dict[str, object]:
        member_id = _require_member_id(request)
        return {"items": _load_reference_data(service.list_deal_stages, member_id)}

    @app.get("/api/ui/groups/reference-data/responsible-fields")
    async def groups_reference_responsible_fields(request: Request) -> dict[str, object]:
        member_id = _require_member_id(request)
        return {"items": _load_reference_data(service.list_responsible_fields, member_id)}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/install", response_class=HTMLResponse)
    async def install_page_get(request: Request) -> str:
        member_id = (request.query_params.get("member_id") or "").strip() or None
        return render_install_page(initial_member_id=member_id)

    @app.head("/install")
    async def install_page_head() -> dict[str, str]:
        return {}

    @app.post("/install", response_class=HTMLResponse)
    async def install_page_post(request: Request) -> str:
        payload = _normalize_bitrix_payload(await _read_bitrix_payload(request))
        member_id = _extract_member_id_from_context(payload)
        if _payload_contains_installable_auth(payload):
            try:
                service.install_portal(payload)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        return render_install_page(initial_member_id=member_id)

    @app.get("/install/callback", response_class=HTMLResponse)
    async def install_callback_get() -> str:
        return "ready"

    @app.head("/install/callback")
    async def install_callback_head() -> dict[str, str]:
        return {}

    @app.post("/install/callback")
    async def install_callback(request: Request) -> dict[str, object]:
        payload = _normalize_bitrix_payload(await _read_bitrix_payload(request))
        try:
            saved = service.install_portal(payload)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "ok", "portal": saved}

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
            "server_endpoint": "https://oauth.bitrix.info/rest/",
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


def _require_member_id(request: Request) -> str:
    member_id = (request.query_params.get("member_id") or "").strip()
    if not member_id:
        raise HTTPException(status_code=400, detail="member_id query parameter is required")
    return member_id


def _load_reference_data(loader: Callable[[str], object], member_id: str) -> object:
    try:
        return loader(member_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BitrixApiError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
