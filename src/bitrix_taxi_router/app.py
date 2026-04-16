from __future__ import annotations

import json
import logging
from collections.abc import Callable
from urllib.parse import parse_qs
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from .bitrix_api import BitrixApiError
from .database import Database
from .service import PortalService
from .settings import Settings
from .ui import render_blank_page, render_install_page

logger = logging.getLogger(__name__)


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
        member_id = (request.query_params.get("member_id") or "").strip() or None
        return render_blank_page(initial_member_id=member_id)

    @app.head("/ui/groups")
    async def groups_ui_head() -> dict[str, str]:
        return {}

    @app.post("/ui/groups", response_class=HTMLResponse)
    async def groups_ui_post(request: Request) -> str:
        payload = _normalize_bitrix_payload(await _read_bitrix_payload(request))
        member_id = _extract_member_id_from_context(payload)
        if _payload_contains_installable_auth(payload):
            try:
                service.install_portal(payload)
                _ensure_binding_for_configured_portal(
                    request,
                    service=service,
                    settings=effective_settings,
                    portal_member_id=member_id,
                    source="groups_ui_post",
                )
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        return render_blank_page(initial_member_id=member_id)

    @app.get("/api/ui/groups/reference-data")
    async def groups_reference_data(request: Request) -> dict[str, object]:
        member_id = _require_member_id(request)
        return _load_reference_data(service.get_reference_data, member_id)

    @app.get("/api/ui/groups/config")
    async def groups_config_get(request: Request) -> dict[str, object]:
        member_id = _require_member_id(request)
        return {"config": _load_reference_data(service.get_distribution_group, member_id)}

    @app.post("/api/ui/groups/config")
    async def groups_config_post(request: Request) -> dict[str, object]:
        member_id = _require_member_id(request)
        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="JSON payload must be an object")
        saved = _load_reference_data(lambda portal_member_id: service.save_distribution_group(portal_member_id, payload), member_id)
        binding = _ensure_binding_for_configured_portal(
            request,
            service=service,
            settings=effective_settings,
            portal_member_id=member_id,
            source="groups_config_post",
        )
        return {"status": "ok", "config": saved, "event_binding": binding}

    @app.post("/api/ui/groups/portal-context")
    async def groups_portal_context(request: Request) -> dict[str, object]:
        payload = _normalize_bitrix_payload(await _read_bitrix_payload(request))
        try:
            saved = service.install_portal(payload)
            binding = _ensure_binding_for_configured_portal(
                request,
                service=service,
                settings=effective_settings,
                portal_member_id=str(saved["member_id"]),
                source="groups_portal_context",
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "ok", "portal": saved, "event_binding": binding}

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

    @app.post("/api/bitrix/events", name="bitrix_event_handler")
    async def bitrix_event_handler(request: Request) -> dict[str, object]:
        payload = _normalize_bitrix_payload(await _read_bitrix_payload(request))
        logger.info(
            "Received /api/bitrix/events hit content_type=%s keys=%s",
            request.headers.get("content-type"),
            sorted(payload.keys()),
        )
        try:
            result = service.handle_bitrix_event(payload)
        except ValueError as exc:
            logger.exception("Bitrix event handler rejected payload")
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except BitrixApiError as exc:
            logger.exception("Bitrix event handler failed during Bitrix API call")
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {"status": "ok", "result": result}

    @app.get("/api/bitrix/events", name="bitrix_event_handler_probe")
    async def bitrix_event_handler_probe() -> dict[str, str]:
        return {"status": "ready"}

    @app.head("/api/bitrix/events")
    async def bitrix_event_handler_probe_head() -> dict[str, str]:
        return {}

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
                _ensure_binding_for_configured_portal(
                    request,
                    service=service,
                    settings=effective_settings,
                    portal_member_id=member_id,
                    source="install_page_post",
                )
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
            binding = _ensure_binding_for_configured_portal(
                request,
                service=service,
                settings=effective_settings,
                portal_member_id=str(saved["member_id"]),
                source="install_callback",
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "ok", "portal": saved, "event_binding": binding}

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


def _ensure_binding_for_configured_portal(
    request: Request,
    *,
    service: PortalService,
    settings: Settings,
    portal_member_id: str | None,
    source: str,
) -> dict[str, object] | None:
    member_id = (portal_member_id or "").strip()
    if not member_id:
        return None

    event_handler_url = _get_public_event_handler_url(request, settings=settings, route_name="bitrix_event_handler")
    if not event_handler_url:
        logger.warning("Skipping event binding source=%s portal=%s: public handler URL unavailable", source, member_id)
        return None

    try:
        binding = service.ensure_configured_deal_created_event_binding(member_id, event_handler_url)
    except Exception:
        logger.exception(
            "Failed to ensure configured ONCRMDEALADD binding source=%s portal=%s handler=%s",
            source,
            member_id,
            event_handler_url,
        )
        raise

    logger.info(
        "Ensured configured ONCRMDEALADD binding source=%s portal=%s handler=%s result=%s",
        source,
        member_id,
        event_handler_url,
        binding,
    )
    return binding


def _get_public_event_handler_url(request: Request, *, settings: Settings, route_name: str) -> str | None:
    configured_base_url = (settings.public_base_url or "").strip()
    if configured_base_url:
        normalized_base_url = configured_base_url.rstrip("/")
        parsed = urlsplit(normalized_base_url)
        if parsed.scheme.lower() != "https" or not parsed.netloc:
            logger.warning("APP_PUBLIC_BASE_URL is not usable for Bitrix event binding: %s", configured_base_url)
            return None
        return f"{normalized_base_url}{request.app.url_path_for(route_name)}"

    scheme = (request.headers.get("x-forwarded-proto") or request.url.scheme or "").strip().lower()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc or "").strip()
    if scheme != "https" or not host:
        return None

    hostname = urlsplit(f"{scheme}://{host}").hostname or ""
    if hostname in {"localhost", "127.0.0.1", "testserver"}:
        return None

    path = str(request.app.url_path_for(route_name))
    return f"{scheme}://{host}{path}"
