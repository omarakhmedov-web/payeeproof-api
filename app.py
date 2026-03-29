import hashlib
import hmac
import html
import json
import os
import re
import sqlite3
import time
import uuid
import logging
import ipaddress

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    psycopg2 = None
    RealDictCursor = None
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from threading import Lock, Thread
from urllib.parse import urlparse

import requests
from flask import Flask, g, jsonify, request, has_request_context
from flask_cors import CORS

APP_VERSION = "2.0.3-pilot-welcome-flow"
TRANSFER_TOPIC = "0xddf252ad00000000000000000000000000000000000000000000000000000000"
ZERO_EVM = "0x0000000000000000000000000000000000000000"
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

CHAIN_ALIASES = {
    "eth": "ethereum",
    "ethereum": "ethereum",
    "mainnet": "ethereum",
    "arb": "arbitrum",
    "arbitrum": "arbitrum",
    "base": "base",
    "polygon": "polygon",
    "matic": "polygon",
    "bsc": "bsc",
    "bnb": "bsc",
    "bnb chain": "bsc",
    "sol": "solana",
    "solana": "solana",
}

EVM_CHAINS = {"ethereum", "arbitrum", "base", "polygon", "bsc"}
EVM_CHAIN_ORDER = ["ethereum", "arbitrum", "base", "polygon", "bsc"]
SOLANA_SIG_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{43,100}$")
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com", "msn.com",
    "yahoo.com", "ymail.com", "rocketmail.com", "icloud.com", "me.com", "mac.com",
    "proton.me", "protonmail.com", "pm.me", "mail.com", "aol.com", "gmx.com",
    "yandex.ru", "yandex.com", "ya.ru", "bk.ru", "inbox.ru", "list.ru", "mail.ru"
}


DEFAULT_PLAN_LIMITS_FALLBACK = {
    "demo": {"monthly_checks": 50, "monthly_records_reads": 250},
    "pilot": {"monthly_checks": 1000, "monthly_records_reads": 2000},
    "growth": {"monthly_checks": 10000, "monthly_records_reads": 25000},
    "enterprise": {},
}
LIMIT_COUNTER_LABELS = {
    "monthly_checks": "Monthly checks",
    "monthly_preflight_checks": "Monthly preflight checks",
    "monthly_recovery_checks": "Monthly recovery checks",
    "monthly_records_reads": "Monthly verification record reads",
}
ROLE_WRITE_BLOCKS = {
    "viewer": {"/api/preflight-check", "/api/recovery-copilot"},
}
BILLING_LIMIT_ENDPOINTS = {
    "/api/preflight-check": ["monthly_checks", "monthly_preflight_checks"],
    "/api/recovery-copilot": ["monthly_checks", "monthly_recovery_checks"],
    "/api/verification-records": ["monthly_records_reads"],
}

DEFAULT_RPC_URLS = {
    "ethereum": os.getenv("ETHEREUM_RPC_URL", ""),
    "arbitrum": os.getenv("ARBITRUM_RPC_URL", ""),
    "base": os.getenv("BASE_RPC_URL", ""),
    "polygon": os.getenv("POLYGON_RPC_URL", ""),
    "bsc": os.getenv("BSC_RPC_URL", ""),
    "solana": os.getenv("SOLANA_RPC_URL", ""),
}

SUPPORTED_ASSETS_BY_CHAIN = {
    "ethereum": {"USDC", "USDT", "DAI", "USDS", "PYUSD"},
    "arbitrum": {"USDC", "USDT", "DAI", "USDS"},
    "base": {"USDC", "USDT"},
    "polygon": {"USDC", "USDT"},
    "bsc": {"USDC", "USDT"},
    "solana": {"USDC", "USDT"},
}

DESTINATION_PROFILE_MAP = {
    "personal_wallet": {
        "label": "Personal wallet",
        "explanation": "Looks like a self-custody style wallet address.",
    },
    "contract_or_app": {
        "label": "Contract or app",
        "explanation": "Funds may arrive, but recovery depends on contract logic and ownership.",
    },
    "exchange_like_deposit": {
        "label": "Exchange-like deposit",
        "explanation": "Confirm the exact venue, network, asset, and memo or tag before sending.",
    },
    "bridge_router": {
        "label": "Bridge / router",
        "explanation": "Infrastructure address. Use a small test first unless this destination is explicitly expected.",
    },
    "invalid": {
        "label": "Invalid address",
        "explanation": "The address format does not match the selected network.",
    },
    "not_found": {
        "label": "Not found on-chain",
        "explanation": "No live account data was found for this destination.",
    },
    "unknown": {
        "label": "Unknown",
        "explanation": "Public signals were not strong enough to classify this destination.",
    },
}

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

allowed_origins = [x.strip() for x in os.getenv(
    "ALLOWED_ORIGINS",
    "https://payeeproof.com,http://localhost:3000,http://127.0.0.1:5500,http://localhost:5500"
).split(",") if x.strip()]
CORS(app, resources={r"/*": {"origins": allowed_origins}})

DATABASE_URL = str(os.getenv("DATABASE_URL", "")).strip()


def resolve_db_backend() -> str:
    return "postgres" if DATABASE_URL else "sqlite"


def resolve_db_path() -> str:
    explicit = str(os.getenv("DB_PATH", "")).strip()
    if explicit:
        return explicit
    return "/tmp/payeeproof.db"


def ensure_parent_dir(path_value: str) -> None:
    try:
        Path(path_value).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


DB_BACKEND = resolve_db_backend()
DB_PATH = resolve_db_path()
if DB_BACKEND == "sqlite":
    ensure_parent_dir(DB_PATH)
RPC_TIMEOUT = float(os.getenv("RPC_TIMEOUT_SEC", os.getenv("REQUEST_TIMEOUT_SEC", "3.5")))
EMAIL_TIMEOUT = float(os.getenv("EMAIL_TIMEOUT_SEC", "10"))
ADDRESS_CACHE_TTL_SEC = int(os.getenv("ADDRESS_CACHE_TTL_SEC", "600"))
_ADDRESS_CLASSIFY_CACHE: Dict[str, Dict[str, Any]] = {}
RESEND_API_BASE = os.getenv("RESEND_API_BASE", "https://api.resend.com").rstrip("/")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
RESEND_FROM = os.getenv("RESEND_FROM", "PayeeProof <alerts@notify.payeeproof.com>").strip()
RESEND_TO = os.getenv("RESEND_TO", "hello@payeeproof.com").strip()
PILOT_AUTO_WELCOME_ENABLED = str(os.getenv("PILOT_AUTO_WELCOME_ENABLED", "1")).strip().lower() not in {"0", "false", "no", "off"}
PILOT_WELCOME_REPLY_TO = os.getenv("PILOT_WELCOME_REPLY_TO", RESEND_TO).strip()
PILOT_WELCOME_NEXT_STEP_HOURS = int(os.getenv("PILOT_WELCOME_NEXT_STEP_HOURS", "24"))
PUBLIC_DEMO_ORIGINS = os.getenv("PUBLIC_DEMO_ORIGINS", ",".join(allowed_origins)).strip()
API_KEYS_JSON = os.getenv("API_KEYS_JSON", "").strip()
ANON_API_RATE_LIMIT = int(os.getenv("ANON_API_RATE_LIMIT", "20"))
ANON_API_RATE_WINDOW_SEC = int(os.getenv("ANON_API_RATE_WINDOW_SEC", "300"))
KEYED_API_RATE_LIMIT = int(os.getenv("KEYED_API_RATE_LIMIT", "180"))
KEYED_API_RATE_WINDOW_SEC = int(os.getenv("KEYED_API_RATE_WINDOW_SEC", "300"))
PILOT_RATE_LIMIT = int(os.getenv("PILOT_RATE_LIMIT", "4"))
PILOT_RATE_WINDOW_SEC = int(os.getenv("PILOT_RATE_WINDOW_SEC", "3600"))
PILOT_DUPLICATE_TTL_SEC = int(os.getenv("PILOT_DUPLICATE_TTL_SEC", "43200"))
PILOT_MIN_FILL_SEC = int(os.getenv("PILOT_MIN_FILL_SEC", "3"))
PILOT_MAX_NOTES_LEN = int(os.getenv("PILOT_MAX_NOTES_LEN", "2500"))
MAX_CONTENT_LENGTH_BYTES = int(os.getenv("MAX_CONTENT_LENGTH_BYTES", "32768"))
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH_BYTES
OBSERVABILITY_WINDOW_SEC = int(os.getenv("OBSERVABILITY_WINDOW_SEC", "900"))
OBSERVABILITY_MIN_REQUESTS = int(os.getenv("OBSERVABILITY_MIN_REQUESTS", "8"))
ALERT_ERROR_RATE_THRESHOLD = float(os.getenv("ALERT_ERROR_RATE_THRESHOLD", "0.35"))
ALERT_TIMEOUT_RATE_THRESHOLD = float(os.getenv("ALERT_TIMEOUT_RATE_THRESHOLD", "0.20"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "600"))
PUBLIC_API_BASE = os.getenv("PUBLIC_API_BASE", "https://payeeproof-api.onrender.com").rstrip("/")
PLAN_LIMITS_JSON = os.getenv("PLAN_LIMITS_JSON", "").strip()
DEFAULT_RECORDS_MAX_PAGE_SIZE = int(os.getenv("DEFAULT_RECORDS_MAX_PAGE_SIZE", "100"))
WEBHOOK_DELIVERY_TIMEOUT_SEC = float(os.getenv("WEBHOOK_DELIVERY_TIMEOUT_SEC", "10"))
WEBHOOK_RETRY_SCHEDULE_SEC = [
    max(1, int(part.strip()))
    for part in str(os.getenv("WEBHOOK_RETRY_SCHEDULE_SEC", "60,300,1800,7200")).split(",")
    if part.strip()
]
WEBHOOK_PROCESS_BATCH_SIZE = int(os.getenv("WEBHOOK_PROCESS_BATCH_SIZE", "5"))
WEBHOOK_PROCESS_MIN_INTERVAL_SEC = float(os.getenv("WEBHOOK_PROCESS_MIN_INTERVAL_SEC", "5"))

RATE_LIMIT_BUCKETS: Dict[str, deque] = {}
RATE_LIMIT_LOCK = Lock()
PILOT_DEDUP_CACHE: Dict[str, float] = {}
PILOT_DEDUP_LOCK = Lock()
ALERT_STATE: Dict[str, float] = {}
ALERT_LOCK = Lock()
WEBHOOK_PROCESS_LOCK = Lock()
WEBHOOK_PROCESS_STATE: Dict[str, Any] = {"running": False, "last_started_at": 0.0}
logger = logging.getLogger("payeeproof")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

API_SCOPE_BY_PATH = {
    "/api/preflight-check": "preflight",
    "/api/recovery-copilot": "recovery",
    "/api/account": "records",
    "/api/usage-summary": "records",
}

ALLOWED_ENVIRONMENTS = {"live", "test", "sandbox", "staging", "production", "public-demo"}
ALLOWED_ROLES = {"owner", "admin", "client", "viewer", "demo"}


def _normalize_chain_raw(value: Any) -> str:
    text = str(value or "").strip().lower()
    return CHAIN_ALIASES.get(text, text)


def normalize_environment(value: Any, default: str = "live") -> str:
    text = str(value or default).strip().lower()
    if not text:
        text = default
    if text not in ALLOWED_ENVIRONMENTS:
        if text in {"prod", "main", "mainnet"}:
            return "live"
        if text in {"dev", "qa"}:
            return "test"
        return default
    return text


def normalize_role(value: Any, default: str = "client") -> str:
    text = str(value or default).strip().lower()
    if not text:
        text = default
    if text not in ALLOWED_ROLES:
        if text in {"read_only", "readonly"}:
            return "viewer"
        return default
    return text


def api_key_fingerprint(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]


def api_key_hint(value: str) -> str:
    token = str(value or "").strip()
    if len(token) <= 8:
        return token
    return f"{token[:6]}...{token[-4:]}"


def resolve_required_api_scope(path: str) -> str:
    normalized = path.rstrip("/") or "/"
    if normalized in API_SCOPE_BY_PATH:
        return API_SCOPE_BY_PATH[normalized]
    if normalized == "/api/verification-records" or normalized.startswith("/api/verification-records/"):
        return "records"
    return ""


def _extract_host(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    candidate = text if "://" in text else f"https://{text}"
    parsed = urlparse(candidate)
    return (parsed.hostname or text.split(":")[0]).strip().lower()



def _split_origin_hosts(value: str) -> List[str]:
    hosts: List[str] = []
    for item in str(value or "").split(","):
        host = _extract_host(item)
        if host and host not in hosts:
            hosts.append(host)
    return hosts


def _normalize_string_list(value: Any, max_len: int = 120) -> List[str]:
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = []
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()[:max_len]
        if text and text not in out:
            out.append(text)
    return out


def _normalize_limit_value(value: Any) -> Optional[int]:
    if value in (None, "", False):
        return None
    try:
        limit = int(str(value).strip())
    except Exception:
        return None
    if limit < 0:
        return None
    return limit


def _normalize_limit_map(raw_limits: Any) -> Dict[str, Optional[int]]:
    if not isinstance(raw_limits, dict):
        return {}
    normalized: Dict[str, Optional[int]] = {}
    aliases = {
        "usage_limit_monthly": "monthly_checks",
        "monthly_usage_limit": "monthly_checks",
        "checks_monthly": "monthly_checks",
        "preflight_monthly": "monthly_preflight_checks",
        "recovery_monthly": "monthly_recovery_checks",
        "records_monthly": "monthly_records_reads",
    }
    for raw_key, raw_value in raw_limits.items():
        key = str(raw_key or "").strip().lower()
        key = aliases.get(key, key)
        if key not in LIMIT_COUNTER_LABELS:
            continue
        normalized[key] = _normalize_limit_value(raw_value)
    return normalized


def _normalize_assets_by_network(value: Any) -> Dict[str, List[str]]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, List[str]] = {}
    for raw_chain, raw_assets in value.items():
        chain = _normalize_chain_raw(raw_chain)
        if not chain:
            continue
        assets = []
        if isinstance(raw_assets, str):
            assets = [item.strip().upper() for item in raw_assets.split(",") if item.strip()]
        elif isinstance(raw_assets, (list, tuple, set)):
            assets = [str(item or "").strip().upper() for item in raw_assets if str(item or "").strip()]
        if assets:
            out[chain] = sorted(set(assets))
    return out


def _normalize_policy_map(raw_policy: Any) -> Dict[str, Any]:
    if not isinstance(raw_policy, dict):
        raw_policy = {}
    allowed_origin_hosts = _split_origin_hosts(",".join(_normalize_string_list(raw_policy.get("allowed_origins") or raw_policy.get("allowed_origin_hosts") or [])))
    allowed_ip_cidrs = _normalize_string_list(raw_policy.get("allowed_ip_cidrs") or raw_policy.get("allowed_ips") or [])
    allowed_networks = [_normalize_chain_raw(item) for item in _normalize_string_list(raw_policy.get("allowed_networks") or [])]
    allowed_networks = [item for item in allowed_networks if item]
    allowed_assets_by_network = _normalize_assets_by_network(raw_policy.get("allowed_assets_by_network") or {})
    require_reference_raw = raw_policy.get("require_reference_id_on")
    require_reference_id_on: List[str] = []
    if raw_policy.get("require_reference_id") is True:
        require_reference_id_on = ["preflight", "recovery"]
    else:
        require_reference_id_on = [
            item for item in [str(v or "").strip().lower() for v in _normalize_string_list(require_reference_raw or [])]
            if item in {"preflight", "recovery"}
        ]
    records_max_page_size = _normalize_limit_value(raw_policy.get("records_max_page_size"))
    out: Dict[str, Any] = {
        "allowed_origin_hosts": allowed_origin_hosts,
        "allowed_ip_cidrs": allowed_ip_cidrs,
        "allowed_networks": allowed_networks,
        "allowed_assets_by_network": allowed_assets_by_network,
        "require_reference_id_on": require_reference_id_on,
    }
    if records_max_page_size is not None:
        out["records_max_page_size"] = max(1, min(DEFAULT_RECORDS_MAX_PAGE_SIZE, records_max_page_size))
    return out


def load_plan_limits() -> Dict[str, Dict[str, Optional[int]]]:
    out = {plan: dict(limits) for plan, limits in DEFAULT_PLAN_LIMITS_FALLBACK.items()}
    raw = PLAN_LIMITS_JSON
    if not raw:
        return out
    try:
        parsed = json.loads(raw)
    except Exception:
        return out
    if not isinstance(parsed, dict):
        return out
    for raw_plan, raw_limits in parsed.items():
        plan = str(raw_plan or "").strip().lower()
        if not plan:
            continue
        merged = dict(out.get(plan, {}))
        merged.update(_normalize_limit_map(raw_limits))
        out[plan] = merged
    return out


def load_api_keys() -> Dict[str, Dict[str, Any]]:
    raw = API_KEYS_JSON
    loaded: Dict[str, Dict[str, Any]] = {}
    if not raw:
        return loaded
    try:
        parsed = json.loads(raw)
    except Exception:
        return loaded

    items: List[Dict[str, Any]] = []
    if isinstance(parsed, list):
        items = [item for item in parsed if isinstance(item, dict)]
    elif isinstance(parsed, dict):
        if all(isinstance(v, dict) for v in parsed.values()):
            items = [{**meta, "key": key} for key, meta in parsed.items() if isinstance(meta, dict)]
        else:
            items = [parsed]

    for item in items:
        key = str(item.get("key") or "").strip()
        if not key:
            continue
        scopes_raw = item.get("scopes") or ["preflight", "recovery"]
        if isinstance(scopes_raw, str):
            scopes = {scope.strip().lower() for scope in scopes_raw.split(",") if scope.strip()}
        else:
            scopes = {str(scope).strip().lower() for scope in scopes_raw if str(scope).strip()}
        webhook_events_raw = item.get("webhook_events") or ["preflight_run", "recovery_run"]
        if isinstance(webhook_events_raw, str):
            webhook_events = [evt.strip() for evt in webhook_events_raw.split(",") if evt.strip()]
        else:
            webhook_events = [str(evt).strip() for evt in webhook_events_raw if str(evt).strip()]
        usage_limit_monthly_raw = item.get("usage_limit_monthly")
        try:
            usage_limit_monthly = int(str(usage_limit_monthly_raw).strip()) if usage_limit_monthly_raw not in (None, "") else None
        except Exception:
            usage_limit_monthly = None
        explicit_limits = _normalize_limit_map(item.get("limits") or {})
        if "monthly_checks" not in explicit_limits and usage_limit_monthly is not None:
            explicit_limits["monthly_checks"] = usage_limit_monthly
        raw_policy = item.get("policy") if isinstance(item.get("policy"), dict) else {}
        merged_policy = dict(raw_policy)
        if item.get("allowed_origins") is not None:
            merged_policy["allowed_origins"] = item.get("allowed_origins")
        if item.get("allowed_origin_hosts") is not None:
            merged_policy["allowed_origin_hosts"] = item.get("allowed_origin_hosts")
        if item.get("allowed_ip_cidrs") is not None:
            merged_policy["allowed_ip_cidrs"] = item.get("allowed_ip_cidrs")
        if item.get("allowed_networks") is not None:
            merged_policy["allowed_networks"] = item.get("allowed_networks")
        if item.get("allowed_assets_by_network") is not None:
            merged_policy["allowed_assets_by_network"] = item.get("allowed_assets_by_network")
        if item.get("require_reference_id") is not None:
            merged_policy["require_reference_id"] = item.get("require_reference_id")
        if item.get("require_reference_id_on") is not None:
            merged_policy["require_reference_id_on"] = item.get("require_reference_id_on")
        if item.get("records_max_page_size") is not None:
            merged_policy["records_max_page_size"] = item.get("records_max_page_size")
        normalized_policy = _normalize_policy_map(merged_policy)
        if not any(normalized_policy.get(name) for name in ("allowed_networks", "allowed_assets_by_network", "require_reference_id_on", "allowed_ip_cidrs", "allowed_origin_hosts")):
            tenant_hint = str(item.get("tenant_id") or item.get("client") or item.get("client_label") or item.get("name") or "").strip().lower()
            env_hint = normalize_environment(item.get("environment") or item.get("env") or "live")
            role_hint = normalize_role(item.get("role") or "client")
            if tenant_hint == "live" and env_hint == "live" and role_hint == "client":
                normalized_policy = {
                    **normalized_policy,
                    "allowed_networks": ["ethereum"],
                    "allowed_assets_by_network": {"ethereum": ["USDT", "USDC"]},
                }
        client_value = str(
            item.get("client")
            or item.get("client_name")
            or item.get("client_label")
            or item.get("name")
            or "unknown-client"
        ).strip()[:120]
        tenant_value = str(item.get("tenant_id") or client_value).strip()[:120]
        label_value = str(
            item.get("label")
            or item.get("name")
            or item.get("client_label")
            or item.get("client")
            or client_value
        ).strip()[:160]
        loaded[key] = {
            "client": client_value,
            "active": bool(item.get("active", True)),
            "scopes": scopes or {"preflight", "recovery", "records"},
            "label": label_value,
            "tenant_id": tenant_value,
            "environment": normalize_environment(item.get("environment") or item.get("env") or "live"),
            "role": normalize_role(item.get("role") or "client"),
            "plan": str(item.get("plan") or "pilot").strip()[:80],
            "usage_limit_monthly": usage_limit_monthly,
            "limits": explicit_limits,
            "policy": normalized_policy,
            "webhook_url": str(item.get("webhook_url") or "").strip(),
            "webhook_secret": str(item.get("webhook_secret") or "").strip(),
            "webhook_active": bool(item.get("webhook_active", True)),
            "webhook_events": webhook_events,
            "display_name": label_value,
            "key_fingerprint": api_key_fingerprint(key),
            "key_hint": api_key_hint(key),
        }
    return loaded


PLAN_LIMITS = load_plan_limits()
PUBLIC_DEMO_HOSTS = _split_origin_hosts(PUBLIC_DEMO_ORIGINS)
API_KEYS = load_api_keys()


def current_request_id() -> str:
    if has_request_context() and getattr(g, "request_id", None):
        return str(g.request_id)
    return ""


def ensure_request_id() -> str:
    incoming = str(request.headers.get("X-Request-ID") or request.headers.get("X-Correlation-ID") or "").strip()
    if incoming:
        return incoming[:128]
    return f"req_{uuid.uuid4().hex[:16]}"


def request_duration_ms() -> int:
    started = getattr(g, "request_started_at", None)
    if started is None:
        return 0
    return max(0, int((time.perf_counter() - started) * 1000))


def json_dumps_safe(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def emit_structured_log(event: str, level: str = "info", **fields: Any) -> None:
    payload = {
        "ts": utc_now_iso(),
        "level": level.upper(),
        "service": "payeeproof-api",
        "version": APP_VERSION,
        "event": event,
    }
    request_id = current_request_id()
    if request_id:
        payload["request_id"] = request_id
    payload.update({k: v for k, v in fields.items() if v is not None})
    log_method = getattr(logger, str(level or "info").lower(), logger.info)
    log_method(json_dumps_safe(payload))


def looks_like_timeout(value: Any) -> bool:
    text = str(value or "").lower()
    if not text:
        return False
    return "timeout" in text or "timed out" in text or "read timed out" in text or "connect timeout" in text


def normalize_metric_status(status: Any, http_status: int) -> str:
    raw = str(status or "").strip().lower()
    if raw in {"ok", "success", "safe", "found", "ready", "review_required", "blocked", "not_found", "reverted"}:
        return "success"
    if raw in {"error", "failed", "invalid"}:
        return "error"
    if raw in {"timeout"}:
        return "timeout"
    if raw in {"rejected", "filtered", "deduplicated"}:
        return "rejected"
    if http_status >= 500:
        return "error"
    if http_status >= 400:
        return "rejected"
    if raw in {"unavailable", "degraded", "partial"}:
        return "degraded"
    return "success"


def current_access_meta() -> Dict[str, str]:
    access = getattr(g, "api_access", None) or {}
    client_label = normalize_text(str(access.get("client") or "website"), 120)
    tenant_id = normalize_text(str(access.get("tenant_id") or client_label), 120)
    environment = normalize_environment(access.get("environment") or "live")
    role = normalize_role(access.get("role") or "client")
    return {
        "access_mode": str(access.get("mode") or "public"),
        "client_label": client_label,
        "tenant_id": tenant_id,
        "environment": environment,
        "role": role,
    }


def infer_network_from_payload(path: str, payload: Optional[Dict[str, Any]]) -> str:
    payload = payload or {}
    if path == "/api/preflight-check":
        provided = payload.get("provided") or {}
        expected = payload.get("expected") or {}
        return normalize_chain(provided.get("network") or provided.get("chain") or expected.get("network") or expected.get("chain") or "")
    if path == "/api/recovery-copilot":
        return normalize_chain(payload.get("network") or payload.get("chain") or payload.get("intended_chain") or "")
    if path == "/pilot-request":
        return "pilot"
    return ""


def current_json_payload() -> Dict[str, Any]:
    try:
        payload = request.get_json(silent=True)
    except Exception:
        payload = None
    return payload if isinstance(payload, dict) else {}


def record_request_event(
    *,
    event_name: str,
    endpoint: str,
    status: str,
    reason_code: str = "",
    network: str = "",
    http_status: int = 200,
    timeout_flag: bool = False,
    error_message: str = "",
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    meta = current_access_meta()
    payload_meta = metadata or {}
    record = {
        "created_at": utc_now_iso(),
        "request_id": current_request_id(),
        "event_name": event_name,
        "endpoint": endpoint,
        "status": normalize_metric_status(status, http_status),
        "reason_code": str(reason_code or "").strip(),
        "network": str(network or "").strip(),
        "http_status": int(http_status),
        "timeout_flag": 1 if timeout_flag else 0,
        "duration_ms": request_duration_ms(),
        "access_mode": meta["access_mode"],
        "tenant_id": meta["tenant_id"],
        "client_label": meta["client_label"],
        "environment": meta["environment"],
        "role": meta["role"],
        "source_ip": get_client_ip(),
        "error_message": (str(error_message or "")[:500] if error_message else ""),
        "metadata_json": json_dumps_safe(payload_meta) if payload_meta else "",
    }
    conn = get_db()
    try:
        db_execute(
            conn,
            """
            INSERT INTO event_log(
                created_at, request_id, event_name, endpoint, status, reason_code, network,
                http_status, timeout_flag, duration_ms, access_mode, tenant_id, client_label,
                environment, role, source_ip, error_message, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["created_at"],
                record["request_id"],
                record["event_name"],
                record["endpoint"],
                record["status"],
                record["reason_code"],
                record["network"],
                record["http_status"],
                record["timeout_flag"],
                record["duration_ms"],
                record["access_mode"],
                record["tenant_id"],
                record["client_label"],
                record["environment"],
                record["role"],
                record["source_ip"],
                record["error_message"],
                record["metadata_json"],
            ),
        )
        conn.commit()
    except Exception as exc:
        emit_structured_log(
            "observability_write_failed",
            level="warning",
            endpoint=endpoint,
            event_name=event_name,
            error=str(exc),
        )
    finally:
        conn.close()

    emit_structured_log(
        event_name,
        level="warning" if (timeout_flag or http_status >= 500) else "info",
        endpoint=endpoint,
        status=record["status"],
        reason_code=record["reason_code"],
        network=record["network"],
        http_status=http_status,
        timeout=bool(timeout_flag),
        duration_ms=record["duration_ms"],
        access_mode=record["access_mode"],
        tenant_id=record["tenant_id"],
        client_label=record["client_label"],
        environment=record["environment"],
        role=record["role"],
    )
    evaluate_recent_alerts(trigger_event=record)
    if has_request_context():
        g.event_logged = True


def event_name_for_path(path: str) -> str:
    return {
        "/api/preflight-check": "preflight_run",
        "/api/recovery-copilot": "recovery_run",
        "/pilot-request": "pilot_submit_fail",
    }.get(path.rstrip("/") or path, "request_event")


def record_request_failure(path: str, status_code: int, message: str) -> None:
    payload = current_json_payload()
    timeout_flag = looks_like_timeout(message)
    event_name = event_name_for_path(path)
    if path == "/pilot-request" and status_code < 500:
        event_name = "pilot_submit_fail"
    record_request_event(
        event_name=event_name,
        endpoint=path,
        status="timeout" if timeout_flag else "error",
        reason_code=("TIMEOUT" if timeout_flag else f"HTTP_{status_code}"),
        network=infer_network_from_payload(path, payload),
        http_status=status_code,
        timeout_flag=timeout_flag,
        error_message=message,
        metadata={"path": path},
    )


def get_recent_event_rows(window_sec: int = OBSERVABILITY_WINDOW_SEC) -> List[Any]:
    cutoff = datetime.fromtimestamp(max(0, time.time() - window_sec), tz=timezone.utc).replace(microsecond=0).isoformat()
    conn = get_db()
    try:
        rows = db_fetchall(
            conn,
            """
            SELECT created_at, event_name, endpoint, status, reason_code, network, http_status, timeout_flag, duration_ms
            FROM event_log
            WHERE created_at >= ?
            ORDER BY id DESC
            """,
            (cutoff,),
        )
        return rows
    except Exception:
        return []
    finally:
        conn.close()


def build_metrics_snapshot(window_sec: int = OBSERVABILITY_WINDOW_SEC) -> Dict[str, Any]:
    rows = get_recent_event_rows(window_sec)
    summary: Dict[str, Any] = {
        "window_sec": window_sec,
        "total_requests": len(rows),
        "success": 0,
        "error": 0,
        "timeout": 0,
        "rejected": 0,
        "degraded": 0,
        "by_event": {},
        "by_network": {},
    }
    for row in rows:
        status = str(row["status"] if isinstance(row, dict) else row[3])
        event_name = str(row["event_name"] if isinstance(row, dict) else row[1])
        network = str((row["network"] if isinstance(row, dict) else row[5]) or "unknown")
        summary[status] = summary.get(status, 0) + 1
        bucket = summary["by_event"].setdefault(event_name, {"total": 0, "success": 0, "error": 0, "timeout": 0, "rejected": 0, "degraded": 0})
        bucket["total"] += 1
        bucket[status] = bucket.get(status, 0) + 1
        n_bucket = summary["by_network"].setdefault(network or "unknown", {"total": 0, "success": 0, "error": 0, "timeout": 0, "rejected": 0, "degraded": 0})
        n_bucket["total"] += 1
        n_bucket[status] = n_bucket.get(status, 0) + 1
    return summary


def evaluate_recent_alerts(trigger_event: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    snapshot = build_metrics_snapshot(OBSERVABILITY_WINDOW_SEC)
    alerts: List[Dict[str, Any]] = []
    for event_name, stats in snapshot.get("by_event", {}).items():
        total = int(stats.get("total", 0))
        if total < OBSERVABILITY_MIN_REQUESTS:
            continue
        error_rate = (stats.get("error", 0) + stats.get("rejected", 0)) / max(1, total)
        timeout_rate = stats.get("timeout", 0) / max(1, total)
        if error_rate >= ALERT_ERROR_RATE_THRESHOLD:
            alerts.append({
                "level": "warning",
                "type": "high_error_rate",
                "event_name": event_name,
                "total": total,
                "error_rate": round(error_rate, 3),
                "window_sec": OBSERVABILITY_WINDOW_SEC,
            })
        if timeout_rate >= ALERT_TIMEOUT_RATE_THRESHOLD:
            alerts.append({
                "level": "warning",
                "type": "high_timeout_rate",
                "event_name": event_name,
                "total": total,
                "timeout_rate": round(timeout_rate, 3),
                "window_sec": OBSERVABILITY_WINDOW_SEC,
            })
    now = time.time()
    if trigger_event and alerts:
        with ALERT_LOCK:
            for alert in alerts:
                key = f"{alert['type']}:{alert['event_name']}"
                last_sent = ALERT_STATE.get(key, 0.0)
                if now - last_sent >= ALERT_COOLDOWN_SEC:
                    ALERT_STATE[key] = now
                    emit_structured_log("alert_triggered", **{**alert, "level": str(alert.get("level") or "warning")})
    return alerts


def get_client_ip() -> str:
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return (request.remote_addr or "unknown").strip()


def extract_api_key() -> str:
    direct = str(request.headers.get("X-API-Key") or "").strip()
    if direct:
        return direct
    auth = str(request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return ""


def is_allowed_public_demo_request() -> bool:
    origin_host = _extract_host(request.headers.get("Origin", ""))
    referer_host = _extract_host(request.headers.get("Referer", ""))
    return bool(origin_host and origin_host in PUBLIC_DEMO_HOSTS) or bool(referer_host and referer_host in PUBLIC_DEMO_HOSTS)


def authenticate_api_request(required_scope: str) -> Dict[str, Any]:
    api_key = extract_api_key()
    if api_key:
        record = API_KEYS.get(api_key)
        if not record or not record.get("active"):
            raise ApiError("Invalid API key.", 401)
        scopes = set(record.get("scopes") or set())
        if required_scope not in scopes and "*" not in scopes:
            raise ApiError("API key is not allowed for this endpoint.", 403)
        touch_tenant_api_key(api_key, record)
        return {
            "mode": "api_key",
            "scope": required_scope,
            "client": record.get("client") or "unknown-client",
            "label": record.get("label") or "",
            "scopes": sorted(list(scopes)),
            "tenant_id": record.get("tenant_id") or (record.get("client") or "unknown-client"),
            "environment": normalize_environment(record.get("environment") or "live"),
            "role": normalize_role(record.get("role") or "client"),
            "plan": record.get("plan") or "pilot",
            "usage_limit_monthly": record.get("usage_limit_monthly"),
            "limits": dict(record.get("limits") or {}),
            "policy": dict(record.get("policy") or {}),
            "webhook_url": record.get("webhook_url") or "",
            "webhook_secret": record.get("webhook_secret") or "",
            "webhook_active": bool(record.get("webhook_active", True)),
            "webhook_events": list(record.get("webhook_events") or []),
            "key_fingerprint": record.get("key_fingerprint") or api_key_fingerprint(api_key),
            "key_hint": record.get("key_hint") or api_key_hint(api_key),
        }

    if is_allowed_public_demo_request():
        return {
            "mode": "public_demo",
            "scope": required_scope,
            "client": "website-demo",
            "label": "Public website demo",
            "scopes": [required_scope],
            "tenant_id": "website-demo",
            "environment": "public-demo",
            "role": "demo",
            "plan": "demo",
            "usage_limit_monthly": None,
            "limits": {},
            "policy": {},
        }

    raise ApiError("API key required for direct API access.", 401)


def consume_rate_limit(bucket: str, limit: int, window_sec: int) -> Tuple[bool, Dict[str, int]]:
    now = time.time()
    with RATE_LIMIT_LOCK:
        queue = RATE_LIMIT_BUCKETS.setdefault(bucket, deque())
        while queue and queue[0] <= now - window_sec:
            queue.popleft()
        if len(queue) >= limit:
            retry_after = max(1, int(window_sec - (now - queue[0])))
            return False, {
                "limit": limit,
                "remaining": 0,
                "retry_after": retry_after,
                "window_sec": window_sec,
            }
        queue.append(now)
        remaining = max(0, limit - len(queue))
        return True, {
            "limit": limit,
            "remaining": remaining,
            "retry_after": 0,
            "window_sec": window_sec,
        }


def pilot_request_id() -> str:
    return f"ppf_{uuid.uuid4().hex[:16]}"


def db_is_postgres() -> bool:
    return DB_BACKEND == "postgres"


def db_sql(sql: str) -> str:
    if db_is_postgres():
        return sql.replace("?", "%s")
    return sql


def db_execute(conn: Any, sql: str, params: Tuple[Any, ...] = ()) -> None:
    if db_is_postgres():
        with conn.cursor() as cur:
            cur.execute(db_sql(sql), params)
        return
    conn.execute(sql, params)


def db_fetchone(conn: Any, sql: str, params: Tuple[Any, ...] = ()) -> Optional[Any]:
    if db_is_postgres():
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(db_sql(sql), params)
            return cur.fetchone()
    return conn.execute(sql, params).fetchone()


def db_fetchall(conn: Any, sql: str, params: Tuple[Any, ...] = ()) -> List[Any]:
    if db_is_postgres():
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(db_sql(sql), params)
            return list(cur.fetchall())
    return conn.execute(sql, params).fetchall()


def get_table_columns(conn: Any, table_name: str) -> set[str]:
    if db_is_postgres():
        rows = db_fetchall(
            conn,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = ?
            """,
            (table_name,),
        )
        return {str(row["column_name"]) for row in rows}
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def ensure_column(conn: Any, table_name: str, column_name: str, ddl: str) -> None:
    if column_name not in get_table_columns(conn, table_name):
        db_execute(conn, f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")


def find_recent_duplicate_request(conn: Any, fingerprint: str, ttl_sec: int) -> Optional[Any]:
    cutoff = datetime.fromtimestamp(max(0, time.time() - ttl_sec), tz=timezone.utc).replace(microsecond=0).isoformat()
    return db_fetchone(
        conn,
        """
        SELECT request_id, created_at, email_status
        FROM pilot_requests
        WHERE fingerprint = ? AND created_at >= ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (fingerprint, cutoff),
    )


def update_pilot_email_status(request_id: str, email_result: Dict[str, Optional[str]], *, prefix: str = "email") -> None:
    status_column = f"{prefix}_status"
    id_column = f"{prefix}_id"
    error_column = f"{prefix}_error"
    attempted_column = f"{prefix}_last_attempt_at"
    conn = get_db()
    try:
        db_execute(
            conn,
            f"""
            UPDATE pilot_requests
            SET {status_column} = ?,
                {id_column} = ?,
                {error_column} = ?,
                {attempted_column} = ?
            WHERE request_id = ?
            """,
            (
                str(email_result.get("status") or "unknown"),
                email_result.get("email_id"),
                email_result.get("debug_detail"),
                utc_now_iso(),
                request_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_pilot_delivery_status(request_id: str, email_result: Dict[str, Optional[str]]) -> None:
    update_pilot_email_status(request_id, email_result, prefix="email")


def update_pilot_welcome_status(request_id: str, email_result: Dict[str, Optional[str]]) -> None:
    update_pilot_email_status(request_id, email_result, prefix="welcome_email")


def pilot_ack_response(
    message: str,
    submitted_at: Optional[str] = None,
    stored: bool = False,
    email_notification: str = "skipped",
    welcome_email_notification: str = "skipped",
    status_code: int = 200,
    request_id: Optional[str] = None,
    persisted: Optional[bool] = None,
):
    payload = {
        "ok": True,
        "message": message,
        "stored": stored,
        "email_notification": email_notification,
        "welcome_email_notification": welcome_email_notification,
        "trace_id": current_request_id(),
    }
    if submitted_at:
        payload["submitted_at"] = submitted_at
    if request_id:
        payload["request_id"] = request_id
    if persisted is not None:
        payload["persisted"] = persisted
    return jsonify(payload), status_code


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_chain(value: str) -> str:
    raw = str(value or "").strip().lower()
    return CHAIN_ALIASES.get(raw, raw)

def normalize_issue_type(value: str) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "": "auto",
        "unknown": "auto",
        "general": "auto",
        "general_analysis": "auto",
        "auto_detect": "auto",
        "auto-detect": "auto",
        "auto": "auto",
    }
    if raw in aliases:
        return aliases[raw]
    if raw in {"wrong_network", "wrong_address", "sent_to_contract", "missing_memo"}:
        return raw
    return "auto"


def issue_type_label(value: str) -> str:
    labels = {
        "auto": "Auto-detect / not specified",
        "wrong_network": "Wrong network",
        "wrong_address": "Wrong address",
        "sent_to_contract": "Sent to smart contract / app",
        "missing_memo": "Missing memo / tag",
    }
    return labels.get(normalize_issue_type(value), str(value or "").replace("_", " ").title() or "Auto-detect / not specified")


def load_rpc_urls() -> Dict[str, str]:
    out = dict(DEFAULT_RPC_URLS)
    raw = os.getenv("RPC_URLS_JSON", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    out[normalize_chain(k)] = str(v or "").strip()
        except Exception:
            pass
    return out


def load_known_destinations() -> Dict[str, Dict[str, Dict[str, str]]]:
    raw = os.getenv("KNOWN_DESTINATIONS_JSON", "").strip()
    candidates: List[Any] = []
    if raw:
        candidates.append(raw)
    file_path = Path(__file__).with_name("known_destinations.json")
    if file_path.exists():
        try:
            candidates.append(file_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    normalized: Dict[str, Dict[str, Dict[str, str]]] = {}
    for item in candidates:
        try:
            parsed = json.loads(item)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        for chain_key, chain_values in parsed.items():
            chain = normalize_chain(chain_key)
            if not isinstance(chain_values, dict):
                continue
            bucket = normalized.setdefault(chain, {})
            for address, meta in chain_values.items():
                if not isinstance(meta, dict):
                    continue
                bucket[str(address).lower()] = {
                    "classification": str(meta.get("classification") or "unknown").strip().lower(),
                    "label": str(meta.get("label") or "").strip(),
                    "explanation": str(meta.get("explanation") or "").strip(),
                }
    return normalized


RPC_URLS = load_rpc_urls()
KNOWN_DESTINATIONS = load_known_destinations()


def is_supported_chain(chain: str) -> bool:
    return chain in SUPPORTED_ASSETS_BY_CHAIN


def is_supported_asset_for_chain(chain: str, asset: str) -> bool:
    if not is_supported_chain(chain):
        return False
    return str(asset or "").strip().upper() in SUPPORTED_ASSETS_BY_CHAIN.get(chain, set())


def lookup_known_destination(chain: str, address: str) -> Optional[Dict[str, str]]:
    if not chain or not address:
        return None
    chain_map = KNOWN_DESTINATIONS.get(chain, {})
    return chain_map.get(str(address).lower())


def normalize_destination_classification(raw_type: str) -> str:
    raw = str(raw_type or "unknown").strip().lower()
    if raw in {"eoa", "wallet", "personal_wallet", "externally_owned_account", "account"}:
        return "personal_wallet"
    if raw in {"contract", "program", "executable", "contract_or_app", "smart_contract"}:
        return "contract_or_app"
    if raw in {"exchange_like_deposit", "exchange_deposit", "deposit_address"}:
        return "exchange_like_deposit"
    if raw in {"bridge_router", "bridge", "router", "bridge_or_router"}:
        return "bridge_router"
    if raw in {"invalid"}:
        return "invalid"
    if raw in {"not_found"}:
        return "not_found"
    return "unknown"


def build_destination_profile(chain: str, address: str, onchain: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    onchain = onchain or {}
    known = lookup_known_destination(chain, address)
    classification = normalize_destination_classification((known or {}).get("classification") or onchain.get("address_type"))
    profile_meta = DESTINATION_PROFILE_MAP.get(classification, DESTINATION_PROFILE_MAP["unknown"])
    label = (known or {}).get("label") or profile_meta["label"]
    explanation = (known or {}).get("explanation") or profile_meta["explanation"]

    if classification == "unknown" and not onchain.get("rpc_used") and onchain.get("details"):
        explanation = "Live destination lookup is unavailable right now. Retry or route this transfer for manual review."
    if classification == "not_found" and onchain.get("details"):
        explanation = str(onchain.get("details"))

    return {
        "classification": classification,
        "label": label,
        "explanation": explanation,
        "address": address,
        "raw_type": onchain.get("address_type", "unknown"),
        "rpc_used": bool(onchain.get("rpc_used")),
        "source": "mapping" if known else ("rpc" if onchain.get("rpc_used") else "unavailable"),
    }


def preflight_next_step_label(action: str) -> str:
    labels = {
        "SAFE_TO_PROCEED": "Proceed with the payment",
        "BLOCK_AND_REVERIFY": "Stop and re-check the details",
        "RECHECK_MEMO_OR_TAG": "Re-check the memo or destination tag",
        "TEST_FIRST": "Send a small test first",
        "CHECK_BACKEND": "Retry when the live service is available",
        "MANUAL_REVIEW": "Escalate for manual review",
        "DO_NOT_SEND": "Do not send",
        "REVIEW_REQUEST_TEMPLATE": "Correct the approved payout instructions",
        "REVERIFY_DESTINATION": "Re-verify the destination route",
        "CONFIRM_DESTINATION": "Confirm the destination before approval",
    }
    return labels.get(str(action or "").upper(), str(action or "").replace("_", " ").title() or "Review required")


POLICY_PROFILE_ALIASES = {
    "": "standard",
    "default": "standard",
    "standard": "standard",
    "payout_strict": "payout_strict",
    "strict": "payout_strict",
    "deposit_review": "deposit_review",
    "deposit": "deposit_review",
    "treasury_review": "treasury_review",
    "treasury": "treasury_review",
}


def normalize_policy_profile(value: Any) -> str:
    key = str(value or "").strip().lower()
    return POLICY_PROFILE_ALIASES.get(key, "standard")


def policy_profile_label(profile: str) -> str:
    labels = {
        "standard": "Standard",
        "payout_strict": "Payout Strict",
        "deposit_review": "Deposit Review",
        "treasury_review": "Treasury Review",
    }
    return labels.get(normalize_policy_profile(profile), "Standard")


def derive_preflight_policy_override(destination_class: str, policy_profile: str) -> Optional[Dict[str, str]]:
    profile = normalize_policy_profile(policy_profile)
    destination_class = str(destination_class or "").lower()
    if profile == "standard" or destination_class in {"", "personal_wallet", "unavailable"}:
        return None

    reason_map = {
        "contract_or_app": "DESTINATION_IS_CONTRACT_OR_APP",
        "bridge_router": "DESTINATION_IS_BRIDGE_ROUTER",
        "exchange_like_deposit": "DESTINATION_REQUIRES_MEMO_OR_VENUE_CHECK",
        "unknown": "DESTINATION_NOT_CLASSIFIED",
        "not_found": "DESTINATION_NOT_CLASSIFIED",
    }
    summary_map = {
        "contract_or_app": "The destination looks like a contract or app rather than a simple wallet route.",
        "bridge_router": "The destination looks like bridge or router infrastructure rather than a simple wallet route.",
        "exchange_like_deposit": "The destination looks like an exchange-style or venue-dependent deposit route.",
        "unknown": "The destination could not be classified confidently.",
        "not_found": "The destination could not be classified confidently.",
    }
    reason_code = reason_map.get(destination_class)
    summary = summary_map.get(destination_class)
    if not reason_code or not summary:
        return None

    if profile == "payout_strict":
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": reason_code,
            "next_action": "BLOCK_AND_REVERIFY",
            "confidence": "High",
            "summary": summary,
            "why": "Payout Strict only approves clear personal-wallet style destinations. Ambiguous, infrastructure, or venue-dependent routes should be stopped and re-verified before funds move.",
        }

    if profile == "deposit_review":
        return {
            "status": "review_required",
            "verdict": "REVERIFY",
            "reason_code": reason_code,
            "next_action": "REVERIFY_DESTINATION" if destination_class != "exchange_like_deposit" else "RECHECK_MEMO_OR_TAG",
            "confidence": "Medium",
            "summary": summary,
            "why": "Deposit Review keeps ambiguous or venue-dependent routes in a confirmation path instead of treating them like normal wallet payouts.",
        }

    if profile == "treasury_review":
        if destination_class in {"exchange_like_deposit", "bridge_router"}:
            return {
                "status": "blocked",
                "verdict": "BLOCK",
                "reason_code": reason_code,
                "next_action": "BLOCK_AND_REVERIFY",
                "confidence": "High",
                "summary": summary,
                "why": "Treasury Review blocks routes that look like infrastructure or venue-dependent deposit paths until the destination is re-confirmed through an approved internal process.",
            }
        return {
            "status": "review_required",
            "verdict": "REVERIFY",
            "reason_code": reason_code,
            "next_action": "REVERIFY_DESTINATION",
            "confidence": "Medium",
            "summary": summary,
            "why": "Treasury Review keeps ambiguous destinations in a cautious review path before release.",
        }

    return None


def derive_preflight_outcome(
    *,
    checks: Dict[str, bool],
    expected_chain: str,
    provided_chain: str,
    expected_asset: str,
    provided_asset: str,
    expected_address: str,
    provided_address: str,
    expected_valid: bool,
    provided_valid: bool,
    provided_destination: Dict[str, Any],
    risk_flags: List[str],
    policy_profile: str = "standard",
) -> Dict[str, str]:
    mismatch_reason = None
    for code, is_failed in [
        ("NETWORK_MISMATCH", not checks.get("network_match", False)),
        ("ASSET_MISMATCH", not checks.get("asset_match", False)),
        ("ADDRESS_MISMATCH", not checks.get("address_match", False)),
        ("MEMO_MISMATCH", not checks.get("memo_match", False)),
    ]:
        if is_failed:
            mismatch_reason = code
            break

    if not is_supported_chain(expected_chain) or not is_supported_chain(provided_chain):
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": "UNSUPPORTED_NETWORK",
            "next_action": "BLOCK_AND_REVERIFY",
            "confidence": "High",
            "summary": "The selected network is outside the current supported scope.",
            "why": "This transfer cannot be verified reliably on the selected network yet. Route it for manual review or use a supported network.",
        }

    if not is_supported_asset_for_chain(expected_chain, expected_asset) or not is_supported_asset_for_chain(provided_chain, provided_asset):
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": "UNSUPPORTED_ASSET_OR_NETWORK",
            "next_action": "BLOCK_AND_REVERIFY",
            "confidence": "High",
            "summary": "The asset and network combination is outside the current supported scope.",
            "why": "The product could not validate this asset and network combination with enough confidence. Re-check the routing details before funds move.",
        }

    if not provided_valid or not expected_valid:
        invalid_side = "provided" if not provided_valid else "expected"
        next_action = "DO_NOT_SEND" if invalid_side == "provided" else "REVIEW_REQUEST_TEMPLATE"
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": "INVALID_ADDRESS",
            "next_action": next_action,
            "confidence": "High",
            "summary": "At least one address is invalid for the selected network.",
            "why": "The address format itself failed validation. This should be blocked before any transfer is attempted.",
        }

    if provided_chain in EVM_CHAINS and provided_address.lower() == ZERO_EVM:
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": "ZERO_ADDRESS",
            "next_action": "DO_NOT_SEND",
            "confidence": "High",
            "summary": "The provided destination is the zero address.",
            "why": "The zero address is not a valid payout destination. Sending there would be a hard operational error.",
        }

    if mismatch_reason:
        why_map = {
            "NETWORK_MISMATCH": "The submitted destination is on a different chain than the approved request. This is the classic wrong-network mistake.",
            "ASSET_MISMATCH": "The payout asset in the submitted details does not match the approved instructions.",
            "ADDRESS_MISMATCH": "The provided payout address does not match the approved destination.",
            "MEMO_MISMATCH": "Memo or tag values differ. For custodial destinations, that can prevent credit even when the transfer lands on-chain.",
        }
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": mismatch_reason,
            "next_action": "BLOCK_AND_REVERIFY",
            "confidence": "High",
            "summary": "The submitted payout details do not match the approved instructions.",
            "why": why_map.get(mismatch_reason, "The submitted payout details do not match the approved instructions."),
        }

    destination_class = provided_destination.get("classification")

    if provided_destination.get("source") == "unavailable":
        return {
            "status": "unavailable",
            "verdict": "UNAVAILABLE",
            "reason_code": "DESTINATION_LOOKUP_UNAVAILABLE",
            "next_action": "CHECK_BACKEND",
            "confidence": "Limited",
            "summary": "Live destination lookup is currently unavailable.",
            "why": "The service could compare the transfer fields, but the live destination classification step did not complete. Retry or review manually.",
        }

    profile_override = derive_preflight_policy_override(destination_class, policy_profile)
    if profile_override:
        return profile_override

    if destination_class == "bridge_router":
        return {
            "status": "review_required",
            "verdict": "TEST FIRST",
            "reason_code": "DESTINATION_IS_BRIDGE_ROUTER",
            "next_action": "TEST_FIRST",
            "confidence": "Medium",
            "summary": "The details match, but the destination looks like bridge or router infrastructure.",
            "why": "Infrastructure destinations can be valid, but they behave differently from a personal wallet. A small test first is safer than a full send.",
        }

    if destination_class == "contract_or_app":
        return {
            "status": "review_required",
            "verdict": "TEST FIRST",
            "reason_code": "DESTINATION_IS_CONTRACT_OR_APP",
            "next_action": "TEST_FIRST",
            "confidence": "Medium",
            "summary": "The details match, but the destination looks like a contract or app.",
            "why": "Contract destinations can accept funds differently from a personal wallet. A small test first reduces irreversible mistakes.",
        }

    if destination_class == "exchange_like_deposit":
        return {
            "status": "review_required",
            "verdict": "REVERIFY",
            "reason_code": "DESTINATION_REQUIRES_MEMO_OR_VENUE_CHECK",
            "next_action": "RECHECK_MEMO_OR_TAG",
            "confidence": "Medium",
            "summary": "The details match, but the destination looks like a deposit-style address.",
            "why": "Deposit-style destinations often depend on the exact venue, network, asset, and memo or tag. Re-verify all of them before sending.",
        }

    if destination_class in {"unknown", "not_found"}:
        return {
            "status": "review_required",
            "verdict": "REVERIFY",
            "reason_code": "DESTINATION_NOT_CLASSIFIED",
            "next_action": "BLOCK_AND_REVERIFY",
            "confidence": "Medium",
            "summary": "Core details match, but the destination could not be classified confidently.",
            "why": "The network, asset, address, and memo checks matched, but the destination does not yet look confidently like a known personal wallet flow.",
        }

    return {
        "status": "verified",
        "verdict": "SAFE",
        "reason_code": "OK",
        "next_action": "SAFE_TO_PROCEED",
        "confidence": "High",
        "summary": "Core details match and the destination looks like a personal wallet.",
        "why": "Network, asset, address, and memo or tag checks matched cleanly, and the destination looks like a normal wallet rather than infrastructure.",
    }


def derive_recovery_outcome(status: str, observed_status: str) -> str:
    status = str(status or "").lower()
    observed_status = str(observed_status or "").lower()
    if status == "not_found":
        return "NOT FOUND"
    if status in {"error", "unavailable"}:
        return "UNAVAILABLE"
    if status in {"failed", "reverted"} or observed_status in {"failed", "reverted"}:
        return "FAILED ON-CHAIN"
    return "RESULT READY"


def derive_recovery_confidence(status: str, observed_status: str) -> str:
    status = str(status or "").lower()
    observed_status = str(observed_status or "").lower()
    if status == "not_found":
        return "High"
    if status in {"error", "unavailable"}:
        return "Limited"
    if observed_status in {"confirmed", "failed", "reverted"} or status in {"found", "failed", "reverted"}:
        return "High"
    if observed_status in {"pending", "pending_or_unknown"}:
        return "Medium"
    return "Medium"


def build_recovery_explanation(analysis: Dict[str, Any], destination_profile: Dict[str, Any]) -> str:
    reason_code = str(analysis.get("reason_code") or "").upper()
    if reason_code == "TX_NOT_FOUND":
        return "No matching transaction was found on the selected network. Re-check the transaction hash and the network before assuming funds moved."
    if reason_code == "RPC_UNAVAILABLE":
        return "The live chain lookup did not complete, so this result should be treated as unavailable rather than final guidance."
    if reason_code in {"TX_REVERTED", "TX_FAILED"}:
        return "The transaction failed on-chain. Funds were not delivered, but fees may have been spent, so the next step is to understand the failure before retrying."
    if reason_code == "WRONG_NETWORK_CONFIRMED":
        return "The transaction exists on a different network than intended. Recovery depends on whether the recipient controls the address on that chain or can assist manually."
    if reason_code == "WRONG_ADDRESS_CONFIRMED":
        return "Funds reached a different address than intended. Recovery is generally unlikely unless that destination is controlled by you or by a platform that can help."
    if reason_code in {"DESTINATION_IS_CONTRACT", "DESTINATION_IS_PROGRAM"}:
        return "The destination looks like application infrastructure rather than a normal wallet. Recovery depends on contract or program design and operator access."
    base = str(analysis.get("summary") or "").strip()
    if destination_profile.get("classification") in {"contract_or_app", "bridge_router"}:
        return f"{base} The destination also looks like infrastructure rather than a personal wallet.".strip()
    return base or "Recovery guidance is ready."




def recovery_verdict_label(recoverability: str, status: str, reason_code: str) -> str:
    recoverability = str(recoverability or "").lower()
    status = str(status or "").lower()
    reason_code = str(reason_code or "").upper()

    if status == "not_found" or reason_code == "TX_NOT_FOUND":
        return "No on-chain match yet"
    if status in {"unavailable", "error"} or reason_code == "RPC_UNAVAILABLE":
        return "Live lookup unavailable"
    if recoverability == "likely":
        return "Likely recoverable"
    if recoverability == "possible":
        return "Possible with support"
    if recoverability in {"manual_review", "depends"}:
        return "Depends on platform or operator"
    if recoverability == "unlikely":
        return "Recovery unlikely"
    return "Needs review"


def recovery_contact_target(destination_profile: Dict[str, Any], issue_type: str, status: str, reason_code: str) -> str:
    classification = str(destination_profile.get("classification") or "").lower()
    issue_type = str(issue_type or "").lower()
    status = str(status or "").lower()
    reason_code = str(reason_code or "").upper()

    if status == "not_found" or reason_code == "TX_NOT_FOUND":
        return "Sender or exchange support"
    if issue_type == "missing_memo" or classification == "exchange_like_deposit":
        return "Destination platform support"
    if classification in {"contract_or_app", "bridge_router"} or issue_type == "sent_to_contract":
        return "App, protocol, or contract operator"
    if issue_type == "wrong_address" and classification == "personal_wallet":
        return "Owner of the receiving wallet"
    if issue_type == "wrong_network":
        return "Recipient or platform support"
    if classification == "personal_wallet":
        return "Recipient or wallet owner"
    return "Recipient or platform support"


def recovery_best_next_step(destination_profile: Dict[str, Any], analysis: Dict[str, Any], issue_type: str) -> str:
    classification = str(destination_profile.get("classification") or "").lower()
    status = str(analysis.get("status") or "").lower()
    reason_code = str(analysis.get("reason_code") or "").upper()
    issue_type = str(issue_type or "").lower()

    if status == "not_found" or reason_code == "TX_NOT_FOUND":
        return "Verify the chain and transaction hash before doing anything else."
    if reason_code in {"TX_REVERTED", "TX_FAILED"}:
        return "Do not resend. Find the failure reason first."
    if issue_type == "missing_memo" or classification == "exchange_like_deposit":
        return "Open a support ticket with the destination platform and include the full transaction record."
    if classification in {"contract_or_app", "bridge_router"} or issue_type == "sent_to_contract":
        return "Contact the app or protocol and ask whether a rescue path exists for this transfer."
    if issue_type == "wrong_network":
        return "Check whether the recipient controls the same address on the chain where funds actually arrived."
    if issue_type == "wrong_address":
        return "Confirm whether the receiving address belongs to you, your team, or a platform that can help."
    return "Use the tx hash as the anchor and contact the party that controls the destination."


def recovery_asset_hint(observed: Dict[str, Any]) -> str:
    token_transfer = observed.get("token_transfer") or {}
    token_contract = str(token_transfer.get("token_contract") or "").strip()
    native_value = observed.get("native_value_wei")
    if token_contract:
        return f"Token transfer via contract {token_contract}"
    if native_value not in (None, "", 0, "0"):
        return "Native token transfer"
    token_balances = observed.get("token_balances") or {}
    if token_balances.get("post") or token_balances.get("pre"):
        return "Token movement observed"
    return "Asset type not fully classified"


def build_recovery_support_packet(
    *,
    chain: str,
    tx_hash: str,
    issue_type: str,
    analysis: Dict[str, Any],
    destination_profile: Dict[str, Any],
    checked_at: str,
    outcome: str,
    confidence: str,
    why: str,
) -> Dict[str, Any]:
    observed = analysis.get("observed") or {}
    contact_target = recovery_contact_target(destination_profile, issue_type, analysis.get("status"), analysis.get("reason_code"))
    best_next_step = recovery_best_next_step(destination_profile, analysis, issue_type)
    verdict_label = recovery_verdict_label(analysis.get("recoverability"), analysis.get("status"), analysis.get("reason_code"))

    packet = {
        "checked_at": checked_at,
        "chain": chain,
        "verdict": verdict_label,
        "outcome": outcome,
        "confidence": confidence,
        "chain": chain,
        "tx_hash": tx_hash,
        "issue_type": issue_type or "auto",
        "issue_type_label": issue_type_label(issue_type),
        "reason_code": analysis.get("reason_code"),
        "summary": analysis.get("summary"),
        "explanation": why,
        "recoverability": analysis.get("recoverability"),
        "best_next_step": best_next_step,
        "contact_target": contact_target,
        "tx_status": observed.get("tx_status"),
        "sender": observed.get("from") or observed.get("signer"),
        "destination": observed.get("destination") or observed.get("to"),
        "destination_type": destination_profile.get("label"),
        "asset_hint": recovery_asset_hint(observed),
        "token_contract": (observed.get("token_transfer") or {}).get("token_contract"),
        "next_actions": analysis.get("next_actions") or [],
    }
    return packet


def build_recovery_support_message(packet: Dict[str, Any]) -> str:
    lines = [
        "Hello,",
        "",
        "I need help reviewing a crypto transfer that may have been sent incorrectly.",
        "",
        f"Transaction hash: {packet.get('tx_hash') or 'Not provided'}",
        f"Network: {packet.get('chain') or 'Not provided'}",
        f"Issue type: {packet.get('issue_type_label') or issue_type_label(packet.get('issue_type'))}",
        f"Observed transaction status: {packet.get('tx_status') or 'Unknown'}",
        f"Destination: {packet.get('destination') or 'Unknown'}",
        f"Destination type: {packet.get('destination_type') or 'Unknown'}",
        f"Asset hint: {packet.get('asset_hint') or 'Unknown'}",
    ]
    if packet.get("sender"):
        lines.append(f"Sender: {packet.get('sender')}")
    lines.extend([
        "",
        "PayeeProof live analysis summary:",
        str(packet.get("summary") or "No summary returned."),
        "",
        "Why this result:",
        str(packet.get("explanation") or "No explanation returned."),
        "",
        "Please confirm:",
        "1) whether this destination is controlled by your platform, team, or application;",
        "2) whether manual recovery or credit is possible for this transfer;",
        "3) what exact information or steps you need from me next.",
    ])
    return "\n".join(lines)


def now_epoch() -> float:
    return time.time()


def iso_from_epoch(ts: float) -> str:
    return datetime.fromtimestamp(max(0, ts), tz=timezone.utc).replace(microsecond=0).isoformat()


def parse_iso_to_epoch(value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def safe_json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def normalize_text(value: Any, max_len: int = 300) -> str:
    return str(value or "").strip()[:max_len]


def row_to_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    keys = getattr(row, "keys", None)
    if callable(keys):
        try:
            return {key: row[key] for key in row.keys()}
        except Exception:
            pass
    return {}


def billing_period_bounds(period: str = "this_month") -> Dict[str, str]:
    normalized = str(period or "this_month").strip().lower()
    now = datetime.now(timezone.utc)
    if normalized == "this_month":
        since_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if since_dt.month == 12:
            resets_dt = since_dt.replace(year=since_dt.year + 1, month=1)
        else:
            resets_dt = since_dt.replace(month=since_dt.month + 1)
        return {
            "period": "this_month",
            "since": since_dt.isoformat(),
            "resets_at": resets_dt.isoformat(),
        }
    return {
        "period": normalized,
        "since": period_start_iso(normalized),
        "resets_at": "",
    }


def limit_counter_names_for_path(path: str) -> List[str]:
    normalized = path.rstrip("/") or "/"
    if normalized.startswith("/api/verification-records"):
        return list(BILLING_LIMIT_ENDPOINTS.get("/api/verification-records", []))
    return list(BILLING_LIMIT_ENDPOINTS.get(normalized, []))


def resolve_effective_limits(access: Dict[str, Any]) -> Dict[str, Optional[int]]:
    plan = str(access.get("plan") or "pilot").strip().lower()
    effective = dict(PLAN_LIMITS.get(plan, {}))
    explicit = access.get("limits") or {}
    if isinstance(explicit, dict):
        effective.update(explicit)
    legacy_monthly = access.get("usage_limit_monthly")
    if legacy_monthly is not None and "monthly_checks" not in effective:
        effective["monthly_checks"] = _normalize_limit_value(legacy_monthly)
    for counter_name in LIMIT_COUNTER_LABELS:
        effective.setdefault(counter_name, None)
    return effective


def fetch_usage_counters(access: Dict[str, Any], since_iso: str) -> Dict[str, int]:
    scope = access_scope(access)
    tenant_id = scope["tenant_id"]
    environment = scope["environment"]
    counters = {name: 0 for name in LIMIT_COUNTER_LABELS}
    conn = get_db()
    try:
        usage_rows = db_fetchall(
            conn,
            """
            SELECT service, COUNT(*) AS total_count
            FROM usage_events
            WHERE tenant_id = ? AND environment = ? AND created_at >= ?
            GROUP BY service
            """,
            (tenant_id, environment, since_iso),
        )
        records_row = db_fetchone(
            conn,
            """
            SELECT COUNT(*) AS total_count
            FROM api_access_log
            WHERE tenant_id = ? AND environment = ? AND created_at >= ? AND path LIKE ?
            """,
            (tenant_id, environment, since_iso, "/api/verification-records%"),
        )
    finally:
        conn.close()
    service_totals: Dict[str, int] = {}
    for row in usage_rows:
        row_dict = row_to_dict(row)
        service = str(row_dict.get("service") or "").strip().lower()
        try:
            service_totals[service] = int(row_dict.get("total_count") or 0)
        except Exception:
            service_totals[service] = 0
    counters["monthly_preflight_checks"] = int(service_totals.get("preflight-check", 0))
    counters["monthly_recovery_checks"] = int(service_totals.get("recovery-copilot", 0))
    counters["monthly_checks"] = counters["monthly_preflight_checks"] + counters["monthly_recovery_checks"]
    records_data = row_to_dict(records_row)
    counters["monthly_records_reads"] = int(records_data.get("total_count") or 0)
    return counters


def build_limit_state(access: Dict[str, Any], period: str = "this_month") -> Dict[str, Any]:
    bounds = billing_period_bounds(period)
    counters = fetch_usage_counters(access, bounds["since"])
    effective_limits = resolve_effective_limits(access)
    state: Dict[str, Any] = {
        "period": bounds["period"],
        "since": bounds["since"],
        "resets_at": bounds["resets_at"],
        "plan": str(access.get("plan") or "pilot"),
        "quota_ok": True,
        "items": {},
    }
    for counter_name, label in LIMIT_COUNTER_LABELS.items():
        limit_value = effective_limits.get(counter_name)
        used_value = int(counters.get(counter_name, 0))
        remaining_value = None if limit_value is None else max(0, int(limit_value) - used_value)
        exceeded = bool(limit_value is not None and used_value >= int(limit_value))
        if exceeded:
            state["quota_ok"] = False
        state["items"][counter_name] = {
            "label": label,
            "limit": limit_value,
            "used": used_value,
            "remaining": remaining_value,
            "exceeded": exceeded,
        }
    return state


def request_endpoint_alias(path: str) -> str:
    normalized = path.rstrip("/") or "/"
    return {
        "/api/preflight-check": "preflight",
        "/api/recovery-copilot": "recovery",
        "/api/verification-records": "records",
        "/api/account": "account",
        "/api/usage-summary": "usage",
    }.get(normalized, normalized)


def current_request_host() -> str:
    return _extract_host(request.headers.get("Origin", "")) or _extract_host(request.headers.get("Referer", ""))


def ip_allowed_by_cidrs(ip_value: str, cidrs: List[str]) -> bool:
    ip_text = str(ip_value or "").strip()
    if not ip_text or not cidrs:
        return False
    try:
        ip_obj = ipaddress.ip_address(ip_text)
    except Exception:
        return False
    for item in cidrs:
        candidate = str(item or "").strip()
        if not candidate:
            continue
        try:
            if ip_obj in ipaddress.ip_network(candidate, strict=False):
                return True
        except Exception:
            if candidate == ip_text:
                return True
    return False


def resolve_access_policy(access: Dict[str, Any]) -> Dict[str, Any]:
    raw_policy = access.get("policy") or {}
    if not isinstance(raw_policy, dict):
        raw_policy = {}
    out = {
        "allowed_origin_hosts": list(raw_policy.get("allowed_origin_hosts") or []),
        "allowed_ip_cidrs": list(raw_policy.get("allowed_ip_cidrs") or []),
        "allowed_networks": list(raw_policy.get("allowed_networks") or []),
        "allowed_assets_by_network": dict(raw_policy.get("allowed_assets_by_network") or {}),
        "require_reference_id_on": list(raw_policy.get("require_reference_id_on") or []),
    }
    max_page_size = raw_policy.get("records_max_page_size")
    if isinstance(max_page_size, int):
        out["records_max_page_size"] = max(1, min(DEFAULT_RECORDS_MAX_PAGE_SIZE, max_page_size))
    return out


def enforce_role_for_request(access: Dict[str, Any], path: str) -> None:
    blocked_paths = ROLE_WRITE_BLOCKS.get(normalize_role(access.get("role") or "client"), set())
    normalized = path.rstrip("/") or "/"
    if normalized in blocked_paths:
        raise ApiError(
            "This API key is read-only for this endpoint.",
            403,
            code="ROLE_READ_ONLY",
            details={"role": normalize_role(access.get("role") or "client"), "path": normalized},
        )


def enforce_policy_for_request(access: Dict[str, Any], path: str, payload: Optional[Dict[str, Any]] = None) -> None:
    policy = resolve_access_policy(access)
    if not policy:
        return
    normalized = path.rstrip("/") or "/"
    payload = payload or current_json_payload()
    origin_host = current_request_host()
    allowed_origin_hosts = policy.get("allowed_origin_hosts") or []
    if allowed_origin_hosts and origin_host and origin_host not in allowed_origin_hosts:
        raise ApiError(
            "Request origin is not allowed for this API key.",
            403,
            code="POLICY_ORIGIN_NOT_ALLOWED",
            details={"origin_host": origin_host or "", "allowed_origin_hosts": allowed_origin_hosts},
        )
    allowed_ip_cidrs = policy.get("allowed_ip_cidrs") or []
    if allowed_ip_cidrs and not ip_allowed_by_cidrs(get_client_ip(), allowed_ip_cidrs):
        raise ApiError(
            "Source IP is not allowed for this API key.",
            403,
            code="POLICY_IP_NOT_ALLOWED",
            details={"source_ip": get_client_ip()},
        )
    endpoint_alias = request_endpoint_alias(normalized)
    require_reference_id_on = set(policy.get("require_reference_id_on") or [])
    if endpoint_alias in require_reference_id_on:
        reference_id = normalize_text(((payload or {}).get("context") or {}).get("reference_id"), 120)
        if not reference_id:
            raise ApiError(
                "reference_id is required by policy for this endpoint.",
                403,
                code="POLICY_REFERENCE_ID_REQUIRED",
                details={"endpoint": endpoint_alias},
            )
    allowed_networks = policy.get("allowed_networks") or []
    if normalized == "/api/preflight-check" and allowed_networks:
        expected = (payload or {}).get("expected") or {}
        provided = (payload or {}).get("provided") or {}
        networks = [
            normalize_chain(expected.get("network") or expected.get("chain") or ""),
            normalize_chain(provided.get("network") or provided.get("chain") or ""),
        ]
        disallowed = sorted({network for network in networks if network and network not in allowed_networks})
        if disallowed:
            raise ApiError(
                "Requested network is outside the policy allowlist.",
                403,
                code="POLICY_NETWORK_NOT_ALLOWED",
                details={"disallowed_networks": disallowed, "allowed_networks": allowed_networks},
            )
    if normalized == "/api/recovery-copilot" and allowed_networks:
        requested_chain = normalize_chain((payload or {}).get("network") or (payload or {}).get("chain") or "")
        if requested_chain and requested_chain != "auto" and requested_chain not in allowed_networks:
            raise ApiError(
                "Requested network is outside the policy allowlist.",
                403,
                code="POLICY_NETWORK_NOT_ALLOWED",
                details={"requested_network": requested_chain, "allowed_networks": allowed_networks},
            )
    allowed_assets_by_network = policy.get("allowed_assets_by_network") or {}
    if normalized == "/api/preflight-check" and allowed_assets_by_network:
        expected = (payload or {}).get("expected") or {}
        provided = (payload or {}).get("provided") or {}
        asset_checks = [
            (normalize_chain(expected.get("network") or expected.get("chain") or ""), str(expected.get("asset") or "").strip().upper(), "expected"),
            (normalize_chain(provided.get("network") or provided.get("chain") or ""), str(provided.get("asset") or "").strip().upper(), "provided"),
        ]
        violations: List[Dict[str, str]] = []
        for network, asset, side in asset_checks:
            allowed_assets = allowed_assets_by_network.get(network) or []
            if network and asset and allowed_assets and asset not in allowed_assets:
                violations.append({"side": side, "network": network, "asset": asset})
        if violations:
            raise ApiError(
                "Requested asset is outside the policy allowlist.",
                403,
                code="POLICY_ASSET_NOT_ALLOWED",
                details={"violations": violations},
            )


def enforce_billing_limits_for_request(access: Dict[str, Any], path: str) -> None:
    normalized = path.rstrip("/") or "/"
    counter_names = limit_counter_names_for_path(normalized)
    if not counter_names:
        return
    limit_state = build_limit_state(access, "this_month")
    if has_request_context():
        g.limit_state = limit_state
    for counter_name in counter_names:
        item = (limit_state.get("items") or {}).get(counter_name) or {}
        if item.get("exceeded"):
            raise ApiError(
                f"{item.get('label') or counter_name} limit reached for the current billing period.",
                429,
                code="QUOTA_EXCEEDED",
                details={
                    "counter": counter_name,
                    "period": limit_state.get("period"),
                    "since": limit_state.get("since"),
                    "resets_at": limit_state.get("resets_at"),
                    **item,
                },
            )


def apply_record_limit_policy(filters: Dict[str, Any], access: Dict[str, Any]) -> Dict[str, Any]:
    policy = resolve_access_policy(access)
    out = dict(filters)
    max_page_size = int(policy.get("records_max_page_size") or DEFAULT_RECORDS_MAX_PAGE_SIZE)
    out["limit"] = max(1, min(int(out.get("limit") or 20), max_page_size))
    return out


def build_record_search_text(parts: List[Any]) -> str:
    joined = " | ".join(str(part or "").strip() for part in parts if str(part or "").strip())
    return joined[:2000]


def verification_record_id() -> str:
    return f"vrf_{uuid.uuid4().hex[:20]}"


def webhook_delivery_id() -> str:
    return f"whd_{uuid.uuid4().hex[:20]}"


def webhook_ack_token() -> str:
    return f"ack_{uuid.uuid4().hex}{uuid.uuid4().hex[:8]}"


def build_public_api_url(path: str) -> str:
    suffix = "/" + str(path or "").lstrip("/")
    return f"{PUBLIC_API_BASE}{suffix}"


def extract_record_filters(args: Any) -> Dict[str, Any]:
    limit = max(1, min(100, int(str(args.get("limit") or "20"))))
    offset = max(0, int(str(args.get("offset") or "0")))
    return {
        "service": normalize_text(args.get("service"), 80),
        "network": normalize_text(args.get("network"), 80),
        "reason_code": normalize_text(args.get("reason_code"), 120),
        "status": normalize_text(args.get("status"), 80),
        "request_id": normalize_text(args.get("request_id"), 120),
        "reference_id": normalize_text(args.get("reference_id"), 120),
        "record_id": normalize_text(args.get("record_id"), 120),
        "address": normalize_text(args.get("address"), 160),
        "tx_hash": normalize_text(args.get("tx_hash"), 180),
        "q": normalize_text(args.get("q"), 200),
        "limit": limit,
        "offset": offset,
    }


def build_record_where_sql(filters: Dict[str, Any], tenant_id: str, environment: str) -> Tuple[str, List[Any]]:
    clauses = ["vr.tenant_id = ?", "vr.environment = ?"]
    params: List[Any] = [tenant_id, environment]
    mapping = {
        "service": "vr.service",
        "network": "vr.network",
        "reason_code": "vr.reason_code",
        "status": "vr.status",
        "request_id": "vr.request_id",
        "reference_id": "vr.reference_id",
        "record_id": "vr.record_id",
        "address": "vr.address",
        "tx_hash": "vr.tx_hash",
    }
    for key, column in mapping.items():
        value = str(filters.get(key) or "").strip()
        if value:
            clauses.append(f"{column} = ?")
            params.append(value)
    q = str(filters.get("q") or "").strip()
    if q:
        clauses.append("vr.search_text LIKE ?")
        params.append(f"%{q}%")
    return " AND ".join(clauses), params


def summarize_record_row(row: Any) -> Dict[str, Any]:
    item = dict(row) if isinstance(row, dict) else {
        "record_id": row[0],
        "created_at": row[1],
        "service": row[2],
        "network": row[3],
        "status": row[4],
        "reason_code": row[5],
        "request_id": row[6],
        "reference_id": row[7],
        "address": row[8],
        "tx_hash": row[9],
        "webhook_delivery_status": row[10],
        "webhook_attempt_count": row[11],
        "webhook_delivered_at": row[12],
        "webhook_acknowledged_at": row[13],
    }
    return item


def store_verification_record(*, service: str, event_name: str, payload: Dict[str, Any], response_payload: Dict[str, Any], network: str, status: str, reason_code: str, access: Dict[str, Any]) -> str:
    record_id = verification_record_id()
    scope = access_scope(access)
    client_label = scope["client_label"]
    tenant_id = scope["tenant_id"]
    environment = scope["environment"]
    role = scope["role"]
    access_mode = str(access.get("mode") or "public_demo")
    reference_id = ""
    address = ""
    counterparty_address = ""
    tx_hash = ""
    verdict = ""
    search_parts: List[Any] = [service, network, reason_code, client_label]
    if service == "preflight-check":
        context = payload.get("context") or {}
        expected = payload.get("expected") or {}
        provided = payload.get("provided") or {}
        reference_id = normalize_text(context.get("reference_id"), 120)
        address = normalize_text(provided.get("address"), 180)
        counterparty_address = normalize_text(expected.get("address"), 180)
        verdict = normalize_text(response_payload.get("verdict"), 80)
        search_parts.extend([reference_id, address, counterparty_address, expected.get("asset"), provided.get("asset")])
    else:
        reference_id = normalize_text((payload.get("context") or {}).get("reference_id"), 120)
        tx_hash = normalize_text(payload.get("tx_hash") or payload.get("hash"), 180)
        address = normalize_text((response_payload.get("support_packet") or {}).get("destination"), 180)
        verdict = normalize_text(response_payload.get("outcome") or response_payload.get("recovery_verdict"), 80)
        search_parts.extend([reference_id, tx_hash, address, payload.get("issue_type")])

    conn = get_db()
    try:
        db_execute(
            conn,
            """
            INSERT INTO verification_records(
                record_id, created_at, request_id, service, event_name, tenant_id, client_label, environment, role,
                access_mode, source_ip, reference_id, network, status, verdict, reason_code, address,
                counterparty_address, tx_hash, search_text, payload_json, response_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                utc_now_iso(),
                current_request_id(),
                service,
                event_name,
                tenant_id,
                client_label,
                environment,
                role,
                access_mode,
                get_client_ip(),
                reference_id,
                normalize_text(network, 80),
                normalize_text(status, 80),
                verdict,
                normalize_text(reason_code, 120),
                address,
                counterparty_address,
                tx_hash,
                build_record_search_text(search_parts),
                json_dumps_safe(payload),
                json_dumps_safe(response_payload),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return record_id


def store_usage_event(*, service: str, event_name: str, network: str, status: str, verdict: str, reason_code: str, access: Dict[str, Any], record_id: str = "", timeout_flag: bool = False, metadata: Optional[Dict[str, Any]] = None) -> None:
    scope = access_scope(access)
    client_label = scope["client_label"]
    conn = get_db()
    try:
        db_execute(
            conn,
            """
            INSERT INTO usage_events(
                created_at, request_id, record_id, tenant_id, client_label, environment, role, plan,
                service, event_name, network, status, verdict, reason_code, timeout_flag, duration_ms, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now_iso(),
                current_request_id(),
                normalize_text(record_id, 80),
                normalize_text(scope["tenant_id"], 120),
                normalize_text(client_label, 120),
                normalize_text(scope["environment"], 40),
                normalize_text(scope["role"], 40),
                normalize_text(str(access.get("plan") or "pilot"), 80),
                normalize_text(service, 80),
                normalize_text(event_name, 80),
                normalize_text(network, 80),
                normalize_text(status, 80),
                normalize_text(verdict, 80),
                normalize_text(reason_code, 120),
                1 if timeout_flag else 0,
                request_duration_ms(),
                json_dumps_safe(metadata or {}),
            ),
        )
        conn.commit()
    except Exception as exc:
        emit_structured_log(
            "usage_event_write_failed",
            level="warning",
            client_label=client_label,
            service=service,
            event_name=event_name,
            error=str(exc),
        )
    finally:
        conn.close()


def get_record_row_by_id(record_id: str, tenant_id: str, environment: str) -> Optional[Any]:
    conn = get_db()
    try:
        return db_fetchone(
            conn,
            """
            SELECT *
            FROM verification_records
            WHERE record_id = ? AND tenant_id = ? AND environment = ?
            LIMIT 1
            """,
            (record_id, tenant_id, environment),
        )
    finally:
        conn.close()


def fetch_record_detail(record_id: str, tenant_id: str, environment: str) -> Optional[Dict[str, Any]]:
    row = get_record_row_by_id(record_id, tenant_id, environment)
    if not row:
        return None
    record = row_to_dict(row)
    record["payload"] = safe_json_loads(record.get("payload_json"), {})
    record["response"] = safe_json_loads(record.get("response_json"), {})
    deliveries = list_webhook_deliveries_for_record(record_id)
    record["webhook_deliveries"] = deliveries
    record.pop("payload_json", None)
    record.pop("response_json", None)
    return record


def search_verification_records(filters: Dict[str, Any], tenant_id: str, environment: str) -> Dict[str, Any]:
    where_sql, params = build_record_where_sql(filters, tenant_id, environment)
    conn = get_db()
    try:
        rows = db_fetchall(
            conn,
            f"""
            SELECT
                vr.record_id, vr.created_at, vr.service, vr.network, vr.status, vr.reason_code,
                vr.request_id, vr.reference_id, vr.address, vr.tx_hash,
                wd.delivery_status AS webhook_delivery_status,
                wd.attempt_count AS webhook_attempt_count,
                wd.delivered_at AS webhook_delivered_at,
                wd.acknowledged_at AS webhook_acknowledged_at
            FROM verification_records vr
            LEFT JOIN webhook_deliveries wd ON wd.record_id = vr.record_id
            WHERE {where_sql}
            ORDER BY vr.created_at DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [int(filters.get("limit") or 20), int(filters.get("offset") or 0)]),
        )
        count_row = db_fetchone(conn, f"SELECT COUNT(*) AS total_count FROM verification_records vr WHERE {where_sql}", tuple(params))
        total = int((count_row.get("total_count") if isinstance(count_row, dict) else count_row[0]) or 0) if count_row else 0
    finally:
        conn.close()
    return {
        "items": [summarize_record_row(row) for row in rows],
        "total": total,
        "limit": int(filters.get("limit") or 20),
        "offset": int(filters.get("offset") or 0),
    }


def month_start_iso(dt: Optional[datetime] = None) -> str:
    current = dt or datetime.now(timezone.utc)
    start = datetime(current.year, current.month, 1, tzinfo=timezone.utc)
    return start.replace(microsecond=0).isoformat()


def period_start_iso(period: str) -> str:
    normalized = str(period or "30d").strip().lower()
    if normalized in {"month", "this_month", "current_month", "mtd"}:
        return month_start_iso()
    match = re.fullmatch(r"(\d{1,3})d", normalized)
    if match:
        days = max(1, min(int(match.group(1)), 365))
        start = datetime.now(timezone.utc).timestamp() - (days * 86400)
        return datetime.fromtimestamp(start, tz=timezone.utc).replace(microsecond=0).isoformat()
    return month_start_iso()


def increment_counter(bucket: Dict[str, int], key: str) -> None:
    key_text = str(key or "unknown").strip() or "unknown"
    bucket[key_text] = int(bucket.get(key_text, 0)) + 1


def sorted_counter(bucket: Dict[str, int]) -> Dict[str, int]:
    return dict(sorted(bucket.items(), key=lambda item: (-item[1], item[0])))


def summarize_usage_for_scope(access: Dict[str, Any], period: str = "30d") -> Dict[str, Any]:
    since_iso = period_start_iso(period)
    scope = access_scope(access)
    tenant_id = scope["tenant_id"]
    environment = scope["environment"]
    conn = get_db()
    try:
        usage_rows = db_fetchall(
            conn,
            """
            SELECT created_at, service, event_name, network, status, verdict, reason_code, timeout_flag, duration_ms
            FROM usage_events
            WHERE tenant_id = ? AND environment = ? AND created_at >= ?
            ORDER BY created_at ASC
            """,
            (tenant_id, environment, since_iso),
        )
        webhook_rows = db_fetchall(
            conn,
            """
            SELECT delivery_status, ack_status
            FROM webhook_deliveries
            WHERE tenant_id = ? AND environment = ? AND created_at >= ?
            ORDER BY created_at ASC
            """,
            (tenant_id, environment, since_iso),
        )
        fallback_event_rows: List[Any] = []
        fallback_record_rows: List[Any] = []
        if not usage_rows:
            fallback_event_rows = db_fetchall(
                conn,
                """
                SELECT created_at, event_name, status, reason_code, network, timeout_flag, duration_ms
                FROM event_log
                WHERE tenant_id = ? AND environment = ? AND created_at >= ?
                ORDER BY created_at ASC
                """,
                (tenant_id, environment, since_iso),
            )
            fallback_record_rows = db_fetchall(
                conn,
                """
                SELECT created_at, service, event_name, network, status, verdict, reason_code
                FROM verification_records
                WHERE tenant_id = ? AND environment = ? AND created_at >= ?
                ORDER BY created_at ASC
                """,
                (tenant_id, environment, since_iso),
            )
    finally:
        conn.close()

    event_totals: Dict[str, int] = {}
    network_totals: Dict[str, int] = {}
    status_totals: Dict[str, int] = {}
    reason_totals: Dict[str, int] = {}
    verdict_totals: Dict[str, int] = {}
    service_totals: Dict[str, int] = {}
    daily_totals: Dict[str, int] = {}
    timeout_count = 0
    duration_values: List[int] = []
    usage_source = "usage_events" if usage_rows else "fallback"

    if usage_rows:
        for row in usage_rows:
            row_dict = row_to_dict(row)
            created_at = str(row_dict.get("created_at") or "")
            increment_counter(event_totals, str(row_dict.get("event_name") or "unknown"))
            increment_counter(service_totals, str(row_dict.get("service") or "unknown"))
            if row_dict.get("status"):
                increment_counter(status_totals, str(row_dict.get("status") or "unknown"))
            if row_dict.get("reason_code"):
                increment_counter(reason_totals, str(row_dict.get("reason_code") or "unknown"))
            if row_dict.get("network"):
                increment_counter(network_totals, str(row_dict.get("network") or "unknown"))
            if row_dict.get("verdict"):
                increment_counter(verdict_totals, str(row_dict.get("verdict") or "unknown"))
            if created_at:
                increment_counter(daily_totals, created_at[:10])
            if int(row_dict.get("timeout_flag") or 0):
                timeout_count += 1
            try:
                duration_ms = int(row_dict.get("duration_ms") or 0)
                if duration_ms > 0:
                    duration_values.append(duration_ms)
            except Exception:
                pass
        total_events = len(usage_rows)
        total_checks = len(usage_rows)
    else:
        for row in fallback_event_rows:
            row_dict = row_to_dict(row)
            created_at = str(row_dict.get("created_at") or "")
            increment_counter(event_totals, str(row_dict.get("event_name") or "unknown"))
            increment_counter(status_totals, str(row_dict.get("status") or "unknown"))
            if row_dict.get("reason_code"):
                increment_counter(reason_totals, str(row_dict.get("reason_code") or "unknown"))
            if row_dict.get("network"):
                increment_counter(network_totals, str(row_dict.get("network") or "unknown"))
            if created_at:
                increment_counter(daily_totals, created_at[:10])
            if int(row_dict.get("timeout_flag") or 0):
                timeout_count += 1
            try:
                duration_ms = int(row_dict.get("duration_ms") or 0)
                if duration_ms > 0:
                    duration_values.append(duration_ms)
            except Exception:
                pass

        for row in fallback_record_rows:
            row_dict = row_to_dict(row)
            increment_counter(service_totals, str(row_dict.get("service") or "unknown"))
            if row_dict.get("verdict"):
                increment_counter(verdict_totals, str(row_dict.get("verdict") or "unknown"))
            if row_dict.get("reason_code"):
                increment_counter(reason_totals, str(row_dict.get("reason_code") or "unknown"))
            if row_dict.get("network"):
                increment_counter(network_totals, str(row_dict.get("network") or "unknown"))

        total_events = len(fallback_event_rows)
        total_checks = len(fallback_record_rows)

    delivered_count = 0
    acknowledged_count = 0
    pending_count = 0
    for row in webhook_rows:
        row_dict = row_to_dict(row)
        delivery_status = str(row_dict.get("delivery_status") or "").strip().lower()
        ack_status = str(row_dict.get("ack_status") or "").strip().lower()
        if delivery_status == "delivered":
            delivered_count += 1
        elif delivery_status:
            pending_count += 1
        if ack_status == "acknowledged":
            acknowledged_count += 1

    avg_duration_ms = round(sum(duration_values) / len(duration_values), 1) if duration_values else 0

    return {
        "period": str(period or "30d"),
        "since": since_iso,
        "usage_source": usage_source,
        "tenant_id": tenant_id,
        "environment": environment,
        "totals": {
            "events": total_events,
            "checks": total_checks,
            "timeouts": timeout_count,
            "avg_duration_ms": avg_duration_ms,
            "webhook_deliveries": len(webhook_rows),
            "webhook_delivered": delivered_count,
            "webhook_acknowledged": acknowledged_count,
            "webhook_pending": pending_count,
        },
        "by_event": sorted_counter(event_totals),
        "by_service": sorted_counter(service_totals),
        "by_status": sorted_counter(status_totals),
        "by_network": sorted_counter(network_totals),
        "by_reason_code": sorted_counter(reason_totals),
        "by_verdict": sorted_counter(verdict_totals),
        "daily": dict(sorted(daily_totals.items())),
    }


def build_account_snapshot(access: Dict[str, Any]) -> Dict[str, Any]:
    client_label = normalize_text(str(access.get("client") or "unknown-client"), 120)
    tenant_summary = build_tenant_key_summary(access)
    monthly_usage = summarize_usage_for_scope(access, "this_month")
    limit_state = build_limit_state(access, "this_month")
    checks_item = ((limit_state.get("items") or {}).get("monthly_checks") or {})

    return {
        "tenant": tenant_summary,
        "current_key": {
            "client_label": client_label,
            "display_name": str(access.get("label") or client_label),
            "key_fingerprint": str(access.get("key_fingerprint") or ""),
            "key_hint": str(access.get("key_hint") or ""),
            "environment": normalize_environment(access.get("environment") or "live"),
            "role": normalize_role(access.get("role") or "client"),
            "plan": str(access.get("plan") or "pilot"),
            "scopes": sorted(list(access.get("scopes") or [])),
        },
        "usage": {
            "billing_period": "this_month",
            "billable_checks": int(monthly_usage.get("totals", {}).get("checks") or 0),
            "usage_limit_monthly": checks_item.get("limit"),
            "usage_remaining_monthly": checks_item.get("remaining"),
            "summary": monthly_usage,
        },
        "limits": limit_state,
        "policy": resolve_access_policy(access),
        "webhooks": {
            "active": bool(access.get("webhook_active", False)),
            "events": list(access.get("webhook_events") or []),
            "url_configured": bool(str(access.get("webhook_url") or "").strip()),
        },
    }


def list_webhook_deliveries_for_record(record_id: str) -> List[Dict[str, Any]]:
    conn = get_db()
    try:
        rows = db_fetchall(
            conn,
            """
            SELECT delivery_id, event_name, delivery_status, ack_status, attempt_count, max_attempts,
                   next_attempt_at, last_attempt_at, delivered_at, acknowledged_at,
                   last_response_code, last_response_excerpt, last_error
            FROM webhook_deliveries
            WHERE record_id = ?
            ORDER BY id DESC
            """,
            (record_id,),
        )
    finally:
        conn.close()
    result: List[Dict[str, Any]] = []
    for row in rows:
        row_dict = row_to_dict(row)
        if row_dict:
            result.append(row_dict)
            continue
        result.append({
            "delivery_id": row[0],
            "event_name": row[1],
            "delivery_status": row[2],
            "ack_status": row[3],
            "attempt_count": row[4],
            "max_attempts": row[5],
            "next_attempt_at": row[6],
            "last_attempt_at": row[7],
            "delivered_at": row[8],
            "acknowledged_at": row[9],
            "last_response_code": row[10],
            "last_response_excerpt": row[11],
            "last_error": row[12],
        })
    return result


def create_webhook_delivery_for_record(record_id: str, event_name: str, access: Dict[str, Any]) -> Optional[str]:
    webhook_url = str(access.get("webhook_url") or "").strip()
    webhook_secret = str(access.get("webhook_secret") or "").strip()
    webhook_events = set(access.get("webhook_events") or [])
    if access.get("mode") != "api_key":
        return None
    if not bool(access.get("webhook_active", True)):
        return None
    if not webhook_url or not webhook_secret or (webhook_events and event_name not in webhook_events):
        return None
    delivery_id = webhook_delivery_id()
    ack_token = webhook_ack_token()
    schedule = WEBHOOK_RETRY_SCHEDULE_SEC or [60, 300, 1800]
    conn = get_db()
    try:
        db_execute(
            conn,
            """
            INSERT INTO webhook_deliveries(
                delivery_id, record_id, created_at, updated_at, tenant_id, client_label, environment, event_name, webhook_url, webhook_secret,
                delivery_status, ack_status, attempt_count, max_attempts, next_attempt_at, ack_token
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delivery_id,
                record_id,
                utc_now_iso(),
                utc_now_iso(),
                normalize_text(str(access.get("tenant_id") or access.get("client") or "unknown-client"), 120),
                normalize_text(str(access.get("client") or "unknown-client"), 120),
                normalize_environment(access.get("environment") or "live"),
                event_name,
                webhook_url,
                webhook_secret,
                "pending",
                "pending",
                0,
                len(schedule) + 1,
                utc_now_iso(),
                ack_token,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return delivery_id


def build_webhook_signature(secret: str, timestamp: str, raw_body: str) -> str:
    signed = f"{timestamp}.{raw_body}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def build_webhook_payload(record: Dict[str, Any], delivery: Dict[str, Any]) -> Dict[str, Any]:
    response_payload = safe_json_loads(record.get("response_json"), {})
    return {
        "delivery_id": delivery.get("delivery_id"),
        "event": delivery.get("event_name"),
        "record_id": record.get("record_id"),
        "request_id": record.get("request_id"),
        "created_at": delivery.get("created_at") or utc_now_iso(),
        "service": record.get("service"),
        "network": record.get("network"),
        "status": record.get("status"),
        "reason_code": record.get("reason_code"),
        "client_label": record.get("client_label"),
        "verification": response_payload,
        "ack": {
            "url": build_public_api_url("/api/webhooks/ack"),
            "delivery_id": delivery.get("delivery_id"),
            "token": delivery.get("ack_token"),
        },
    }


def update_webhook_delivery_result(delivery_id: str, *, delivery_status: str, ack_status: Optional[str] = None,
                                   next_attempt_at: str = "", last_response_code: Optional[int] = None,
                                   last_response_excerpt: str = "", last_error: str = "", delivered_at: str = "",
                                   acknowledged_at: str = "", increment_attempt: bool = False) -> None:
    conn = get_db()
    try:
        row = db_fetchone(conn, "SELECT attempt_count FROM webhook_deliveries WHERE delivery_id = ? LIMIT 1", (delivery_id,))
        current_attempts = int((row.get("attempt_count") if isinstance(row, dict) else row[0]) or 0) if row else 0
        new_attempts = current_attempts + 1 if increment_attempt else current_attempts
        db_execute(
            conn,
            """
            UPDATE webhook_deliveries
            SET updated_at = ?,
                delivery_status = ?,
                ack_status = COALESCE(?, ack_status),
                attempt_count = ?,
                next_attempt_at = ?,
                last_attempt_at = ?,
                delivered_at = COALESCE(?, delivered_at),
                acknowledged_at = COALESCE(?, acknowledged_at),
                last_response_code = ?,
                last_response_excerpt = ?,
                last_error = ?
            WHERE delivery_id = ?
            """,
            (
                utc_now_iso(),
                delivery_status,
                ack_status,
                new_attempts,
                next_attempt_at,
                utc_now_iso(),
                delivered_at or None,
                acknowledged_at or None,
                last_response_code,
                normalize_text(last_response_excerpt, 500),
                normalize_text(last_error, 500),
                delivery_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_webhook_delivery_by_id(delivery_id: str) -> Dict[str, Any]:
    conn = get_db()
    try:
        row = db_fetchone(
            conn,
            """
            SELECT wd.*, vr.response_json, vr.request_id, vr.service, vr.network, vr.status, vr.reason_code, vr.client_label, vr.record_id
            FROM webhook_deliveries wd
            JOIN verification_records vr ON vr.record_id = wd.record_id
            WHERE wd.delivery_id = ?
            LIMIT 1
            """,
            (delivery_id,),
        )
    finally:
        conn.close()
    return row_to_dict(row)


def fetch_pending_webhook_deliveries(limit: int) -> List[Dict[str, Any]]:
    now_iso = utc_now_iso()
    conn = get_db()
    try:
        rows = db_fetchall(
            conn,
            """
            SELECT wd.*, vr.response_json, vr.request_id, vr.service, vr.network, vr.status, vr.reason_code, vr.client_label, vr.record_id
            FROM webhook_deliveries wd
            JOIN verification_records vr ON vr.record_id = wd.record_id
            WHERE wd.delivery_status IN ('pending', 'retry_scheduled')
              AND (wd.next_attempt_at IS NULL OR wd.next_attempt_at = '' OR wd.next_attempt_at <= ?)
            ORDER BY wd.created_at ASC
            LIMIT ?
            """,
            (now_iso, int(limit)),
        )
    finally:
        conn.close()
    return [row_to_dict(row) for row in rows]


def fetch_next_pending_webhook_due_epoch() -> float:
    conn = get_db()
    try:
        row = db_fetchone(
            conn,
            """
            SELECT next_attempt_at
            FROM webhook_deliveries
            WHERE delivery_status IN ('pending', 'retry_scheduled')
            ORDER BY CASE WHEN next_attempt_at IS NULL OR next_attempt_at = '' THEN 0 ELSE 1 END, next_attempt_at ASC
            LIMIT 1
            """,
        )
    finally:
        conn.close()
    if not row:
        return 0.0
    value = (row.get("next_attempt_at") if isinstance(row, dict) else row[0]) or ""
    if not str(value).strip():
        return now_epoch()
    return parse_iso_to_epoch(str(value))


def compute_retry_schedule(attempt_count: int) -> Tuple[bool, str]:
    schedule = WEBHOOK_RETRY_SCHEDULE_SEC or [60, 300, 1800]
    if attempt_count <= len(schedule):
        delay = int(schedule[max(0, attempt_count - 1)])
        return True, iso_from_epoch(now_epoch() + delay)
    return False, ""


def should_mark_webhook_acknowledged(response: Any) -> bool:
    try:
        status_code = int(getattr(response, "status_code", 0) or 0)
    except Exception:
        status_code = 0
    if status_code < 200 or status_code >= 300:
        return False
    text = str(getattr(response, "text", "") or "").strip()
    if not text:
        return True
    try:
        payload = response.json()
    except Exception:
        lowered = text.lower()
        return lowered in {"ok", "accepted", "ack", "acknowledged", "received"}
    if isinstance(payload, dict):
        if payload.get("acknowledged") is True or payload.get("ack") is True or payload.get("ok") is True:
            return True
        status_value = str(payload.get("status") or "").strip().lower()
        if status_value in {"ok", "accepted", "acknowledged", "received"}:
            return True
    return False


def attempt_webhook_delivery(delivery: Dict[str, Any]) -> None:
    delivery_id = str(delivery.get("delivery_id") or "")
    record_id = str(delivery.get("record_id") or "")
    if not delivery_id or not record_id:
        return
    payload = build_webhook_payload(delivery, delivery)
    raw_body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    timestamp = utc_now_iso()
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "PayeeProof-Webhooks/1.0",
        "X-PayeeProof-Event": str(delivery.get("event_name") or ""),
        "X-PayeeProof-Delivery-ID": delivery_id,
        "X-PayeeProof-Record-ID": record_id,
        "X-PayeeProof-Timestamp": timestamp,
        "X-PayeeProof-Signature": build_webhook_signature(str(delivery.get("webhook_secret") or ""), timestamp, raw_body),
    }
    try:
        response = requests.post(
            str(delivery.get("webhook_url") or ""),
            data=raw_body.encode("utf-8"),
            headers=headers,
            timeout=WEBHOOK_DELIVERY_TIMEOUT_SEC,
        )
        excerpt = normalize_text(response.text, 500)
        if 200 <= int(response.status_code) < 300:
            delivered_at = utc_now_iso()
            acknowledged_at = delivered_at if should_mark_webhook_acknowledged(response) else ""
            update_webhook_delivery_result(
                delivery_id,
                delivery_status="delivered",
                ack_status="acknowledged" if acknowledged_at else "http_accepted",
                last_response_code=int(response.status_code),
                last_response_excerpt=excerpt,
                last_error="",
                delivered_at=delivered_at,
                acknowledged_at=acknowledged_at,
                increment_attempt=True,
            )
            emit_structured_log("webhook_delivery_succeeded", event_name=delivery.get("event_name"), delivery_id=delivery_id, record_id=record_id, status_code=int(response.status_code), acknowledged=bool(acknowledged_at))
            return
        should_retry, next_attempt_at = compute_retry_schedule(int(delivery.get("attempt_count") or 0) + 1)
        update_webhook_delivery_result(
            delivery_id,
            delivery_status="retry_scheduled" if should_retry else "failed",
            ack_status="pending",
            next_attempt_at=next_attempt_at,
            last_response_code=int(response.status_code),
            last_response_excerpt=excerpt,
            last_error=f"HTTP {response.status_code}",
            increment_attempt=True,
        )
        emit_structured_log("webhook_delivery_failed", level="warning", event_name=delivery.get("event_name"), delivery_id=delivery_id, record_id=record_id, status_code=int(response.status_code), retry=should_retry)
    except Exception as exc:
        should_retry, next_attempt_at = compute_retry_schedule(int(delivery.get("attempt_count") or 0) + 1)
        update_webhook_delivery_result(
            delivery_id,
            delivery_status="retry_scheduled" if should_retry else "failed",
            ack_status="pending",
            next_attempt_at=next_attempt_at,
            last_error=str(exc),
            increment_attempt=True,
        )
        emit_structured_log("webhook_delivery_exception", level="warning", event_name=delivery.get("event_name"), delivery_id=delivery_id, record_id=record_id, retry=should_retry, error=str(exc))


def process_pending_webhooks(batch_size: int = WEBHOOK_PROCESS_BATCH_SIZE) -> None:
    deliveries = fetch_pending_webhook_deliveries(batch_size)
    for delivery in deliveries:
        attempt_webhook_delivery(delivery)


def dispatch_webhook_delivery_now(delivery_id: str) -> Dict[str, Any]:
    delivery = fetch_webhook_delivery_by_id(delivery_id)
    if not delivery:
        return {}
    status = str(delivery.get("delivery_status") or "")
    if status not in {"pending", "retry_scheduled"}:
        return delivery
    attempt_webhook_delivery(delivery)
    return fetch_webhook_delivery_by_id(delivery_id)


def _run_webhook_processor() -> None:
    try:
        idle_cycles = 0
        while True:
            deliveries = fetch_pending_webhook_deliveries(WEBHOOK_PROCESS_BATCH_SIZE)
            if deliveries:
                idle_cycles = 0
                for delivery in deliveries:
                    attempt_webhook_delivery(delivery)
                continue
            next_due_epoch = fetch_next_pending_webhook_due_epoch()
            if not next_due_epoch:
                break
            sleep_for = max(0.0, next_due_epoch - now_epoch())
            if sleep_for <= 0.25:
                idle_cycles += 1
                if idle_cycles > 2:
                    break
                time.sleep(0.25)
                continue
            if sleep_for > 120:
                time.sleep(30.0)
                continue
            time.sleep(min(sleep_for, 30.0))
    finally:
        with WEBHOOK_PROCESS_LOCK:
            WEBHOOK_PROCESS_STATE["running"] = False
            WEBHOOK_PROCESS_STATE["last_started_at"] = now_epoch()


def kick_webhook_processor(force: bool = False) -> None:
    with WEBHOOK_PROCESS_LOCK:
        if WEBHOOK_PROCESS_STATE.get("running"):
            return
        last_started_at = float(WEBHOOK_PROCESS_STATE.get("last_started_at") or 0.0)
        if not force and (now_epoch() - last_started_at) < WEBHOOK_PROCESS_MIN_INTERVAL_SEC:
            return
        WEBHOOK_PROCESS_STATE["running"] = True
        WEBHOOK_PROCESS_STATE["last_started_at"] = now_epoch()
    Thread(target=_run_webhook_processor, daemon=True).start()


def acknowledge_webhook_delivery(delivery_id: str, ack_token: str, status: str, detail: str, payload: Dict[str, Any]) -> bool:
    conn = get_db()
    try:
        row = db_fetchone(conn, "SELECT delivery_id, ack_token FROM webhook_deliveries WHERE delivery_id = ? LIMIT 1", (delivery_id,))
        if not row:
            return False
        stored_token = str((row.get("ack_token") if isinstance(row, dict) else row[1]) or "")
        if not stored_token or stored_token != ack_token:
            return False
        db_execute(
            conn,
            """
            UPDATE webhook_deliveries
            SET updated_at = ?,
                ack_status = ?,
                acknowledged_at = ?,
                last_response_excerpt = ?,
                ack_payload_json = ?
            WHERE delivery_id = ?
            """,
            (
                utc_now_iso(),
                normalize_text(status or "confirmed", 80),
                utc_now_iso(),
                normalize_text(detail, 500),
                json_dumps_safe(payload),
                delivery_id,
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def get_db() -> Any:
    if db_is_postgres():
        if psycopg2 is None:
            raise RuntimeError("Postgres backend requested but psycopg2-binary is not installed.")
        return psycopg2.connect(DATABASE_URL)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def upsert_tenant_registry_from_api_keys() -> None:
    if not API_KEYS:
        return
    conn = get_db()
    now_iso = utc_now_iso()
    try:
        for record in API_KEYS.values():
            tenant_id = normalize_text(str(record.get("tenant_id") or record.get("client") or "unknown-client"), 120)
            display_name = normalize_text(str(record.get("label") or record.get("display_name") or record.get("client") or tenant_id), 160)
            plan_value = normalize_text(str(record.get("plan") or "pilot"), 80)
            tenant_exists = db_fetchone(conn, "SELECT tenant_id FROM tenants WHERE tenant_id = ? LIMIT 1", (tenant_id,))
            if tenant_exists:
                db_execute(
                    conn,
                    "UPDATE tenants SET updated_at = ?, display_name = ?, default_plan = ?, status = ? WHERE tenant_id = ?",
                    (now_iso, display_name, plan_value, "active", tenant_id),
                )
            else:
                db_execute(
                    conn,
                    "INSERT INTO tenants(tenant_id, created_at, updated_at, display_name, status, default_plan) VALUES (?, ?, ?, ?, ?, ?)",
                    (tenant_id, now_iso, now_iso, display_name, "active", plan_value),
                )

            scopes_json = json_dumps_safe(sorted(list(record.get("scopes") or [])))
            limits_json = json_dumps_safe(record.get("limits") or {})
            policy_json = json_dumps_safe(record.get("policy") or {})
            webhook_events_json = json_dumps_safe(list(record.get("webhook_events") or []))
            key_fingerprint = normalize_text(str(record.get("key_fingerprint") or ""), 64)
            key_hint = normalize_text(str(record.get("key_hint") or ""), 32)
            key_exists = db_fetchone(conn, "SELECT key_fingerprint FROM tenant_api_keys WHERE key_fingerprint = ? LIMIT 1", (key_fingerprint,))
            key_params = (
                tenant_id,
                normalize_text(str(record.get("client") or tenant_id), 120),
                display_name,
                normalize_environment(record.get("environment") or "live"),
                normalize_role(record.get("role") or "client"),
                plan_value,
                scopes_json,
                limits_json,
                policy_json,
                1 if bool(record.get("active", True)) else 0,
                record.get("usage_limit_monthly"),
                1 if bool(record.get("webhook_active", True)) else 0,
                webhook_events_json,
                normalize_text(_extract_host(str(record.get("webhook_url") or "")), 180),
                key_hint,
                now_iso,
                now_iso,
            )
            if key_exists:
                db_execute(
                    conn,
                    "UPDATE tenant_api_keys SET tenant_id = ?, client_label = ?, display_name = ?, environment = ?, role = ?, plan = ?, scopes_json = ?, limits_json = ?, policy_json = ?, active = ?, usage_limit_monthly = ?, webhook_active = ?, webhook_events_json = ?, webhook_url_host = ?, key_hint = ?, updated_at = ?, last_synced_at = ? WHERE key_fingerprint = ?",
                    key_params + (key_fingerprint,),
                )
            else:
                db_execute(
                    conn,
                    "INSERT INTO tenant_api_keys(tenant_id, client_label, display_name, environment, role, plan, scopes_json, limits_json, policy_json, active, usage_limit_monthly, webhook_active, webhook_events_json, webhook_url_host, key_fingerprint, key_hint, created_at, updated_at, last_synced_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    key_params[:14] + (key_fingerprint, key_hint, now_iso, now_iso, now_iso),
                )
        conn.commit()
    finally:
        conn.close()


def touch_tenant_api_key(api_key: str, record: Dict[str, Any]) -> None:
    key_fingerprint = normalize_text(str(record.get("key_fingerprint") or api_key_fingerprint(api_key)), 64)
    if not key_fingerprint:
        return
    conn = get_db()
    try:
        db_execute(
            conn,
            "UPDATE tenant_api_keys SET last_seen_at = ?, updated_at = ? WHERE key_fingerprint = ?",
            (utc_now_iso(), utc_now_iso(), key_fingerprint),
        )
        conn.commit()
    finally:
        conn.close()


def build_tenant_key_summary(access: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = normalize_text(str(access.get("tenant_id") or access.get("client") or "unknown-client"), 120)
    environment = normalize_environment(access.get("environment") or "live")
    conn = get_db()
    try:
        tenant_row = db_fetchone(
            conn,
            "SELECT tenant_id, display_name, status, default_plan, created_at, updated_at FROM tenants WHERE tenant_id = ? LIMIT 1",
            (tenant_id,),
        )
        key_rows = db_fetchall(
            conn,
            "SELECT environment, role, active FROM tenant_api_keys WHERE tenant_id = ? ORDER BY environment, role",
            (tenant_id,),
        )
    finally:
        conn.close()

    tenant_data = row_to_dict(tenant_row) if tenant_row else {}
    environments: List[str] = []
    active_keys = 0
    for row in key_rows:
        row_dict = row_to_dict(row)
        env_value = normalize_environment((row_dict or {}).get("environment") or "live")
        if env_value not in environments:
            environments.append(env_value)
        if int((row_dict or {}).get("active") or 0):
            active_keys += 1

    return {
        "tenant_id": tenant_id,
        "display_name": str((tenant_data or {}).get("display_name") or access.get("label") or access.get("client") or tenant_id),
        "status": str((tenant_data or {}).get("status") or "active"),
        "default_plan": str((tenant_data or {}).get("default_plan") or access.get("plan") or "pilot"),
        "environments": environments or [environment],
        "keys_total": len(key_rows),
        "keys_active": active_keys,
        "current_environment": environment,
    }


def access_scope(access: Dict[str, Any]) -> Dict[str, str]:
    client_label = normalize_text(str(access.get("client") or "website-demo"), 120)
    return {
        "tenant_id": normalize_text(str(access.get("tenant_id") or client_label), 120),
        "client_label": client_label,
        "environment": normalize_environment(access.get("environment") or ("public-demo" if access.get("mode") == "public_demo" else "live")),
        "role": normalize_role(access.get("role") or ("demo" if access.get("mode") == "public_demo" else "client")),
    }


def ensure_db() -> None:
    conn = get_db()
    try:
        if db_is_postgres():
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS pilot_requests (
                  id BIGSERIAL PRIMARY KEY,
                  request_id TEXT,
                  fingerprint TEXT,
                  created_at TEXT NOT NULL,
                  name TEXT NOT NULL,
                  company TEXT NOT NULL,
                  email TEXT NOT NULL,
                  volume TEXT,
                  notes TEXT NOT NULL,
                  source_ip TEXT,
                  user_agent TEXT,
                  origin TEXT,
                  email_status TEXT DEFAULT 'pending',
                  email_id TEXT,
                  email_error TEXT,
                  email_last_attempt_at TEXT,
                  welcome_email_status TEXT DEFAULT 'pending',
                  welcome_email_id TEXT,
                  welcome_email_error TEXT,
                  welcome_email_last_attempt_at TEXT
                )
                """
            )
        else:
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS pilot_requests (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  request_id TEXT,
                  fingerprint TEXT,
                  created_at TEXT NOT NULL,
                  name TEXT NOT NULL,
                  company TEXT NOT NULL,
                  email TEXT NOT NULL,
                  volume TEXT,
                  notes TEXT NOT NULL,
                  source_ip TEXT,
                  user_agent TEXT,
                  origin TEXT,
                  email_status TEXT DEFAULT 'pending',
                  email_id TEXT,
                  email_error TEXT,
                  email_last_attempt_at TEXT,
                  welcome_email_status TEXT DEFAULT 'pending',
                  welcome_email_id TEXT,
                  welcome_email_error TEXT,
                  welcome_email_last_attempt_at TEXT
                )
                """
            )
        ensure_column(conn, "pilot_requests", "request_id", "TEXT")
        ensure_column(conn, "pilot_requests", "fingerprint", "TEXT")
        ensure_column(conn, "pilot_requests", "origin", "TEXT")
        ensure_column(conn, "pilot_requests", "email_status", "TEXT DEFAULT 'pending'")
        ensure_column(conn, "pilot_requests", "email_id", "TEXT")
        ensure_column(conn, "pilot_requests", "email_error", "TEXT")
        ensure_column(conn, "pilot_requests", "email_last_attempt_at", "TEXT")
        ensure_column(conn, "pilot_requests", "welcome_email_status", "TEXT DEFAULT 'pending'")
        ensure_column(conn, "pilot_requests", "welcome_email_id", "TEXT")
        ensure_column(conn, "pilot_requests", "welcome_email_error", "TEXT")
        ensure_column(conn, "pilot_requests", "welcome_email_last_attempt_at", "TEXT")
        db_execute(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_pilot_requests_request_id ON pilot_requests(request_id)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_pilot_requests_fingerprint_created_at ON pilot_requests(fingerprint, created_at)")
        if db_is_postgres():
            db_execute(
                conn,
                """
                UPDATE pilot_requests
                SET request_id = 'ppf_' || substr(md5(random()::text || clock_timestamp()::text), 1, 16)
                WHERE request_id IS NULL OR trim(request_id) = ''
                """
            )
        else:
            db_execute(
                conn,
                """
                UPDATE pilot_requests
                SET request_id = COALESCE(request_id, 'ppf_' || lower(hex(randomblob(8))))
                WHERE request_id IS NULL OR trim(request_id) = ''
                """
            )
        if db_is_postgres():
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS api_access_log (
                  id BIGSERIAL PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  path TEXT NOT NULL,
                  access_mode TEXT NOT NULL,
                  client_label TEXT NOT NULL,
                  source_ip TEXT NOT NULL
                )
                """
            )
        else:
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS api_access_log (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  path TEXT NOT NULL,
                  access_mode TEXT NOT NULL,
                  client_label TEXT NOT NULL,
                  source_ip TEXT NOT NULL
                )
                """
            )
        if db_is_postgres():
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS event_log (
                  id BIGSERIAL PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  request_id TEXT,
                  event_name TEXT NOT NULL,
                  endpoint TEXT NOT NULL,
                  status TEXT NOT NULL,
                  reason_code TEXT,
                  network TEXT,
                  http_status INTEGER NOT NULL,
                  timeout_flag INTEGER DEFAULT 0,
                  duration_ms INTEGER DEFAULT 0,
                  access_mode TEXT,
                  tenant_id TEXT,
                  client_label TEXT,
                  environment TEXT,
                  role TEXT,
                  source_ip TEXT,
                  error_message TEXT,
                  metadata_json TEXT
                )
                """
            )
        else:
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS event_log (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  request_id TEXT,
                  event_name TEXT NOT NULL,
                  endpoint TEXT NOT NULL,
                  status TEXT NOT NULL,
                  reason_code TEXT,
                  network TEXT,
                  http_status INTEGER NOT NULL,
                  timeout_flag INTEGER DEFAULT 0,
                  duration_ms INTEGER DEFAULT 0,
                  access_mode TEXT,
                  tenant_id TEXT,
                  client_label TEXT,
                  environment TEXT,
                  role TEXT,
                  source_ip TEXT,
                  error_message TEXT,
                  metadata_json TEXT
                )
                """
            )
        ensure_column(conn, "event_log", "tenant_id", "TEXT")
        ensure_column(conn, "event_log", "environment", "TEXT")
        ensure_column(conn, "event_log", "role", "TEXT")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_event_log_created_at ON event_log(created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_event_log_tenant_env_created_at ON event_log(tenant_id, environment, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_event_log_event_name_created_at ON event_log(event_name, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_event_log_network_created_at ON event_log(network, created_at)")
        if db_is_postgres():
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS tenants (
                  tenant_id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  display_name TEXT,
                  status TEXT,
                  default_plan TEXT
                )
                """
            )
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS tenant_api_keys (
                  key_fingerprint TEXT PRIMARY KEY,
                  tenant_id TEXT NOT NULL,
                  client_label TEXT,
                  display_name TEXT,
                  environment TEXT,
                  role TEXT,
                  plan TEXT,
                  scopes_json TEXT,
                  limits_json TEXT,
                  policy_json TEXT,
                  active INTEGER DEFAULT 1,
                  usage_limit_monthly INTEGER,
                  webhook_active INTEGER DEFAULT 0,
                  webhook_events_json TEXT,
                  webhook_url_host TEXT,
                  key_hint TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  last_synced_at TEXT,
                  last_seen_at TEXT
                )
                """
            )
        else:
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS tenants (
                  tenant_id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  display_name TEXT,
                  status TEXT,
                  default_plan TEXT
                )
                """
            )
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS tenant_api_keys (
                  key_fingerprint TEXT PRIMARY KEY,
                  tenant_id TEXT NOT NULL,
                  client_label TEXT,
                  display_name TEXT,
                  environment TEXT,
                  role TEXT,
                  plan TEXT,
                  scopes_json TEXT,
                  limits_json TEXT,
                  policy_json TEXT,
                  active INTEGER DEFAULT 1,
                  usage_limit_monthly INTEGER,
                  webhook_active INTEGER DEFAULT 0,
                  webhook_events_json TEXT,
                  webhook_url_host TEXT,
                  key_hint TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  last_synced_at TEXT,
                  last_seen_at TEXT
                )
                """
            )
        if db_is_postgres():
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS usage_events (
                  id BIGSERIAL PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  request_id TEXT,
                  record_id TEXT,
                  tenant_id TEXT,
                  client_label TEXT NOT NULL,
                  environment TEXT,
                  role TEXT,
                  plan TEXT,
                  service TEXT NOT NULL,
                  event_name TEXT NOT NULL,
                  network TEXT,
                  status TEXT,
                  verdict TEXT,
                  reason_code TEXT,
                  timeout_flag INTEGER DEFAULT 0,
                  duration_ms INTEGER DEFAULT 0,
                  metadata_json TEXT
                )
                """
            )
        else:
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS usage_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  request_id TEXT,
                  record_id TEXT,
                  tenant_id TEXT,
                  client_label TEXT NOT NULL,
                  environment TEXT,
                  role TEXT,
                  plan TEXT,
                  service TEXT NOT NULL,
                  event_name TEXT NOT NULL,
                  network TEXT,
                  status TEXT,
                  verdict TEXT,
                  reason_code TEXT,
                  timeout_flag INTEGER DEFAULT 0,
                  duration_ms INTEGER DEFAULT 0,
                  metadata_json TEXT
                )
                """
            )
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_usage_events_client_created_at ON usage_events(client_label, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_usage_events_tenant_env_created_at ON usage_events(tenant_id, environment, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_tenant_api_keys_tenant_env ON tenant_api_keys(tenant_id, environment)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_usage_events_service_created_at ON usage_events(service, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_usage_events_event_created_at ON usage_events(event_name, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_usage_events_network_created_at ON usage_events(network, created_at)")
        if db_is_postgres():
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS verification_records (
                  id BIGSERIAL PRIMARY KEY,
                  record_id TEXT,
                  created_at TEXT NOT NULL,
                  request_id TEXT,
                  service TEXT NOT NULL,
                  event_name TEXT NOT NULL,
                  tenant_id TEXT,
                  client_label TEXT NOT NULL,
                  environment TEXT,
                  role TEXT,
                  access_mode TEXT NOT NULL,
                  source_ip TEXT,
                  reference_id TEXT,
                  network TEXT,
                  status TEXT,
                  verdict TEXT,
                  reason_code TEXT,
                  address TEXT,
                  counterparty_address TEXT,
                  tx_hash TEXT,
                  search_text TEXT,
                  payload_json TEXT,
                  response_json TEXT
                )
                """
            )
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS webhook_deliveries (
                  id BIGSERIAL PRIMARY KEY,
                  delivery_id TEXT,
                  record_id TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  tenant_id TEXT,
                  client_label TEXT NOT NULL,
                  environment TEXT,
                  event_name TEXT NOT NULL,
                  webhook_url TEXT NOT NULL,
                  webhook_secret TEXT NOT NULL,
                  delivery_status TEXT NOT NULL,
                  ack_status TEXT NOT NULL DEFAULT 'pending',
                  attempt_count INTEGER NOT NULL DEFAULT 0,
                  max_attempts INTEGER NOT NULL DEFAULT 0,
                  next_attempt_at TEXT,
                  last_attempt_at TEXT,
                  delivered_at TEXT,
                  acknowledged_at TEXT,
                  last_response_code INTEGER,
                  last_response_excerpt TEXT,
                  last_error TEXT,
                  ack_token TEXT,
                  ack_payload_json TEXT
                )
                """
            )
        else:
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS verification_records (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  record_id TEXT,
                  created_at TEXT NOT NULL,
                  request_id TEXT,
                  service TEXT NOT NULL,
                  event_name TEXT NOT NULL,
                  tenant_id TEXT,
                  client_label TEXT NOT NULL,
                  environment TEXT,
                  role TEXT,
                  access_mode TEXT NOT NULL,
                  source_ip TEXT,
                  reference_id TEXT,
                  network TEXT,
                  status TEXT,
                  verdict TEXT,
                  reason_code TEXT,
                  address TEXT,
                  counterparty_address TEXT,
                  tx_hash TEXT,
                  search_text TEXT,
                  payload_json TEXT,
                  response_json TEXT
                )
                """
            )
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS webhook_deliveries (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  delivery_id TEXT,
                  record_id TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  tenant_id TEXT,
                  client_label TEXT NOT NULL,
                  environment TEXT,
                  event_name TEXT NOT NULL,
                  webhook_url TEXT NOT NULL,
                  webhook_secret TEXT NOT NULL,
                  delivery_status TEXT NOT NULL,
                  ack_status TEXT NOT NULL DEFAULT 'pending',
                  attempt_count INTEGER NOT NULL DEFAULT 0,
                  max_attempts INTEGER NOT NULL DEFAULT 0,
                  next_attempt_at TEXT,
                  last_attempt_at TEXT,
                  delivered_at TEXT,
                  acknowledged_at TEXT,
                  last_response_code INTEGER,
                  last_response_excerpt TEXT,
                  last_error TEXT,
                  ack_token TEXT,
                  ack_payload_json TEXT
                )
                """
            )
        ensure_column(conn, "verification_records", "record_id", "TEXT")
        ensure_column(conn, "verification_records", "request_id", "TEXT")
        ensure_column(conn, "verification_records", "service", "TEXT")
        ensure_column(conn, "verification_records", "event_name", "TEXT")
        ensure_column(conn, "tenants", "display_name", "TEXT")
        ensure_column(conn, "tenants", "status", "TEXT")
        ensure_column(conn, "tenants", "default_plan", "TEXT")
        ensure_column(conn, "tenant_api_keys", "tenant_id", "TEXT")
        ensure_column(conn, "tenant_api_keys", "client_label", "TEXT")
        ensure_column(conn, "tenant_api_keys", "display_name", "TEXT")
        ensure_column(conn, "tenant_api_keys", "environment", "TEXT")
        ensure_column(conn, "tenant_api_keys", "role", "TEXT")
        ensure_column(conn, "tenant_api_keys", "plan", "TEXT")
        ensure_column(conn, "tenant_api_keys", "scopes_json", "TEXT")
        ensure_column(conn, "tenant_api_keys", "limits_json", "TEXT")
        ensure_column(conn, "tenant_api_keys", "policy_json", "TEXT")
        ensure_column(conn, "tenant_api_keys", "active", "INTEGER DEFAULT 1")
        ensure_column(conn, "tenant_api_keys", "usage_limit_monthly", "INTEGER")
        ensure_column(conn, "tenant_api_keys", "webhook_active", "INTEGER DEFAULT 0")
        ensure_column(conn, "tenant_api_keys", "webhook_events_json", "TEXT")
        ensure_column(conn, "tenant_api_keys", "webhook_url_host", "TEXT")
        ensure_column(conn, "tenant_api_keys", "key_hint", "TEXT")
        ensure_column(conn, "tenant_api_keys", "last_synced_at", "TEXT")
        ensure_column(conn, "tenant_api_keys", "last_seen_at", "TEXT")
        ensure_column(conn, "usage_events", "record_id", "TEXT")
        ensure_column(conn, "usage_events", "tenant_id", "TEXT")
        ensure_column(conn, "usage_events", "client_label", "TEXT")
        ensure_column(conn, "usage_events", "environment", "TEXT")
        ensure_column(conn, "usage_events", "role", "TEXT")
        ensure_column(conn, "usage_events", "plan", "TEXT")
        ensure_column(conn, "usage_events", "service", "TEXT")
        ensure_column(conn, "usage_events", "event_name", "TEXT")
        ensure_column(conn, "usage_events", "network", "TEXT")
        ensure_column(conn, "usage_events", "status", "TEXT")
        ensure_column(conn, "usage_events", "verdict", "TEXT")
        ensure_column(conn, "usage_events", "reason_code", "TEXT")
        ensure_column(conn, "usage_events", "timeout_flag", "INTEGER DEFAULT 0")
        ensure_column(conn, "usage_events", "duration_ms", "INTEGER DEFAULT 0")
        ensure_column(conn, "usage_events", "metadata_json", "TEXT")
        ensure_column(conn, "api_access_log", "tenant_id", "TEXT")
        ensure_column(conn, "api_access_log", "environment", "TEXT")
        ensure_column(conn, "api_access_log", "role", "TEXT")
        ensure_column(conn, "event_log", "tenant_id", "TEXT")
        ensure_column(conn, "event_log", "environment", "TEXT")
        ensure_column(conn, "event_log", "role", "TEXT")
        ensure_column(conn, "verification_records", "tenant_id", "TEXT")
        ensure_column(conn, "verification_records", "client_label", "TEXT")
        ensure_column(conn, "verification_records", "environment", "TEXT")
        ensure_column(conn, "verification_records", "role", "TEXT")
        ensure_column(conn, "verification_records", "access_mode", "TEXT")
        ensure_column(conn, "verification_records", "source_ip", "TEXT")
        ensure_column(conn, "verification_records", "reference_id", "TEXT")
        ensure_column(conn, "verification_records", "network", "TEXT")
        ensure_column(conn, "verification_records", "status", "TEXT")
        ensure_column(conn, "verification_records", "verdict", "TEXT")
        ensure_column(conn, "verification_records", "reason_code", "TEXT")
        ensure_column(conn, "verification_records", "address", "TEXT")
        ensure_column(conn, "verification_records", "counterparty_address", "TEXT")
        ensure_column(conn, "verification_records", "tx_hash", "TEXT")
        ensure_column(conn, "verification_records", "search_text", "TEXT")
        ensure_column(conn, "verification_records", "payload_json", "TEXT")
        ensure_column(conn, "verification_records", "response_json", "TEXT")
        ensure_column(conn, "webhook_deliveries", "delivery_id", "TEXT")
        ensure_column(conn, "webhook_deliveries", "record_id", "TEXT")
        ensure_column(conn, "webhook_deliveries", "tenant_id", "TEXT")
        ensure_column(conn, "webhook_deliveries", "client_label", "TEXT")
        ensure_column(conn, "webhook_deliveries", "environment", "TEXT")
        ensure_column(conn, "webhook_deliveries", "event_name", "TEXT")
        ensure_column(conn, "webhook_deliveries", "webhook_url", "TEXT")
        ensure_column(conn, "webhook_deliveries", "webhook_secret", "TEXT")
        ensure_column(conn, "webhook_deliveries", "delivery_status", "TEXT")
        ensure_column(conn, "webhook_deliveries", "ack_status", "TEXT")
        ensure_column(conn, "webhook_deliveries", "attempt_count", "INTEGER DEFAULT 0")
        ensure_column(conn, "webhook_deliveries", "max_attempts", "INTEGER DEFAULT 0")
        ensure_column(conn, "webhook_deliveries", "next_attempt_at", "TEXT")
        ensure_column(conn, "webhook_deliveries", "last_attempt_at", "TEXT")
        ensure_column(conn, "webhook_deliveries", "delivered_at", "TEXT")
        ensure_column(conn, "webhook_deliveries", "acknowledged_at", "TEXT")
        ensure_column(conn, "webhook_deliveries", "last_response_code", "INTEGER")
        ensure_column(conn, "webhook_deliveries", "last_response_excerpt", "TEXT")
        ensure_column(conn, "webhook_deliveries", "last_error", "TEXT")
        ensure_column(conn, "webhook_deliveries", "ack_token", "TEXT")
        ensure_column(conn, "webhook_deliveries", "ack_payload_json", "TEXT")
        db_execute(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_verification_records_record_id ON verification_records(record_id)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_verification_records_client_created_at ON verification_records(client_label, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_verification_records_tenant_env_created_at ON verification_records(tenant_id, environment, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_verification_records_service_created_at ON verification_records(service, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_verification_records_request_id ON verification_records(request_id)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_verification_records_reference_id ON verification_records(reference_id)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_delivery_id ON webhook_deliveries(delivery_id)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_record_id ON webhook_deliveries(record_id)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_tenant_env_created_at ON webhook_deliveries(tenant_id, environment, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_status_next_attempt ON webhook_deliveries(delivery_status, next_attempt_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_api_access_log_tenant_env_created_at ON api_access_log(tenant_id, environment, created_at)")
        db_execute(conn, "CREATE INDEX IF NOT EXISTS idx_api_access_log_path_created_at ON api_access_log(path, created_at)")
        conn.commit()
    finally:
        conn.close()


ensure_db()
upsert_tenant_registry_from_api_keys()
kick_webhook_processor(force=True)


@app.before_request
def apply_access_controls():
    g.request_started_at = time.perf_counter()
    g.request_id = ensure_request_id()
    g.event_logged = False
    if request.method == "OPTIONS":
        return None

    path = request.path.rstrip("/") or "/"
    scope = resolve_required_api_scope(path)
    if scope:
        access = authenticate_api_request(scope)
        g.api_access = access
        if access["mode"] == "api_key":
            enforce_role_for_request(access, path)
            enforce_policy_for_request(access, path)
            enforce_billing_limits_for_request(access, path)
            bucket = f"api:key:{access['client']}:{scope}"
            allowed, meta = consume_rate_limit(bucket, KEYED_API_RATE_LIMIT, KEYED_API_RATE_WINDOW_SEC)
        else:
            g.limit_state = build_limit_state(access, "this_month")
            bucket = f"api:anon:{get_client_ip()}:{scope}"
            allowed, meta = consume_rate_limit(bucket, ANON_API_RATE_LIMIT, ANON_API_RATE_WINDOW_SEC)
        g.rate_limit_meta = meta
        if not allowed:
            response = jsonify({
                "ok": False,
                "error": "RATE_LIMITED",
                "message": "Too many requests. Retry later.",
            })
            response.status_code = 429
            response.headers["Retry-After"] = str(meta["retry_after"])
            response.headers["X-RateLimit-Limit"] = str(meta["limit"])
            response.headers["X-RateLimit-Remaining"] = str(meta["remaining"])
            response.headers["X-RateLimit-Window"] = str(meta["window_sec"])
            return response
    elif path == "/pilot-request" and request.method == "POST":
        bucket = f"pilot:{get_client_ip()}"
        allowed, meta = consume_rate_limit(bucket, PILOT_RATE_LIMIT, PILOT_RATE_WINDOW_SEC)
        g.rate_limit_meta = meta
        if not allowed:
            response = jsonify({
                "ok": False,
                "error": "RATE_LIMITED",
                "message": "Too many pilot requests from this source. Retry later.",
            })
            response.status_code = 429
            response.headers["Retry-After"] = str(meta["retry_after"])
            response.headers["X-RateLimit-Limit"] = str(meta["limit"])
            response.headers["X-RateLimit-Remaining"] = str(meta["remaining"])
            response.headers["X-RateLimit-Window"] = str(meta["window_sec"])
            return response

    return None


@app.after_request
def attach_response_headers(response):
    response.headers.setdefault("Cache-Control", "no-store")
    response.headers.setdefault("X-Request-ID", current_request_id())
    response.headers.setdefault("X-Response-Time-Ms", str(request_duration_ms()))
    meta = getattr(g, "rate_limit_meta", None)
    if meta:
        response.headers.setdefault("X-RateLimit-Limit", str(meta.get("limit", 0)))
        response.headers.setdefault("X-RateLimit-Remaining", str(meta.get("remaining", 0)))
        response.headers.setdefault("X-RateLimit-Window", str(meta.get("window_sec", 0)))
        retry_after = int(meta.get("retry_after", 0) or 0)
        if retry_after > 0:
            response.headers.setdefault("Retry-After", str(retry_after))
    access = getattr(g, "api_access", None)
    if access:
        response.headers.setdefault("X-API-Access-Mode", str(access.get("mode") or ""))
        response.headers.setdefault("X-API-Client", str(access.get("client") or ""))
        response.headers.setdefault("X-API-Environment", str(access.get("environment") or ""))
        response.headers.setdefault("X-API-Plan", str(access.get("plan") or ""))
    limit_state = getattr(g, "limit_state", None)
    if isinstance(limit_state, dict):
        checks_item = ((limit_state.get("items") or {}).get("monthly_checks") or {})
        if checks_item:
            response.headers.setdefault("X-Billing-Period", str(limit_state.get("period") or "this_month"))
            if checks_item.get("limit") is not None:
                response.headers.setdefault("X-Usage-Checks-Limit", str(checks_item.get("limit")))
            response.headers.setdefault("X-Usage-Checks-Used", str(checks_item.get("used") or 0))
            if checks_item.get("remaining") is not None:
                response.headers.setdefault("X-Usage-Checks-Remaining", str(checks_item.get("remaining")))
            if limit_state.get("resets_at"):
                response.headers.setdefault("X-Billing-Resets-At", str(limit_state.get("resets_at")))
    kick_webhook_processor()
    return response


@dataclass
class RpcResult:
    ok: bool
    result: Any = None
    error: Optional[str] = None


class ApiError(Exception):
    def __init__(self, message: str, status_code: int = 400, code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code
        self.details = details or {}


@app.errorhandler(ApiError)
def handle_api_error(err: ApiError):
    if not getattr(g, "event_logged", False):
        record_request_failure(request.path, err.status_code, err.code or err.message)
    payload = {"ok": False, "error": err.code or err.message, "trace_id": current_request_id()}
    if err.code:
        payload["message"] = err.message
    if err.details:
        payload["details"] = err.details
    return jsonify(payload), err.status_code


@app.errorhandler(Exception)
def handle_unexpected_error(err: Exception):
    message = f"Unexpected server error: {type(err).__name__}"
    if not getattr(g, "event_logged", False):
        record_request_failure(request.path, 500, message)
    emit_structured_log("unhandled_exception", level="error", path=request.path, error_type=type(err).__name__)
    return jsonify({"ok": False, "error": message, "trace_id": current_request_id()}), 500


@app.get("/health")
def health():
    configured = {k: bool(v) for k, v in RPC_URLS.items()}
    live_networks = [k for k, v in configured.items() if v]
    metrics = build_metrics_snapshot(OBSERVABILITY_WINDOW_SEC)
    alerts = evaluate_recent_alerts()
    return jsonify({
        "ok": True,
        "service": "payeeproof-api",
        "version": APP_VERSION,
        "time": utc_now_iso(),
        "rpc_configured": configured,
        "configured_networks": live_networks,
        "api_status": "live",
        "db_backend": DB_BACKEND,
        "trace_id": current_request_id(),
        "observability": {
            "request_id_header": "X-Request-ID",
            "metrics_window_sec": OBSERVABILITY_WINDOW_SEC,
            "metrics": metrics,
            "alerts": alerts,
        },
    })


@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "service": "payeeproof-api",
        "version": APP_VERSION,
        "routes": [
            "/health",
            "/api/preflight-check",
            "/api/recovery-copilot",
            "/api/account",
            "/api/usage-summary",
            "/pilot-request",
            "/api/verification-records",
            "/api/webhooks/ack",
        ]
    })


@app.get("/api/account")
def account_summary():
    access = getattr(g, "api_access", None) or {}
    if access.get("mode") != "api_key":
        raise ApiError("API key required for account summary.", 401)
    log_api_access("/api/account")
    snapshot = build_account_snapshot(access)
    g.limit_state = snapshot.get("limits")
    if "current_key" in snapshot:
        snapshot["client"] = {
            "tenant_id": snapshot.get("tenant", {}).get("tenant_id"),
            "client_label": snapshot.get("current_key", {}).get("client_label"),
            "display_name": snapshot.get("current_key", {}).get("display_name"),
            "environment": snapshot.get("current_key", {}).get("environment"),
            "role": snapshot.get("current_key", {}).get("role"),
            "plan": snapshot.get("current_key", {}).get("plan"),
            "scopes": snapshot.get("current_key", {}).get("scopes", []),
        }
    return jsonify({
        "ok": True,
        "trace_id": current_request_id(),
        **snapshot,
    })


@app.get("/api/usage-summary")
def usage_summary():
    access = getattr(g, "api_access", None) or {}
    if access.get("mode") != "api_key":
        raise ApiError("API key required for usage summary.", 401)
    log_api_access("/api/usage-summary")
    period = str(request.args.get("period") or "30d").strip().lower()
    summary = summarize_usage_for_scope(access, period)
    limit_state = build_limit_state(access, "this_month")
    g.limit_state = limit_state
    return jsonify({
        "ok": True,
        "trace_id": current_request_id(),
        "tenant_id": str(access.get("tenant_id") or access.get("client") or "unknown-client"),
        "environment": str(access.get("environment") or "live"),
        "limits": limit_state,
        **summary,
    })


@app.get("/api/verification-records")
def verification_records_history():
    access = getattr(g, "api_access", None) or {}
    if access.get("mode") != "api_key":
        raise ApiError("API key required for verification history.", 401)
    log_api_access("/api/verification-records")
    filters = apply_record_limit_policy(extract_record_filters(request.args), access)
    result = search_verification_records(filters, str(access.get("tenant_id") or access.get("client") or ""), normalize_environment(access.get("environment") or "live"))
    g.limit_state = build_limit_state(access, "this_month")
    return jsonify({
        "ok": True,
        "trace_id": current_request_id(),
        "filters": {k: v for k, v in filters.items() if k not in {"limit", "offset"} and v},
        **result,
    })


@app.get("/api/verification-records/<record_id>")
def verification_record_detail(record_id: str):
    access = getattr(g, "api_access", None) or {}
    if access.get("mode") != "api_key":
        raise ApiError("API key required for verification history.", 401)
    log_api_access(f"/api/verification-records/{record_id}")
    record = fetch_record_detail(record_id, str(access.get("tenant_id") or access.get("client") or ""), normalize_environment(access.get("environment") or "live"))
    g.limit_state = build_limit_state(access, "this_month")
    if not record:
        raise ApiError("Verification record not found.", 404)
    return jsonify({
        "ok": True,
        "trace_id": current_request_id(),
        "record": record,
    })


@app.post("/api/webhooks/ack")
def webhook_ack():
    payload = request.get_json(silent=True) or {}
    delivery_id = normalize_text(payload.get("delivery_id"), 120)
    ack_token = normalize_text(payload.get("token") or payload.get("ack_token"), 200)
    status = normalize_text(payload.get("status") or "confirmed", 80)
    detail = normalize_text(payload.get("detail") or payload.get("message"), 500)
    if not delivery_id or not ack_token:
        raise ApiError("delivery_id and token are required.", 400)
    ok = acknowledge_webhook_delivery(delivery_id, ack_token, status, detail, payload)
    if not ok:
        raise ApiError("Webhook acknowledgement not accepted.", 403)
    return jsonify({
        "ok": True,
        "delivery_id": delivery_id,
        "status": status,
        "acknowledged_at": utc_now_iso(),
        "trace_id": current_request_id(),
    })


@app.post("/api/preflight-check")
def preflight_check():
    payload = request.get_json(silent=True) or {}
    expected = payload.get("expected") or {}
    provided = payload.get("provided") or {}
    context = payload.get("context") or {}
    policy_profile = normalize_policy_profile(payload.get("policy_profile") or context.get("policy_profile"))

    expected_chain = normalize_chain(expected.get("network") or expected.get("chain"))
    provided_chain = normalize_chain(provided.get("network") or provided.get("chain"))
    expected_asset = str(expected.get("asset") or "").strip().upper()
    provided_asset = str(provided.get("asset") or "").strip().upper()
    expected_address = str(expected.get("address") or "").strip()
    provided_address = str(provided.get("address") or "").strip()
    expected_memo = str(expected.get("memo") or expected.get("tag") or "").strip()
    provided_memo = str(provided.get("memo") or provided.get("tag") or "").strip()

    if not expected_chain or not provided_chain:
        raise ApiError("Both expected.network and provided.network are required.")
    if not expected_asset or not provided_asset:
        raise ApiError("Both expected.asset and provided.asset are required.")
    if not expected_address or not provided_address:
        raise ApiError("Both expected.address and provided.address are required.")

    expected_valid, expected_validation_note = validate_address(expected_chain, expected_address)
    provided_valid, provided_validation_note = validate_address(provided_chain, provided_address)

    checks = {
        "network_match": expected_chain == provided_chain,
        "asset_match": expected_asset == provided_asset,
        "address_match": compare_addresses(expected_chain, expected_address, provided_chain, provided_address),
        "memo_match": expected_memo == provided_memo if (expected_memo or provided_memo) else True,
        "expected_address_valid": expected_valid,
        "provided_address_valid": provided_valid,
        "expected_network_supported": is_supported_chain(expected_chain),
        "provided_network_supported": is_supported_chain(provided_chain),
        "expected_asset_supported": is_supported_asset_for_chain(expected_chain, expected_asset),
        "provided_asset_supported": is_supported_asset_for_chain(provided_chain, provided_asset),
    }

    if expected_valid:
        expected_onchain = skipped_expected_onchain(
            expected_chain,
            "Skipped for performance. Expected destination classification is not required for the pre-send verdict.",
        )
    else:
        expected_onchain = {
            "chain": expected_chain,
            "address_type": "invalid",
            "rpc_used": False,
            "details": expected_validation_note,
        }

    if not provided_valid:
        provided_onchain = {
            "chain": provided_chain,
            "address_type": "invalid",
            "rpc_used": False,
            "details": provided_validation_note,
        }
    elif not checks["provided_network_supported"]:
        provided_onchain = {
            "chain": provided_chain,
            "address_type": "unsupported",
            "rpc_used": False,
            "details": f"Unsupported chain: {provided_chain}",
        }
    elif not checks["provided_asset_supported"]:
        provided_onchain = skipped_expected_onchain(
            provided_chain,
            f"Skipped live lookup because {provided_asset or 'asset'} is not currently supported on {provided_chain}.",
        )
    elif provided_chain in EVM_CHAINS and provided_address.lower() == ZERO_EVM:
        provided_onchain = {
            "chain": provided_chain,
            "address_type": "invalid",
            "rpc_used": False,
            "details": "Zero address blocked before live lookup.",
        }
    else:
        provided_onchain = classify_address(provided_chain, provided_address)

    risk_flags: List[str] = []
    if not checks["network_match"]:
        risk_flags.append("NETWORK_MISMATCH")
    if not checks["asset_match"]:
        risk_flags.append("ASSET_MISMATCH")
    if not checks["address_match"]:
        risk_flags.append("ADDRESS_MISMATCH")
    if not checks["memo_match"]:
        risk_flags.append("MEMO_MISMATCH")
    if not checks["expected_address_valid"] or not checks["provided_address_valid"]:
        risk_flags.append("INVALID_ADDRESS")
    if not checks["expected_network_supported"] or not checks["provided_network_supported"]:
        risk_flags.append("UNSUPPORTED_NETWORK")
    if not checks["expected_asset_supported"] or not checks["provided_asset_supported"]:
        risk_flags.append("UNSUPPORTED_ASSET_OR_NETWORK")
    if provided_chain in EVM_CHAINS and provided_address.lower() == ZERO_EVM:
        risk_flags.append("ZERO_ADDRESS")

    provided_destination = build_destination_profile(provided_chain, provided_address, provided_onchain)
    expected_destination = build_destination_profile(expected_chain, expected_address, expected_onchain)

    if provided_destination.get("classification") == "contract_or_app":
        risk_flags.append("DESTINATION_IS_CONTRACT_OR_APP")
    elif provided_destination.get("classification") == "exchange_like_deposit":
        risk_flags.append("DESTINATION_REQUIRES_MEMO_OR_VENUE_CHECK")
    elif provided_destination.get("classification") == "bridge_router":
        risk_flags.append("DESTINATION_IS_BRIDGE_ROUTER")
    elif provided_destination.get("source") == "unavailable":
        risk_flags.append("DESTINATION_LOOKUP_UNAVAILABLE")
    elif provided_destination.get("classification") in {"unknown", "not_found"}:
        risk_flags.append("DESTINATION_NOT_CLASSIFIED")

    outcome = derive_preflight_outcome(
        checks=checks,
        expected_chain=expected_chain,
        provided_chain=provided_chain,
        expected_asset=expected_asset,
        provided_asset=provided_asset,
        expected_address=expected_address,
        provided_address=provided_address,
        expected_valid=expected_valid,
        provided_valid=provided_valid,
        provided_destination=provided_destination,
        risk_flags=risk_flags,
        policy_profile=policy_profile,
    )

    checked_at = utc_now_iso()
    log_api_access("/api/preflight-check")
    record_request_event(
        event_name="preflight_run",
        endpoint="/api/preflight-check",
        status=outcome["status"],
        reason_code=outcome["reason_code"],
        network=provided_chain,
        http_status=200,
        timeout_flag=looks_like_timeout(provided_onchain.get("details")) or looks_like_timeout(expected_onchain.get("details")),
        metadata={
            "verdict": outcome["verdict"],
            "next_action": outcome["next_action"],
            "risk_flags": sorted(set(risk_flags)),
            "policy_profile": policy_profile,
        },
    )
    response_payload = {
        "ok": True,
        "service": "preflight-check",
        "version": APP_VERSION,
        "trace_id": current_request_id(),
        "checked_at": checked_at,
        "chain": provided_chain,
        "policy_profile": policy_profile,
        "policy_profile_label": policy_profile_label(policy_profile),
        "status": outcome["status"],
        "verdict": outcome["verdict"],
        "reason_code": outcome["reason_code"],
        "next_action": outcome["next_action"],
        "next_action_label": preflight_next_step_label(outcome["next_action"]),
        "confidence": outcome["confidence"],
        "summary": outcome["summary"],
        "why_this_verdict": outcome["why"],
        "explanation": outcome["why"],
        "risk_flags": sorted(set(risk_flags)),
        "checks": checks,
        "proof": {
            "checked_at": checked_at,
            "chain": provided_chain,
            "trace_id": current_request_id(),
            "verdict": outcome["verdict"],
            "confidence": outcome["confidence"],
            "reason_code": outcome["reason_code"],
            "next_action": outcome["next_action"],
            "next_action_label": preflight_next_step_label(outcome["next_action"]),
            "policy_profile": policy_profile,
        },
        "expected": {
            "network": expected_chain,
            "asset": expected_asset,
            "address": expected_address,
            "memo": expected_memo,
        },
        "provided": {
            "network": provided_chain,
            "asset": provided_asset,
            "address": provided_address,
            "memo": provided_memo,
        },
        "destination": provided_destination,
        "onchain": {
            "expected": expected_onchain,
            "provided": provided_onchain,
        },
        "classification": {
            "expected": expected_destination,
            "provided": provided_destination,
        },
        "supported_scope": {
            "expected_network_supported": checks["expected_network_supported"],
            "provided_network_supported": checks["provided_network_supported"],
            "expected_asset_supported": checks["expected_asset_supported"],
            "provided_asset_supported": checks["provided_asset_supported"],
        },
    }
    access = getattr(g, "api_access", None) or {}
    record_id = store_verification_record(
        service="preflight-check",
        event_name="preflight_run",
        payload=payload,
        response_payload=response_payload,
        network=provided_chain,
        status=outcome["status"],
        reason_code=outcome["reason_code"],
        access=access,
    )
    response_payload["record_id"] = record_id
    response_payload["history_url"] = f"/api/verification-records/{record_id}"
    store_usage_event(
        service="preflight-check",
        event_name="preflight_run",
        network=provided_chain,
        status=outcome["status"],
        verdict=outcome["verdict"],
        reason_code=outcome["reason_code"],
        access=access,
        record_id=record_id,
        timeout_flag=looks_like_timeout(provided_onchain.get("details")) or looks_like_timeout(expected_onchain.get("details")),
        metadata={
            "next_action": outcome["next_action"],
            "confidence": outcome["confidence"],
            "policy_profile": policy_profile,
        },
    )
    g.limit_state = build_limit_state(access, "this_month")
    response_payload["limits"] = g.limit_state
    delivery_id = create_webhook_delivery_for_record(record_id, "preflight_run", access)
    if delivery_id:
        response_payload["webhook"] = {
            "queued": True,
            "delivery_id": delivery_id,
            "event": "preflight_run",
        }
        delivery_state = dispatch_webhook_delivery_now(delivery_id)
        if str(delivery_state.get("delivery_status") or "") in {"pending", "retry_scheduled"}:
            kick_webhook_processor(force=True)
    return jsonify(response_payload)


def is_likely_evm_tx_hash(value: str) -> bool:
    text = str(value or '').strip()
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{64}", text))


def is_likely_solana_signature(value: str) -> bool:
    text = str(value or '').strip()
    return bool(SOLANA_SIG_RE.fullmatch(text))


def ordered_candidates(candidates: List[str], preferred: str = '') -> List[str]:
    out: List[str] = []
    preferred = normalize_chain(preferred)
    if preferred and preferred in candidates:
        out.append(preferred)
    for item in candidates:
        if item not in out:
            out.append(item)
    return out


def analyze_transaction_on_chain(chain: str, tx_hash: str, issue_type: str, intended_address: str, intended_chain: str) -> Dict[str, Any]:
    if chain in EVM_CHAINS:
        return analyze_evm_transaction(chain, tx_hash, issue_type, intended_address, intended_chain)
    if chain == 'solana':
        return analyze_solana_transaction(tx_hash, issue_type, intended_address, intended_chain)
    return {
        'status': 'unavailable',
        'reason_code': 'UNSUPPORTED_CHAIN',
        'summary': f'Unsupported chain: {chain}',
        'recoverability': 'unknown',
        'next_actions': ['Choose a supported network and try again.'],
        'observed': {'chain': chain, 'tx_hash': tx_hash},
    }


def auto_detect_recovery_chain(tx_hash: str, issue_type: str, intended_address: str, intended_chain: str, candidate_override: Optional[List[str]] = None) -> Tuple[str, Dict[str, Any]]:
    if candidate_override:
        candidates = ordered_candidates([normalize_chain(item) for item in candidate_override if normalize_chain(item)], intended_chain)
    elif is_likely_evm_tx_hash(tx_hash):
        candidates = ordered_candidates(EVM_CHAIN_ORDER, intended_chain)
    elif is_likely_solana_signature(tx_hash):
        candidates = ['solana']
    else:
        candidates = ordered_candidates(EVM_CHAIN_ORDER + ['solana'], intended_chain)

    results: Dict[str, Dict[str, Any]] = {}
    max_workers = min(len(candidates), 5) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(analyze_transaction_on_chain, chain, tx_hash, issue_type, intended_address, intended_chain): chain
            for chain in candidates
        }
        for future in as_completed(future_map):
            chain = future_map[future]
            try:
                result = future.result() or {}
            except Exception as exc:
                result = {
                    'status': 'unavailable',
                    'reason_code': 'AUTO_DETECT_ERROR',
                    'summary': f'Auto-detect error on {chain}: {exc}',
                    'recoverability': 'unknown',
                    'next_actions': ['Retry the lookup or choose the exact network manually.'],
                    'observed': {'chain': chain, 'tx_hash': tx_hash},
                }
            result['auto_detected'] = True
            result['candidate_chain'] = chain
            results[chain] = result

    found_chains = [chain for chain in candidates if str((results.get(chain) or {}).get('status')) in {'found', 'reverted'}]
    if found_chains:
        selected_chain = found_chains[0]
        selected = dict(results[selected_chain])
        observed = dict(selected.get('observed') or {})
        observed['chain'] = selected_chain
        selected['observed'] = observed
        selected['auto_detected'] = True
        selected['auto_detect_scope'] = candidates
        selected['auto_detect_summary'] = f'Network auto-detected as {selected_chain}.'
        return selected_chain, selected

    not_found_chains = [chain for chain in candidates if str((results.get(chain) or {}).get('status')) == 'not_found']
    unavailable_chains = [chain for chain in candidates if str((results.get(chain) or {}).get('status')) == 'unavailable']

    if len(not_found_chains) == len(candidates):
        return 'auto', {
            'status': 'not_found',
            'reason_code': 'TX_NOT_FOUND_ANY_SUPPORTED_CHAIN',
            'summary': 'Transaction was not found across the supported auto-detect network scope.',
            'recoverability': 'unknown',
            'next_actions': [
                'Verify the transaction hash carefully.',
                'If you know the network, select it manually and retry.',
                'If this came from an exchange or wallet app, ask them which network the transfer used.'
            ],
            'observed': {'chain': 'auto', 'tx_hash': tx_hash},
            'auto_detected': True,
            'auto_detect_scope': candidates,
        }

    if unavailable_chains and not found_chains:
        return 'auto', {
            'status': 'unavailable',
            'reason_code': 'AUTO_DETECT_INCONCLUSIVE',
            'summary': 'Auto-detect could not confirm the network because one or more chain lookups were unavailable.',
            'recoverability': 'unknown',
            'next_actions': [
                'Retry once in a moment.',
                'If you know the network, select it manually for a narrower lookup.',
                'If the sender used an exchange or wallet app, ask them which network the transfer used.'
            ],
            'observed': {'chain': 'auto', 'tx_hash': tx_hash},
            'auto_detected': True,
            'auto_detect_scope': candidates,
            'auto_detect_results': {chain: {
                'status': (results.get(chain) or {}).get('status'),
                'reason_code': (results.get(chain) or {}).get('reason_code'),
            } for chain in candidates},
        }

    fallback_chain = candidates[0] if candidates else 'auto'
    fallback = dict(results.get(fallback_chain) or {})
    observed = dict(fallback.get('observed') or {})
    observed['chain'] = fallback_chain
    fallback['observed'] = observed
    fallback['auto_detected'] = True
    fallback['auto_detect_scope'] = candidates
    return fallback_chain, fallback


@app.post("/api/recovery-copilot")
def recovery_copilot():
    payload = request.get_json(silent=True) or {}
    chain = normalize_chain(payload.get("network") or payload.get("chain"))
    tx_hash = str(payload.get("tx_hash") or payload.get("hash") or "").strip()
    issue_type = normalize_issue_type(payload.get("issue_type"))
    intended_address = str(payload.get("intended_address") or "").strip()
    intended_chain = normalize_chain(payload.get("intended_chain") or "") if payload.get("intended_chain") else ""

    if not tx_hash:
        raise ApiError("tx_hash is required.")

    access = getattr(g, "api_access", None) or {}
    policy = resolve_access_policy(access)
    allowed_networks = list(policy.get("allowed_networks") or [])

    if not chain or chain == "auto":
        candidate_override = allowed_networks if allowed_networks else None
        chain, analysis = auto_detect_recovery_chain(tx_hash, issue_type, intended_address, intended_chain, candidate_override=candidate_override)
    elif chain in EVM_CHAINS or chain == "solana":
        analysis = analyze_transaction_on_chain(chain, tx_hash, issue_type, intended_address, intended_chain)
    else:
        raise ApiError(f"Unsupported chain: {chain}")

    observed = analysis.get("observed") or {}
    effective_chain = normalize_chain(str(observed.get("chain") or chain or "")) or chain or "auto"
    if allowed_networks and effective_chain not in {"", "auto"} and effective_chain not in allowed_networks:
        raise ApiError(
            "Resolved network is outside the policy allowlist.",
            403,
            code="POLICY_NETWORK_NOT_ALLOWED",
            details={"resolved_network": effective_chain, "allowed_networks": allowed_networks},
        )
    destination_profile = build_destination_profile(effective_chain if effective_chain != 'auto' else '', str(observed.get("destination") or ""), {
        "address_type": observed.get("destination_type") or "unknown",
        "rpc_used": bool(observed.get("destination") and analysis.get("status") not in {"unavailable", "not_found"}),
    })
    outcome = derive_recovery_outcome(analysis.get("status"), observed.get("tx_status"))
    confidence = derive_recovery_confidence(analysis.get("status"), observed.get("tx_status"))
    why = build_recovery_explanation(analysis, destination_profile)
    checked_at = utc_now_iso()
    log_api_access("/api/recovery-copilot")
    support_packet = build_recovery_support_packet(
        chain=effective_chain,
        tx_hash=tx_hash,
        issue_type=issue_type,
        analysis=analysis,
        destination_profile=destination_profile,
        checked_at=checked_at,
        outcome=outcome,
        confidence=confidence,
        why=why,
    )
    support_message = build_recovery_support_message(support_packet)
    record_request_event(
        event_name="recovery_run",
        endpoint="/api/recovery-copilot",
        status=analysis.get("status") or outcome,
        reason_code=analysis.get("reason_code") or "",
        network=effective_chain,
        http_status=200,
        timeout_flag=looks_like_timeout(analysis.get("summary")) or looks_like_timeout((analysis.get("observed") or {}).get("details")),
        metadata={
            "outcome": outcome,
            "recoverability": analysis.get("recoverability"),
            "auto_detected": bool(analysis.get("auto_detected")),
        },
    )

    response_payload = {
        "ok": True,
        "service": "recovery-copilot",
        "version": APP_VERSION,
        "trace_id": current_request_id(),
        "checked_at": checked_at,
        "chain": effective_chain,
        "outcome": outcome,
        "confidence": confidence,
        "why_this_result": why,
        "explanation": why,
        "destination": destination_profile,
        "recovery_verdict": support_packet.get("verdict"),
        "best_next_step": support_packet.get("best_next_step"),
        "contact_target": support_packet.get("contact_target"),
        "support_packet": support_packet,
        "support_message": support_message,
        "proof": {
            "checked_at": checked_at,
            "chain": effective_chain,
            "trace_id": current_request_id(),
            "outcome": outcome,
            "confidence": confidence,
            "reason_code": analysis.get("reason_code"),
        },
        **analysis,
    }
    access = getattr(g, "api_access", None) or {}
    record_id = store_verification_record(
        service="recovery-copilot",
        event_name="recovery_run",
        payload=payload,
        response_payload=response_payload,
        network=effective_chain,
        status=str(analysis.get("status") or outcome),
        reason_code=str(analysis.get("reason_code") or ""),
        access=access,
    )
    response_payload["record_id"] = record_id
    response_payload["history_url"] = f"/api/verification-records/{record_id}"
    store_usage_event(
        service="recovery-copilot",
        event_name="recovery_run",
        network=effective_chain,
        status=str(analysis.get("status") or outcome),
        verdict=str(support_packet.get("verdict") or outcome),
        reason_code=str(analysis.get("reason_code") or ""),
        access=access,
        record_id=record_id,
        timeout_flag=looks_like_timeout(analysis.get("summary")) or looks_like_timeout((analysis.get("observed") or {}).get("details")),
        metadata={
            "outcome": outcome,
            "recoverability": analysis.get("recoverability"),
            "confidence": confidence,
        },
    )
    g.limit_state = build_limit_state(access, "this_month")
    response_payload["limits"] = g.limit_state
    delivery_id = create_webhook_delivery_for_record(record_id, "recovery_run", access)
    if delivery_id:
        response_payload["webhook"] = {
            "queued": True,
            "delivery_id": delivery_id,
            "event": "recovery_run",
        }
        delivery_state = dispatch_webhook_delivery_now(delivery_id)
        if str(delivery_state.get("delivery_status") or "") in {"pending", "retry_scheduled"}:
            kick_webhook_processor(force=True)
    return jsonify(response_payload)


@app.post("/pilot-request")
def pilot_request():
    payload = request.get_json(silent=True) or {}
    honeypot_value = str(payload.get("website") or payload.get("company_website") or "").strip()
    form_started_at_raw = payload.get("form_started_at")

    if honeypot_value:
        record_request_event(
            event_name="pilot_submit_fail",
            endpoint="/pilot-request",
            status="rejected",
            reason_code="HONEYPOT_TRIGGERED",
            network="pilot",
            http_status=200,
            metadata={"filter": "honeypot"},
        )
        return pilot_ack_response(
            "Your request has been sent successfully.",
            stored=False,
            persisted=False,
            email_notification="filtered",
        )

    if form_started_at_raw not in (None, ""):
        try:
            started_at_value = float(form_started_at_raw)
            if started_at_value > 10_000_000_000:
                started_at_value = started_at_value / 1000.0
            age_sec = time.time() - started_at_value
            if 0 <= age_sec < PILOT_MIN_FILL_SEC:
                record_request_event(
                    event_name="pilot_submit_fail",
                    endpoint="/pilot-request",
                    status="rejected",
                    reason_code="MIN_FILL_TIME",
                    network="pilot",
                    http_status=200,
                    metadata={"filter": "speed_guard"},
                )
                return pilot_ack_response(
                    "Your request has been sent successfully.",
                    stored=False,
                    persisted=False,
                    email_notification="filtered",
                )
        except Exception:
            pass

    name = str(payload.get("name") or "").strip()
    company = str(payload.get("company") or "").strip()
    email = str(payload.get("email") or "").strip().lower()
    volume = str(payload.get("volume") or "").strip()
    notes = str(payload.get("notes") or payload.get("use_case") or "").strip()
    source_ip = get_client_ip()
    user_agent = request.headers.get("User-Agent", "")
    origin = request.headers.get("Origin", "")

    if not all([name, company, email, notes]):
        raise ApiError("name, company, email and notes are required.")
    if len(notes) > PILOT_MAX_NOTES_LEN:
        raise ApiError(f"notes is too long. Max {PILOT_MAX_NOTES_LEN} characters.")
    if not is_valid_email(email):
        raise ApiError("Please enter a valid work email address.")
    if is_personal_email(email):
        raise ApiError("Please use your work email. Personal email domains are not accepted.")

    fingerprint_payload = {
        "name": name.lower(),
        "company": company.lower(),
        "email": email.lower(),
        "notes": " ".join(notes.lower().split()),
    }
    fingerprint = _pilot_payload_fingerprint(fingerprint_payload)
    created_at = utc_now_iso()
    request_id = pilot_request_id()

    conn = get_db()
    try:
        duplicate = find_recent_duplicate_request(conn, fingerprint, PILOT_DUPLICATE_TTL_SEC)
        if duplicate:
            record_request_event(
                event_name="pilot_submit_fail",
                endpoint="/pilot-request",
                status="rejected",
                reason_code="DUPLICATE_REQUEST",
                network="pilot",
                http_status=200,
                metadata={"existing_request_id": str(duplicate["request_id"])},
            )
            return pilot_ack_response(
                "This pilot request is already in review.",
                submitted_at=str(duplicate["created_at"]),
                stored=True,
                persisted=True,
                email_notification="deduplicated",
                request_id=str(duplicate["request_id"]),
            )

        db_execute(
            conn,
            """
            INSERT INTO pilot_requests(
                request_id, fingerprint, created_at, name, company, email, volume, notes,
                source_ip, user_agent, origin, email_status, email_last_attempt_at,
                welcome_email_status, welcome_email_last_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                fingerprint,
                created_at,
                name,
                company,
                email,
                volume,
                notes,
                source_ip,
                user_agent,
                origin,
                "pending",
                None,
                "pending",
                None,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    email_result = send_pilot_notification({
        "request_id": request_id,
        "created_at": created_at,
        "name": name,
        "company": company,
        "email": email,
        "volume": volume,
        "notes": notes,
        "source_ip": source_ip or "",
        "user_agent": user_agent or "",
        "origin": origin or "",
    })
    update_pilot_delivery_status(request_id, email_result)
    welcome_email_result = send_pilot_welcome_email({
        "request_id": request_id,
        "created_at": created_at,
        "name": name,
        "company": company,
        "email": email,
        "volume": volume,
        "notes": notes,
    })
    update_pilot_welcome_status(request_id, welcome_email_result)

    if email_result.get("status") == "sent":
        record_request_event(
            event_name="pilot_submit_success",
            endpoint="/pilot-request",
            status="success",
            reason_code="EMAIL_SENT",
            network="pilot",
            http_status=200,
            metadata={
                "request_id": request_id,
                "email_notification": "sent",
                "welcome_email_notification": str(welcome_email_result.get("status") or "unknown"),
            },
        )
        return jsonify({
            "ok": True,
            "message": "Your request was received successfully. A confirmation email is on the way.",
            "request_id": request_id,
            "submitted_at": created_at,
            "next_step": f"We will review it within about {PILOT_WELCOME_NEXT_STEP_HOURS} hours." if PILOT_WELCOME_NEXT_STEP_HOURS > 0 else "We will review it soon.",
        })

    if email_result.get("status") == "not_configured":
        record_request_event(
            event_name="pilot_submit_success",
            endpoint="/pilot-request",
            status="degraded",
            reason_code="EMAIL_NOT_CONFIGURED",
            network="pilot",
            http_status=200,
            metadata={
                "request_id": request_id,
                "email_notification": "not_configured",
                "welcome_email_notification": str(welcome_email_result.get("status") or "unknown"),
            },
        )
        return jsonify({
            "ok": True,
            "message": "Your request was received successfully.",
            "request_id": request_id,
            "submitted_at": created_at,
            "next_step": f"We will review it within about {PILOT_WELCOME_NEXT_STEP_HOURS} hours." if PILOT_WELCOME_NEXT_STEP_HOURS > 0 else "We will review it soon.",
        }), 200

    record_request_event(
        event_name="pilot_submit_success",
        endpoint="/pilot-request",
        status="degraded",
        reason_code="EMAIL_FORWARDING_DELAYED",
        network="pilot",
        http_status=200,
        timeout_flag=looks_like_timeout(email_result.get("debug_detail")),
        error_message=str(email_result.get("debug_detail") or ""),
        metadata={
            "request_id": request_id,
            "email_notification": "failed",
            "welcome_email_notification": str(welcome_email_result.get("status") or "unknown"),
        },
    )
    return jsonify({
        "ok": True,
        "message": "Your request was recorded successfully, but internal email forwarding is delayed right now.",
        "stored": True,
        "persisted": True,
        "request_id": request_id,
        "trace_id": current_request_id(),
        "email_notification": "failed",
        "welcome_email_notification": str(welcome_email_result.get("status") or "unknown"),
        "submitted_at": created_at,
        "debug_detail": email_result.get("debug_detail"),
    }), 200


def _pilot_payload_fingerprint(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def is_valid_email(value: str) -> bool:
    return bool(re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", value or ""))


def is_personal_email(value: str) -> bool:
    parts = (value or "").lower().split("@")
    return len(parts) == 2 and parts[1] in PERSONAL_EMAIL_DOMAINS


def send_pilot_notification(payload: Dict[str, str]) -> Dict[str, Optional[str]]:
    if not RESEND_API_KEY or not RESEND_FROM or not RESEND_TO:
        return {"status": "not_configured", "email_id": None, "debug_detail": None}

    subject = f"New PayeeProof pilot request — {payload['company']} ({payload.get('request_id') or 'pending-id'})"
    notes_html = html.escape(payload["notes"]).replace("\n", "<br>")
    text_body = "\n".join([
        "New PayeeProof pilot request",
        "",
        f"Request ID: {payload.get('request_id') or 'Not assigned'}",
        f"Submitted at: {payload['created_at']}",
        f"Name: {payload['name']}",
        f"Company / team: {payload['company']}",
        f"Work email: {payload['email']}",
        f"Monthly payout volume: {payload['volume'] or 'Not provided'}",
        "",
        "Protected payout / verification flow:",
        payload["notes"],
        "",
        f"Origin: {payload.get('origin') or 'Not provided'}",
        f"IP: {payload.get('source_ip') or 'Not provided'}",
        f"User-Agent: {payload.get('user_agent') or 'Not provided'}",
    ])

    html_body = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.6;color:#111">
      <h2>New PayeeProof pilot request</h2>
      <p><strong>Request ID:</strong> {html.escape(payload.get('request_id') or 'Not assigned')}<br>
      <strong>Submitted at:</strong> {html.escape(payload['created_at'])}<br>
      <strong>Name:</strong> {html.escape(payload['name'])}<br>
      <strong>Company / team:</strong> {html.escape(payload['company'])}<br>
      <strong>Work email:</strong> {html.escape(payload['email'])}<br>
      <strong>Monthly payout volume:</strong> {html.escape(payload['volume'] or 'Not provided')}</p>
      <p><strong>Protected payout / verification flow:</strong><br>{notes_html}</p>
      <hr>
      <p style="font-size:12px;color:#555">Origin: {html.escape(payload.get('origin') or 'Not provided')}<br>
      IP: {html.escape(payload.get('source_ip') or 'Not provided')}<br>
      User-Agent: {html.escape(payload.get('user_agent') or 'Not provided')}</p>
    </div>
    """.strip()

    resend_payload = {
        "from": RESEND_FROM,
        "to": [RESEND_TO],
        "subject": subject,
        "reply_to": payload["email"],
        "text": text_body,
        "html": html_body,
        "tags": [
            {"name": "source", "value": "pilot_request"},
            {"name": "product", "value": "payeeproof"},
        ],
    }

    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "PayeeProof/1.0 (+https://payeeproof.com)",
        "Idempotency-Key": f"pilot-{_pilot_payload_fingerprint(resend_payload)}",
    }

    try:
        response = requests.post(
            f"{RESEND_API_BASE}/emails",
            headers=headers,
            json=resend_payload,
            timeout=EMAIL_TIMEOUT,
        )
        raw_text = response.text[:1000]
        if response.status_code < 200 or response.status_code >= 300:
            return {
                "status": "failed",
                "email_id": None,
                "debug_detail": f"RESEND_HTTP_{response.status_code}: {raw_text}",
            }
        data = response.json() if response.text else {}
        return {
            "status": "sent",
            "email_id": data.get("id"),
            "debug_detail": None,
        }
    except requests.RequestException as exc:
        return {
            "status": "failed",
            "email_id": None,
            "debug_detail": f"RESEND_REQUEST_ERROR: {exc}",
        }
    except ValueError as exc:
        return {
            "status": "failed",
            "email_id": None,
            "debug_detail": f"RESEND_JSON_ERROR: {exc}",
        }


def send_pilot_welcome_email(payload: Dict[str, str]) -> Dict[str, Optional[str]]:
    if not PILOT_AUTO_WELCOME_ENABLED:
        return {"status": "disabled", "email_id": None, "debug_detail": None}
    if not RESEND_API_KEY or not RESEND_FROM:
        return {"status": "not_configured", "email_id": None, "debug_detail": None}

    next_step_label = f"within about {PILOT_WELCOME_NEXT_STEP_HOURS} hours" if PILOT_WELCOME_NEXT_STEP_HOURS > 0 else "soon"
    notes_preview = " ".join((payload.get("notes") or "").split())[:500]
    if len(notes_preview) == 500:
        notes_preview = notes_preview.rstrip() + "…"

    full_name = " ".join((payload.get("name") or "").split())
    greeting_name = (full_name.split()[0] if full_name else "there")

    subject = f"PayeeProof pilot request received — {payload['company']}"
    text_body = "\n".join([
        f"Hi {greeting_name},",
        "",
        "Thanks — we received your PayeeProof pilot request.",
        "",
        f"Request ID: {payload.get('request_id') or 'pending'}",
        f"Submitted at: {payload.get('created_at') or utc_now_iso()}",
        f"Company / team: {payload.get('company') or 'Not provided'}",
        f"Work email: {payload.get('email') or 'Not provided'}",
        f"Monthly payout volume: {payload.get('volume') or 'Not provided'}",
        "",
        "What happens next:",
        f"- We review the workflow and the decision you want before approval.",
        f"- We reply {next_step_label} with either a pilot fit confirmation or a short clarification request.",
        f"- If the fit is good, we lock the narrow pilot scope and next step.",
        "",
        "What we received:",
        notes_preview or "No notes provided.",
        "",
        "If you want to speed up review, just reply to this email with any extra detail on networks, assets, approval logic, or current failure cases.",
        "",
        "— PayeeProof",
        "https://payeeproof.com",
    ])

    html_body = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.6;color:#111">
      <h2>PayeeProof pilot request received</h2>
      <p>Hi {html.escape(greeting_name)},</p>
      <p>Thanks — we received your PayeeProof pilot request.</p>
      <p><strong>Request ID:</strong> {html.escape(payload.get('request_id') or 'pending')}<br>
      <strong>Submitted at:</strong> {html.escape(payload.get('created_at') or utc_now_iso())}<br>
      <strong>Company / team:</strong> {html.escape(payload.get('company') or 'Not provided')}<br>
      <strong>Work email:</strong> {html.escape(payload.get('email') or 'Not provided')}<br>
      <strong>Monthly payout volume:</strong> {html.escape(payload.get('volume') or 'Not provided')}</p>
      <p><strong>What happens next</strong><br>
      • We review the workflow and the decision you want before approval.<br>
      • We reply {html.escape(next_step_label)} with either a pilot fit confirmation or a short clarification request.<br>
      • If the fit is good, we lock the narrow pilot scope and next step.</p>
      <p><strong>What we received</strong><br>{html.escape(notes_preview or 'No notes provided.').replace(chr(10), '<br>')}</p>
      <p>If you want to speed up review, just reply to this email with any extra detail on networks, assets, approval logic, or current failure cases.</p>
      <p>— PayeeProof<br><a href="https://payeeproof.com">payeeproof.com</a></p>
    </div>
    """.strip()

    resend_payload = {
        "from": RESEND_FROM,
        "to": [payload["email"]],
        "subject": subject,
        "reply_to": PILOT_WELCOME_REPLY_TO or RESEND_TO or RESEND_FROM,
        "text": text_body,
        "html": html_body,
        "tags": [
            {"name": "source", "value": "pilot_welcome"},
            {"name": "product", "value": "payeeproof"},
        ],
    }

    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "PayeeProof/1.0 (+https://payeeproof.com)",
        "Idempotency-Key": f"pilot-welcome-{payload.get('request_id') or _pilot_payload_fingerprint(resend_payload)}",
    }

    try:
        response = requests.post(
            f"{RESEND_API_BASE}/emails",
            headers=headers,
            json=resend_payload,
            timeout=EMAIL_TIMEOUT,
        )
        raw_text = response.text[:1000]
        if response.status_code < 200 or response.status_code >= 300:
            return {
                "status": "failed",
                "email_id": None,
                "debug_detail": f"RESEND_HTTP_{response.status_code}: {raw_text}",
            }
        data = response.json() if response.text else {}
        return {
            "status": "sent",
            "email_id": data.get("id"),
            "debug_detail": None,
        }
    except requests.RequestException as exc:
        return {
            "status": "failed",
            "email_id": None,
            "debug_detail": f"RESEND_REQUEST_ERROR: {exc}",
        }
    except ValueError as exc:
        return {
            "status": "failed",
            "email_id": None,
            "debug_detail": f"RESEND_JSON_ERROR: {exc}",
        }


def log_api_access(path: str) -> None:
    access = getattr(g, "api_access", None) or {}
    if not access:
        return
    scope = access_scope(access)
    emit_structured_log(
        "api_access",
        endpoint=path,
        access_mode=str(access.get("mode") or "unknown"),
        client_label=str(access.get("client") or "unknown-client"),
        tenant_id=scope.get("tenant_id"),
        environment=scope.get("environment"),
    )
    conn = get_db()
    try:
        db_execute(
            conn,
            "INSERT INTO api_access_log(created_at, path, access_mode, client_label, tenant_id, environment, role, source_ip) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                utc_now_iso(),
                path,
                str(access.get("mode") or "unknown"),
                str(access.get("client") or "unknown-client"),
                scope.get("tenant_id"),
                scope.get("environment"),
                scope.get("role"),
                get_client_ip(),
            ),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def compare_addresses(expected_chain: str, expected_address: str, provided_chain: str, provided_address: str) -> bool:
    if expected_chain != provided_chain:
        return False
    if expected_chain in EVM_CHAINS:
        return expected_address.lower() == provided_address.lower()
    return expected_address == provided_address


def validate_address(chain: str, address: str) -> Tuple[bool, str]:
    if chain in EVM_CHAINS:
        if not re.fullmatch(r"0x[a-fA-F0-9]{40}", address or ""):
            return False, "Expected a 20-byte EVM address in 0x-prefixed hex format."
        return True, "Valid EVM format."
    if chain == "solana":
        if not is_base58_string(address) or not (32 <= len(address) <= 44):
            return False, "Expected a base58 Solana public key or signature-like address string."
        try:
            raw = b58decode(address)
            if not (32 <= len(raw) <= 64):
                return False, "Base58 value decoded but length is not typical for a Solana public key."
        except Exception:
            return False, "Invalid base58 for Solana."
        return True, "Valid Solana base58 format."
    return False, f"Unsupported chain: {chain}"


def is_base58_string(value: str) -> bool:
    return bool(value) and all(ch in BASE58_ALPHABET for ch in value)


def b58decode(value: str) -> bytes:
    num = 0
    for char in value:
        num = num * 58 + BASE58_ALPHABET.index(char)
    result = bytearray()
    while num > 0:
        num, rem = divmod(num, 256)
        result.insert(0, rem)
    pad = 0
    for c in value:
        if c == "1":
            pad += 1
        else:
            break
    return bytes([0] * pad) + bytes(result)


def _address_cache_key(chain: str, address: str) -> str:
    normalized = str(address or "").strip()
    if chain in EVM_CHAINS:
        normalized = normalized.lower()
    return f"{chain}:{normalized}"


def get_cached_classification(chain: str, address: str) -> Optional[Dict[str, Any]]:
    key = _address_cache_key(chain, address)
    if not key:
        return None
    cached = _ADDRESS_CLASSIFY_CACHE.get(key)
    if not cached:
        return None
    cached_at = float(cached.get("cached_at") or 0)
    if (time.time() - cached_at) > ADDRESS_CACHE_TTL_SEC:
        _ADDRESS_CLASSIFY_CACHE.pop(key, None)
        return None
    value = cached.get("value")
    return dict(value) if isinstance(value, dict) else None


def set_cached_classification(chain: str, address: str, value: Dict[str, Any]) -> None:
    if not isinstance(value, dict):
        return
    if not value.get("rpc_used"):
        return
    key = _address_cache_key(chain, address)
    if not key:
        return
    _ADDRESS_CLASSIFY_CACHE[key] = {
        "cached_at": time.time(),
        "value": dict(value),
    }


def skipped_expected_onchain(chain: str, details: str) -> Dict[str, Any]:
    return {
        "chain": chain,
        "address_type": "not_checked",
        "rpc_used": False,
        "details": details,
    }


def classify_address(chain: str, address: str) -> Dict[str, Any]:
    cached = get_cached_classification(chain, address)
    if cached is not None:
        return cached

    if chain in EVM_CHAINS:
        rpc = rpc_call(chain, "eth_getCode", [address, "latest"])
        if not rpc.ok:
            return {"chain": chain, "address_type": "unknown", "rpc_used": False, "details": rpc.error}
        code = str(rpc.result or "0x")
        address_type = "contract" if code not in {"0x", "0x0", ""} else "eoa"
        result = {
            "chain": chain,
            "address_type": address_type,
            "rpc_used": True,
            "code_present": address_type == "contract",
        }
        set_cached_classification(chain, address, result)
        return result

    if chain == "solana":
        rpc = rpc_call(chain, "getAccountInfo", [address, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        if not rpc.ok:
            return {"chain": chain, "address_type": "unknown", "rpc_used": False, "details": rpc.error}
        value = (rpc.result or {}).get("value") if isinstance(rpc.result, dict) else None
        if value is None:
            result = {"chain": chain, "address_type": "not_found", "rpc_used": True, "exists": False}
            set_cached_classification(chain, address, result)
            return result
        owner = value.get("owner")
        executable = bool(value.get("executable"))
        address_type = "executable" if executable else "account"
        result = {
            "chain": chain,
            "address_type": address_type,
            "rpc_used": True,
            "exists": True,
            "owner_program": owner,
            "lamports": value.get("lamports"),
            "executable": executable,
        }
        set_cached_classification(chain, address, result)
        return result

    return {"chain": chain, "address_type": "unsupported", "rpc_used": False}


def summarize_preflight(status: str, reason_code: str, provided_onchain: Dict[str, Any]) -> str:
    if status == "verified":
        if provided_onchain.get("address_type") == "contract":
            return "Format and request details match, but the destination is a contract address and should be reviewed before payout."
        return "Expected and provided payout details match."
    if status == "blocked":
        return f"Blocked: {reason_code.replace('_', ' ').title()}."
    if status == "review_required":
        return f"Review required: {reason_code.replace('_', ' ').title()}."
    return f"Mismatch detected: {reason_code.replace('_', ' ').title()}."


def build_preflight_explanation(status: str, reason_code: str, risk_flags: List[str], expected_onchain: Dict[str, Any], provided_onchain: Dict[str, Any]) -> str:
    if status == "verified":
        if provided_onchain.get("address_type") == "contract":
            return "The request fields match, but the destination resolves to a smart contract. For treasury or exchange payouts, that deserves human review before funds move."
        return "Network, asset and address match. The destination also passed basic on-chain classification."
    if reason_code == "NETWORK_MISMATCH":
        return "The submitted destination is on a different chain than the request. This is the classic wrong-network mistake."
    if reason_code == "ASSET_MISMATCH":
        return "The destination details match structurally, but the asset requested and the asset submitted differ."
    if reason_code == "ADDRESS_MISMATCH":
        return "The provided payout address does not match the expected destination. Treat as a hard stop unless the payer intentionally changed the destination."
    if reason_code == "MEMO_MISMATCH":
        return "Memo or tag values differ. For custodial destinations, that can mean funds arrive on-chain but fail to be credited."
    if reason_code == "DESTINATION_IS_CONTRACT_OR_PROGRAM":
        return "The destination address exists on-chain, but it resolves to a contract or executable account. Sending to such destinations can be irreversible or require special recovery steps."
    if "INVALID" in reason_code:
        return "The submitted or expected address is not valid for the selected chain, so the payout should be blocked before any funds move."
    if risk_flags:
        return "The pre-send check found one or more risk flags. Review them before payout approval."
    return "The request needs review before funds move."


def decide_preflight_next_action(risk_flags: List[str]) -> str:
    if any(flag in risk_flags for flag in ["PROVIDED_ADDRESS_INVALID", "EXPECTED_ADDRESS_INVALID", "ZERO_ADDRESS"]):
        return "DO_NOT_SEND"
    if "MEMO_MISMATCH" in risk_flags:
        return "RECHECK_MEMO_OR_TAG"
    if "DESTINATION_IS_CONTRACT_OR_PROGRAM" in risk_flags:
        return "MANUAL_REVIEW"
    return "BLOCK_AND_REVERIFY"


def rpc_call(chain: str, method: str, params: List[Any]) -> RpcResult:
    url = RPC_URLS.get(chain, "")
    if not url:
        return RpcResult(ok=False, error=f"RPC not configured for chain: {chain}")
    body = {"jsonrpc": "2.0", "id": int(time.time() * 1000), "method": method, "params": params}
    try:
        response = requests.post(url, json=body, timeout=RPC_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            err = data["error"]
            return RpcResult(ok=False, error=f"{err.get('message', 'RPC error')} ({err.get('code', 'n/a')})")
        return RpcResult(ok=True, result=data.get("result"))
    except requests.RequestException as exc:
        return RpcResult(ok=False, error=f"RPC request failed: {exc}")
    except ValueError:
        return RpcResult(ok=False, error="RPC returned non-JSON response")


def analyze_evm_transaction(chain: str, tx_hash: str, issue_type: str, intended_address: str, intended_chain: str) -> Dict[str, Any]:
    tx = rpc_call(chain, "eth_getTransactionByHash", [tx_hash])
    if not tx.ok:
        return {
            "status": "unavailable",
            "reason_code": "RPC_UNAVAILABLE",
            "summary": tx.error,
            "recoverability": "unknown",
            "next_actions": ["Check the RPC configuration for this chain and try again."],
            "observed": {"chain": chain, "tx_hash": tx_hash},
        }
    if not tx.result:
        return {
            "status": "not_found",
            "reason_code": "TX_NOT_FOUND",
            "summary": "Transaction was not found on the selected chain.",
            "recoverability": "unknown",
            "next_actions": ["Verify the chain and hash. A wrong-network error often starts here."],
            "observed": {"chain": chain, "tx_hash": tx_hash},
        }

    receipt = rpc_call(chain, "eth_getTransactionReceipt", [tx_hash])
    receipt_result = receipt.result or {}
    tx_result = tx.result or {}

    tx_to = tx_result.get("to")
    tx_from = tx_result.get("from")
    native_value = hex_to_int(tx_result.get("value"))
    status_hex = receipt_result.get("status") if isinstance(receipt_result, dict) else None
    tx_status = parse_evm_receipt_status(status_hex)
    block_number = hex_to_int(tx_result.get("blockNumber")) if tx_result.get("blockNumber") else None

    token_transfer = extract_erc20_transfer(receipt_result.get("logs") or [])
    destination = token_transfer.get("to") or tx_to
    destination_kind = classify_address(chain, destination) if destination else {"address_type": "unknown", "rpc_used": False}

    observed = {
        "chain": chain,
        "tx_hash": tx_hash,
        "tx_status": tx_status,
        "block_number": block_number,
        "from": tx_from,
        "to": tx_to,
        "native_value_wei": native_value,
        "destination": destination,
        "destination_type": destination_kind.get("address_type"),
        "token_transfer": token_transfer or None,
    }

    if tx_status == "reverted":
        return {
            "status": "reverted",
            "reason_code": "TX_REVERTED",
            "summary": "The transaction reverted on-chain. Funds were not delivered, but gas was spent.",
            "recoverability": "likely",
            "next_actions": [
                "Do not resend blindly.",
                "Check why the transaction reverted before trying again.",
                "For token transfers, confirm allowance, destination, and chain settings."
            ],
            "observed": observed,
        }

    summary, recoverability, next_actions, reason_code = build_evm_recovery_guidance(
        chain=chain,
        issue_type=issue_type,
        intended_address=intended_address,
        intended_chain=intended_chain,
        destination=destination,
        destination_kind=destination_kind,
        token_transfer=token_transfer,
        tx_status=tx_status,
    )

    return {
        "status": "found",
        "reason_code": reason_code,
        "summary": summary,
        "recoverability": recoverability,
        "next_actions": next_actions,
        "observed": observed,
    }


def build_evm_recovery_guidance(
    chain: str,
    issue_type: str,
    intended_address: str,
    intended_chain: str,
    destination: Optional[str],
    destination_kind: Dict[str, Any],
    token_transfer: Dict[str, Any],
    tx_status: str,
) -> Tuple[str, str, List[str], str]:
    same_destination = bool(intended_address and destination and intended_address.lower() == destination.lower())
    issue_type = normalize_issue_type(issue_type)
    auto_mode = issue_type == "auto"

    if auto_mode and intended_chain and intended_chain != chain:
        return (
            f"The transaction exists on {chain}, while the intended destination chain was {intended_chain}. This points to a likely wrong-network send.",
            "possible",
            [
                "Confirm whether the recipient controls the same address on the intended chain.",
                "If the destination belongs to an exchange or custodial platform, open a support case with the tx hash, asset, amount, and both chain names.",
                "Do not resend until the recipient confirms the supported network for this asset."
            ],
            "WRONG_NETWORK_DETECTED",
        )

    if auto_mode and intended_address and destination and not same_destination:
        return (
            "The on-chain destination differs from the intended address that was provided. This points to a likely wrong-address send.",
            "unlikely",
            [
                "Verify whether the receiving address belongs to you, your team, or the expected platform.",
                "If the destination belongs to a custodial service, contact support immediately with the tx hash and amount.",
                "If it is an unknown self-custody address, recovery is usually not possible."
            ],
            "WRONG_ADDRESS_DETECTED",
        )

    if auto_mode and destination_kind.get("address_type") == "contract":
        return (
            "The destination resolves to a smart contract or app. Recovery depends on whether that contract exposes a withdrawal, sweep, or rescue path.",
            "manual_review",
            [
                "Identify the contract owner, protocol team, or application operator.",
                "Check whether the contract has a documented recovery or administrative withdrawal path.",
                "Do not resend until the contract behavior is understood."
            ],
            "DESTINATION_IS_CONTRACT",
        )

    if issue_type == "wrong_network":
        if intended_chain and intended_chain != chain:
            return (
                f"The transaction exists on {chain}, while the intended destination chain was {intended_chain}. This is consistent with a wrong-network send.",
                "possible",
                [
                    "Check whether the recipient controls the same address on the source chain.",
                    "If the destination was an exchange or custodial platform, contact their support with the tx hash, token, amount, and chain.",
                    "Do not assume automatic recovery just because the address string looks identical across EVM chains."
                ],
                "WRONG_NETWORK_CONFIRMED",
            )
        return (
            f"The transaction was found and confirmed on {chain}. If the user expected another network, treat this as a probable wrong-network send.",
            "possible",
            [
                "Confirm which chain the recipient actually supports for this asset.",
                "If the destination is custodial, open a support case with the full transaction details.",
                "For future sends, require a chain match before funds move."
            ],
            "WRONG_NETWORK_SUSPECTED",
        )

    if issue_type == "wrong_address":
        if same_destination:
            return (
                "The on-chain recipient matches the intended address, so this does not look like a wrong-address event.",
                "n/a",
                ["Review the issue type and the intended address before proceeding."],
                "INTENDED_ADDRESS_MATCHES",
            )
        return (
            "Funds were delivered to a different address than the intended destination. On-chain transfers are generally final unless the recipient cooperates or controls the address.",
            "unlikely",
            [
                "Verify whether the receiving address belongs to you, your team, or the target platform.",
                "If it belongs to a custodial service, contact support immediately with the tx hash and amount.",
                "If it is an unknown self-custody address, recovery is usually not possible."
            ],
            "WRONG_ADDRESS_CONFIRMED",
        )

    if issue_type == "sent_to_contract":
        if destination_kind.get("address_type") == "contract":
            return (
                "The destination resolves to a smart contract. Recovery depends on whether that contract exposes a withdrawal or rescue path for the asset sent.",
                "manual_review",
                [
                    "Identify the contract owner or application team.",
                    "Check whether the contract has a token rescue, sweep, or administrative withdrawal function.",
                    "Do not resend until the contract behavior is understood."
                ],
                "DESTINATION_IS_CONTRACT",
            )
        return (
            "The transaction was found, but the destination does not look like a contract address on this chain.",
            "unknown",
            ["Review the transaction details and confirm the issue type."],
            "CONTRACT_DESTINATION_NOT_CONFIRMED",
        )

    if issue_type == "missing_memo":
        return (
            "The transaction is on-chain. If this was a custodial deposit that required a memo or tag, the most likely path is manual credit by the receiving platform.",
            "possible",
            [
                "Collect tx hash, token, amount, network, sender address, and destination address.",
                "Open a support ticket with the receiving platform and mention that the transfer completed without the required memo/tag.",
                "Do not send duplicates until support confirms the status."
            ],
            "MISSING_MEMO_SUPPORT_PATH",
        )

    if token_transfer:
        return (
            f"The transaction is confirmed on {chain} and includes an ERC-20 transfer to {destination}.",
            "depends",
            [
                "Check whether the recipient address is controlled by the intended party.",
                "If this was a custodial destination, send the tx hash to their support team.",
                "If this was self-custody to an unknown address, recovery is usually not possible."
            ],
            "TOKEN_TRANSFER_OBSERVED",
        )

    return (
        f"The transaction is confirmed on {chain}. Recovery depends on who controls the receiving address and whether the transfer went to a custodial platform or a self-custody wallet.",
        "depends",
        [
            "Confirm the exact recipient and whether it is controlled by a platform or an individual wallet.",
            "Use the tx hash as the anchor for any recovery discussion.",
            "Treat any resend as a separate risk event."
        ],
        "GENERAL_ONCHAIN_GUIDANCE",
    )


def parse_evm_receipt_status(status_hex: Optional[str]) -> str:
    if status_hex is None:
        return "pending_or_unknown"
    try:
        return "confirmed" if int(status_hex, 16) == 1 else "reverted"
    except Exception:
        return "pending_or_unknown"


def extract_erc20_transfer(logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    for log in logs:
        topics = log.get("topics") or []
        if len(topics) >= 3 and str(topics[0]).lower() == TRANSFER_TOPIC:
            from_addr = "0x" + str(topics[1])[-40:]
            to_addr = "0x" + str(topics[2])[-40:]
            amount_raw = hex_to_int(log.get("data"))
            return {
                "token_contract": log.get("address"),
                "from": from_addr,
                "to": to_addr,
                "amount_raw": amount_raw,
            }
    return {}


def analyze_solana_transaction(tx_hash: str, issue_type: str, intended_address: str, intended_chain: str) -> Dict[str, Any]:
    issue_type = normalize_issue_type(issue_type)
    rpc = rpc_call("solana", "getTransaction", [
        tx_hash,
        {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}
    ])
    if not rpc.ok:
        return {
            "status": "unavailable",
            "reason_code": "RPC_UNAVAILABLE",
            "summary": rpc.error,
            "recoverability": "unknown",
            "next_actions": ["Check SOLANA_RPC_URL and try again."],
            "observed": {"chain": "solana", "tx_hash": tx_hash},
        }
    if not rpc.result:
        return {
            "status": "not_found",
            "reason_code": "TX_NOT_FOUND",
            "summary": "Transaction was not found on Solana.",
            "recoverability": "unknown",
            "next_actions": ["Verify the transaction signature and network."],
            "observed": {"chain": "solana", "tx_hash": tx_hash},
        }

    result = rpc.result
    meta = result.get("meta") or {}
    transaction = result.get("transaction") or {}
    message = transaction.get("message") or {}
    account_keys = message.get("accountKeys") or []
    parsed_instructions = message.get("instructions") or []

    signer = None
    for key in account_keys:
        if isinstance(key, dict) and key.get("signer"):
            signer = key.get("pubkey")
            break

    destination = extract_solana_destination(parsed_instructions)
    tx_status = "confirmed" if not meta.get("err") else "failed"
    destination_kind = classify_address("solana", destination) if destination else {"address_type": "unknown", "rpc_used": False}

    observed = {
        "chain": "solana",
        "tx_hash": tx_hash,
        "slot": result.get("slot"),
        "tx_status": tx_status,
        "fee_lamports": meta.get("fee"),
        "signer": signer,
        "destination": destination,
        "destination_type": destination_kind.get("address_type"),
        "error": meta.get("err"),
        "token_balances": {
            "pre": meta.get("preTokenBalances") or [],
            "post": meta.get("postTokenBalances") or [],
        },
    }

    if tx_status == "failed":
        return {
            "status": "failed",
            "reason_code": "TX_FAILED",
            "summary": "The Solana transaction failed. Funds were not delivered, but fees may have been spent.",
            "recoverability": "likely",
            "next_actions": [
                "Do not resend until the failure reason is understood.",
                "Check the program error and destination account assumptions."
            ],
            "observed": observed,
        }

    auto_mode = issue_type == "auto"

    if auto_mode and intended_chain and intended_chain != "solana":
        return {
            "status": "found",
            "reason_code": "WRONG_NETWORK_DETECTED",
            "summary": f"The transfer exists on Solana, while the intended chain was {intended_chain}. This points to a likely wrong-network send.",
            "recoverability": "possible",
            "next_actions": [
                "Confirm whether the recipient supports Solana for this asset.",
                "If this was a custodial destination, contact platform support with the signature, asset, amount, and both chain names.",
                "Do not resend until the supported network is confirmed."
            ],
            "observed": observed,
        }

    if auto_mode and intended_address and destination and intended_address != destination:
        return {
            "status": "found",
            "reason_code": "WRONG_ADDRESS_DETECTED",
            "summary": "The on-chain destination differs from the intended address that was provided. This points to a likely wrong-address send.",
            "recoverability": "unlikely",
            "next_actions": [
                "Check whether the destination belongs to the expected platform or to you.",
                "If it belongs to a custodial service, contact support immediately with the signature and amount.",
                "If it is an unknown self-custody address, recovery is usually not possible."
            ],
            "observed": observed,
        }

    if auto_mode and destination_kind.get("address_type") == "executable":
        return {
            "status": "found",
            "reason_code": "DESTINATION_IS_PROGRAM",
            "summary": "The destination resolves to an executable Solana program account. Recovery depends on program design and operator access.",
            "recoverability": "manual_review",
            "next_actions": [
                "Identify the protocol or application that owns the program.",
                "Check whether there is a documented recovery path for mistaken deposits.",
                "Do not resend until the operator confirms the destination behavior."
            ],
            "observed": observed,
        }

    if issue_type == "missing_memo":
        return {
            "status": "found",
            "reason_code": "MISSING_MEMO_SUPPORT_PATH",
            "summary": "The transfer is on-chain. If this was a custodial deposit that required a memo, manual credit by the receiving platform is the likely path.",
            "recoverability": "possible",
            "next_actions": [
                "Open a support ticket with the receiving platform.",
                "Include the signature, token, amount, destination address, and timestamp.",
                "Do not send duplicates unless support instructs you to do so."
            ],
            "observed": observed,
        }

    if issue_type == "wrong_address" and intended_address and destination and intended_address != destination:
        return {
            "status": "found",
            "reason_code": "WRONG_ADDRESS_CONFIRMED",
            "summary": "The Solana transfer landed at a different destination than intended.",
            "recoverability": "unlikely",
            "next_actions": [
                "Check whether the destination belongs to a platform that can assist.",
                "If it is an unknown self-custody address, recovery is usually not possible."
            ],
            "observed": observed,
        }

    if issue_type == "sent_to_contract" and destination_kind.get("address_type") == "executable":
        return {
            "status": "found",
            "reason_code": "DESTINATION_IS_PROGRAM",
            "summary": "The destination resolves to an executable Solana program account. Recovery depends on program design and operator access.",
            "recoverability": "manual_review",
            "next_actions": [
                "Identify the application or protocol that owns the program.",
                "Check whether there is a documented recovery path for mistaken deposits."
            ],
            "observed": observed,
        }

    return {
        "status": "found",
        "reason_code": "GENERAL_ONCHAIN_GUIDANCE",
        "summary": "The Solana transaction is confirmed. Recovery depends on who controls the receiving account and whether a platform can manually assist.",
        "recoverability": "depends",
        "next_actions": [
            "Confirm whether the receiving address belongs to you or to a custodial platform.",
            "Use the transaction signature as the anchor for any recovery conversation."
        ],
        "observed": observed,
    }


def extract_solana_destination(instructions: List[Dict[str, Any]]) -> Optional[str]:
    for ins in instructions:
        parsed = ins.get("parsed") if isinstance(ins, dict) else None
        if not isinstance(parsed, dict):
            continue
        info = parsed.get("info") or {}
        for key in ("destination", "account", "to", "newAccount"):
            if info.get(key):
                return info.get(key)
    return None


def hex_to_int(value: Any) -> Optional[int]:
    if value in (None, "", "0x"):
        return 0 if value == "0x" else None
    try:
        return int(str(value), 16)
    except Exception:
        return None


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
