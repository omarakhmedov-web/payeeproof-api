from __future__ import annotations

import base64
import hashlib
import html
import secrets
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse


def parse_bool_flag(value: Any, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}



def monerium_pkce_verifier() -> str:
    return secrets.token_urlsafe(72)[:96]



def monerium_pkce_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(str(code_verifier or "").encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")



def build_monerium_authorize_url(
    *,
    state: str,
    code_challenge: str,
    chain: str,
    skip_kyc: bool,
    client_id: str,
    redirect_uri: str,
    api_base: str,
    include_response_type: bool,
    include_chain_in_auth_url: bool,
    normalize_chain: Callable[[Any], str],
    append_url_query: Callable[[str, Dict[str, Any]], str],
) -> str:
    params: Dict[str, Any] = {
        "client_id": client_id,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "redirect_uri": redirect_uri,
        "state": state,
    }
    if include_response_type:
        params["response_type"] = "code"
    normalized_chain = normalize_chain(chain)
    if include_chain_in_auth_url and normalized_chain:
        params["chain"] = normalized_chain
    if skip_kyc:
        params["skip_kyc"] = "true"
    return append_url_query(f"{api_base.rstrip('/')}/auth", params)



def normalize_monerium_return_to(
    value: Any,
    *,
    public_demo_hosts: Iterable[str],
    public_demo_origins: str,
    allowed_origins: Iterable[str],
    split_origin_hosts: Callable[[str], list[str]],
) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}:
        return ""
    host = (parsed.hostname or "").strip().lower()
    trusted_hosts = set(public_demo_hosts) | set(split_origin_hosts(public_demo_origins)) | set(split_origin_hosts(",".join(allowed_origins)))
    if not host or host not in trusted_hosts:
        return ""
    return raw



def monerium_response_page(
    title: str,
    message: str,
    *,
    status_code: int = 200,
    details: Optional[Dict[str, Any]] = None,
) -> Tuple[str, int, Dict[str, str]]:
    title_html = html.escape(str(title or "Monerium"))
    message_html = html.escape(str(message or ""))
    detail_html = ""
    if isinstance(details, dict) and details:
        items = []
        for key, value in details.items():
            if value in (None, "", [], {}):
                continue
            items.append(f"<li><strong>{html.escape(str(key))}:</strong> {html.escape(str(value))}</li>")
        if items:
            detail_html = '<ul style="margin-top:14px;line-height:1.6;">' + "".join(items) + "</ul>"
    page = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{title_html}</title>
  <style>
    body {{ font-family: Arial, sans-serif; background:#0F172A; color:#E5E7EB; margin:0; padding:32px; }}
    .card {{ max-width:720px; margin:48px auto; background:#111827; border:1px solid #1F2937; border-radius:16px; padding:28px; box-shadow:0 10px 30px rgba(0,0,0,.25); }}
    h1 {{ margin:0 0 12px; font-size:30px; }}
    p {{ margin:0; font-size:16px; line-height:1.6; color:#CBD5E1; }}
    code {{ background:#0B1220; padding:2px 6px; border-radius:6px; }}
    a {{ color:#60A5FA; }}
  </style>
</head>
<body>
  <div class=\"card\">
    <h1>{title_html}</h1>
    <p>{message_html}</p>
    {detail_html}
  </div>
</body>
</html>"""
    return page, status_code, {"Content-Type": "text/html; charset=utf-8"}



def monerium_order_state_text(order: Dict[str, Any]) -> str:
    return str(order.get("state") or order.get("status") or "").strip().lower()



def monerium_order_phase(order: Dict[str, Any]) -> str:
    state = monerium_order_state_text(order)
    if state in {"processed", "completed", "credited", "settled", "executed", "confirmed", "succeeded", "paid", "done"}:
        return "credited"
    if state in {"failed", "rejected", "cancelled", "canceled", "expired"}:
        return "failed"
    if state in {"pending", "created", "submitted", "queued", "processing", "in_progress", "initiated"}:
        return "processing"
    return state or "unknown"



def monerium_order_phase_label(phase: str) -> str:
    normalized = str(phase or "").strip().lower()
    if normalized == "credited":
        return "Funds credited"
    if normalized == "failed":
        return "Order failed"
    if normalized == "processing":
        return "Processing"
    return normalized.replace("_", " ").title() if normalized else "Unknown"



def monerium_order_summary(
    order: Dict[str, Any],
    *,
    normalize_text: Callable[[Any, int], str],
    normalize_money_amount: Callable[[Any], str],
    normalize_chain: Callable[[Any], str],
    normalize_iban: Callable[[Any], str],
) -> Dict[str, Any]:
    counterpart = order.get("counterpart") if isinstance(order.get("counterpart"), dict) else {}
    details = counterpart.get("details") if isinstance(counterpart.get("details"), dict) else {}
    identifier = counterpart.get("identifier") if isinstance(counterpart.get("identifier"), dict) else {}
    meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
    phase = monerium_order_phase(order)
    recipient_name = str(details.get("name") or "").strip()
    if not recipient_name:
        recipient_name = " ".join(
            part
            for part in [str(details.get("firstName") or "").strip(), str(details.get("lastName") or "").strip()]
            if part
        ).strip()
    return {
        "order_id": normalize_text(order.get("id"), 120),
        "state": monerium_order_state_text(order) or "unknown",
        "phase": phase,
        "phase_label": monerium_order_phase_label(phase),
        "amount": normalize_money_amount(order.get("amount")),
        "currency": str(order.get("currency") or "").strip().lower(),
        "chain": normalize_chain(order.get("chain")),
        "wallet_address": str(order.get("address") or "").strip(),
        "recipient_iban": normalize_iban(identifier.get("iban")),
        "recipient_name": recipient_name,
        "recipient_country": str(details.get("country") or "").strip().upper(),
        "memo": str(order.get("memo") or "").strip(),
        "kind": str(order.get("kind") or "").strip().lower(),
        "placed_at": str(meta.get("placedAt") or order.get("createdAt") or order.get("placedAt") or "").strip(),
        "updated_at": str(meta.get("updatedAt") or order.get("updatedAt") or order.get("processedAt") or order.get("completedAt") or "").strip(),
    }



def monerium_token_symbol(currency: Any) -> str:
    value = str(currency or "eur").strip().lower()
    if value == "eur":
        return "EURe"
    return value.upper()



def monerium_place_order_message_hint(
    amount: str,
    iban: str,
    *,
    normalize_money_amount: Callable[[Any], str],
    normalize_iban: Callable[[Any], str],
) -> str:
    amount_text = normalize_money_amount(amount)
    iban_text = normalize_iban(iban)
    now_hint = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%SZ")
    display_amount = amount_text[:-3] if amount_text.endswith('.00') else amount_text
    return f"Send EUR {display_amount} to {iban_text} at {now_hint}"



def monerium_build_counterpart_from_payload(
    payload: Dict[str, Any],
    *,
    normalize_iban: Callable[[Any], str],
) -> Dict[str, Any]:
    recipient = payload.get("recipient") if isinstance(payload.get("recipient"), dict) else {}
    counterpart = payload.get("counterpart") if isinstance(payload.get("counterpart"), dict) else {}
    if counterpart:
        return counterpart
    iban = normalize_iban(recipient.get("iban") or payload.get("iban"))
    details = recipient.get("details") if isinstance(recipient.get("details"), dict) else {}
    name = str(recipient.get("name") or details.get("name") or "").strip()
    first_name = str(recipient.get("first_name") or recipient.get("firstName") or details.get("firstName") or "").strip()
    last_name = str(recipient.get("last_name") or recipient.get("lastName") or details.get("lastName") or "").strip()
    country = str(recipient.get("country") or details.get("country") or "").strip().upper()

    if name and (not first_name or not last_name):
        parts = [part for part in name.split() if part]
        if len(parts) >= 2:
            if not first_name:
                first_name = parts[0]
            if not last_name:
                last_name = " ".join(parts[1:])
        elif len(parts) == 1:
            if not first_name:
                first_name = parts[0]
            if not last_name:
                last_name = parts[0]

    details_payload: Dict[str, Any] = {}
    if name:
        details_payload["name"] = name
    if first_name:
        details_payload["firstName"] = first_name
    if last_name:
        details_payload["lastName"] = last_name
    if country:
        details_payload["country"] = country
    result: Dict[str, Any] = {"identifier": {"standard": "iban", "iban": iban}}
    if details_payload:
        result["details"] = details_payload
    return result



def monerium_counterpart_details_complete(counterpart: Dict[str, Any]) -> bool:
    details = counterpart.get("details") if isinstance(counterpart.get("details"), dict) else {}
    if not isinstance(details, dict):
        return False
    if str(details.get("name") or "").strip() and str(details.get("country") or "").strip():
        return True
    if str(details.get("firstName") or "").strip() and str(details.get("lastName") or "").strip() and str(details.get("country") or "").strip():
        return True
    return False



def monerium_build_order_submit_gate(
    *,
    has_source: bool,
    iban_ok: bool,
    counterpart_ready: bool,
    balance_known: bool,
    balance_ok: bool,
) -> Dict[str, Any]:
    if not has_source:
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": "SOURCE_ADDRESS_NOT_LINKED",
            "next_action": "RECONNECT_SOURCE",
            "summary": "No linked Monerium source address was found for the selected chain.",
        }
    if not iban_ok:
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": "INVALID_IBAN",
            "next_action": "FIX_INPUT",
            "summary": "Recipient IBAN is missing or invalid.",
        }
    if not counterpart_ready:
        return {
            "status": "review_needed",
            "verdict": "REVERIFY",
            "reason_code": "COUNTERPART_DETAILS_MISSING",
            "next_action": "ADD_COUNTERPART_DETAILS",
            "summary": "Recipient name and country details are still missing for Monerium order submission.",
        }
    if balance_known and not balance_ok:
        return {
            "status": "blocked",
            "verdict": "BLOCK",
            "reason_code": "INSUFFICIENT_FUNDS",
            "next_action": "FUND_SOURCE",
            "summary": "Linked Monerium source balance looks lower than the requested amount.",
        }
    return {
        "status": "verified",
        "verdict": "SAFE",
        "reason_code": "OK",
        "next_action": "SIGN_AND_SUBMIT_TO_MONERIUM",
        "summary": "Source address, recipient details, and order inputs are ready for Monerium signature and submission.",
    }
