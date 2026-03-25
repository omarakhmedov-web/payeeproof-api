import hashlib
import html
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

APP_VERSION = "1.2.0-recovery-packet"
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
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com", "msn.com",
    "yahoo.com", "ymail.com", "rocketmail.com", "icloud.com", "me.com", "mac.com",
    "proton.me", "protonmail.com", "pm.me", "mail.com", "aol.com", "gmx.com",
    "yandex.ru", "yandex.com", "ya.ru", "bk.ru", "inbox.ru", "list.ru", "mail.ru"
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

DB_PATH = os.getenv("DB_PATH", "/tmp/payeeproof.db")
RPC_TIMEOUT = float(os.getenv("RPC_TIMEOUT_SEC", os.getenv("REQUEST_TIMEOUT_SEC", "3.5")))
EMAIL_TIMEOUT = float(os.getenv("EMAIL_TIMEOUT_SEC", "10"))
ADDRESS_CACHE_TTL_SEC = int(os.getenv("ADDRESS_CACHE_TTL_SEC", "600"))
_ADDRESS_CLASSIFY_CACHE: Dict[str, Dict[str, Any]] = {}
RESEND_API_BASE = os.getenv("RESEND_API_BASE", "https://api.resend.com").rstrip("/")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
RESEND_FROM = os.getenv("RESEND_FROM", "PayeeProof <alerts@notify.payeeproof.com>").strip()
RESEND_TO = os.getenv("RESEND_TO", "hello@payeeproof.com").strip()



def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_chain(value: str) -> str:
    raw = str(value or "").strip().lower()
    return CHAIN_ALIASES.get(raw, raw)


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
    }
    return labels.get(str(action or "").upper(), str(action or "").replace("_", " ").title() or "Review required")


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
        "verdict": verdict_label,
        "outcome": outcome,
        "confidence": confidence,
        "chain": chain,
        "tx_hash": tx_hash,
        "issue_type": issue_type or "unknown",
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
        f"Issue type: {str(packet.get('issue_type') or 'unknown').replace('_', ' ').title()}",
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


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def ensure_db() -> None:
    conn = get_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pilot_requests (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              name TEXT NOT NULL,
              company TEXT NOT NULL,
              email TEXT NOT NULL,
              volume TEXT,
              notes TEXT NOT NULL,
              source_ip TEXT,
              user_agent TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


ensure_db()


@dataclass
class RpcResult:
    ok: bool
    result: Any = None
    error: Optional[str] = None


class ApiError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


@app.errorhandler(ApiError)
def handle_api_error(err: ApiError):
    return jsonify({"ok": False, "error": err.message}), err.status_code


@app.errorhandler(Exception)
def handle_unexpected_error(err: Exception):
    return jsonify({"ok": False, "error": f"Unexpected server error: {type(err).__name__}"}), 500


@app.get("/health")
def health():
    configured = {k: bool(v) for k, v in RPC_URLS.items()}
    live_networks = [k for k, v in configured.items() if v]
    return jsonify({
        "ok": True,
        "service": "payeeproof-api",
        "version": APP_VERSION,
        "time": utc_now_iso(),
        "rpc_configured": configured,
        "configured_networks": live_networks,
        "api_status": "live",
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
            "/pilot-request",
        ]
    })


@app.post("/api/preflight-check")
def preflight_check():
    payload = request.get_json(silent=True) or {}
    expected = payload.get("expected") or {}
    provided = payload.get("provided") or {}

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
    )

    checked_at = utc_now_iso()
    return jsonify({
        "ok": True,
        "service": "preflight-check",
        "version": APP_VERSION,
        "checked_at": checked_at,
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
            "verdict": outcome["verdict"],
            "confidence": outcome["confidence"],
            "reason_code": outcome["reason_code"],
            "next_action": outcome["next_action"],
            "next_action_label": preflight_next_step_label(outcome["next_action"]),
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
    })


@app.post("/api/recovery-copilot")
def recovery_copilot():
    payload = request.get_json(silent=True) or {}
    chain = normalize_chain(payload.get("network") or payload.get("chain"))
    tx_hash = str(payload.get("tx_hash") or payload.get("hash") or "").strip()
    issue_type = str(payload.get("issue_type") or "unknown").strip().lower()
    intended_address = str(payload.get("intended_address") or "").strip()
    intended_chain = normalize_chain(payload.get("intended_chain") or "") if payload.get("intended_chain") else ""

    if not chain:
        raise ApiError("chain is required.")
    if not tx_hash:
        raise ApiError("tx_hash is required.")

    if chain in EVM_CHAINS:
        analysis = analyze_evm_transaction(chain, tx_hash, issue_type, intended_address, intended_chain)
    elif chain == "solana":
        analysis = analyze_solana_transaction(tx_hash, issue_type, intended_address, intended_chain)
    else:
        raise ApiError(f"Unsupported chain: {chain}")

    observed = analysis.get("observed") or {}
    destination_profile = build_destination_profile(chain, str(observed.get("destination") or ""), {
        "address_type": observed.get("destination_type") or "unknown",
        "rpc_used": bool(observed.get("destination") and analysis.get("status") not in {"unavailable", "not_found"}),
    })
    outcome = derive_recovery_outcome(analysis.get("status"), observed.get("tx_status"))
    confidence = derive_recovery_confidence(analysis.get("status"), observed.get("tx_status"))
    why = build_recovery_explanation(analysis, destination_profile)
    checked_at = utc_now_iso()
    support_packet = build_recovery_support_packet(
        chain=chain,
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

    return jsonify({
        "ok": True,
        "service": "recovery-copilot",
        "version": APP_VERSION,
        "checked_at": checked_at,
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
            "outcome": outcome,
            "confidence": confidence,
            "reason_code": analysis.get("reason_code"),
        },
        **analysis,
    })


@app.post("/pilot-request")
def pilot_request():
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name") or "").strip()
    company = str(payload.get("company") or "").strip()
    email = str(payload.get("email") or "").strip().lower()
    volume = str(payload.get("volume") or "").strip()
    notes = str(payload.get("notes") or payload.get("use_case") or "").strip()
    source_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    user_agent = request.headers.get("User-Agent", "")
    origin = request.headers.get("Origin", "")

    if not all([name, company, email, notes]):
        raise ApiError("name, company, email and notes are required.")
    if not is_valid_email(email):
        raise ApiError("Please enter a valid work email address.")
    if is_personal_email(email):
        raise ApiError("Please use your work email. Personal email domains are not accepted.")

    created_at = utc_now_iso()
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO pilot_requests(created_at, name, company, email, volume, notes, source_ip, user_agent) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                created_at,
                name,
                company,
                email,
                volume,
                notes,
                source_ip,
                user_agent,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    email_result = send_pilot_notification({
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

    if email_result.get("status") == "sent":
        return jsonify({
            "ok": True,
            "message": "Your request has been sent successfully.",
            "stored": True,
            "email_notification": "sent",
            "email_id": email_result.get("email_id"),
            "submitted_at": created_at,
        })

    if email_result.get("status") == "not_configured":
        return jsonify({
            "ok": False,
            "error": "PILOT_EMAIL_NOT_CONFIGURED",
            "message": "Pilot request email is not configured on the server yet.",
            "stored": True,
            "email_notification": "not_configured",
            "submitted_at": created_at,
        }), 500

    return jsonify({
        "ok": False,
        "error": "EMAIL_DELIVERY_FAILED",
        "message": "Could not send your request right now. Please try again in a moment.",
        "stored": True,
        "email_notification": "failed",
        "submitted_at": created_at,
        "debug_detail": email_result.get("debug_detail"),
    }), 502


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

    subject = f"New PayeeProof pilot request — {payload['company']}"
    notes_html = html.escape(payload["notes"]).replace("\n", "<br>")
    text_body = "\n".join([
        "New PayeeProof pilot request",
        "",
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
      <p><strong>Submitted at:</strong> {html.escape(payload['created_at'])}<br>
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
