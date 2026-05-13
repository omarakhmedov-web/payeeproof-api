# PayeeProof API

Backend service for **PayeeProof**.

PayeeProof is a **pre-send verification layer for stablecoin payouts**. The API is built to help payout, treasury, exchange-like deposit, and operations workflows stop wrong-network and wrong-destination mistakes **before funds move**.

## What this service does
The API evaluates a requested route and returns a compact operational result with fields such as:
- `verdict`
- `reason_code`
- `confidence`
- `destination_type`
- `next_action`
- `checked_at`
- `request_id`

The service also supports:
- verification record history,
- account and usage endpoints,
- webhook delivery for authenticated clients,
- pilot request intake.

Production-oriented Monerium endpoints require an explicit `connection_id`.
The older "use latest stored connection" behavior is disabled by default and
should only be enabled for local sandbox work with
`MONERIUM_ALLOW_LATEST_CONNECTION_FALLBACK=1`.

## Current endpoint surface
### Core
- `GET /health`
- `POST /api/preflight-check`
- `POST /api/recovery-copilot`

### Authenticated client endpoints
- `GET /api/account`
- `GET /api/usage-summary`
- `GET /api/verification-records`
- `GET /api/verification-records/<record_id>`
- `POST /api/webhooks/ack`

### Commercial intake
- `POST /pilot-request`

## Product framing
Public commercial positioning is **pre-send first**.

That means:
- the main product story is about preventing payout mistakes before funds move,
- verification records support operational review and auditability,
- recovery guidance exists as a separate secondary module.

## Policy profiles
The backend includes policy profiles that can shape the final decision outcome:
- `payout_strict`
- `deposit_review`
- `treasury_review`

## Stable verdicts
- `SAFE`
- `BLOCK`
- `REVERIFY`
- `TEST_FIRST`
- `UNAVAILABLE`

## Example request
```json
{
  "expected": {
    "network": "ethereum",
    "asset": "USDC",
    "address": "0x59d779BED4dB1E734D3fDa3172d45bc3063eCD69",
    "memo": null
  },
  "provided": {
    "network": "ethereum",
    "asset": "USDC",
    "address": "0x59d779BED4dB1E734D3fDa3172d45bc3063eCD69",
    "memo": null
  },
  "context": {
    "reference_id": "payout_102948",
    "flow_type": "payout_approval",
    "policy_profile": "payout_strict"
  }
}
```

## Example cURL
```bash
curl -X POST "https://payeeproof-api.onrender.com/api/preflight-check" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_API_KEY" \
  -d '{
    "expected": {
      "network": "ethereum",
      "asset": "USDC",
      "address": "0x59d779BED4dB1E734D3fDa3172d45bc3063eCD69"
    },
    "provided": {
      "network": "ethereum",
      "asset": "USDC",
      "address": "0x59d779BED4dB1E734D3fDa3172d45bc3063eCD69"
    },
    "context": {
      "reference_id": "payout_102948",
      "flow_type": "payout_approval",
      "policy_profile": "payout_strict"
    }
  }'
```

## Local run
```bash
pip install -r requirements.txt
cp env.example .env
python app.py
```

## Deployment
This service is intended for deployment as a Python web app.
The repository includes:
- `requirements.txt`
- `render.yaml`
- `env.example`
- `pytest.ini`

Production has been deployed on Render.

## Scope note
This repository contains backend service logic.
The public website, pricing, pilot pages, trust pages, and public contract presentation live in the separate **payeeproof** website repository.
