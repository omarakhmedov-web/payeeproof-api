from types import SimpleNamespace


def test_pilot_request_crm_bridge_success(client, app_module, monkeypatch):
    app_module.RESEND_API_KEY = ""
    app_module.RESEND_FROM = ""
    app_module.RESEND_TO = ""
    app_module.CRM_INTAKE_ENABLED = True
    app_module.CRM_INTAKE_URL = "https://crm.example.com/intake"
    app_module.CRM_INTAKE_SECRET = "crm_test_secret"
    app_module.CRM_INTAKE_AUTH_HEADER = "X-Test-CRM-Secret"
    app_module.CRM_INTAKE_EVENT_NAME = "pilot_request_created"

    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = dict(headers or {})
        captured["json"] = dict(json or {})
        captured["timeout"] = timeout
        return SimpleNamespace(status_code=202, text='{"ok":true}')

    monkeypatch.setattr(app_module.requests, "post", fake_post)

    response = client.post(
        "/pilot-request",
        json={
            "name": "Omar Example",
            "company": "Acme Pay",
            "email": "ops@acmepay.com",
            "volume": "$500k / month",
            "notes": "Need pre-send checks for ETH and Base payouts.",
            "form_started_at": "0",
        },
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["ok"] is True
    assert data["crm_notification"] == "sent"
    assert captured["url"] == "https://crm.example.com/intake"
    assert captured["headers"]["X-Test-CRM-Secret"] == "crm_test_secret"
    assert captured["json"]["event"] == "pilot_request_created"
    assert captured["json"]["request"]["company"] == "Acme Pay"
    assert captured["json"]["request"]["email"] == "ops@acmepay.com"

    conn = app_module.get_db()
    try:
        row = app_module.db_fetchone(
            conn,
            "SELECT crm_status, crm_delivery_id, crm_response_code FROM pilot_requests WHERE request_id = ? LIMIT 1",
            (data["request_id"],),
        )
    finally:
        conn.close()

    assert row["crm_status"] == "sent"
    assert row["crm_delivery_id"] == data["request_id"]
    assert int(row["crm_response_code"]) == 202


def test_pilot_request_crm_bridge_failure_does_not_block_intake(client, app_module, monkeypatch):
    app_module.RESEND_API_KEY = ""
    app_module.RESEND_FROM = ""
    app_module.RESEND_TO = ""
    app_module.CRM_INTAKE_ENABLED = True
    app_module.CRM_INTAKE_URL = "https://crm.example.com/intake"
    app_module.CRM_INTAKE_SECRET = "crm_test_secret"

    def fake_post(url, headers=None, json=None, timeout=None):
        return SimpleNamespace(status_code=500, text='upstream error')

    monkeypatch.setattr(app_module.requests, "post", fake_post)

    response = client.post(
        "/pilot-request",
        json={
            "name": "Omar Example",
            "company": "Acme Pay",
            "email": "ops@acmepay.com",
            "volume": "$500k / month",
            "notes": "Need pre-send checks for ETH and Base payouts.",
            "form_started_at": "0",
        },
    )
    data = response.get_json()

    assert response.status_code == 200
    assert data["ok"] is True
    assert data["crm_notification"] == "failed"

    conn = app_module.get_db()
    try:
        row = app_module.db_fetchone(
            conn,
            "SELECT crm_status, crm_response_code, crm_error FROM pilot_requests WHERE request_id = ? LIMIT 1",
            (data["request_id"],),
        )
    finally:
        conn.close()

    assert row["crm_status"] == "failed"
    assert int(row["crm_response_code"]) == 500
    assert "CRM_HTTP_500" in str(row["crm_error"])
