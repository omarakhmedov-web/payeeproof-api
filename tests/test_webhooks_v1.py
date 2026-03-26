import json


def _wallet_stub(chain, address):
    return {
        'chain': chain,
        'address_type': 'personal_wallet',
        'rpc_used': False,
        'details': 'stubbed for webhook tests',
    }


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=''):
        self.status_code = status_code
        self._payload = payload
        self.text = text if text else (json.dumps(payload) if payload is not None else '')

    def json(self):
        if self._payload is None:
            raise ValueError('no json payload')
        return self._payload


def test_successful_webhook_delivery_marks_acknowledged(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)

    api_key_record = app_module.API_KEYS['pp_test_suite_key']
    api_key_record['webhook_active'] = True
    api_key_record['webhook_url'] = 'https://receiver.example.test/hook'
    api_key_record['webhook_secret'] = 'receiver-secret'
    api_key_record['webhook_events'] = ['preflight_run']

    captured = {}

    def _fake_post(url, data=None, headers=None, timeout=None):
        captured['url'] = url
        captured['data'] = data.decode('utf-8') if isinstance(data, (bytes, bytearray)) else data
        captured['headers'] = headers or {}
        captured['timeout'] = timeout
        return _FakeResponse(status_code=200, payload={'ok': True})

    monkeypatch.setattr(app_module.requests, 'post', _fake_post)

    payload = {
        'expected': {
            'network': 'ethereum',
            'asset': 'USDT',
            'address': '0x1111111111111111111111111111111111111111',
        },
        'provided': {
            'network': 'ethereum',
            'asset': 'USDT',
            'address': '0x1111111111111111111111111111111111111111',
        },
    }

    response = client.post('/api/preflight-check', json=payload, headers=api_headers)
    body = response.get_json()
    record_id = body['record_id']

    details_response = client.get(f'/api/verification-records/{record_id}', headers=api_headers)
    details_body = details_response.get_json()
    deliveries = details_body['record']['webhook_deliveries']

    assert response.status_code == 200
    assert details_response.status_code == 200
    assert captured['url'] == 'https://receiver.example.test/hook'
    assert captured['headers']['X-PayeeProof-Event'] == 'preflight_run'
    assert deliveries[0]['delivery_status'] == 'delivered'
    assert deliveries[0]['ack_status'] == 'acknowledged'
    assert deliveries[0]['acknowledged_at']

    usage_response = client.get('/api/usage-summary?period=this_month', headers=api_headers)
    usage_body = usage_response.get_json()
    assert usage_response.status_code == 200
    assert usage_body['totals']['webhook_deliveries'] == 1
    assert usage_body['totals']['webhook_delivered'] == 1
    assert usage_body['totals']['webhook_acknowledged'] == 1
    assert usage_body['totals']['webhook_pending'] == 0
