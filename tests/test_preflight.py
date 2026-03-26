def _wallet_stub(chain, address):
    return {
        'chain': chain,
        'address_type': 'personal_wallet',
        'rpc_used': False,
        'details': 'stubbed for regression tests',
    }


def test_preflight_requires_api_key(client):
    response = client.post('/api/preflight-check', json={})

    assert response.status_code == 401
    assert response.get_json()['error'] == 'API key required for direct API access.'


def test_preflight_network_mismatch_returns_block(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)

    payload = {
        'expected': {
            'network': 'ethereum',
            'asset': 'USDT',
            'address': '0x1111111111111111111111111111111111111111',
        },
        'provided': {
            'network': 'arbitrum',
            'asset': 'USDT',
            'address': '0x1111111111111111111111111111111111111111',
        },
    }
    response = client.post('/api/preflight-check', json=payload, headers=api_headers)
    body = response.get_json()

    assert response.status_code == 200
    assert body['reason_code'] == 'NETWORK_MISMATCH'
    assert body['verdict'] == 'BLOCK'
    assert body['next_action'] == 'BLOCK_AND_REVERIFY'
    assert 'NETWORK_MISMATCH' in body['risk_flags']


def test_preflight_zero_address_returns_do_not_send(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)

    payload = {
        'expected': {
            'network': 'ethereum',
            'asset': 'USDT',
            'address': '0x1111111111111111111111111111111111111111',
        },
        'provided': {
            'network': 'ethereum',
            'asset': 'USDT',
            'address': '0x0000000000000000000000000000000000000000',
        },
    }
    response = client.post('/api/preflight-check', json=payload, headers=api_headers)
    body = response.get_json()

    assert response.status_code == 200
    assert body['reason_code'] == 'ZERO_ADDRESS'
    assert body['verdict'] == 'BLOCK'
    assert body['next_action'] == 'DO_NOT_SEND'
    assert 'ZERO_ADDRESS' in body['risk_flags']


def test_preflight_unsupported_network_is_blocked(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)

    payload = {
        'expected': {
            'network': 'tron',
            'asset': 'USDT',
            'address': 'TXYZ',
        },
        'provided': {
            'network': 'tron',
            'asset': 'USDT',
            'address': 'TXYZ',
        },
    }
    response = client.post('/api/preflight-check', json=payload, headers=api_headers)
    body = response.get_json()

    assert response.status_code == 200
    assert body['reason_code'] == 'UNSUPPORTED_NETWORK'
    assert body['verdict'] == 'BLOCK'
    assert body['next_action'] == 'BLOCK_AND_REVERIFY'
    assert body['supported_scope']['provided_network_supported'] is False


def test_preflight_missing_memo_stays_blocked(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)

    payload = {
        'expected': {
            'network': 'solana',
            'asset': 'USDT',
            'address': '4Nd1m8H2Y9F4iRciMghvYDn8ApX2HFqRSbVSSMzdg3vT',
            'memo': '12345',
        },
        'provided': {
            'network': 'solana',
            'asset': 'USDT',
            'address': '4Nd1m8H2Y9F4iRciMghvYDn8ApX2HFqRSbVSSMzdg3vT',
        },
    }
    response = client.post('/api/preflight-check', json=payload, headers=api_headers)
    body = response.get_json()

    assert response.status_code == 200
    assert body['reason_code'] == 'MEMO_MISMATCH'
    assert body['verdict'] == 'BLOCK'
    assert body['checks']['memo_match'] is False
    assert 'MEMO_MISMATCH' in body['risk_flags']
