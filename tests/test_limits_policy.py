import copy



import pytest


@pytest.fixture(autouse=True)
def _reset_key_state(app_module):
    original_live = copy.deepcopy(app_module.API_KEYS['pp_test_suite_key'])
    original_test = copy.deepcopy(app_module.API_KEYS['pp_test_suite_key_test'])
    yield
    app_module.API_KEYS['pp_test_suite_key'].clear()
    app_module.API_KEYS['pp_test_suite_key'].update(original_live)
    app_module.API_KEYS['pp_test_suite_key_test'].clear()
    app_module.API_KEYS['pp_test_suite_key_test'].update(original_test)

def _wallet_stub(chain, address):
    return {
        'chain': chain,
        'address_type': 'personal_wallet',
        'rpc_used': False,
        'details': 'stubbed for limits/policy tests',
    }


def _preflight_payload(network='ethereum', asset='USDT', address='0x1111111111111111111111111111111111111111'):
    return {
        'expected': {
            'network': network,
            'asset': asset,
            'address': address,
        },
        'provided': {
            'network': network,
            'asset': asset,
            'address': address,
        },
    }


def test_monthly_check_quota_blocks_second_request(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)
    app_module.API_KEYS['pp_test_suite_key']['limits'] = {'monthly_checks': 1}

    first = client.post('/api/preflight-check', json=_preflight_payload(), headers=api_headers)
    second = client.post('/api/preflight-check', json=_preflight_payload(), headers=api_headers)
    account = client.get('/api/account', headers=api_headers)

    assert first.status_code == 200
    assert second.status_code == 429
    second_body = second.get_json()
    account_body = account.get_json()

    assert second_body['error'] == 'QUOTA_EXCEEDED'
    assert second_body['details']['counter'] == 'monthly_checks'
    assert second_body['details']['remaining'] == 0
    assert account.status_code == 200
    assert account_body['limits']['items']['monthly_checks']['limit'] == 1
    assert account_body['limits']['items']['monthly_checks']['used'] == 1
    assert account_body['limits']['items']['monthly_checks']['remaining'] == 0
    assert account_body['limits']['items']['monthly_checks']['exceeded'] is True



def test_policy_origin_allowlist_blocks_unapproved_origin(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)
    app_module.API_KEYS['pp_test_suite_key']['policy'] = {
        'allowed_origin_hosts': ['allowed.client.test'],
    }

    response = client.post(
        '/api/preflight-check',
        json=_preflight_payload(),
        headers={**api_headers, 'Origin': 'https://blocked.example'},
    )

    assert response.status_code == 403
    body = response.get_json()
    assert body['error'] == 'POLICY_ORIGIN_NOT_ALLOWED'
    assert 'allowed_origin_hosts' in body['details']



def test_policy_network_allowlist_blocks_disallowed_network(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)
    app_module.API_KEYS['pp_test_suite_key']['policy'] = {
        'allowed_networks': ['ethereum'],
    }

    payload = _preflight_payload(network='arbitrum')
    response = client.post('/api/preflight-check', json=payload, headers=api_headers)

    assert response.status_code == 403
    body = response.get_json()
    assert body['error'] == 'POLICY_NETWORK_NOT_ALLOWED'
    assert 'arbitrum' in body['details']['disallowed_networks']



def test_viewer_role_is_read_only_for_write_endpoints(client, api_headers_test, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)
    response = client.post('/api/preflight-check', json=_preflight_payload(), headers=api_headers_test)

    assert response.status_code == 403
    body = response.get_json()
    assert body['error'] == 'ROLE_READ_ONLY'
    assert body['details']['role'] == 'viewer'
