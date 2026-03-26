def _wallet_stub(chain, address):
    return {
        'chain': chain,
        'address_type': 'personal_wallet',
        'rpc_used': False,
        'details': 'stubbed for tenant tests',
    }


def test_account_endpoint_exposes_tenant_summary(client, api_headers):
    response = client.get('/api/account', headers=api_headers)
    body = response.get_json()

    assert response.status_code == 200
    assert body['tenant']['tenant_id'] == 'tenant-ci'
    assert body['tenant']['current_environment'] == 'live'
    assert set(body['tenant']['environments']) == {'live', 'test'}
    assert body['tenant']['keys_total'] == 2
    assert body['current_key']['environment'] == 'live'
    assert body['current_key']['role'] == 'client'



def test_usage_and_records_are_isolated_by_environment(client, api_headers, api_headers_test, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)
    monkeypatch.setattr(
        app_module,
        'analyze_transaction_on_chain',
        lambda chain, tx_hash, issue_type, intended_address, intended_chain: {
            'status': 'found',
            'reason_code': 'GENERAL_ONCHAIN_GUIDANCE',
            'recoverability': 'depends',
            'summary': 'stubbed transaction for tenant scope tests',
            'observed': {
                'chain': chain,
                'tx_status': 'confirmed',
                'destination': '0x2222222222222222222222222222222222222222',
                'destination_type': 'contract_or_app',
            },
        },
    )

    preflight_payload = {
        'context': {'reference_id': 'tenant-scope-live-001'},
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

    live_preflight = client.post('/api/preflight-check', json=preflight_payload, headers=api_headers)
    live_records = client.get('/api/verification-records?reference_id=tenant-scope-live-001', headers=api_headers)
    test_records = client.get('/api/verification-records?reference_id=tenant-scope-live-001', headers=api_headers_test)
    live_usage = client.get('/api/usage-summary?period=this_month', headers=api_headers)
    test_usage = client.get('/api/usage-summary?period=this_month', headers=api_headers_test)

    assert live_preflight.status_code == 200
    assert live_records.status_code == 200
    assert test_records.status_code == 200
    assert live_usage.status_code == 200
    assert test_usage.status_code == 200

    live_body = live_records.get_json()
    test_body = test_records.get_json()
    live_usage_body = live_usage.get_json()
    test_usage_body = test_usage.get_json()

    assert live_body['total'] == 1
    assert test_body['total'] == 0
    assert live_usage_body['totals']['checks'] >= 1
    assert test_usage_body['totals']['checks'] == 0
    assert live_usage_body['environment'] == 'live'
    assert test_usage_body['environment'] == 'test'
