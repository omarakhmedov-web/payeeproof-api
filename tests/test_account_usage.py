def _wallet_stub(chain, address):
    return {
        'chain': chain,
        'address_type': 'personal_wallet',
        'rpc_used': False,
        'details': 'stubbed for usage/account tests',
    }


def test_account_endpoint_returns_client_metadata(client, api_headers):
    response = client.get('/api/account', headers=api_headers)
    body = response.get_json()

    assert response.status_code == 200
    assert body['client']['client_label'] == 'ci-suite'
    assert body['client']['environment'] == 'live'
    assert body['client']['plan'] == 'pilot'
    assert 'records' in body['client']['scopes']
    assert body['usage']['billable_checks'] == 0


def test_usage_summary_counts_checks(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(app_module, 'classify_address', _wallet_stub)
    monkeypatch.setattr(
        app_module,
        'analyze_transaction_on_chain',
        lambda chain, tx_hash, issue_type, intended_address, intended_chain: {
            'status': 'found',
            'reason_code': 'GENERAL_ONCHAIN_GUIDANCE',
            'recoverability': 'depends',
            'summary': 'stubbed transaction for usage summary tests',
            'observed': {
                'chain': chain,
                'tx_status': 'confirmed',
                'destination': '0x2222222222222222222222222222222222222222',
                'destination_type': 'contract_or_app',
            },
        },
    )

    preflight_payload = {
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
    recovery_payload = {
        'network': 'ethereum',
        'tx_hash': '0x' + '3' * 64,
        'issue_type': 'missing_memo',
    }

    preflight_response = client.post('/api/preflight-check', json=preflight_payload, headers=api_headers)
    recovery_response = client.post('/api/recovery-copilot', json=recovery_payload, headers=api_headers)
    summary_response = client.get('/api/usage-summary?period=this_month', headers=api_headers)
    body = summary_response.get_json()

    assert preflight_response.status_code == 200
    assert recovery_response.status_code == 200
    assert summary_response.status_code == 200
    assert body['totals']['checks'] == 2
    assert body['by_event']['preflight_run'] >= 1
    assert body['by_event']['recovery_run'] >= 1
    assert body['by_service']['preflight-check'] == 1
    assert body['by_service']['recovery-copilot'] == 1
    assert body['by_network']['ethereum'] >= 2
