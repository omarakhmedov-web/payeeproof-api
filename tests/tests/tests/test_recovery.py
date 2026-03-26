def test_recovery_requires_tx_hash(client, api_headers):
    response = client.post('/api/recovery-copilot', json={'network': 'ethereum'}, headers=api_headers)

    assert response.status_code == 400
    assert response.get_json()['error'] == 'tx_hash is required.'


def test_recovery_rejects_unsupported_chain(client, api_headers):
    payload = {
        'network': 'tron',
        'tx_hash': '0x' + '1' * 64,
    }
    response = client.post('/api/recovery-copilot', json=payload, headers=api_headers)

    assert response.status_code == 400
    assert response.get_json()['error'] == 'Unsupported chain: tron'


def test_recovery_missing_memo_builds_support_guidance(client, api_headers, app_module, monkeypatch):
    monkeypatch.setattr(
        app_module,
        'analyze_transaction_on_chain',
        lambda chain, tx_hash, issue_type, intended_address, intended_chain: {
            'status': 'found',
            'reason_code': 'GENERAL_ONCHAIN_GUIDANCE',
            'recoverability': 'depends',
            'summary': 'stubbed transaction for regression tests',
            'observed': {
                'chain': chain,
                'tx_status': 'confirmed',
                'destination': '0x2222222222222222222222222222222222222222',
                'destination_type': 'contract_or_app',
            },
        },
    )

    payload = {
        'network': 'ethereum',
        'tx_hash': '0x' + '2' * 64,
        'issue_type': 'missing_memo',
    }
    response = client.post('/api/recovery-copilot', json=payload, headers=api_headers)
    body = response.get_json()

    assert response.status_code == 200
    assert body['recovery_verdict'] == 'Depends on platform or operator'
    assert body['contact_target'] == 'Destination platform support'
    assert body['best_next_step'] == 'Open a support ticket with the destination platform and include the full transaction record.'
    assert body['support_packet']['issue_type'] == 'missing_memo'
