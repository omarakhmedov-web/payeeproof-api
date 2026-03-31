import json


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=''):
        self.status_code = status_code
        self._payload = payload
        self.text = text if text else (json.dumps(payload) if payload is not None else '')

    def json(self):
        if self._payload is None:
            raise ValueError('no json payload')
        return self._payload


def test_create_nowpayments_invoice_and_store_order(client, app_module, monkeypatch):
    captured = {}

    def _fake_post(url, headers=None, json=None, timeout=None):
        captured['url'] = url
        captured['headers'] = headers or {}
        captured['json'] = json or {}
        captured['timeout'] = timeout
        return _FakeResponse(
            status_code=200,
            payload={
                'id': 'inv_123456',
                'invoice_url': 'https://nowpayments.test/i/inv_123456',
                'payment_status': 'waiting',
            },
        )

    monkeypatch.setattr(app_module.requests, 'post', _fake_post)

    response = client.post('/api/payments/nowpayments/invoice', json={'sku': 'pilot_399', 'customer_email': 'buyer@example.com'})
    body = response.get_json()

    assert response.status_code == 200
    assert body['ok'] is True
    assert body['checkout_url'] == 'https://nowpayments.test/i/inv_123456'
    assert body['product']['sku'] == 'pilot_399'
    assert captured['url'] == 'https://api.nowpayments.io/v1/invoice'
    assert captured['headers']['x-api-key'] == 'np_test_key'
    assert captured['json']['price_amount'] == 399.0
    assert captured['json']['price_currency'] == 'usd'
    assert captured['json']['ipn_callback_url'].endswith('/api/payments/nowpayments/ipn')

    order_response = client.get(f"/api/payments/nowpayments/order/{body['order_id']}")
    order_body = order_response.get_json()
    assert order_response.status_code == 200
    assert order_body['order']['payment_status'] == 'waiting'
    assert order_body['order']['paid'] is False


def test_nowpayments_ipn_updates_order_status(client, app_module, monkeypatch):
    def _fake_post(url, headers=None, json=None, timeout=None):
        return _FakeResponse(
            status_code=200,
            payload={
                'id': 'inv_abc',
                'invoice_url': 'https://nowpayments.test/i/inv_abc',
                'payment_status': 'waiting',
            },
        )

    monkeypatch.setattr(app_module.requests, 'post', _fake_post)
    create_response = client.post('/api/payments/nowpayments/invoice', json={'sku': 'pilot_399'})
    order_id = create_response.get_json()['order_id']

    ipn_payload = {
        'order_id': order_id,
        'invoice_id': 'inv_abc',
        'payment_id': 'pay_987',
        'payment_status': 'finished',
        'pay_currency': 'usdttrc20',
        'pay_amount': '399',
        'actually_paid': '399',
        'actually_paid_at_fiat': '399',
    }
    signature = app_module.nowpayments_ipn_signature(ipn_payload)
    ipn_response = client.post('/api/payments/nowpayments/ipn', json=ipn_payload, headers={'x-nowpayments-sig': signature})
    ipn_body = ipn_response.get_json()

    assert ipn_response.status_code == 200
    assert ipn_body['paid'] is True
    assert ipn_body['payment_status'] == 'finished'

    order_response = client.get(f'/api/payments/nowpayments/order/{order_id}')
    order_body = order_response.get_json()
    assert order_response.status_code == 200
    assert order_body['order']['paid'] is True
    assert order_body['order']['payment_status'] == 'finished'
    assert order_body['order']['fulfillment_status'] == 'ready_for_manual_fulfillment'



def test_nowpayments_ipn_rejects_bad_signature(client):
    payload = {'order_id': 'ppo_test', 'payment_status': 'finished'}
    response = client.post('/api/payments/nowpayments/ipn', json=payload, headers={'x-nowpayments-sig': 'bad'})
    body = response.get_json()

    assert response.status_code == 403
    assert body['error'] == 'NOWPAYMENTS_SIG_INVALID'
