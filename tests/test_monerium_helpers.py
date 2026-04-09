from payeeproof_api.monerium_helpers import (
    monerium_build_counterpart_from_payload,
    monerium_build_order_submit_gate,
    monerium_order_summary,
)


def _normalize_text(value, _limit):
    return str(value or "").strip()



def _normalize_money_amount(value):
    return str(value or "").strip()



def _normalize_chain(value):
    return str(value or "").strip().lower() or "ethereum"



def _normalize_iban(value):
    return str(value or "").replace(" ", "").upper()



def test_counterpart_builder_splits_name_for_monerium():
    payload = {
        "recipient": {
            "iban": "EE10 6555 8043 0152 4255",
            "name": "Omar Ahmad",
            "country": "az",
        }
    }
    result = monerium_build_counterpart_from_payload(payload, normalize_iban=_normalize_iban)
    assert result["identifier"]["iban"] == "EE106555804301524255"
    assert result["details"]["firstName"] == "Omar"
    assert result["details"]["lastName"] == "Ahmad"
    assert result["details"]["country"] == "AZ"



def test_order_submit_gate_blocks_missing_source():
    gate = monerium_build_order_submit_gate(
        has_source=False,
        iban_ok=True,
        counterpart_ready=True,
        balance_known=True,
        balance_ok=True,
    )
    assert gate["verdict"] == "BLOCK"
    assert gate["reason_code"] == "SOURCE_ADDRESS_NOT_LINKED"



def test_order_summary_builds_phase_and_recipient_name():
    order = {
        "id": "ord_123",
        "state": "processed",
        "amount": "15",
        "currency": "eur",
        "chain": "ethereum",
        "address": "0xabc",
        "counterpart": {
            "identifier": {"iban": "EE10 6555 8043 0152 4255"},
            "details": {"firstName": "Omar", "lastName": "Ahmad", "country": "AZ"},
        },
        "meta": {"placedAt": "2026-04-09T12:00:00Z"},
    }
    summary = monerium_order_summary(
        order,
        normalize_text=_normalize_text,
        normalize_money_amount=_normalize_money_amount,
        normalize_chain=_normalize_chain,
        normalize_iban=_normalize_iban,
    )
    assert summary["phase"] == "credited"
    assert summary["phase_label"] == "Funds credited"
    assert summary["recipient_name"] == "Omar Ahmad"
