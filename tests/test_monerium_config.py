from payeeproof_api.monerium_config import (
    monerium_chain_variants,
    monerium_effective_source_chain,
    normalize_monerium_chain,
)


def test_normalize_monerium_chain_uses_alias_and_fallback():
    assert normalize_monerium_chain("eth") == "ethereum"
    assert normalize_monerium_chain("matic") == "polygon"
    assert normalize_monerium_chain("unknown", default_chain="base") == "base"



def test_monerium_chain_variants_expand_only_in_dev_sandbox():
    assert monerium_chain_variants("ethereum", api_base="https://api.monerium.dev") == ["ethereum", "sepolia"]
    assert monerium_chain_variants("polygon", api_base="https://api.monerium.dev") == ["polygon", "amoy"]
    assert monerium_chain_variants("ethereum", api_base="https://api.monerium.com") == ["ethereum"]



def test_effective_source_chain_prefers_precise_linked_variant():
    source = {"chain": "ethereum", "chains": ["sepolia"]}
    resolved = monerium_effective_source_chain(source, "ethereum", api_base="https://api.monerium.dev")
    assert resolved == "sepolia"
