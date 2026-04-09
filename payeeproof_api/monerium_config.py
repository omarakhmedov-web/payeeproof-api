from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Set

MONERIUM_CHAIN_REGISTRY: Dict[str, Dict[str, str]] = {
    "ethereum": {
        "label": "Ethereum",
        "wallet_chain_id_hex": "0x1",
        "sandbox_partner": "sepolia",
    },
    "sepolia": {
        "label": "Sepolia",
        "wallet_chain_id_hex": "0xaa36a7",
        "sandbox_partner": "ethereum",
    },
    "arbitrum": {
        "label": "Arbitrum",
        "wallet_chain_id_hex": "0xa4b1",
        "sandbox_partner": "arbitrum sepolia",
    },
    "arbitrum sepolia": {
        "label": "Arbitrum Sepolia",
        "wallet_chain_id_hex": "0x66eee",
        "sandbox_partner": "arbitrum",
    },
    "base": {
        "label": "Base",
        "wallet_chain_id_hex": "0x2105",
        "sandbox_partner": "base sepolia",
    },
    "base sepolia": {
        "label": "Base Sepolia",
        "wallet_chain_id_hex": "0x14a34",
        "sandbox_partner": "base",
    },
    "polygon": {
        "label": "Polygon",
        "wallet_chain_id_hex": "0x89",
        "sandbox_partner": "amoy",
    },
    "amoy": {
        "label": "Polygon Amoy",
        "wallet_chain_id_hex": "0x13882",
        "sandbox_partner": "polygon",
    },
    "chiado": {
        "label": "Gnosis Chiado",
        "wallet_chain_id_hex": "0x27d8",
        "sandbox_partner": "chiado",
    },
}

MONERIUM_CHAIN_ALIASES: Dict[str, str] = {
    "eth": "ethereum",
    "ethereum": "ethereum",
    "mainnet": "ethereum",
    "ethereum mainnet": "ethereum",
    "eth mainnet": "ethereum",
    "sepolia": "sepolia",
    "ethereum sepolia": "sepolia",
    "eth sepolia": "sepolia",
    "arb": "arbitrum",
    "arbitrum": "arbitrum",
    "arbitrum sepolia": "arbitrum sepolia",
    "arbitrum-sepolia": "arbitrum sepolia",
    "arbitrum_sepolia": "arbitrum sepolia",
    "arbitrumsepolia": "arbitrum sepolia",
    "base": "base",
    "base sepolia": "base sepolia",
    "base-sepolia": "base sepolia",
    "base_sepolia": "base sepolia",
    "basesepolia": "base sepolia",
    "polygon": "polygon",
    "matic": "polygon",
    "amoy": "amoy",
    "polygon amoy": "amoy",
    "polygon-amoy": "amoy",
    "polygon_amoy": "amoy",
    "polygonamoy": "amoy",
    "chiado": "chiado",
    "gnosis chiado": "chiado",
}

DEFAULT_ALLOWED_CHAINS: Set[str] = set(MONERIUM_CHAIN_REGISTRY)


def normalize_monerium_chain(
    value: Any,
    *,
    default_chain: str = "ethereum",
    allowed_chains: Optional[Iterable[str]] = None,
    aliases: Optional[Dict[str, str]] = None,
) -> str:
    alias_map = aliases or MONERIUM_CHAIN_ALIASES
    allowed = set(allowed_chains or DEFAULT_ALLOWED_CHAINS)
    normalized_default = str(default_chain or "ethereum").strip().lower() or "ethereum"
    candidate = str(value or normalized_default).strip().lower() or normalized_default
    normalized = alias_map.get(candidate, candidate)
    if normalized not in allowed:
        normalized = normalized_default if normalized_default in allowed else "ethereum"
    return normalized


def monerium_is_sandbox_env(api_base: Any) -> bool:
    return ".dev" in str(api_base or "").strip().lower()


def monerium_chain_variants(
    chain: Any,
    *,
    api_base: Any = "",
    default_chain: str = "ethereum",
    allowed_chains: Optional[Iterable[str]] = None,
    aliases: Optional[Dict[str, str]] = None,
) -> List[str]:
    normalized = normalize_monerium_chain(
        chain or default_chain,
        default_chain=default_chain,
        allowed_chains=allowed_chains,
        aliases=aliases,
    )
    variants: List[str] = []

    def _add(candidate: Any) -> None:
        value = normalize_monerium_chain(
            candidate,
            default_chain=default_chain,
            allowed_chains=allowed_chains,
            aliases=aliases,
        )
        if value and value not in variants:
            variants.append(value)

    _add(normalized)
    if monerium_is_sandbox_env(api_base):
        partner = str(MONERIUM_CHAIN_REGISTRY.get(normalized, {}).get("sandbox_partner") or "").strip().lower()
        if partner:
            _add(partner)
    return variants


def monerium_effective_source_chain(
    source_address_record: Dict[str, Any],
    requested_chain: Any,
    *,
    api_base: Any = "",
    default_chain: str = "ethereum",
    allowed_chains: Optional[Iterable[str]] = None,
    aliases: Optional[Dict[str, str]] = None,
) -> str:
    requested = normalize_monerium_chain(
        requested_chain,
        default_chain=default_chain,
        allowed_chains=allowed_chains,
        aliases=aliases,
    )
    requested_variants = monerium_chain_variants(
        requested,
        api_base=api_base,
        default_chain=default_chain,
        allowed_chains=allowed_chains,
        aliases=aliases,
    )
    chains = source_address_record.get("chains") if isinstance(source_address_record.get("chains"), list) else []
    normalized_chains = [
        normalize_monerium_chain(
            chain,
            default_chain=default_chain,
            allowed_chains=allowed_chains,
            aliases=aliases,
        )
        for chain in chains
        if normalize_monerium_chain(
            chain,
            default_chain=default_chain,
            allowed_chains=allowed_chains,
            aliases=aliases,
        )
    ]

    if normalized_chains:
        for candidate in normalized_chains:
            if candidate in requested_variants:
                return candidate
        return normalized_chains[0]

    item_chain = normalize_monerium_chain(
        source_address_record.get("chain"),
        default_chain=default_chain,
        allowed_chains=allowed_chains,
        aliases=aliases,
    )
    if item_chain:
        return item_chain
    return requested
