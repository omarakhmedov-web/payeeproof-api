from pathlib import Path

from payeeproof_api.monerium_config import monerium_chain_variants, normalize_monerium_chain


APP_PY = Path(__file__).resolve().parents[1] / 'app.py'


def load_select_account(mon_api_base: str):
    text = APP_PY.read_text()
    marker = 'def monerium_select_account(profile_payload: Dict[str, Any], *, account_id: str = "", chain: str = "", currency: str = "eur") -> Dict[str, Any]:'
    start = text.index(marker)
    end = text.index('\n\ndef monerium_fetch_addresses', start)
    fn_src = text[start:end]
    namespace = {
        'Dict': dict,
        'Any': object,
        'MONERIUM_DEFAULT_CHAIN': 'ethereum',
        'normalize_text': lambda value, _limit=120: str(value or '').strip(),
        'normalize_monerium_chain': normalize_monerium_chain,
        'MONERIUM_API_BASE': mon_api_base,
    }

    def chain_variants(chain):
        return monerium_chain_variants(chain, api_base=namespace['MONERIUM_API_BASE'])

    namespace['monerium_chain_variants'] = chain_variants
    exec(fn_src, namespace)
    return namespace['monerium_select_account']



def test_monerium_select_account_matches_arbitrum_sandbox_variant():
    select_account = load_select_account('https://api.monerium.dev')
    profile_payload = {
        'accounts': [
            {'id': 'acc_eth', 'chain': 'ethereum', 'currency': 'eur', 'address': '0xeth'},
            {'id': 'acc_arb_sep', 'chain': 'arbitrum sepolia', 'currency': 'eur', 'address': '0xarb'},
        ]
    }

    selected = select_account(profile_payload, chain='arbitrum', currency='eur')

    assert selected['id'] == 'acc_arb_sep'
    assert selected['chain'] == 'arbitrum sepolia'



def test_monerium_select_account_keeps_exact_match_outside_sandbox():
    select_account = load_select_account('https://api.monerium.com')
    profile_payload = {
        'accounts': [
            {'id': 'acc_arb_sep', 'chain': 'arbitrum sepolia', 'currency': 'eur', 'address': '0xarbsep'},
            {'id': 'acc_arb', 'chain': 'arbitrum', 'currency': 'eur', 'address': '0xarb'},
        ]
    }

    selected = select_account(profile_payload, chain='arbitrum', currency='eur')

    assert selected['id'] == 'acc_arb'
    assert selected['chain'] == 'arbitrum'
