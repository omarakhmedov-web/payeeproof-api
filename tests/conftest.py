import importlib.util
import json
import os
import pathlib
import sys
import threading

import pytest


@pytest.fixture(scope='session')
def app_module(tmp_path_factory):
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    db_dir = tmp_path_factory.mktemp('db')
    db_path = db_dir / 'payeeproof_test.db'

    os.environ['DB_PATH'] = str(db_path)
    os.environ['API_KEYS_JSON'] = json.dumps([
        {
            'key': 'pp_test_suite_key',
            'name': 'ci-suite',
            'client_label': 'ci-suite',
            'tenant_id': 'tenant-ci',
            'environment': 'live',
            'role': 'client',
            'plan': 'pilot',
            'scopes': ['preflight', 'recovery', 'records'],
            'webhook_active': False,
        },
        {
            'key': 'pp_test_suite_key_test',
            'name': 'ci-suite test',
            'client_label': 'ci-suite-test',
            'tenant_id': 'tenant-ci',
            'environment': 'test',
            'role': 'viewer',
            'plan': 'pilot',
            'scopes': ['preflight', 'recovery', 'records'],
            'webhook_active': False,
        }
    ])

    module_name = 'payeeproof_app_under_test'
    if module_name in sys.modules:
        return sys.modules[module_name]

    original_start = threading.Thread.start
    threading.Thread.start = lambda self: None
    try:
        spec = importlib.util.spec_from_file_location(module_name, repo_root / 'app.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        assert spec.loader is not None
        spec.loader.exec_module(module)
    finally:
        threading.Thread.start = original_start

    return module


@pytest.fixture()
def client(app_module):
    app_module.RATE_LIMIT_BUCKETS.clear()
    conn = app_module.get_db()
    try:
        for table_name in [
            'webhook_deliveries',
            'verification_records',
            'usage_events',
            'api_access_log',
            'event_log',
            'pilot_requests',
            'tenant_api_keys',
            'tenants',
        ]:
            app_module.db_execute(conn, f'DELETE FROM {table_name}')
        conn.commit()
    finally:
        conn.close()
    app_module.upsert_tenant_registry_from_api_keys()
    return app_module.app.test_client()


@pytest.fixture()
def api_headers():
    return {'X-API-Key': 'pp_test_suite_key'}


@pytest.fixture()
def api_headers_test():
    return {'X-API-Key': 'pp_test_suite_key_test'}
