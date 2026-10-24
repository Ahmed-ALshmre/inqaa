"""Run the full suite using isolated storage and explicit fake provider boundaries.

Usage: python -m account_app.tests.run_offline_audit [pytest arguments]

This is an offline integration audit, not a live model or delivery acceptance
test. Assertions are unchanged. Legacy image-routing tests receive valid PNG
bytes only when they have already mocked the catalog recognizer and their URL
uses the reserved .test domain. Real image/download/transport tests retain the
original implementation. All actual network connection attempts are blocked.
"""
import base64
import hashlib
import io
import os
from pathlib import Path
import socket
import sys
import tempfile
from unittest.mock import Mock, patch
from urllib.parse import urlparse

from PIL import Image
import pytest


def fixture_image(url):
    if not (urlparse(str(url)).hostname or '').endswith('.test'):
        raise ValueError('Synthetic image bytes are restricted to reserved .test hosts')
    digest = hashlib.sha256(str(url).encode()).digest()
    image = Image.frombytes('RGB', (2, 2), digest[:12])
    output = io.BytesIO()
    image.save(output, format='PNG')
    return 'data:image/png;base64,' + base64.b64encode(output.getvalue()).decode()


class LegacyFixtureBoundaries:
    @pytest.fixture(autouse=True)
    def image_transport(self, request):
        from account_app import app
        download = app._download_image_to_data_url
        independent_transport = request.node.path.name in {
            'test_inbound_media_archive.py', 'test_ai_efficiency.py',
            'test_ai_reliability.py', 'test_chatwoot.py', 'test_zernio.py',
        }

        def fixture_when_recognizer_is_mocked(url):
            if (not independent_transport
                    and isinstance(app.match_customer_image_with_catalog, Mock)
                    and (urlparse(str(url)).hostname or '').endswith('.test')):
                return fixture_image(url)
            return download(url)

        with patch.object(app, '_download_image_to_data_url', side_effect=fixture_when_recognizer_is_mocked), \
             patch.object(app, 'TELEGRAM_ORDERS_CHAT_ID', 'offline-default-group'):
            yield


def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    root = Path(__file__).resolve().parents[2]
    audit_root = root / 'outputs' / 'offline-audit'
    audit_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='run-', dir=audit_root) as directory:
        env = dict(DATA_DIR=directory, DB_PATH=str(Path(directory) / 'audit.db'),
                   BOOKINGS_FILE=str(Path(directory) / 'bookings.jsonl'),
                   ENABLE_BACKGROUND_JOBS='0', DISABLE_CLIP='1', ZERNIO_AL_FATENA_ENABLED='0')
        original_exists = os.path.exists
        def without_local_secrets(path):
            if str(path).replace('\\', '/').endswith('/account_app/.env'):
                return False
            return original_exists(path)
        with (patch.dict(os.environ, env), patch.object(tempfile, 'tempdir', directory),
             patch('dotenv.load_dotenv', return_value=False),
             patch('os.path.exists', side_effect=without_local_secrets),
             patch('requests.sessions.Session.request', side_effect=RuntimeError('Real network blocked in offline audit')),
             patch.object(socket, 'create_connection', side_effect=RuntimeError('Real network blocked in offline audit')),
             patch.object(socket.socket, 'connect', side_effect=RuntimeError('Real network blocked in offline audit'))):
            print('OFFLINE AUDIT: synthetic provider/delivery responses; no live calls; disposable storage.')
            return pytest.main(arguments or ['-q', str(Path(__file__).parent)], plugins=[LegacyFixtureBoundaries()])


if __name__ == '__main__':
    raise SystemExit(main())
