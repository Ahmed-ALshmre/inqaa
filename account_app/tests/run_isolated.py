"""Run regression tests with a disposable database and all real network calls blocked.

Usage: python -m account_app.tests.run_isolated
"""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import base64
import hashlib
import io
import json
import requests
from PIL import Image


def offline_request(*args, **kwargs):
    """Mock the NEW external dependency for legacy primary-model fixtures.

    A known missing fact remains a human case. Recoverable bad primary outputs
    receive generic advice so existing retry/checkout assertions still run.
    Dedicated consultant tests override this with success/failure wire fixtures.
    This function never opens a socket; every other HTTP call is rejected.
    """
    url = kwargs.get('url') or next((arg for arg in args if isinstance(arg, str) and arg.startswith('https://')), '')
    payload = kwargs.get('json') or {}
    if url == 'https://openrouter.ai/api/v1/chat/completions' and payload.get('model') == 'openai/gpt-6.1-sol':
        context = json.loads(payload['messages'][-1]['content'])
        missing = (context.get('unsuccessful_answer') or {}).get('missing_topics')
        advice = dict(decision='needs_human' if missing else 'assist',
            guidance='' if missing else 'راجعي السياق والاختيارات المعروفة وأجيبي عن كل أجزاء السؤال من حقائق المنتج، دون اختراع معلومات أو إعادة سؤال معروف.',
            reason='Synthetic consultant fixture; no network.', focus_product_id=None)
        response = requests.Response();response.status_code = 200
        response._content = json.dumps({'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps(advice)}}]}).encode()
        return response
    raise RuntimeError('Real network calls are disabled in tests')


def fixture_image(url):
    """Deterministic image bytes for offline integration fixtures only."""
    from urllib.parse import urlparse
    if not (urlparse(url).hostname or '').endswith('.test'):
        raise RuntimeError('Only .test image fixtures are permitted')
    digest = hashlib.sha256(url.encode()).digest()
    output = io.BytesIO()
    Image.new('RGB', (2, 2), tuple(digest[:3])).save(output, format='PNG')
    return 'data:image/png;base64,' + base64.b64encode(output.getvalue()).decode()


def main():
    with tempfile.TemporaryDirectory() as directory:
        os.environ.update(
            DATA_DIR=directory,
            DB_PATH=str(Path(directory) / "tests.db"),
            BOOKINGS_FILE=str(Path(directory) / "bookings.jsonl"),
            ENABLE_BACKGROUND_JOBS="0",
            DISABLE_CLIP="1",
            ZERNIO_AL_FATENA_ENABLED="0",
            ZERNIO_BARAAH_KIDS_ENABLED="0",
            # Synthetic fixture credentials; the Session.request patch below
            # blocks every real request. Tests must not depend on live keys.
            OPENROUTER_API_KEY="offline-test-only",
            TELEGRAM_BOT_TOKEN="offline-test-only",
            TELEGRAM_CHAT_ID="-10000000001",
        )
        with patch("requests.sessions.Session.request", side_effect=offline_request):
            suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent))
            with patch('account_app.app._download_image_to_data_url', side_effect=fixture_image):
                result = unittest.TextTestRunner(verbosity=1).run(suite)
        return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
