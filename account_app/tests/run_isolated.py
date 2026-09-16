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
from PIL import Image


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
        )
        with patch("requests.sessions.Session.request", side_effect=RuntimeError("Real network calls are disabled in tests")):
            suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent))
            with patch('account_app.app._download_image_to_data_url', side_effect=fixture_image):
                result = unittest.TextTestRunner(verbosity=1).run(suite)
        return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
