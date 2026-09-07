"""Run regression tests with a disposable database and all real network calls blocked.

Usage: python -m account_app.tests.run_isolated
"""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


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
            result = unittest.TextTestRunner(verbosity=1).run(suite)
        return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
