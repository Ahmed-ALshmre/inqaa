import base64
import io
import json
import sqlite3
import tempfile
import unittest
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock

from PIL import Image
from account_app import ai_efficiency as e


class EfficiencyTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        e.init_db(self.db)

    def tearDown(self):
        self.db.close()

    def test_compaction_preserves_zero_false_and_unicode(self):
        result = json.loads(e.dumps({'price': 0, 'available': False, 'name': 'فستان', 'empty': None}))
        self.assertEqual(result, {'price': 0, 'available': False, 'name': 'فستان'})

    def test_unique_legacy_memory_is_preserved_without_duplicate_messages(self):
        history = [{'direction': 'incoming', 'text': 'قياس 44'}]
        memory = [{'role': 'user', 'content': 'اللون زيتي'}, {'role': 'user', 'content': 'قياس 44'}]
        merged = e.merge_history(history, memory)
        self.assertEqual([m['text'] for m in merged], ['اللون زيتي', 'قياس 44'])
        self.assertEqual(len(history), 1)

    def test_success_failure_and_store_isolation(self):
        recognize = Mock(return_value={'product_found': True, 'product_id': 'A'})
        for _ in range(3):
            self.assertTrue(e.recognize_once(self.db, 's', 'u', 'hash', [], recognize)['product_found'])
        self.assertEqual(recognize.call_count, 1)
        e.recognize_once(self.db, 'other', 'u', 'hash', [], recognize)
        self.assertEqual(recognize.call_count, 2)
        failure = Mock(side_effect=TimeoutError)
        for _ in range(2):
            self.assertTrue(e.recognize_once(self.db, 's', 'u', 'failed', [], failure)['service_error'])
        failure.assert_called_once()

    def test_started_attempt_survives_restart_and_catalog_change(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'test.db'
            db = sqlite3.connect(path)
            e.init_db(db)
            db.execute("INSERT INTO image_recognition_attempts(store_id,sender_id,fingerprint,status,catalog_version) VALUES('s','u','x','started','old')")
            db.commit()
            db.close()
            db = sqlite3.connect(path)
            recognize = Mock()
            result = e.recognize_once(db, 's', 'u', 'x', [{'product_id': 'new'}], recognize)
            self.assertTrue(result['service_error'])
            recognize.assert_not_called()
            db.close()

    def test_fingerprint_ignores_png_encoding(self):
        image = Image.new('RGB', (4, 5), 'red')
        urls = []
        for compression in (0, 9):
            buffer = io.BytesIO()
            image.save(buffer, format='PNG', compress_level=compression)
            urls.append('data:image/png;base64,' + base64.b64encode(buffer.getvalue()).decode())
        self.assertEqual(e.fingerprint(urls[0]), e.fingerprint(urls[1]))

    def test_usage_unavailable_is_null(self):
        e.record_usage(self.db, 'store', 'main', 'unchanged-model', {}, 12)
        row = self.db.execute('SELECT prompt_tokens,completion_tokens,reasoning_tokens,cached_tokens FROM ai_usage_events').fetchone()
        self.assertEqual(row, (None, None, None, None))

    def test_rejected_association_does_not_reappear_or_retry(self):
        recognize = Mock(return_value={'product_found': True, 'product_id': 'A'})
        e.recognize_once(self.db, 's', 'u', 'image', [], recognize)
        e.invalidate_product(self.db, 's', 'u', 'A')
        self.db.commit()
        result = e.recognize_once(self.db, 's', 'u', 'image', [], recognize)
        self.assertFalse(result['product_found'])
        recognize.assert_called_once()

    def test_two_workers_make_only_one_external_call(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'concurrent.db'
            db = sqlite3.connect(path)
            e.init_db(db); db.commit(); db.close()
            started = threading.Event()
            release = threading.Event()
            def recognize():
                started.set()
                self.assertTrue(release.wait(5))
                return {'product_found': True, 'product_id': 'P'}
            model = Mock(side_effect=recognize)
            def worker():
                connection = sqlite3.connect(path, timeout=5)
                try:
                    return e.recognize_once(connection, 's', 'u', 'x', [], model)
                finally:
                    connection.close()
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(worker)
                self.assertTrue(started.wait(5))
                second = pool.submit(worker)
                try:
                    self.assertTrue(second.result(timeout=5)['service_error'])
                finally:
                    release.set()
                self.assertTrue(first.result(timeout=5)['product_found'])
            model.assert_called_once()
