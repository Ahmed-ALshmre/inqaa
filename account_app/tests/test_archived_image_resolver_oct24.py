"""Private local image resolution is based on scoped database evidence only."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from PIL import Image

from account_app import ai_efficiency
from account_app.tests import test_audit_orders as fixtures


class ArchivedImageResolverOct24Tests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set('default')
        self.sender = 'synthetic-private-image'
        fixtures.AuditFixTests.customer(self, self.sender)
        self.source = 'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=synthetic'
        self.directory = Path(self.temp.name) / 'incoming'
        self.directory.mkdir()
        self.filename = 'a' * 32 + '.png'
        self.path = self.directory / self.filename
        Image.new('RGB', (8, 9), 'red').save(self.path)
        self.storage = patch.object(self.m, 'INCOMING_MEDIA_DIR', str(self.directory))
        self.storage.start()

    def tearDown(self):
        self.storage.stop()
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def archive_record(self, *, filename=None, store='default', sender=None, url=None):
        sender = sender or self.sender
        url = url or self.source
        mid = self.m.save_message(self.db, sender, 'incoming', 'image', '', url, None, None, {})
        metadata = [{'type': 'image', 'url': url, 'local_file': filename or self.filename, 'mime': 'image/png'}]
        self.db.execute('UPDATE messages SET media_json=?,store_id=? WHERE id=?', (json.dumps(metadata), store, mid))
        self.db.commit()
        return mid

    def test_archived_image_retains_original_fingerprint_without_network(self):
        self.archive_record()
        with patch.object(self.m.requests, 'get') as get:
            data = self.m._archived_incoming_image_data(self.db, self.sender, self.source)
        get.assert_not_called()
        self.assertEqual(ai_efficiency.fingerprint(data),
                         ai_efficiency.fingerprint(self.m._file_to_data_url(str(self.path))))

    def test_same_url_cannot_read_another_sender_or_store_archive(self):
        self.archive_record(sender='foreign-synthetic')
        self.assertEqual(self.m._archived_incoming_image_data(self.db, self.sender, self.source), '')
        self.archive_record(store='another-store')
        self.assertEqual(self.m._archived_incoming_image_data(self.db, self.sender, self.source), '')

    def test_exact_url_is_required_even_when_a_file_exists(self):
        self.archive_record()
        self.assertEqual(self.m._archived_incoming_image_data(self.db, self.sender, self.source + '-other'), '')

    def test_untrusted_filename_missing_file_and_corrupt_file_fall_back_safely(self):
        for filename in ['../outside.png', str(self.path.resolve()), 'b' * 32 + '.png', 'a' * 32 + '.mp3']:
            with self.subTest(filename=Path(filename).suffix):
                self.db.execute('DELETE FROM messages')
                self.archive_record(filename=filename)
                self.assertEqual(self.m._archived_incoming_image_data(self.db, self.sender, self.source), '')
        self.db.execute('DELETE FROM messages')
        self.archive_record()
        self.path.write_bytes(b'not-an-image')
        self.assertEqual(self.m._archived_incoming_image_data(self.db, self.sender, self.source), '')

    def test_local_file_hint_in_an_incoming_payload_is_not_authoritative(self):
        self.m.save_message(self.db, self.sender, 'incoming', 'image', '', self.source, None, None,
                            {'attachments': [{'type': 'image', 'url': self.source, 'local_file': self.filename}]})
        self.assertEqual(self.m._archived_incoming_image_data(self.db, self.sender, self.source), '')

    def test_filesystem_failure_returns_unknown_instead_of_crashing_recognition(self):
        self.archive_record()
        with patch.object(self.m.os.path, 'getsize', side_effect=PermissionError):
            self.assertEqual(self.m._archived_incoming_image_data(self.db, self.sender, self.source), '')
