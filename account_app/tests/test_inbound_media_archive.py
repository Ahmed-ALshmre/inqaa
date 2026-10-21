import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from account_app import app as m, inbound_media


class _Response:
    def __init__(self, data=b'', status=200, mime='image/png', location=None):
        self.status_code = status
        self.headers = {'Content-Type': mime, 'Content-Length': str(len(data))}
        if location:
            self.headers['Location'] = location
        self.data = data

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def iter_content(self, _size): yield self.data


class InboundMediaArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path_patch = patch.object(m, 'INCOMING_MEDIA_DIR', self.temp.name)
        self.path_patch.start(); self.addCleanup(self.path_patch.stop)
        self.ctx = m.app.app_context(); self.ctx.push(); self.addCleanup(self.ctx.pop)
        self.token = m._current_store_id.set('default')
        self.addCleanup(m._current_store_id.reset, self.token)
        self.db = m.get_db()
        self.sender = 'media-archive-' + uuid.uuid4().hex
        m.get_or_create_customer(self.db, self.sender, 'page')
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True

    def _save(self, url, kind='image'):
        return m.save_message(self.db, self.sender, 'incoming', kind, url, None, None, None,
                              {'attachments': [{'type': kind, 'payload': {'url': url}}]})

    def test_private_photo_is_archived_and_old_source_is_not_reused(self):
        source = 'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=photo'
        mid = self._save(source)
        listed = self.client.get(f'/api/conversations/{self.sender}/messages').json['messages'][-1]
        local = listed['media'][0]['url']
        self.assertNotIn('lookaside', local)
        self.assertIsNone(listed['text'])
        png = b'\x89PNG\r\n\x1a\n' + b'0' * 24
        with patch.object(inbound_media.requests, 'get', return_value=_Response(png)) as get:
            opened = self.client.get(local)
            self.assertEqual(opened.status_code, 200)
            self.assertEqual(opened.data, png)
            self.assertEqual(opened.headers['X-Content-Type-Options'], 'nosniff')
            self.assertEqual(self.client.get(local).status_code, 200)
            self.assertEqual(get.call_count, 1)
        saved = json.loads(self.db.execute('SELECT media_json FROM messages WHERE id=?', (mid,)).fetchone()[0])
        self.assertTrue(Path(self.temp.name, saved[0]['local_file']).is_file())
        other = self.client.get(local.replace(self.sender, 'wrong-sender'))
        self.assertEqual(other.status_code, 404)
        self.assertEqual(self.client.get(local.replace('store_id=default', 'store_id=another')).status_code, 404)
        guest = m.app.test_client()
        self.assertEqual(guest.get(local).status_code, 403)

    def test_expired_link_returns_actionable_error_and_does_not_cache(self):
        mid = self._save('https://app.chatwoot.com/rails/active_storage/blobs/redirect/old/file.mp4', 'video')
        local = self.client.get(f'/api/conversations/{self.sender}/messages').json['messages'][-1]['media'][0]['url']
        with patch.object(inbound_media.requests, 'get', return_value=_Response(status=404)):
            response = self.client.get(local)
        self.assertEqual(response.status_code, 410)
        self.assertIn('إعادة إرساله', response.json['error'])
        self.assertFalse(list(Path(self.temp.name).iterdir()))

    def test_rejects_untrusted_redirect_html_and_large_files(self):
        source = 'https://lookaside.fbsbx.com/media/one'
        with patch.object(inbound_media.requests, 'get', return_value=_Response(status=302, location='https://example.com/private')) as get:
            self.assertIsNone(inbound_media.fetch(source, 'image'))
            self.assertEqual(get.call_count, 1)
        with patch.object(inbound_media.requests, 'get', return_value=_Response(b'<html>no</html>', mime='text/html')):
            self.assertIsNone(inbound_media.fetch(source, 'image'))
        with patch.object(inbound_media.requests, 'get', return_value=_Response(b'\x89PNG\r\n\x1a\n', mime='image/png')) as get:
            get.return_value.headers['Content-Length'] = str(inbound_media.MAX_BYTES + 1)
            self.assertIsNone(inbound_media.fetch(source, 'image'))
        self.assertFalse(inbound_media.trusted_source('http://lookaside.fbsbx.com/media/one'))
        self.assertFalse(inbound_media.trusted_source('https://lookaside.fbsbx.com.evil.test/file'))

    def test_background_job_archives_incoming_audio(self):
        source = 'https://cdn.fbsbx.com/audioclip-note.ogg'
        with patch.dict(os.environ, {'ENABLE_BACKGROUND_JOBS': '1'}), patch.object(m.ai_jobs, 'start'):
            mid = self._save(source, 'audio')
        job = self.db.execute("SELECT * FROM ai_jobs WHERE kind='media_archive' AND sender_id=?",
                              (self.sender,)).fetchone()
        self.assertIsNotNone(job)
        with patch.object(inbound_media.requests, 'get', return_value=_Response(b'OggS123456', mime='audio/ogg')):
            result = m.run_ai_job(dict(job), {'message_id': mid})
        self.assertEqual(result['archived'], 1)
        local = self.client.get(f'/api/conversations/{self.sender}/messages').json['messages'][-1]['media'][0]['url']
        self.assertEqual(self.client.get(local).data, b'OggS123456')


if __name__ == '__main__':
    unittest.main()
