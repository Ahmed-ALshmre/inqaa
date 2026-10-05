import io
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
from PIL import Image
from account_app import app as m
from account_app.tests import test_chatwoot as fixtures


class ComposerImageSendTests(unittest.TestCase):
    setUp = fixtures.ChatwootTests.setUp
    tearDown = fixtures.ChatwootTests.tearDown

    def image(self):
        stream=io.BytesIO()
        Image.new('RGB',(8,8),'red').save(stream,format='PNG')
        stream.seek(0)
        return stream

    def test_device_upload_sends_image_without_text_or_checkout(self):
        with self.client.session_transaction() as session:
            session['dashboard_authenticated']=True
        with m.app.app_context():
            m.get_or_create_customer(m.get_db(),'default::cw-1-3','page','facebook')
        response=Mock(status_code=201)
        response.json.return_value={'id':4321}
        def send(url,**kwargs):
            self.assertNotIn('json',kwargs)
            self.assertNotIn('content',kwargs['data'])
            self.assertEqual(kwargs['files']['attachments[]'][1].read(8),b'\x89PNG\r\n\x1a\n')
            return response
        with patch.object(m,'UPLOADS_DIR',str(Path(self.temp.name)/'uploads')), patch.object(m,'apply_dashboard_ai_checkout') as checkout, patch('account_app.chatwoot.requests.post',side_effect=send) as post:
            uploaded=self.client.post('/api/upload_image',data={'image':(self.image(),'phone.png')})
            self.assertEqual(uploaded.status_code,200)
            sent=self.client.post('/api/conversations/default::cw-1-3/send',json={'text':'','image_url':uploaded.json['image_url']})
            self.assertTrue(sent.json['ok'],sent.json)
            post.assert_called_once()
            checkout.assert_not_called()
        with m.app.app_context():
            row=m.get_db().execute("SELECT message_type,text FROM messages WHERE sender_id='default::cw-1-3' AND direction='outgoing'").fetchone()
            self.assertEqual(row['message_type'],'image')
            self.assertIsNone(row['text'])

    def test_unsupported_upload_is_explicit(self):
        with self.client.session_transaction() as session:
            session['dashboard_authenticated']=True
        response=self.client.post('/api/upload_image',data={'image':(io.BytesIO(b'not image'),'photo.heic')})
        self.assertEqual(response.status_code,400)
        self.assertIn('error',response.json)
