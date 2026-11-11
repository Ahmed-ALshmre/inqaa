import base64
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from PIL import Image, ImageDraw
from requests import Timeout

from account_app import checkout
import account_app.app as app_module
from account_app.sales_runtime.orders import CONFIRMATION
from account_app.sales_runtime.preview import install
from account_app.sales_runtime.recovery import safe_text
from account_app.sales_runtime.store import Scope


def drawing(kind):
    """Synthetic garments, with no customer or production catalog images."""
    image = Image.new('RGB', (384, 384), 'white')
    canvas = ImageDraw.Draw(image)
    if kind == 'dress':
        canvas.polygon([(158,55), (134,60), (92,130), (133,155), (154,129),
                        (150,190), (106,324), (278,324), (234,190),
                        (230,129), (250,155), (292,130), (250,60), (226,55)], fill='#69183c')
        canvas.rectangle((154, 183, 230, 194), fill='#f1bb89')
        canvas.ellipse((165, 46, 220, 77), fill='white')
    else:
        canvas.polygon([(115,74), (269,74), (282,322), (214,322),
                        (192,165), (170,322), (102,322)], fill='#203d72')
        canvas.line((122, 92, 260, 92), fill='#97accf', width=7)
    output = io.BytesIO()
    image.save(output, format='PNG')
    return output.getvalue()


class ChannelPreviewTests(unittest.TestCase):
    account = 'a' * 24
    photo_url = 'https://images.fbcdn.net/preview-dress.png'
    alternate_url = 'https://images.fbcdn.net/preview-trousers.png'

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.addCleanup(setattr, app_module, 'DB_PATH', app_module.DB_PATH)
        app_module.DB_PATH = str(Path(temporary.name) / 'preview.db')
        app_module.init_db()
        self.addCleanup(setattr, app_module.app, 'testing', app_module.app.testing)
        app_module.app.testing = True
        self.addCleanup(app_module.app.extensions.pop, 'sales_runtime_preview', None)
        env = patch.dict(os.environ, {
            'MESSAGING_PROVIDER': 'chatwoot', 'CHATWOOT_BASE_URL': 'https://chat.example.test',
            'CHATWOOT_ACCOUNT_ID': '1', 'CHATWOOT_INBOX_STORES': '{"1:2":"default"}',
            'CHATWOOT_API_TOKEN': 'preview-cw-key', 'CHATWOOT_WEBHOOK_SECRET': 'preview-cw-signature',
            'ZERNIO_AL_FATENA_ENABLED': '1', 'ZERNIO_BARAAH_KIDS_ENABLED': '0',
            'ZERNIO_API_KEY': 'preview-zn-key', 'ZERNIO_WEBHOOK_SECRET': 'preview-zn-signature',
            'ZERNIO_ACCOUNTS': json.dumps({self.account: {'store_id': 'al-fatena', 'platform': 'facebook'}}),
            'ENABLE_BACKGROUND_JOBS': '0',
        })
        env.start()
        self.addCleanup(env.stop)
        with app_module.app.app_context():
            app_module.ensure_store(app_module.get_db(), 'al-fatena', 'متجر اختبار')
        self.fixtures = {self.photo_url:drawing('dress'), self.alternate_url:drawing('trousers')}
        self.posts = []
        self.enabled = Mock(return_value=True)
        self.fetch = Mock(side_effect=self.fetch_fixture)
        self.match = Mock(side_effect=self.match_fixture)
        self.decide = Mock(side_effect=self.decide_fixture)
        self.post = Mock(side_effect=self.send_fixture)
        self.preview = install(app_module, products=self.products, delivery_fee=lambda s, p:5000,
                               decide=self.decide, writer=safe_text, fetch_image=self.fetch,
                               match_image=self.match, post=self.post, enabled=self.enabled)
        self.client = app_module.app.test_client()

    def products(self, store):
        return [dict(store_id=store, product_id=pid, product_name=name, price=price,
                     colors=colors, sizes='40 إلى 48', stock='متوفر', status='active',
                     image_url='data:image/png;base64,'+base64.b64encode(self.fixtures[url]).decode('ascii'))
                for pid,name,price,colors,url in (
                    ('P1', 'فستان', '15000' if store == 'default' else '17000', 'عنابي، أسود', self.photo_url),
                    ('P2', 'بنطلون', '12000', 'أزرق', self.alternate_url))]

    def fetch_fixture(self, url, kind, base):
        if url not in self.fixtures:
            return None
        return self.fixtures[url], 'image', '.png', 'image/png'

    def match_fixture(self, data, products):
        match = next((p for p in products if p['image_url'] == data), None)
        return dict(product_found=bool(match), product_id=match['product_id'] if match else '')

    def decide_fixture(self, context):
        state, reference, event = (context[key] for key in ('state', 'reference', 'event'))
        if reference.get('source') == 'image':
            return dict(intent='continue', answer=dict(facts=[dict(product_id=reference['product_id'], field='price')]))
        text = event['text']
        if text == 'أريد عنابي قياس 44':
            return dict(intent='buy', changes=[dict(values=dict(product_id=state['focus_product_id'],color='عنابي',size='44'))])
        fields = checkout.contact_fields(text, state['contact'])
        return dict(intent='continue', contact={key:value for key,value in fields.items() if key in {'address','phone'}})

    def send_fixture(self, url, *, json, headers, timeout, allow_redirects):
        self.assertFalse(allow_redirects)
        text = json.get('content') or json.get('message')
        self.posts.append((url, text, headers))
        if text == CONFIRMATION:
            order = app_module.get_db().execute('SELECT 1 FROM orders WHERE store_id=?',
                        ('default' if 'chat.example.test' in url else 'al-fatena',)).fetchone()
            self.assertIsNotNone(order)
        body = ({'success': True, 'data': {'messageId': f'preview-{len(self.posts)}'}} if 'zernio.com' in url
                else {'id': f'preview-{len(self.posts)}'})
        response = Mock(status_code=200)
        response.json.return_value = body
        return response

    def incoming(self, channel, mid, text='', photo=None, *, bad_signature=False, sticker=False):
        if channel == 'chatwoot':
            data = dict(event='message_created', id=mid, account={'id':1}, inbox={'id':2},
                        conversation={'id':3, 'channel':'Channel::Instagram'}, message_type='incoming', content=text)
            if photo:
                data['attachments'] = [dict(file_type='sticker' if sticker else 'image', data_url=photo)]
            raw = json.dumps(data).encode()
            timestamp = str(int(time.time()))
            signature = 'sha256=' + hmac.new(b'preview-cw-signature', timestamp.encode()+b'.'+raw, hashlib.sha256).hexdigest()
            headers = {'X-Chatwoot-Timestamp':timestamp, 'X-Chatwoot-Signature': 'invalid' if bad_signature else signature}
        else:
            timestamp = '2026-11-11T09:00:00Z'
            data = dict(id=f'event-{mid}', event='message.received', timestamp=timestamp,
                        account={'id':self.account,'platform':'facebook'},
                        conversation={'id':'preview-conversation','participantId':'preview-person'},
                        message=dict(id=f'message-{mid}', direction='incoming', platform='facebook',
                                     sender={'id':'preview-person'}, text=text,
                                     attachments=[{'type':'sticker' if sticker else 'image','url':photo}] if photo else []))
            raw = json.dumps(data).encode()
            signature = hmac.new(b'preview-zn-signature', raw, hashlib.sha256).hexdigest()
            headers = {'X-Zernio-Signature': 'invalid' if bad_signature else signature}
        return self.client.post(f'/{channel}/webhook', data=raw, content_type='application/json', headers=headers)

    def checkout_messages(self, channel):
        for mid, text, photo in ((1,'',self.photo_url), (2,'أريد عنابي قياس 44',None),
                                (3,'07701234567',None), (4,'بغداد حي التجربة قرب المدرسة',None)):
            response = self.incoming(channel, mid, text, photo)
            self.assertEqual(200, response.status_code)
        return response.get_json()

    def test_both_signed_routes_match_photo_and_confirm_one_saved_order(self):
        for channel in ('chatwoot', 'zernio'):
            with self.subTest(channel=channel):
                self.assertEqual('booked', self.checkout_messages(channel)['action'])
                before = self.post.call_count
                self.incoming(channel, 4, 'بغداد حي التجربة قرب المدرسة')
                self.assertEqual('superseded', self.incoming(channel, 1, photo=self.photo_url).get_json()['action'])
                self.assertEqual(before, self.post.call_count)
        with app_module.app.app_context():
            db = app_module.get_db()
            orders = db.execute('SELECT store_id,product_id,color,size,total_amount FROM orders ORDER BY id').fetchall()
            self.assertEqual([('default','P1','عنابي','44',20000), ('al-fatena','P1','عنابي','44',22000)], [tuple(row) for row in orders])
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM sales_v2_order_tasks WHERE kind='confirmation' AND status='accepted' AND provider_id IS NOT NULL").fetchone()[0])
            self.assertEqual(2, db.execute("SELECT COUNT(*) FROM sales_v2_order_tasks WHERE kind='fulfillment' AND status='queued'").fetchone()[0])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM ai_jobs').fetchone()[0])
        self.assertEqual(2, self.match.call_count)
        self.assertEqual(2, sum(text == CONFIRMATION for _,text,_ in self.posts))
        self.assertTrue(all('Idempotency-Key' in headers for url,_,headers in self.posts if 'zernio.com' in url))
        for call in self.decide.call_args_list:
            context = json.dumps(call.args[0])
            self.assertNotIn(self.photo_url, context)
            self.assertNotIn('data:image/', context)

    def test_unknown_image_clarifies_without_model_and_sticker_skips_vision(self):
        bad = self.incoming('chatwoot', 1, photo='https://127.0.0.1/secret.png')
        self.assertEqual('media_unresolved',bad.get_json()['action'])
        self.incoming('chatwoot', 1, photo='https://127.0.0.1/secret.png')
        self.fetch.assert_not_called()
        self.match.assert_not_called()
        self.decide.assert_not_called()
        self.assertEqual(1,self.post.call_count)
        self.assertEqual(200,self.incoming('zernio', 2, photo=self.photo_url, sticker=True).status_code)
        self.match.assert_not_called()

    def test_staff_handoff_pauses_then_trusted_operator_resumes(self):
        self.assertEqual(401,self.incoming('chatwoot', 1, photo=self.photo_url, bad_signature=True).status_code)
        self.match.assert_not_called()
        self.assertEqual('human',self.incoming('chatwoot', 2, 'اريد موظف').get_json()['action'])
        self.assertEqual('human',self.incoming('chatwoot', 3, photo=self.photo_url).get_json()['action'])
        self.post.assert_not_called()
        self.match.assert_not_called()
        scope = Scope('default','chatwoot','1','default::cw-1-3')
        with app_module.app.app_context():
            self.assertEqual(1,app_module.get_db().execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])
            self.preview.staff_control(scope, 'takeover', 'human')
            self.preview.staff_control(scope, 'resume', 'active')
            self.assertEqual('superseded',self.preview.staff_control(scope, 'takeover', 'human')['action'])
        self.assertEqual('continue',self.incoming('chatwoot', 4, photo=self.photo_url).get_json()['action'])
        self.match.assert_called_once()

    def test_confirmation_timeout_stays_unknown_and_does_not_resend(self):
        def timeout_confirmation(url, **kwargs):
            if kwargs['json'].get('content') == CONFIRMATION:
                raise Timeout()
            return self.send_fixture(url, **kwargs)
        self.post.side_effect = timeout_confirmation
        result = self.checkout_messages('chatwoot')
        self.assertEqual(['unknown'],result['deliveries'])
        before = self.post.call_count
        self.incoming('chatwoot', 4, 'بغداد حي التجربة قرب المدرسة')
        self.assertEqual(before,self.post.call_count)
        with app_module.app.app_context():
            db = app_module.get_db()
            self.assertEqual(1,db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
            self.assertEqual('unknown',db.execute("SELECT status FROM sales_v2_order_tasks WHERE kind='confirmation'").fetchone()[0])

    def test_different_photo_leaves_existing_cart_for_staff(self):
        self.incoming('chatwoot', 1, photo=self.photo_url)
        self.incoming('chatwoot', 2, 'أريد عنابي قياس 44')
        before = self.post.call_count
        result = self.incoming('chatwoot', 3, photo=self.alternate_url).get_json()
        self.assertEqual('human',result['action'])
        self.assertEqual(before,self.post.call_count)
        with app_module.app.app_context():
            scope = Scope('default','chatwoot','1','default::cw-1-3')
            state = self.preview.entry().store.snapshot(scope)['data']
            self.assertEqual(['P1'],[row['product_id'] for row in state['cart']])
            self.assertEqual('image_changes_existing_cart',state['handoff_reason'])
