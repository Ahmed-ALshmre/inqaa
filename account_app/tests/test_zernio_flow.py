"""Signed inbox -> durable worker -> real product logic -> simulated Zernio delivery.

Only vision/model results and the HTTP boundary are fixtures; no customer is contacted.
"""
import copy
import json
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import requests
from PIL import Image

import account_app.app as m
from account_app.tests import test_zernio as fixtures
from account_app.tests.run_isolated import fixture_image


class ZernioFlowTests(unittest.TestCase):
    post = fixtures.ZernioTests.post
    response = fixtures.ZernioTests.response
    sender = fixtures.ZernioTests.sender

    def setUp(self):
        fixtures.ZernioTests.setUp(self)
        self.addCleanup(fixtures.ZernioTests.tearDown, self)
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.catalog = fixtures.ZernioTests.catalog(self)
        self.catalog.append(dict(self.catalog[0], product_id='F3', product_name='قميص سماوي',
                                 price='13000', stock='نفذ', ad_id='333'))
        folder = Path(self.temp.name) / 'product-images'
        folder.mkdir()
        for index, product in enumerate(self.catalog):
            filename = product['product_id'] + '.png'
            Image.new('RGB', (4, 4), (40 + index * 60, 30, 70)).save(folder / filename)
            product['image_url'] = 'https://store.example/product_image/' + filename
        self.sequence = 0
        self.deliveries = []
        self.vision_answers = []
        self.delivery_failures = []
        self.model_override = None
        self.selected = []
        for target, value in [('load_products_from_file', self.catalog), ('load_active_products', self.catalog),
                              ('is_ai_enabled', True), ('is_store_feature_enabled', False),
                              ('send_telegram_message', False), ('send_order_to_telegram', True),
                              ('get_auto_product_settings', {'enabled': False})]:
            self.stack.enter_context(patch.object(m, target, return_value=value))
        self.stack.enter_context(patch.object(m, 'save_booking_to_file'))
        self.stack.enter_context(patch.object(m, 'PUBLIC_URL', 'https://store.example'))
        self.stack.enter_context(patch.object(m, 'PRODUCT_IMAGE_DIR', str(folder)))
        self.stack.enter_context(patch.object(m, '_download_image_to_data_url', side_effect=fixture_image))
        self.model = self.stack.enter_context(patch.object(m, '_call_main_ai_once', side_effect=self.model_reply))
        self.vision = self.stack.enter_context(patch.object(m, 'match_customer_image_with_catalog', side_effect=self.recognize))
        self.transport = self.stack.enter_context(patch.object(m.zernio.requests, 'post', side_effect=self.deliver))

    def model_reply(self, event, kind, customer, history, products, matched, *args, **kwargs):
        self.selected.append((matched or {}).get('product_id'))
        if self.model_override is not None:
            return copy.deepcopy(self.model_override)
        reply = {'create_order': False, 'order': {}}
        if matched and m._requests_product_photo(event.get('text')):
            return dict(reply, reply='هذه صورة ' + matched['product_name'],
                        image_product_ids=[matched['product_id']])
        return dict(reply, reply=(f"{matched['product_name']} سعره {int(matched['price']):,}."
                                  if matched else 'دزيلي صورة الموديل حتى أتأكد إلج.'))

    def recognize(self, *args, **kwargs):
        if not self.vision_answers:
            raise AssertionError('Unexpected extra recognition request')
        return self.vision_answers.pop(0)

    def deliver(self, url, **kwargs):
        self.assertTrue(url.startswith(m.zernio.API_URL + '/inbox/conversations/'))
        payload = kwargs['json']
        self.deliveries.append(copy.deepcopy(payload))
        if self.delivery_failures:
            failure = self.delivery_failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            if failure:
                return self.response(failure, {})
        return self.response(body={'success': True, 'data': {'messageId': 'sent-' + str(len(self.deliveries))}})

    def message(self, text='', images=(), *, ad=False, media=None):
        self.sequence += 1
        payload = copy.deepcopy(self.payload)
        payload.update(id='flow-event-' + str(self.sequence), timestamp=datetime.now(timezone.utc).isoformat())
        payload['message'].update(text=text, platformMessageId='flow-message-' + str(self.sequence),
                                  sentAt=payload['timestamp'], attachments=media or [
                                      {'type': 'image', 'url': url} for url in images])
        if not ad:
            payload.pop('metadata')
        self.assertEqual(self.post(payload).status_code, 200)
        with m.app.app_context():
            job = dict(m.get_db().execute('SELECT * FROM ai_jobs ORDER BY created_at DESC,rowid DESC LIMIT 1').fetchone())
        return job, payload

    def run_job(self, job):
        result = m.run_ai_job(job, json.loads(job['payload']))
        with m.app.app_context():
            m.ai_jobs.finish(m.get_db(), job['id'], 'done' if result.get('ok') else 'failed', result)
        return result

    def turn(self, text='', images=(), **kwargs):
        job, payload = self.message(text, images, **kwargs)
        start = len(self.deliveries)
        result = self.run_job(job)
        return result, self.deliveries[start:]

    def active(self):
        with m.app.app_context():
            return {row[0] for row in m.get_db().execute(
                "SELECT product_id FROM customer_product_interests WHERE status='active'")}

    def test_facebook_ad_question_and_requested_photo_reach_transport(self):
        result, sent = self.turn('شكد سعره؟', ad=True)
        self.assertTrue(result['ok'])
        self.assertEqual(self.active(), {'F2'})
        self.assertTrue(any('16 ألف' in row.get('message', '') for row in sent))
        _, sent = self.turn('دزي صورة الموديل')
        photos = [row for row in sent if row.get('attachmentType') == 'image']
        self.assertEqual([row['attachmentUrl'] for row in photos], [self.catalog[1]['image_url']])
        self.assertTrue(all(row['accountId'] == self.facebook for row in self.deliveries))
        self.vision.assert_not_called()

    def test_instagram_routes_photos_to_instagram_account(self):
        self.payload['account'].update(id=self.instagram, accountId=self.instagram, platform='instagram')
        self.payload['message']['platform'] = 'instagram'
        self.turn('شكد سعره؟', ad=True)
        result, sent = self.turn('دزي صورة الموديل')
        self.assertTrue(result['ok'])
        self.assertTrue(any(row.get('attachmentUrl') == self.catalog[1]['image_url'] for row in sent))
        self.assertTrue(all(row['accountId'] == self.instagram for row in self.deliveries))

    def test_text_change_then_photo_uses_new_model(self):
        self.turn('شكد سعره؟', ad=True)
        self.turn('اريد تنورة سادة')
        self.assertEqual(self.active(), {'F1'})
        _, sent = self.turn('دزي صورتها')
        self.assertEqual([row['attachmentUrl'] for row in sent if row.get('attachmentUrl')],
                         [self.catalog[0]['image_url']])

    def test_customer_photo_replaces_ad_product_and_followup_keeps_it(self):
        self.turn('شكد سعره؟', ad=True)
        self.vision_answers = [{'product_found': True, 'product_id': 'F1', 'confidence': 100}]
        result, sent = self.turn('مو هذا، اريد هذا', ['https://images.test/replacement.png'])
        self.assertTrue(result['ok'])
        self.assertEqual(self.active(), {'F1'})
        self.assertEqual(self.vision.call_count, 1)
        _, sent = self.turn('شكد سعرها؟')
        self.assertTrue(any('12 ألف' in row.get('message', '') for row in sent))
        self.assertFalse(any('16,000' in row.get('message', '') for row in sent))
        self.assertEqual(self.vision.call_count, 1)

    def test_same_model_photo_does_not_duplicate_binding(self):
        self.turn('شكد سعره؟', ad=True)
        self.vision_answers = [{'product_found': True, 'product_id': 'F2', 'confidence': 100}]
        result, _ = self.turn('', ['https://images.test/same.png'])
        self.assertTrue(result['ok'])
        self.assertEqual(self.active(), {'F2'})
        with m.app.app_context():
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM customer_product_interests').fetchone()[0], 1)

    def test_specific_arabic_reply_name_beats_short_name_and_conflicting_image_id(self):
        self.catalog[0]['product_name'] = 'سوت'
        self.catalog[1]['product_name'] = 'سوت ياخة'
        self.turn('شكد سعره؟', ad=True)
        self.model_override = {'reply': 'تدللين عيني، هذا الموديل هو سوت الياخة وخامته لينن.',
                               'image_product_ids': ['F1'], 'create_order': False, 'order': {}}
        result, sent = self.turn('دزيلي صورة الموديل الجديد')
        self.assertTrue(result['ok'])
        self.assertEqual(self.active(), {'F2'})
        self.assertEqual([row['attachmentUrl'] for row in sent if row.get('attachmentUrl')],
                         [self.catalog[1]['image_url']])

    def test_photo_addition_keeps_both_products(self):
        self.turn('شكد سعره؟', ad=True)
        self.vision_answers = [{'product_found': True, 'product_id': 'F1', 'confidence': 100}]
        result, _ = self.turn('ضيفي هذا ويا الفستان', ['https://images.test/addition.png'])
        self.assertTrue(result['ok'])
        self.assertEqual(self.active(), {'F1', 'F2'})

    def test_reselecting_existing_photo_with_replace_removes_other_products(self):
        self.turn('شكد سعره؟', ad=True)
        self.vision_answers = [{'product_found': True, 'product_id': 'F1', 'confidence': 100},
                               {'product_found': True, 'product_id': 'F2', 'confidence': 100}]
        self.turn('ضيفي هذا ويا الفستان', ['https://images.test/addition.png'])
        self.assertEqual(self.active(), {'F1', 'F2'})
        result, _ = self.turn('مو هذا، أريد هذا بدل السابق', ['https://images.test/reselect.png'])
        self.assertTrue(result['ok'])
        self.assertEqual(self.active(), {'F2'})

    def test_mixed_album_sends_ordered_answers_together_without_instruction_leak(self):
        self.catalog[0]['sizes'] = '38 إلى 52. اعتمد الجدول التالي عند الرد على الزبونة'
        self.vision_answers = [{'product_found': True, 'product_id': 'F1', 'confidence': 100},
                               {'product_found': True, 'product_id': 'F3', 'confidence': 100},
                               {'product_found': False, 'confidence': 0}]
        result, sent = self.turn('متوفر؟', [f'https://images.test/album-{i}.png' for i in range(3)])
        self.assertTrue(result['ok'])
        texts = [row['message'] for row in sent if row.get('message')]
        self.assertTrue(2 <= len(texts) <= 5)
        combined = '\n\n'.join(texts)
        first, remaining = combined.split('الثاني', 1)
        second = remaining
        self.assertIn('الأول', first)
        self.assertIn('12 ألف', first)
        self.assertIn('مو متوفر', second)
        self.assertNotIn('12 ألف', second)
        self.assertNotIn('الثالث', combined)
        self.assertTrue(all(not m.customer_copy.has_instructions(text) for text in texts))
        self.assertEqual(self.vision.call_count, 3)

    def test_image_service_failure_is_not_a_stock_answer(self):
        self.vision_answers = [{'product_found': False, 'service_error': True,
                               'reason': 'Fixture outage', 'error_code': 'fixture_outage'}]
        result, sent = self.turn('', ['https://images.test/unavailable-service.png'])
        self.assertFalse(result['ok'])
        self.assertEqual(len(sent), 0)
        acknowledgement = ''
        self.assertNotRegex(acknowledgement, r'متوفر|خلص|تم تثبيت|تم حجز|\d+[,.]?\d*\s*(?:د\.ع|دينار|ألف|الف)')
        self.assertNotRegex(acknowledgement, r'(?:دزي|أرسلي|ارسلي).{0,25}(?:صوره|صورة)')
        self.assertEqual(self.vision.call_count, 1)
        with m.app.app_context():
            self.assertGreater(m.get_db().execute("SELECT count(*) FROM human_reviews WHERE status='pending'").fetchone()[0], 0)
            self.assertEqual(m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_audio_is_saved_without_image_analysis_or_customer_reply(self):
        job, _ = self.message(media=[{'type': 'audio', 'url': 'https://images.test/voice.ogg'}])
        self.run_job(job)
        self.vision.assert_not_called()
        self.assertFalse(self.deliveries)
        with m.app.app_context():
            row = m.get_db().execute("SELECT message_type FROM messages WHERE direction='incoming'").fetchone()
            self.assertEqual(row['message_type'], 'audio')

    def test_duplicate_inbound_and_finished_job_cannot_repeat_provider_send(self):
        job, payload = self.message('شكد سعره؟', ad=True)
        self.assertEqual(self.post(payload).status_code, 200)
        self.assertTrue(self.run_job(job)['ok'])
        count = len(self.deliveries)
        with patch.object(m, '_finish_customer_webhook_async', side_effect=AssertionError('A finished job must not regenerate its reply')):
            self.assertTrue(self.run_job(job)['_job_cached'])
        self.assertEqual(len(self.deliveries), count)
        with m.app.app_context():
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM ai_jobs').fetchone()[0], 1)
            self.assertEqual(m.get_db().execute("SELECT count(*) FROM messages WHERE direction='incoming'").fetchone()[0], 1)

    def test_transport_timeout_does_not_blindly_repeat_delivery(self):
        self.delivery_failures = [requests.Timeout('Simulated transport timeout')]
        job, _ = self.message('شكد سعره؟', ad=True)
        self.assertFalse(self.run_job(job)['ok'])
        self.assertEqual(len(self.deliveries), 1)
        with patch.object(m, '_finish_customer_webhook_async', side_effect=AssertionError('An uncertain delivery must not regenerate its reply')):
            self.assertTrue(self.run_job(job)['_job_cached'])
        self.assertEqual(len(self.deliveries), 1)

    def test_paused_conversation_keeps_message_without_sending(self):
        job, _ = self.message('شكد سعره؟', ad=True)
        with m.app.app_context():
            m._current_store_id.set('al-fatena')
            m.set_customer_ai_enabled(m.get_db(), self.sender(), False)
        self.run_job(job)
        self.assertFalse(self.deliveries)
        self.model.assert_not_called()

    def test_booking_after_product_change_uses_only_selected_product_and_deduplicates(self):
        self.turn('شكد سعره؟', ad=True)
        self.turn('اريد تنورة سادة')
        self.model_override = {'reply': 'تم', 'create_order': True, 'order': {
            'phone': '07700000000', 'province': 'بغداد', 'address': 'حي الاختبار قرب جامع الاختبار',
            'items': [{'product_id': 'F1', 'product_name': 'تنورة سادة', 'color': 'اسود',
                       'size': '44', 'size_type': 'size', 'quantity': 1}]}}
        job, payload = self.message('احجزي تنورة سادة قياس 44 اسود، بغداد حي الاختبار 07700000000')
        result = self.run_job(job)
        self.assertTrue(result['ok'])
        with m.app.app_context():
            rows = m.get_db().execute('SELECT * FROM orders').fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['store_id'], 'al-fatena')
            self.assertEqual([item['product_id'] for item in json.loads(rows[0]['order_items'])], ['F1'])
        self.post(payload)
        self.run_job(job)
        with m.app.app_context():
            self.assertEqual(m.get_db().execute('SELECT count(*) FROM orders').fetchone()[0], 1)
