"""Synthetic image -> follow-up continuity, with every external boundary blocked.

These regressions separate a recognizer's NONE from a service outage and from
losing a previously successful product association. All images are tiny local
PNG fixtures; no snapshot customers, source messages or accounts are loaded.
"""
import base64
from contextlib import ExitStack
import io
import json
from pathlib import Path
import uuid
from unittest.mock import patch
import unittest

from PIL import Image

from account_app.tests import test_audit_orders as fixtures


class ImageContinuityOct24Tests(unittest.TestCase):
    setUp_base = fixtures.AuditFixTests.setUp
    tearDown_base = fixtures.AuditFixTests.tearDown

    def setUp(self):
        self.setUp_base()
        self.store_token = self.m._current_store_id.set('default')
        self.sender = 'default::synthetic-image-continuity-' + uuid.uuid4().hex
        self.m.get_or_create_customer(self.db, self.sender, 'synthetic-page', 'facebook')
        self.source = 'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=synthetic-photo'
        self.product = dict(product_id='SYNTHETIC-PHOTO', product_name='فستان الاختبار',
                            price='17000', colors='اسود', sizes='38 إلى 52', fabric='قطن',
                            status='active', stock='متوفر', image_url='https://images.test/catalog.png')
        output = io.BytesIO()
        Image.new('RGB', (3, 3), 'blue').save(output, format='PNG')
        self.png = output.getvalue()
        self.data_url = 'data:image/png;base64,' + base64.b64encode(self.png).decode()
        self.paths = patch.object(self.m, 'INCOMING_MEDIA_DIR', str(Path(self.temp.name) / 'archived'))
        self.paths.start()

    def tearDown(self):
        self.paths.stop()
        self.m._current_store_id.reset(self.store_token)
        self.tearDown_base()

    def body(self, text='', image=False):
        return {'entry': [{'id': 'synthetic-page', '_store_id': 'default', 'messaging': [{
            'sender': {'id': self.sender}, 'message': {
                'mid': 'synthetic-' + uuid.uuid4().hex, 'text': text,
                'attachments': [{'type': 'image', 'payload': {'url': self.source}}] if image else [],
            }}]}]}

    def saved_photo(self):
        body = self.body(image=True)
        return self.m.save_message(self.db, self.sender, 'incoming', 'image', '', self.source,
                                   None, None, body)

    def archived_photo(self):
        message_id = self.saved_photo()
        with patch.object(self.m.inbound_media, 'fetch', return_value=(self.png, 'image', '.png', 'image/png')):
            self.assertEqual(self.m._archive_incoming_message_media(self.db, message_id), 1)
        row = self.db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchone()
        entry = json.loads(row['media_json'])[0]
        self.assertTrue(Path(self.m.INCOMING_MEDIA_DIR, entry['local_file']).is_file())
        return message_id, dict(row), entry

    def integration(self, vision):
        stack = ExitStack()
        stack.enter_context(patch.object(self.m, 'load_products_from_file', return_value=[self.product]))
        stack.enter_context(patch.object(self.m, 'load_active_products', return_value=[self.product]))
        stack.enter_context(patch.object(self.m, 'get_auto_product_settings', return_value={'enabled': False}))
        stack.enter_context(patch.object(self.m, 'is_ai_enabled', return_value=True))
        stack.enter_context(patch.object(self.m, 'schedule_followup_if_needed'))
        stack.enter_context(patch.object(self.m, '_resolve_catalog_image_paths', return_value=[]))
        stack.enter_context(patch.object(self.m, 'send_order_to_telegram', return_value=False))
        stack.enter_context(patch.object(self.m, 'send_telegram_message', return_value=False))
        stack.enter_context(patch.object(self.m, 'send_image_to_facebook', return_value=True))
        send = stack.enter_context(patch.object(self.m, 'send_text_to_facebook', return_value=True))
        download = stack.enter_context(patch.object(self.m, '_download_image_to_data_url', return_value=self.data_url))
        recognize = stack.enter_context(patch.object(self.m, 'match_customer_image_with_catalog', return_value=vision))
        model = stack.enter_context(patch.object(self.m, '_call_main_ai_once', return_value={
            'reply': 'الموديل متوفر عيني، قماشه قطن وقياساته من 38 إلى 52. شنو قياسج؟',
            'create_order': False, 'order': {}, 'requires_human': False}))
        return stack, send, download, recognize, model

    def process(self, body):
        result = self.m.process_webhook(self.db, body, use_debounce=False)
        if result.get('reply'):
            self.m.send_webhook_result_to_facebook(result, self.sender)
        return result

    def assert_received_without_reasking(self, reply):
        self.assertTrue(str(reply or '').strip(), 'An already received photo needs an explicit acknowledgement')
        normalized = self.m.sales_strategy.normalized(reply)
        self.assertRegex(normalized, r'(?:الصوره|الصورة|صورتج|صورتك).{0,25}(?:وصل|عندي|استلم|موجود|محفوظ)|'
                                    r'(?:وصل|استلم|شفت|عندي|محفوظ).{0,25}(?:الصوره|الصورة|صورتج|صورتك)')
        self.assertNotRegex(normalized, r'(?:دز|ارسل|ابعث).{0,25}(?:صوره|صورة)|'
                                       r'(?:صوره|صورة).{0,20}(?:من جديد|مره ثانيه|مرة ثانية)')
        self.assertNotIn('17000', normalized.replace(',', '').replace('٬', ''),
                         'An unknown image cannot borrow the only catalog product price')
        self.assertNotRegex(normalized, r'17\s*(?:الف|ألف)|تم تثبيت|تم حجز')

    def test_successful_photo_then_price_followup_keeps_identity_without_reanalysis(self):
        stack, send, download, recognize, model = self.integration(
            dict(product_found=True, product_id=self.product['product_id'], confidence=100))
        with stack:
            first = self.process(self.body(image=True))
            self.assertTrue(first['meta']['catalog_match'])
            self.assertTrue(first['meta']['auto_reply']['sent'])
            self.assertEqual(self.m.get_active_product_binding(self.db, self.sender)['product_id'], self.product['product_id'])
            before_calls = download.call_count
            model.return_value = {'reply': 'دزيلي صورة الموديل حتى أعرف سعره.', 'create_order': False, 'order': {}}
            second = self.process(self.body('شكد سعره؟'))
            self.assertIn('17,000', second['reply'])
            self.assertFalse(self.m.reply_reasks_known_product(second['reply']))
            self.assertNotRegex(second['reply'], r'دزي|أرسلي|ارسلي')
            self.assertEqual(download.call_count, before_calls)
            self.assertEqual(recognize.call_count, 1)
            self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 0)

    def test_unknown_photo_then_price_followup_acknowledges_receipt_without_requesting_it_again(self):
        stack, send, download, recognize, model = self.integration(
            dict(product_found=False, reason='Synthetic catalog NONE'))
        with stack:
            first = self.process(self.body(image=True))
            self.assertTrue(first['meta'].get('image_unresolved'))
            model.return_value = {'reply': 'دزي صورة الموديل حتى أعرف سعره.', 'create_order': False, 'order': {}}
            second = self.process(self.body('شكد سعره؟'))
            self.assert_received_without_reasking(second.get('reply'))
            self.assertIsNone(self.m.get_active_product_binding(self.db, self.sender))
            self.assertEqual(recognize.call_count, 1)

    def test_image_service_failure_then_price_followup_acknowledges_receipt_without_stock_claim(self):
        stack, send, download, recognize, model = self.integration(dict(
            product_found=False, service_error=True, error_code='invalid_vision_response',
            reason='Synthetic provider returned incomplete analysis'))
        with stack:
            first = self.process(self.body(image=True))
            self.assertTrue(first['meta'].get('service_error'))
            self.assertFalse(first['meta'].get('image_unresolved', False))
            model.return_value = {'reply': 'أرسلي صورة الموديل حتى أعرف سعره.', 'create_order': False, 'order': {}}
            second = self.process(self.body('شكد سعره؟'))
            self.assert_received_without_reasking(second.get('reply'))
            self.assertNotIn('مو متوفر', second['reply'])
            self.assertIsNone(self.m.get_active_product_binding(self.db, self.sender))
            self.assertEqual(recognize.call_count, 1)

    def test_archived_photo_recovers_successful_cache_when_remote_signed_url_expires(self):
        message_id, row, entry = self.archived_photo()
        event = dict(sender_id=self.sender, image_url=self.source,
                     attachments=self.m.message_media(row), message_id=message_id, text='')
        with patch.object(self.m, '_resolve_catalog_image_paths', return_value=[]), \
             patch.object(self.m, '_download_image_to_data_url', return_value=self.data_url), \
             patch.object(self.m, 'match_customer_image_with_catalog', return_value={
                 'product_found': True, 'product_id': self.product['product_id'], 'confidence': 100}):
            matched, _, _ = self.m._match_single_product(self.db, event, [self.product])
            self.assertEqual(matched['product_id'], self.product['product_id'])
        with patch.object(self.m, '_resolve_catalog_image_paths', return_value=[]), \
             patch.object(self.m, '_download_image_to_data_url', side_effect=RuntimeError('Synthetic expired URL 403')) as download, \
             patch.object(self.m, 'match_customer_image_with_catalog') as recognize:
            recovered, _, result = self.m._match_single_product(self.db, event, [self.product])
        self.assertIsNotNone(recovered, 'The archived bytes and successful fingerprint must survive CDN expiry')
        self.assertEqual(recovered['product_id'], self.product['product_id'])
        self.assertFalse(result.get('service_error'))
        recognize.assert_not_called()
        download.assert_not_called()

    def test_unanswered_photo_collection_retains_identity_and_uses_private_archive_without_download(self):
        message_id, row, entry = self.archived_photo()
        saved_media = self.m.message_media(row)
        self.assertEqual(saved_media[0]['url'], self.source)
        with patch.object(self.m, '_resolve_catalog_image_paths', return_value=[]), \
             patch.object(self.m, '_download_image_to_data_url', side_effect=RuntimeError('Network forbidden for archived image')) as first_download, \
             patch.object(self.m, 'match_customer_image_with_catalog', return_value={
                 'product_found': True, 'product_id': self.product['product_id'], 'confidence': 100}):
            first, _, first_result = self.m._match_single_product(self.db,
                dict(sender_id=self.sender, image_url=self.source, attachments=saved_media, text=''), [self.product])
            self.assertEqual(first['product_id'], self.product['product_id'])
            first_download.assert_not_called()
        followup_id = self.m.save_message(self.db, self.sender, 'incoming', 'text', 'شكد سعره؟', None,
                                          None, None, self.body('شكد سعره؟'))
        event = dict(sender_id=self.sender, text='شكد سعره؟', image_url=None, attachments=[])
        combined = self.m.collect_unanswered_event(self.db, event, followup_id)
        image = next(item for item in combined['attachments'] if item['type'] == 'image')
        self.assertEqual(image['url'], self.source)
        with patch.object(self.m, '_resolve_catalog_image_paths', return_value=[]), \
             patch.object(self.m, '_download_image_to_data_url', side_effect=RuntimeError('Synthetic expired URL 403')) as download, \
             patch.object(self.m, 'match_customer_image_with_catalog') as recognize:
            recovered, _, result = self.m._match_single_product(self.db, combined, [self.product])
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered['product_id'], self.product['product_id'])
        self.assertEqual(result['image_fingerprint'], first_result['image_fingerprint'])
        download.assert_not_called()
        recognize.assert_not_called()

    def test_old_recognized_photo_in_same_second_is_not_reattached_after_trusted_link(self):
        message_id = self.saved_photo()
        self.m.complete_customer_product_link(self.db, self.sender, self.product, 'image_recognition', source='image_recognition')
        timestamp = '2026-10-24T17:00:00+03:00'
        self.db.execute('UPDATE messages SET created_at=? WHERE id=?', (timestamp, message_id))
        self.db.execute('UPDATE customer_product_interests SET last_seen_at=? WHERE sender_id=?', (timestamp, self.sender))
        self.m.customer_image_positions(self.db, self.sender, [dict(image_index=1, image_url=self.source,
            product_found=True, product_id=self.product['product_id'], product_name=self.product['product_name'])])
        followup_id = self.m.save_message(self.db, self.sender, 'incoming', 'text', 'شكد سعره؟', None, None, None, self.body('شكد سعره؟'))
        self.db.commit()
        event = dict(sender_id=self.sender, text='شكد سعره؟', image_url=None, attachments=[])
        with patch.object(self.m, 'load_products_from_file', return_value=[self.product]):
            self.assertEqual(self.m.load_customer_products(self.db, self.sender)[0]['product_id'], self.product['product_id'])
            combined = self.m.collect_unanswered_event(self.db, event, followup_id)
        self.assertFalse(combined.get('image_url'), 'A known photo must not be downloaded again because timestamps have only second precision')
        self.assertFalse(any(item['type'] == 'image' for item in combined.get('attachments', [])))

    def test_new_photo_with_equal_timestamp_survives_text_followup_after_old_photo_is_resolved(self):
        known_photo_id = self.saved_photo()
        self.m.complete_customer_product_link(self.db, self.sender, self.product, 'image_recognition', source='image_recognition')
        self.m.customer_image_positions(self.db, self.sender, [dict(
            image_index=1, image_url=self.source, product_found=True,
            product_id=self.product['product_id'], product_name=self.product['product_name'])])
        fresh = 'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=synthetic-new-in-same-second'
        body = self.body(image=True)
        body['entry'][0]['messaging'][0]['message']['attachments'][0]['payload']['url'] = fresh
        fresh_photo_id = self.m.save_message(self.db, self.sender, 'incoming', 'image', '', fresh,
                                             None, None, body)
        timestamp = '2026-10-24T17:00:00+03:00'
        self.db.execute('UPDATE messages SET created_at=? WHERE id IN (?,?)',
                        (timestamp, known_photo_id, fresh_photo_id))
        self.db.execute('UPDATE customer_product_interests SET last_seen_at=? WHERE sender_id=?',
                        (timestamp, self.sender))
        followup_id = self.m.save_message(self.db, self.sender, 'incoming', 'text', 'وهذا شكد سعره؟',
                                          None, None, None, self.body('وهذا شكد سعره؟'))
        event = dict(sender_id=self.sender, text='وهذا شكد سعره؟', image_url=None, attachments=[])
        with patch.object(self.m, 'load_products_from_file', return_value=[self.product]):
            self.assertEqual(self.m.load_customer_products(self.db, self.sender)[0]['product_id'], self.product['product_id'])
            combined = self.m.collect_unanswered_event(self.db, event, followup_id)
        self.assertEqual(combined['image_url'], fresh)
        images = [item['url'] for item in combined.get('attachments', []) if item['type'] == 'image']
        self.assertEqual(images, [fresh], 'Equal timestamps cannot hide an unresolved photo with a different URL')

    def test_a_fresh_different_photo_is_not_suppressed_by_existing_product_memory(self):
        self.m.complete_customer_product_link(self.db, self.sender, self.product, 'image_recognition', source='image_recognition')
        fresh = 'https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=synthetic-different-photo'
        body = self.body(image=True)
        body['entry'][0]['messaging'][0]['message']['attachments'][0]['payload']['url'] = fresh
        message_id = self.m.save_message(self.db, self.sender, 'incoming', 'image', '', fresh, None, None, body)
        event = dict(sender_id=self.sender, text='', image_url=fresh, attachments=[{'type': 'image', 'url': fresh}])
        combined = self.m.collect_unanswered_event(self.db, event, message_id)
        self.assertEqual(combined['image_url'], fresh)


if __name__ == '__main__':
    unittest.main()
