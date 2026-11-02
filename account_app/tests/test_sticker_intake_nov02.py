"""Sticker metadata survives transport without entering product image matching."""
import io
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from account_app import media
from account_app.tests import test_chatwoot, test_zernio


class StickerMediaTests(unittest.TestCase):
    def test_explicit_sticker_keeps_type_after_storage_round_trip(self):
        payload = {'message': {'sticker_id': 'messenger-sticker', 'attachments': [
            {'type': 'image', 'payload': {'url': 'https://images.test/thumb.png'}}]}}
        items = media.extract_media(payload)
        self.assertEqual(items, [{'type': 'sticker', 'url': 'https://images.test/thumb.png'}])
        row = dict(direction='incoming', media_json=json.dumps(items),
                   image_url='https://images.test/thumb.png')
        self.assertEqual(media.message_media(row), items)
        self.assertEqual(media.extract_media({'message': {'sticker_id': '42',
                         'image_url': 'https://images.test/thumb.png'}}), items)

    def test_sticker_and_product_photo_remain_separate(self):
        payload = {'attachments': [
            {'type': 'sticker', 'url': 'https://images.test/sticker.png'},
            {'type': 'image', 'url': 'https://images.test/dress.png'}]}
        items = media.extract_media(payload)
        self.assertEqual([item['type'] for item in items], ['sticker', 'image'])
        self.assertEqual([item['url'] for item in items if item['type'] == 'image'],
                         ['https://images.test/dress.png'])

    def test_small_picture_or_blue_icon_is_not_automatically_a_sticker(self):
        for color in ('white', 'blue', 'red'):
            output = io.BytesIO()
            Image.new('RGB', (72, 72), color).save(output, 'PNG')
            self.assertFalse(media.known_sticker(output.getvalue()))
        self.assertFalse(media.known_sticker(b'not an image'))
        self.assertFalse(media.known_sticker(b'x' * 65537))
        self.assertFalse(media.sticker_attachment({'type': 'image', 'width': 72, 'height': 72}))

    def test_metadata_markers_are_explicit_and_do_not_use_truthy_false(self):
        for attachment, metadata in (({'type': 'sticker'}, {}),
                                      ({'type': 'image'}, {'image_type': 'sticker'}),
                                      ({'type': 'image'}, {'isSticker': True}),
                                      ({'type': 'image', 'payload': {'sticker_id': '42'}}, {})):
            self.assertTrue(media.sticker_attachment(attachment, metadata))
        self.assertFalse(media.sticker_attachment({'isSticker': 'false', 'type': 'image'}))


class ChatwootStickerTests(unittest.TestCase):
    setUp = test_chatwoot.ChatwootTests.setUp
    tearDown = test_chatwoot.ChatwootTests.tearDown
    post = test_chatwoot.ChatwootTests.post

    def sticker_body(self, text=''):
        self.payload['content'] = text
        self.payload['content_attributes'] = {'image_type': 'sticker'}
        self.payload['attachments'] = [{'file_type': 'image', 'data_url': 'https://images.test/thumb.png'}]
        with patch.object(test_chatwoot.m, '_process_manychat_webhook_async') as process:
            self.assertEqual(self.post().status_code, 200)
        return process.call_args.args[0]

    def test_sticker_is_saved_without_product_vision_provider_handoff_or_pause(self):
        m = test_chatwoot.m
        body = self.sticker_body()
        sender = f'{m.DEFAULT_STORE_ID}::cw-1-3'
        with m.app.app_context(), patch.object(m, 'match_customer_image_with_catalog') as vision, \
                patch.object(m, '_call_main_ai_once') as model, patch.object(m, 'create_human_review') as review:
            db = m.get_db()
            result = m.process_webhook(db, body, use_debounce=False)
            self.assertEqual(result['meta']['reason'], 'non_product_sticker')
            row = db.execute('SELECT * FROM messages WHERE sender_id=?', (sender,)).fetchone()
            self.assertEqual(row['message_type'], 'sticker')
            self.assertEqual(media.message_media(row)[0]['type'], 'sticker')
            self.assertTrue(m.is_customer_ai_enabled(db, sender))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM human_reviews WHERE sender_id=?', (sender,)).fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM conversation_pause_state WHERE sender_id=?', (sender,)).fetchone()[0], 0)
        vision.assert_not_called()
        model.assert_not_called()
        review.assert_not_called()

    def test_text_with_sticker_answers_the_question_using_saved_product(self):
        m = test_chatwoot.m
        body = self.sticker_body('شنو نوع القماش')
        sender = f'{m.DEFAULT_STORE_ID}::cw-1-3'
        product = dict(product_id='P1', product_name='قميص الاختبار', fabric='قطن', stock='متوفر',
                       status='active', colors='أسود', sizes='38 إلى 52', price='12000')
        with m.app.app_context():
            db = m.get_db()
            m.bind_customer_to_product(db, sender, product, source='manual_admin')
            m.save_message(db, sender, 'outgoing', 'text', 'شنو تحبين تسألين؟', None, None, None, {})
            with patch.object(m, 'load_products_from_file', return_value=[product]), \
                    patch.object(m, 'load_active_products', return_value=[product]), \
                    patch.object(m, 'is_ai_enabled', return_value=True), \
                    patch.object(m, 'is_customer_ai_enabled', return_value=True), \
                    patch.object(m, 'match_customer_image_with_catalog') as vision, \
                    patch.object(m, 'create_human_review') as review:
                result = m.process_webhook(db, body, use_debounce=False)
            self.assertIn('قطن', result['reply'])
            self.assertNotIn('non_product_sticker', str(result.get('meta')))
        vision.assert_not_called()
        review.assert_not_called()

    def test_sticker_archive_preserves_display_type_and_skips_repeat_download(self):
        m = test_chatwoot.m
        body = self.sticker_body()
        body['entry'][0]['messaging'][0]['message']['attachments'][0]['payload']['url'] = \
            'https://app.chatwoot.com/media/sticker.png'
        sender = f'{m.DEFAULT_STORE_ID}::cw-1-3'
        output = io.BytesIO()
        Image.new('RGB', (72, 72), 'blue').save(output, 'PNG')
        with m.app.app_context(), patch.object(m, 'INCOMING_MEDIA_DIR', self.temp.name):
            db = m.get_db()
            m.process_webhook(db, body, use_debounce=False)
            mid = db.execute('SELECT id FROM messages WHERE sender_id=?', (sender,)).fetchone()[0]
            with patch.object(m.inbound_media, 'fetch', return_value=(output.getvalue(), 'image', '.png', 'image/png')) as fetch:
                self.assertEqual(m._archive_incoming_message_media(db, mid), 1)
                self.assertEqual(m._archive_incoming_message_media(db, mid), 0)
            fetch.assert_called_once()
            row = db.execute('SELECT * FROM messages WHERE id=?', (mid,)).fetchone()
            self.assertEqual(m._dashboard_message_media(row)[0]['type'], 'sticker')
            self.assertEqual(media.message_media(row)[0]['type'], 'sticker')

    def test_already_archived_known_sticker_is_reclassified_without_redownload(self):
        m = test_chatwoot.m
        sender = f'{m.DEFAULT_STORE_ID}::cw-1-3'
        source = 'https://app.chatwoot.com/media/thumb.png'
        filename = 'a' * 32 + '.png'
        Path(self.temp.name, filename).write_bytes(b'local archived sticker fixture')
        with m.app.app_context(), patch.object(m, 'INCOMING_MEDIA_DIR', self.temp.name):
            db = m.get_db()
            mid = m.save_message(db, sender, 'incoming', 'image', '', source, None, None,
                {'attachments': [{'type': 'image', 'payload': {'url': source}}]})
            db.execute('UPDATE messages SET media_json=? WHERE id=?',
                (json.dumps([dict(type='image', url=source, local_file=filename)]), mid))
            db.commit()
            with patch.object(m, 'known_sticker', return_value=True), patch.object(m.inbound_media, 'fetch') as fetch:
                m._archive_incoming_message_media(db, mid)
            fetch.assert_not_called()
            row = db.execute('SELECT * FROM messages WHERE id=?', (mid,)).fetchone()
            self.assertEqual(media.message_media(row)[0]['type'], 'sticker')

    def test_chatwoot_marker_is_preserved_for_sticker_only_or_text_and_sticker(self):
        for text in ('', 'عمر أربع سنوات'):
            self.payload['content'] = text
            self.payload['content_attributes'] = {'image_type': 'sticker'}
            self.payload['attachments'] = [{'file_type': 'image', 'data_url': 'https://images.test/thumb.png'}]
            with patch.object(test_chatwoot.m, '_process_manychat_webhook_async') as process:
                self.assertEqual(self.post().status_code, 200)
                body = process.call_args.args[0]
                event = test_chatwoot.m.extract_facebook_event(body)
            self.assertIsNone(event['image_url'])
            self.assertEqual(event['text'], text)
            self.assertEqual(body['entry'][0]['messaging'][0]['message']['attachments'][0]['type'], 'sticker')
            self.assertTrue(event['_has_sticker'])
            self.assertEqual(event['attachments'], [])

    def test_small_real_product_photo_stays_image_after_fingerprint_check(self):
        output = io.BytesIO()
        Image.new('RGB', (72, 72), 'blue').save(output, 'PNG')
        self.payload['content'] = ''
        self.payload['attachments'] = [dict(file_type='image', data_url='https://images.test/product.png',
                                           width=72, height=72, file_size=100)]
        with patch.object(test_chatwoot.m.inbound_media, 'fetch', return_value=(output.getvalue(), 'image', '.png', 'image/png')), \
                patch.object(test_chatwoot.m, '_process_manychat_webhook_async') as process:
            self.assertEqual(self.post().status_code, 200)
            event = test_chatwoot.m.extract_facebook_event(process.call_args.args[0])
        self.assertEqual(event['image_url'], 'https://images.test/product.png')
        self.assertEqual(event['attachments'][0]['type'], 'image')


class ZernioStickerTests(unittest.TestCase):
    setUp = test_zernio.ZernioTests.setUp
    tearDown = test_zernio.ZernioTests.tearDown
    post = test_zernio.ZernioTests.post

    def test_zernio_explicit_sticker_and_real_photo_keep_separate_types(self):
        self.payload['message']['text'] = ''
        self.payload['message']['attachments'] = [
            {'type': 'sticker', 'url': 'https://images.test/sticker.webp'},
            {'type': 'image', 'url': 'https://images.test/product.jpg'}]
        with patch.object(test_zernio.m, '_process_manychat_webhook_async') as process:
            self.assertEqual(self.post().status_code, 200)
            body = process.call_args.args[0]
            event = test_zernio.m.extract_facebook_event(body)
        self.assertEqual(event['image_url'], 'https://images.test/product.jpg')
        self.assertEqual([item['type'] for item in body['entry'][0]['messaging'][0]['message']['attachments']],
                         ['sticker', 'image'])
        self.assertTrue(event['_has_sticker'])
        self.assertEqual([item['type'] for item in event['attachments']], ['image'])


if __name__ == '__main__':
    unittest.main()
