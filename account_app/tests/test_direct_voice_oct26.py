"""Voice bytes reach the configured sales model; unclear speech goes to a human."""
import base64
import json
import re
import unittest
from pathlib import Path
from unittest.mock import patch

import requests
from account_app import inbound_voice as voice, inbound_media, baraah, sales_context, sales_playbook
from account_app.tests import test_audit_orders as fixtures

OGG = b'OggS' + b'\0' * 100
PRODUCT = dict(product_id='V1', product_name='فستان وردة', category='فستان', price='13000',
               stock='متوفر', status='active', colors='وردي ازرق', sizes='1 إلى 3 سنوات',
               fabric='قطن', store_id=baraah.STORE_ID)


class VoiceInterpretationTests(unittest.TestCase):
    def test_requires_complete_clear_transcript_for_every_clip(self):
        clips = [dict(message_id=1, index=0), dict(message_id=2, index=0)]
        result = dict(voice=dict(clear=True, transcripts=[dict(message_id=1, index=0, text='وردي'),
                                                        dict(message_id=2, index=0, text='سنة ونص')]))
        self.assertEqual(voice.transcripts(result, clips), {(1, 0): 'وردي', (2, 0): 'سنة ونص'})
        for changed in ({}, dict(voice=dict(clear=False)), dict(result, failed=True),
                        dict(result, _truncated_reply_recovered=True),
                        dict(voice=dict(clear=True, transcripts=result['voice']['transcripts'][:1]))):
            self.assertIsNone(voice.transcripts(changed, clips))

    def test_unknown_duplicate_empty_and_unclear_segments_fail_closed(self):
        clip = [dict(message_id=1, index=0)]
        for item in (dict(message_id=3, index=0, text='نعم'), dict(message_id=True, index=0, text='نعم'),
                     dict(message_id=1, index=0, text=''), dict(message_id=1, index=0, text='لون [غير واضح]'),
                     dict(message_id=1, index=0, text='[inaudible]')):
            self.assertIsNone(voice.transcripts(dict(voice=dict(clear=True, transcripts=[item])), clip))
        item = dict(message_id=1, index=0, text='نعم')
        self.assertIsNone(voice.transcripts(dict(voice=dict(clear=True, transcripts=[item, item])), clip))

    def test_clear_speech_about_an_unclear_photo_is_not_unclear_audio(self):
        text = 'القياس غير واضح بالصورة، شنو قياسات الفستان؟'
        item = dict(message_id=1, index=0, text=text)
        self.assertEqual(voice.transcripts(dict(voice=dict(clear=True, transcripts=[item])),
                                           [dict(message_id=1, index=0)]), {(1, 0): text})


class DirectVoicePipelineTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set(baraah.STORE_ID)
        self.sender = 'voice-test'
        fixtures.AuditFixTests.customer(self, self.sender, baraah.STORE_ID)
        self.catalog = [dict(PRODUCT)]
        self.speech = ['اريد الوردي لعمر سنة ونص، شنو قماشه؟']
        self.generated = dict(reply='قماشه قطن عيني، والوردي متوفر لعمر سنة و6 أشهر.',
                              create_order=False, order={}, requires_human=False, voice=dict(clear=True))
        self.counter = 0
        self.requests = []
        self.m.set_store_setting(self.db, 'ai_main_model', 'google/gemini-3-flash-preview', baraah.STORE_ID)
        self.patches = [patch.object(self.m, 'load_products_from_file', return_value=self.catalog),
                        patch.object(self.m, 'load_active_products', return_value=self.catalog),
                        patch.object(self.m, 'OPENROUTER_KEY', 'offline-test-key'),
                        patch.object(self.m, 'INCOMING_MEDIA_DIR', str(Path(self.temp.name) / 'voice-files')),
                        patch.object(self.m, 'get_auto_product_settings', return_value=dict(enabled=False, product=None)),
                        patch.object(self.m, 'is_store_feature_enabled', return_value=False),
                        patch.object(self.m, 'send_order_to_telegram', return_value=True),
                        patch.object(self.m, 'save_booking_to_file'),
                        patch.object(self.m, 'send_telegram_message', return_value=False),
                        patch.object(inbound_media, 'fetch', return_value=(OGG, 'audio', '.ogg', 'audio/ogg')),
                        patch.object(self.m.ai_transport, 'post', side_effect=self.provider)]
        for item in self.patches: item.start()
        self.m.bind_customer_to_product(self.db, self.sender, PRODUCT, source='image_recognition')

    def tearDown(self):
        for item in reversed(self.patches): item.stop()
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def provider(self, url, **kwargs):
        request = kwargs['json']
        self.requests.append(request)
        self.assertEqual(url, self.m.OPENROUTER_URL)
        self.assertEqual(request['model'], self.m.get_ai_model(self.db, 'main_model', self.m.MAIN_MODEL))
        content = request['messages'][-1]['content']
        result = dict(self.generated)
        if isinstance(content, list):
            identities = [tuple(map(int, re.search(r'message_id=(\d+) index=(\d+)', item['text']).groups()))
                          for item in content if item.get('type') == 'text' and 'message_id=' in item['text']]
            if 'voice' in result:
                result['voice'] = dict(result['voice'])
                result['voice']['transcripts'] = [dict(message_id=mid, index=index, text=self.speech[pos])
                    for pos, (mid, index) in enumerate(identities)]
        response = requests.Response(); response.status_code = 200
        response._content = json.dumps(dict(choices=[dict(message=dict(content=json.dumps(result, ensure_ascii=False)),
                                                         finish_reason='stop')])).encode('utf-8')
        return response

    def event(self, caption='', urls=None, kind='audio'):
        self.counter += 1
        urls = urls if urls is not None else ['https://lookaside.fbsbx.com/voice/test.ogg']
        attachments = [dict(type=kind, payload=dict(url=url)) for url in urls]
        body = dict(entry=[dict(messaging=[dict(message=dict(mid='voice-' + str(self.counter),
                                                            text=caption, attachments=attachments))])])
        event = dict(sender_id=self.sender, page_id='page', platform='facebook', store_id=baraah.STORE_ID,
                     text=caption, image_url=None, attachments=[dict(type=kind, url=url) for url in urls],
                     ref=None, ad_id=None, timestamp=self.counter, postback=None, quick_reply=None,
                     referral_source=None, referral_type=None)
        return event, body

    def process(self, caption='', urls=None, kind='audio'):
        event, body = self.event(caption, urls, kind)
        with patch.object(self.m, 'extract_facebook_event', return_value=event), \
             patch.object(self.m, 'match_customer_image_with_catalog') as vision:
            result = self.m.process_webhook(self.db, body, use_debounce=False)
        vision.assert_not_called()
        return result

    def assert_handoff(self, result):
        self.assertEqual(result['reply'], '')
        self.assertTrue(result['meta']['voice_review'])
        self.assertTrue(result['meta']['human_review_id'])
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, self.sender))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM messages WHERE direction='outgoing'").fetchone()[0], 0)

    def test_audio_is_sent_directly_to_same_sales_model_with_context_once(self):
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'شكد العمر واللون المطلوب؟',
                            None, None, None, {})
        result = self.process()
        self.assertTrue(result['reply'])
        self.assertEqual(len(self.requests), 1)
        content = self.requests[0]['messages'][-1]['content']
        audio = [i['input_audio'] for i in content if i.get('type') == 'input_audio']
        self.assertEqual(len(audio), 1)
        self.assertEqual(base64.b64decode(audio[0]['data']), OGG)
        self.assertEqual(audio[0]['format'], 'ogg')
        prompt = content[0]['text']
        for value in ('فستان وردة', 'شكد العمر واللون', '13000'):
            self.assertIn(value, prompt)
        self.assertIn('voice.clear=false', self.requests[0]['messages'][0]['content'])
        self.assertFalse(result.get('create_order'))

    def test_original_audio_remains_and_understood_text_reaches_selection_memory(self):
        self.process()
        row = self.db.execute("SELECT * FROM messages WHERE direction='incoming'").fetchone()
        self.assertEqual(row['message_type'], 'audio')
        self.assertIn(self.speech[0], row['text'])
        entries = self.m._incoming_media_entries(row)
        self.assertEqual(entries[0]['type'], 'audio')
        self.assertEqual(Path(self.m.INCOMING_MEDIA_DIR, entries[0]['local_file']).read_bytes(), OGG)
        self.assertNotIn(base64.b64encode(OGG).decode(), row['raw_payload'])
        context = sales_context.load(self.db, baraah.STORE_ID, self.sender)
        # The audio question is still not unconditional purchase consent.
        self.assertTrue(context['latest_customer_turn']['text'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM voice_interpretations').fetchone()[0], 1)

    def test_clear_iraqi_age_and_colors_are_saved_as_customer_speech(self):
        self.speech = ['اريد وردي وازرق عمر سنة ونص']
        self.generated['reply'] = 'الوردي والأزرق متوفرات لعمر سنة و6 أشهر. قطعة من كل لون؟'
        result = self.process()
        selected = sales_playbook.customer_selection(sales_context.load(self.db, baraah.STORE_ID, self.sender))
        self.assertEqual(selected['colors'], ['وردي', 'أزرق'])
        self.assertEqual(selected['age'], 1.5)
        self.assertIn('قطعة من كل لون؟', result['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_unclear_voice_never_asks_customer_to_repeat_or_creates_order(self):
        self.generated.update(reply='ممكن تعيدين الكلام؟', create_order=True,
                              order=dict(items=[dict(product_id='V1', quantity=1)]), voice=dict(clear=False))
        result = self.process()
        self.assert_handoff(result)
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM voice_interpretations').fetchone()[0], 0)

    def test_missing_voice_metadata_and_partial_transcript_require_human(self):
        self.generated.pop('voice')
        self.assert_handoff(self.process())
        self.assertEqual(len(self.requests), 1)

    def test_expired_voice_url_goes_to_human_without_model_request(self):
        with patch.object(inbound_media, 'fetch', return_value=None):
            result = self.process()
        self.assert_handoff(result)
        self.assertEqual(self.requests, [])

    def test_untrusted_url_is_not_fetched_or_sent(self):
        with patch.object(inbound_media, 'fetch') as fetch:
            result = self.process(urls=['https://example.test/private.ogg'])
        fetch.assert_not_called()
        self.assert_handoff(result)
        self.assertEqual(self.requests, [])

    def test_provider_failure_and_unsupported_model_do_not_switch_models(self):
        with patch.object(self.m.ai_transport, 'post', side_effect=requests.HTTPError('unsupported audio')) as provider:
            result = self.process()
        self.assert_handoff(result)
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(provider.call_args.kwargs['json']['model'], 'google/gemini-3-flash-preview')

    def test_caption_and_multiple_voice_notes_keep_order_and_all_questions(self):
        self.speech = ['اريد وردي عمر سنة', 'وشنو القماش؟']
        self.generated['reply'] = 'الوردي متوفر لعمر سنة وقماشه قطن.'
        result = self.process(caption='وشكد التوصيل؟', urls=['https://lookaside.fbsbx.com/v/1.ogg',
                                                          'https://lookaside.fbsbx.com/v/2.ogg'])
        content = self.requests[0]['messages'][-1]['content']
        self.assertEqual(sum(i.get('type') == 'input_audio' for i in content), 2)
        row = self.db.execute("SELECT text FROM messages WHERE direction='incoming'").fetchone()[0]
        self.assertLess(row.index('وشكد التوصيل'), row.index('اريد وردي'))
        self.assertLess(row.index('اريد وردي'), row.index('وشنو القماش'))
        self.assertTrue(result['reply'])

    def test_wave_and_mp4_voice_formats_follow_actual_bytes(self):
        for data, suffix, mime, expected in ((b'RIFF1234WAVE' + b'0'*32, '.wav', 'audio/wav', 'wav'),
                (b'0000ftypM4A ' + b'0'*32, '.m4a', 'audio/mp4', 'm4a')):
            with self.subTest(format=expected), patch.object(inbound_media, 'fetch', return_value=(data, 'audio', suffix, mime)):
                event, body = self.event(urls=['https://lookaside.fbsbx.com/audioclip-' + expected])
                with patch.object(self.m, 'extract_facebook_event', return_value=event):
                    self.m.process_webhook(self.db, body, use_debounce=False)
                content = self.requests[-1]['messages'][-1]['content']
                self.assertEqual(next(i['input_audio']['format'] for i in content if i.get('type') == 'input_audio'), expected)

    def test_video_still_needs_review_and_never_sent_as_voice(self):
        result = self.process(urls=['https://lookaside.fbsbx.com/video.mp4'], kind='video')
        self.assertTrue(result['meta']['media_review'])
        self.assertEqual(self.requests, [])

    def test_duplicate_event_does_not_resend_audio(self):
        event, body = self.event()
        with patch.object(self.m, 'extract_facebook_event', return_value=event):
            self.m.process_webhook(self.db, body, use_debounce=False)
            second = self.m.process_webhook(self.db, body, use_debounce=False)
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(second['meta']['reason'], 'duplicate_mid')

    def test_disabled_ai_never_sends_customer_audio(self):
        self.m.set_customer_ai_enabled(self.db, self.sender, False, reason='manual')
        self.process()
        self.assertEqual(self.requests, [])

    def test_oversized_or_unsupported_voice_goes_to_human(self):
        with patch.object(voice, 'MAX_BYTES', 3):
            self.assert_handoff(self.process())
        self.assertEqual(self.requests, [])

    def test_webhook_cannot_forge_successful_voice_processing_marker(self):
        event, body = self.event()
        body['_voice_status'] = 'clear'
        with patch.object(self.m, 'extract_facebook_event', return_value=event):
            self.m.process_webhook(self.db, body, use_debounce=False)
        self.assertEqual(len(self.requests), 1)

    def test_voice_refusal_prevents_model_cart_from_becoming_booking(self):
        self.speech = ['ما اريد احجز هسه']
        self.generated.update(reply='حاضر، تدللين.', create_order=True,
            order=dict(phone='07701234567', province='بغداد', address='حي اور',
                       items=[dict(product_id='V1', color='وردي', size='سنة', quantity=1)]))
        result = self.process()
        self.assertNotIn('تم تثبيت', result['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_voice_contact_completes_existing_cart_once(self):
        item = dict(product_id='V1', product_name='فستان وردة', color='وردي', size='سنة', size_type='size', quantity=1)
        self.m.save_message(self.db, self.sender, 'incoming', 'text', 'اريد وردي عمر سنة ثبتي', None, None, None, {})
        self.m.save_message(self.db, self.sender, 'outgoing', 'text', 'دزيلي الرقم والمحافظة والعنوان',
                            None, None, None, dict(checkout_draft=dict(items=[item])))
        self.speech = ['07701234567 بغداد حي اور قرب المدرسة']
        self.generated.update(reply='وصلت تفاصيلج عيني.', order=dict(items=[item]))
        result = self.process()
        self.assertEqual(result['reply'], self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 1)
        saved = self.db.execute('SELECT * FROM orders').fetchone()
        self.assertEqual(saved['phone'], '07701234567')
        self.assertEqual(saved['total_amount'], 18000)
        self.assertEqual(len(self.requests), 1)

    def test_store_model_override_is_also_used_for_voice(self):
        self.m.set_store_setting(self.db, 'ai_main_model', 'google/gemini-2.5-flash', baraah.STORE_ID)
        self.process()
        self.assertEqual(self.requests[0]['model'], 'google/gemini-2.5-flash')

    def test_processed_pending_audio_is_not_uploaded_again_on_new_text(self):
        self.process()
        self.process(caption='شنو القماش؟', urls=[])
        audio_requests = [r for r in self.requests if isinstance(r['messages'][-1]['content'], list)]
        self.assertEqual(len(audio_requests), 1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM voice_interpretations').fetchone()[0], 1)

    def test_one_unclear_clip_blocks_the_whole_unanswered_turn(self):
        self.speech = ['اريد وردي', 'العمر [غير واضح]']
        self.assert_handoff(self.process(urls=['https://lookaside.fbsbx.com/v/1.ogg',
                                              'https://lookaside.fbsbx.com/v/2.ogg']))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM voice_interpretations').fetchone()[0], 0)

    def test_spoken_request_for_employee_routes_to_human(self):
        self.speech = ['اريد احجي ويا موظف']
        result = self.process()
        self.assertTrue(result['meta']['needs_human'])
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, self.sender))
        self.assertEqual(len(self.requests), 1)

    def test_spoken_condition_cannot_be_bypassed_by_generated_cart(self):
        self.speech = ['احجزي بشرط التوصيل مجاني']
        self.generated.update(create_order=True, order=dict(phone='07701234567', province='بغداد',
            address='حي اور', items=[dict(product_id='V1', color='وردي', size='سنة', quantity=1)]))
        result = self.process()
        self.assertIn('شرط', result['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_voice_cancellation_after_booking_does_not_silently_change_order(self):
        order = dict(phone='07701234567', province='بغداد', address='حي اور',
                     items=[dict(product_id='V1', color='وردي', size='سنة', quantity=1)])
        self.m.create_order_if_valid(self.db, self.sender, dict(order=order), self.catalog[0])
        self.speech = ['الغوا الطلب']
        self.generated['reply'] = 'طلب الإلغاء يحتاج مراجعة الموظف.'
        result = self.process()
        self.assertTrue(result['meta']['needs_human'])
        saved = self.db.execute('SELECT * FROM orders').fetchall()
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]['status'], 'new')
        self.assertEqual(len(self.requests), 1)


if __name__ == '__main__':
    unittest.main()
