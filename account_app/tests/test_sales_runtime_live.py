"""Exercise the production queue with signed test events and simulated providers."""
import hashlib
import hmac
import json
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from account_app import app as m
from account_app.sales_runtime import live
from account_app.sales_runtime.gateway import BACKUP_MODEL
from account_app.sales_runtime.orders import CONFIRMATION
from account_app.tests import test_chatwoot as fixtures


class LiveSalesRuntimeTests(unittest.TestCase):
    sender = 'khuyoot::cw-1-3'
    phone = '07701234567'
    address = 'بغداد حي التجربة قرب المدرسة'

    def setUp(self):
        fixtures.ChatwootTests.setUp(self)
        self.addCleanup(fixtures.ChatwootTests.tearDown, self)
        self.start_patch(patch.dict(os.environ, CHATWOOT_INBOX_STORES='{"1:2":"khuyoot"}',
                                    ENABLE_BACKGROUND_JOBS='1', SALES_RUNTIME_KHUYOOT_PERCENT='100'))
        self.start_patch(patch.object(m, 'OPENROUTER_KEY', 'synthetic-test-only'))
        self.start_patch(patch.object(m, 'DEBOUNCE_DELAY', 0))
        self.start_patch(patch.object(m.ai_jobs, 'start'))
        self.start_patch(patch.object(m, 'load_products_from_file', return_value=[dict(
            product_id='P1', store_id='khuyoot', product_name='قميص اختبار', stock='متوفر',
            colors='ماروني، أزرق', sizes='40 إلى 48', price='15000', status='active', ad_ids=['1234'])]))
        self.notify = self.start_patch(patch.object(m, 'send_order_to_telegram', return_value=True))
        self.start_patch(patch.object(m, 'save_booking_to_file'))
        self.transport = self.start_patch(patch.object(m.chatwoot, 'send',
            return_value={'ok': True, 'status_code': 200, 'provider_id': 'synthetic-receipt'}))
        self.payload['conversation']['additional_attributes'] = {'referral': {'ad_id': '1234'}}
        with self.client.session_transaction() as session:
            session.update(dashboard_authenticated=True, csrf_token='test-csrf')

    post = fixtures.ChatwootTests.post

    def start_patch(self, patcher):
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def incoming(self, mid, text, **message):
        response = self.post(dict(self.payload, id=mid, content=text, **message))
        self.assertEqual(200, response.status_code, response.get_json())

    def work(self):
        with m.app.app_context():
            db = m.get_db()
            db.execute("UPDATE ai_jobs SET available_at=0 WHERE status='queued'")
            db.commit()
            job = m.ai_jobs.claim(db, 'synthetic-worker')
            if job:
                result = m.run_ai_job(job, json.loads(job['payload']))
                m.ai_jobs.finish_result(db, job['id'], result)
                return job['kind'], result
        return None

    def model_plan(self, *, intent='buy', color='أزرق'):
        return dict(intent=intent, changes=[dict(values=dict(product_id='P1', size='44', color=color))],
                    contact={'phone': self.phone, 'address': self.address})

    @staticmethod
    def model_response(plan, status=200):
        response = Mock(status_code=status)
        response.json.return_value = {'choices': [{'message': {'parsed': plan}}]}
        return response

    def test_stock_editor_and_manual_order_share_the_same_inventory(self):
        products = m.load_products_from_file()
        endpoint = '/api/products/manage/P1/stock?store_id=khuyoot'
        catalog = '/api/products/manage?store_id=khuyoot'
        headers = {'X-CSRF-Token': 'test-csrf'}
        self.incoming(10, 'مرحبا')
        saved = self.client.get(catalog).json['products'][0]
        variants = [dict(color='ازرق', size='٤٤', quantity=2), dict(color='ماروني', size='44', quantity=1)]
        response = self.client.put(endpoint, headers=headers,
                                   json=dict(revision=saved['variant_stock_revision'], variants=variants))
        self.assertEqual(200, response.status_code, response.json)
        order_url = f'/api/conversations/{self.sender}/create_order?store_id=khuyoot'
        with patch.object(m, 'send_saved_order_confirmation', return_value=True):
            booked = self.client.post(order_url, headers=headers, json={
                'phone': self.phone, 'province': 'بغداد', 'address': self.address, 'items': [
                    {'product_id': 'P1', 'color': 'ماروني', 'size': '44', 'quantity': 1}]})
        self.assertEqual(200, booked.status_code, booked.json)
        latest = self.client.get(catalog).json['products'][0]
        self.assertEqual([2, 0], [row['quantity'] for row in latest['variant_stock']])
        available = self.client.get('/api/products?store_id=khuyoot').json['products'][0]
        self.assertEqual([dict(color='أزرق', size='44', quantity=2)], available['variant_stock'])
        self.assertTrue(available['tracks_variants'])
        stale = self.client.put(endpoint, headers=headers,
                                json=dict(revision=response.json['revision'], variants=variants))
        self.assertEqual(409, stale.status_code)
        change = f"/api/orders/{booked.json['order_id']}?store_id=khuyoot"
        self.assertEqual(200, self.client.patch(change, headers=headers, json={'color': 'أزرق'}).status_code)
        self.assertEqual([1, 1], [row['quantity'] for row in self.client.get(catalog).json['products'][0]['variant_stock']])
        self.assertEqual(200, self.client.patch(change, headers=headers, json={'status': 'cancelled'}).status_code)
        self.assertEqual([2, 1], [row['quantity'] for row in self.client.get(catalog).json['products'][0]['variant_stock']])
        self.assertEqual('40 إلى 48', products[0]['sizes'])

    def test_stock_mode_switch_controls_the_catalog_and_preserves_saved_counts(self):
        from account_app import variant_inventory
        endpoint = '/api/products/manage/P1/stock?store_id=khuyoot'
        editor = '/api/products/manage?store_id=khuyoot'
        public = '/api/products?store_id=khuyoot'
        headers = {'X-CSRF-Token': 'test-csrf'}
        variants = [dict(color='ماروني', size='44', quantity=1)]
        product = self.client.get(editor).json['products'][0]
        self.assertEqual('general', product['variant_stock_mode'])
        detailed = self.client.put(endpoint, headers=headers, json=dict(
            mode='detailed', variants=variants, revision=product['variant_stock_revision']))
        self.assertEqual(200, detailed.status_code, detailed.json)
        self.assertEqual(variants, self.client.get(public).json['products'][0]['variant_stock'])
        with m.app.app_context():
            db = m.get_db()
            items = [dict(product_id='P1', color='ماروني', size='44', quantity=1)]
            with variant_inventory.transaction(db):
                variant_inventory.allocate(db, 501, 'khuyoot', items, m.load_products_from_file())
        detailed = self.client.get(editor).json['products'][0]
        general = self.client.put(endpoint, headers=headers, json=dict(
            mode='general', revision=detailed['variant_stock_revision']))
        self.assertEqual(200, general.status_code, general.json)
        self.assertEqual([dict(variants[0], quantity=0)], general.json['variants'])
        current = self.client.get(public).json['products'][0]
        self.assertFalse(current['tracks_variants'])
        self.assertEqual('40 إلى 48', current['sizes'])
        self.assertIn('أزرق', current['colors'])
        with m.app.app_context():
            db = m.get_db()
            with variant_inventory.transaction(db):
                variant_inventory.allocate(db, 502, 'khuyoot', items, m.load_products_from_file())
                variant_inventory.allocate(db, 501, 'khuyoot', [], [])
            self.assertEqual(1, variant_inventory.rows_by_product(db, 'khuyoot')['P1'][0]['quantity'])
            self.assertFalse(db.execute('SELECT 1 FROM order_variant_stock WHERE order_id=502').fetchone())
        product = self.client.get(editor).json['products'][0]
        enabled = self.client.put(endpoint, headers=headers, json=dict(mode='detailed', variants=variants,
                                  revision=product['variant_stock_revision']))
        self.assertEqual(200, enabled.status_code, enabled.json)
        self.assertTrue(self.client.get(public).json['products'][0]['tracks_variants'])

    def test_model_reads_variant_quantities_only_in_detailed_mode(self):
        from account_app import variant_inventory
        from account_app.sales_runtime.facts import product_fact

        rows = [dict(color='أزرق', size='44', quantity=3), dict(color='ماروني', size='44', quantity=0)]
        with m.app.app_context():
            db = m.get_db()
            variant_inventory.replace(db, 'khuyoot', 'P1', rows)
            db.commit()
        plan = self.model_response(dict(intent='continue'))
        wording = self.model_response(dict(parts=[
            dict(type='text', text='الأزرق موجود بقياس 44، المتبقي 3 قطع.'),
        ]))
        self.incoming(10, 'متوفر أزرق قياس 44؟')
        with patch.object(m.ai_transport, 'post', side_effect=[plan, wording, wording, wording]) as provider:
            self.work()
        decision = json.loads(provider.call_args_list[0].kwargs['json']['messages'][1]['content'])
        copy = json.loads(provider.call_args_list[1].kwargs['json']['messages'][1]['content'])
        self.assertEqual(rows, decision['products'][0]['variant_stock'])
        self.assertEqual(3, copy['variant_stock']['selected_quantity'])
        self.assertEqual([rows[0]], copy['variant_stock']['available'])
        with m.app.app_context():
            db = m.get_db()
            variant_inventory.set_mode(db, 'khuyoot', 'P1', 'general')
            db.commit()
            product = variant_inventory.with_stock(db, 'khuyoot', m.load_products_from_file())[0]
        self.assertNotIn('variant_stock', product)
        self.assertEqual('40 إلى 48', product_fact(product, 'sizes')['value'])

    def test_text_campaign_uses_configured_product_without_asking_for_photo(self):
        with m.app.app_context():
            db = m.get_db()
            m.set_store_setting(db, 'auto_product_enabled', '1', 'khuyoot')
            m.set_store_setting(db, 'auto_product_id', 'P1', 'khuyoot')
        self.payload['conversation']['additional_attributes'] = {'referral': {'ad_id': 'unmapped-5678'}}
        self.incoming(10, 'شكد سعر هالموديل؟')
        decision = self.model_response(dict(intent='continue'))
        wording = self.model_response({'parts': [
            dict(type='text', text='سعره {{unit_price}} حبيبتي، شكد قياس تلبسين؟')
        ]})
        with patch.object(m.ai_transport, 'post', side_effect=[decision, wording]) as model:
            self.assertEqual('continue', self.work()[1]['action'])
        context = json.loads(model.call_args_list[0].kwargs['json']['messages'][-1]['content'])
        self.assertEqual(dict(status='known', source='auto_product', product_id='P1'), context['reference'])
        sent = [call.args[2][0] for call in self.transport.call_args_list]
        self.assertEqual(['text', 'text'], [part['type'] for part in sent])
        self.assertIn('15 ألف', sent[0]['text'])
        self.assertTrue(sent[1]['question_field'].endswith('.size'))
        self.assertNotIn('صور', ' '.join(part['text'] for part in sent))

    def test_checkout_captures_split_address_and_asks_only_for_phone_without_a_model(self):
        self.incoming(10, 'أريد ماروني قياس 44')
        decision = self.model_response(dict(intent='buy', changes=[
            dict(values=dict(product_id='P1', size='44', color='ماروني'))]))
        wording = self.model_response({'parts': [
            dict(type='text', text='حبيبتي، دزيلي رقم الموبايل والعنوان حتى أثبت الحجز.')
        ]})
        with patch.object(m.ai_transport, 'post', side_effect=[decision, wording]):
            self.assertEqual('continue', self.work()[1]['action'])
        self.transport.reset_mock()
        self.incoming(11, 'حي التجربة')
        self.incoming(12, 'الموصل')
        with patch.object(m.ai_transport, 'post') as model:
            self.assertEqual('conversation_changed', self.work()[1]['reason'])
            self.assertEqual('continue', self.work()[1]['action'])
            self.transport.assert_called_once()
            self.assertEqual('حبيبتي، باقي بس رقم الموبايل حتى أثبت الحجز.',
                             self.transport.call_args.args[2][0]['text'])
            self.incoming(13, self.phone)
            self.assertEqual('booked', self.work()[1]['action'])
            model.assert_not_called()
        with m.app.app_context():
            orders = m.get_db().execute('SELECT phone,province,address FROM orders').fetchall()
            self.assertEqual([(self.phone, 'نينوى', 'حي التجربة')], [tuple(row) for row in orders])

    def test_catalog_photo_request_sends_attachments_before_the_color_question(self):
        variants = [{'url': f'https://catalog.test/product_image/{name}.jpg', 'color': color}
                    for name, color in [('red', 'ماروني'), ('blue', 'أزرق')]]
        with patch.object(m, 'PUBLIC_URL', 'https://catalog.test'), \
             patch.object(m, 'product_image_variants', return_value=variants), \
             patch.object(m.ai_transport, 'post', return_value=self.model_response(dict(intent='continue'))) as model:
            for mid, phrase in enumerate(('اي اريد صوره', 'دزيلي صور الألوان', 'حبي وين اصور',
                                          'حبي شوكت. تنزليهم هيه صوره', 'الوانه؟', 'صور كل الوان السوت'), 10):
                with self.subTest(phrase=phrase):
                    self.transport.reset_mock()
                    self.incoming(mid, phrase)
                    self.assertEqual('continue', self.work()[1]['action'])
                    sent = [call.args[2][0] for call in self.transport.call_args_list]
                    self.assertEqual(['image', 'image', 'text'] if mid == 10 else ['image', 'image'],
                                     [part['type'] for part in sent])
                    self.assertEqual([variant['url'] for variant in variants], [part['url'] for part in sent[:2]])
                    if mid == 10:
                        self.assertIn('لون', sent[-1]['text'])
            self.assertEqual(6, model.call_count)

    def test_checkout_books_with_bare_local_mobile_and_an_inspection_question_in_burst(self):
        self.incoming(10, 'أريد ماروني قياس 44')
        plan = self.model_response(dict(intent='buy', changes=[
            dict(values=dict(product_id='P1', color='ماروني', size='44'))]))
        reply = self.model_response(dict(parts=[
            dict(type='text', text='دزيلي رقم الموبايل والعنوان حتى أثبت الحجز.')]))
        with patch.object(m.ai_transport, 'post', side_effect=[plan, reply]):
            self.assertEqual('continue', self.work()[1]['action'])
        self.incoming(11, 'بغداد حي الاختبار\nاذا القياس صغير ارجعه\n٧٧٠١٢٣٤٥٦٧')
        with patch.object(m.ai_transport, 'post') as model:
            self.assertEqual('booked', self.work()[1]['action'])
            model.assert_not_called()
        with m.app.app_context():
            order = m.get_db().execute('SELECT phone,province,address FROM orders').fetchone()
            self.assertEqual(('07701234567', 'بغداد', 'بغداد حي الاختبار'), tuple(order))
        self.assertEqual(CONFIRMATION, self.transport.call_args.args[2][0]['text'])

    def test_photo_followup_waits_for_the_attachment_to_be_accepted(self):
        self.incoming(10, 'إي، أريد صورة')
        self.transport.return_value = {'ok': False, 'status_code': 422}
        variants = [dict(url='https://catalog.test/product_image/navy.jpg', color='أزرق')]
        with patch.object(m, 'PUBLIC_URL', 'https://catalog.test'), \
             patch.object(m, 'product_image_variants', return_value=variants), \
             patch.object(m.ai_transport, 'post', return_value=self.model_response({}, status=503)) as model:
            self.work()
            model.assert_called_once()
        self.transport.assert_called_once()
        self.assertEqual('image', self.transport.call_args.args[2][0]['type'])

    def test_model_resolves_a_contextual_photo_request_and_ignores_a_stale_quote(self):
        greeting = self.model_response(dict(intent='continue'))
        opening = self.model_response({'parts': [
            dict(type='text', text='سعره {{unit_price}} حبيبتي، شكد قياس تلبسين؟')]})
        self.incoming(10, 'شكد سعر هالموديل؟')
        with patch.object(m.ai_transport, 'post', side_effect=[greeting, opening]):
            self.assertEqual('continue', self.work()[1]['action'])

        self.payload['conversation']['additional_attributes'] = {}
        phrase = 'إي ورّيني إياه هنانا'
        decision = self.model_response(dict(intent='continue', answer=dict(catalog_photos=phrase)))
        variants = [dict(url='https://catalog.test/product_image/navy.jpg', color='أزرق')]
        self.transport.reset_mock()
        with patch.object(m, 'PUBLIC_URL', 'https://catalog.test'), \
             patch.object(m, 'product_image_variants', return_value=variants), \
             patch.object(m.ai_transport, 'post', return_value=decision) as model:
            self.incoming(11, phrase)
            self.assertEqual('continue', self.work()[1]['action'])
        model.assert_called_once()
        request = model.call_args.kwargs['json']
        context = json.loads(request['messages'][-1]['content'])
        self.assertEqual('P1', context['state']['focus_product_id'])
        self.assertEqual(phrase, context['event']['text'])
        self.assertTrue(context['recent_replies'])
        self.assertIn('catalog_photos', request['response_format']['json_schema']['schema']
                      ['properties']['answer']['properties'])
        parts = [call.args[2][0] for call in self.transport.call_args_list]
        self.assertEqual(['image', 'text'], [part['type'] for part in parts])
        self.assertEqual(variants[0]['url'], parts[0]['url'])
        self.assertIn('لون', parts[1]['text'])

        self.transport.reset_mock()
        sizes = self.model_response({'parts': [dict(type='text', text='قياساته من 40 إلى 48.')]})
        with patch.object(m.ai_transport, 'post', side_effect=[decision, sizes]):
            self.incoming(12, 'اكو قياسات؟')
            self.assertEqual('continue', self.work()[1]['action'])
        self.assertEqual(['text'], [call.args[2][0]['type'] for call in self.transport.call_args_list])
        self.assertIn('40 إلى 48', self.transport.call_args.args[2][0]['text'])

    def test_unresolved_photo_continues_with_configured_campaign_facts(self):
        with m.app.app_context():
            db = m.get_db()
            m.set_store_setting(db, 'auto_product_enabled', '1', 'khuyoot')
            m.set_store_setting(db, 'auto_product_id', 'P1', 'khuyoot')
        self.payload['conversation']['additional_attributes'] = {}
        image = dict(attachments=[{'file_type': 'image', 'data_url': 'https://cdn.example/fixture.jpg'}])
        price = self.model_response({'parts': [dict(type='text', text='سعره {{unit_price}} حبيبتي.')]})
        sizes = self.model_response({'parts': [dict(type='text', text='إي حبيبتي، من 40 إلى 48.')]})
        with patch.object(live.ImageObserver, '__call__', return_value=dict(status='unknown')), \
             patch.object(m.ai_transport, 'post', side_effect=[price, sizes]) as model:
            self.incoming(10, '', **image)
            self.assertEqual('continue', self.work()[1]['action'])
            self.incoming(11, 'اكو قياسات')
            self.assertEqual('continue', self.work()[1]['action'])
            self.incoming(12, f'{self.phone}\n{self.address}')
            self.assertEqual('continue', self.work()[1]['action'])
            self.assertEqual(2, model.call_count)
        sent = [call.args[2][0]['text'] for call in self.transport.call_args_list]
        self.assertEqual(2, len(sent))
        self.assertIn('15 ألف', sent[0])
        self.assertIn('40 إلى 48', sent[1])
        self.assertFalse(any('؟' in reply or 'دزي' in reply for reply in sent))
        with m.app.app_context():
            db = m.get_db()
            state = json.loads(db.execute('SELECT data FROM sales_v2_scopes').fetchone()[0])
            self.assertIsNone(state.get('focus_product_id'))
            self.assertFalse(state['cart'])
            self.assertEqual(self.phone, state['contact']['phone'])
            self.assertIn('حي التجربة', state['contact']['address'])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])

    def test_same_day_question_survives_two_photo_events_in_the_live_queue(self):
        self.incoming(10, 'يوصلن اليوم عنواني بغداد')
        for mid in (11, 12):
            self.incoming(mid, '', attachments=[dict(file_type='image', data_url=f'https://cdn.example/{mid}.jpg')])
        price = self.model_response(dict(parts=[dict(type='text', text='سعره خمسطعش ألف.')]))
        timing = self.model_response(dict(parts=[dict(type='text',
            text='ما أگدر أضمن الوصول اليوم. عادةً التوصيل من 24 ساعة إلى يومين.')]))
        with patch.object(live.ImageObserver, '__call__', return_value=dict(status='unavailable')), \
             patch.object(m.ai_transport, 'post', side_effect=[price, timing]) as model:
            self.assertEqual('conversation_changed', self.work()[1]['reason'])
            self.assertEqual('conversation_changed', self.work()[1]['reason'])
            self.assertEqual('continue', self.work()[1]['action'])
        first_input = json.loads(model.call_args_list[0].kwargs['json']['messages'][-1]['content'])
        self.assertEqual('يوصلن اليوم عنواني بغداد', first_input['customer_message'])
        sent = ' '.join(call.args[2][0]['text'] for call in self.transport.call_args_list)
        self.assertIn('من 24 ساعة إلى يومين', sent)
        self.assertNotIn('ألف', sent)
        with m.app.app_context():
            db = m.get_db()
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])

    def test_missing_photo_for_requested_color_returns_an_honest_answer(self):
        self.incoming(10, 'دزيلي صور الأزرق')
        variants = [dict(url='https://catalog.test/product_image/red.jpg', color='ماروني')]
        with patch.object(m, 'PUBLIC_URL', 'https://catalog.test'), \
             patch.object(m, 'product_image_variants', return_value=variants), \
             patch.object(m.ai_transport, 'post', return_value=self.model_response(dict(intent='continue'))) as model:
            self.assertEqual('continue', self.work()[1]['action'])
            model.assert_called_once()
        self.transport.assert_called_once()
        part, = self.transport.call_args.args[2]
        self.assertEqual('text', part['type'])
        self.assertIn('ما عدنا صور لهاللون', part['text'])

    def test_verified_image_beats_configured_default(self):
        products = m.load_products_from_file()
        products.append(dict(product_id='P2', store_id='khuyoot', product_name='جاكيت اختبار',
                             fabric='قطن', colors='أسود', price='25000', status='active', stock='متوفر'))
        with m.app.app_context():
            db = m.get_db()
            m.set_store_setting(db, 'auto_product_enabled', '1', 'khuyoot')
            m.set_store_setting(db, 'auto_product_id', 'P1', 'khuyoot')
        image = dict(attachments=[{'file_type': 'image', 'data_url': 'https://cdn.example/other.jpg'}])
        decision = self.model_response(dict(intent='continue'))
        wording = self.model_response({'parts': [dict(type='text', text='قماشه قطن حبيبتي، تحبين الأسود؟')]})
        with patch.object(live.ImageObserver, '__call__', return_value=dict(status='matched', product_ids=['P2'])), \
             patch.object(m.ai_transport, 'post', side_effect=[decision, wording]) as model:
            self.incoming(10, 'شنو قماشه؟', **image)
            self.assertEqual('continue', self.work()[1]['action'])
        context = json.loads(model.call_args_list[0].kwargs['json']['messages'][-1]['content'])
        self.assertEqual(dict(status='known', source='image', product_id='P2'), context['reference'])
        sent = [call.args[2][0] for call in self.transport.call_args_list]
        self.assertEqual(['text', 'text'], [part['type'] for part in sent])
        self.assertIn('قطن', sent[0]['text'])
        self.assertTrue(sent[1]['question_field'].endswith('.color'))

    def test_sticker_then_verified_photo_and_weight_are_worded_by_the_model(self):
        product = m.load_products_from_file()[0]
        product.update(sizes='قياسات من وزن 60 كيلو إلى 130 كيلو.',
                       price='15000 او بكج قطعتين ب 25000')
        denied = self.model_response(None, status=402)
        image = dict(attachments=[{'file_type': 'image', 'data_url': 'https://cdn.example/verified.jpg'}])
        opening = self.model_response(dict(parts=[
            dict(type='text', text='ينحسب لج بـ{{unit_price}}، والقطعتين بالعرض بـ{{offer_total}}.'),
            dict(type='text', text='تحبين وحدة لو تستفادين من عرض القطعتين؟'),
        ]))
        fit = self.model_response(dict(parts=[
            dict(type='text', text='إي عيني، أكو قياس يناسبج بهالموديل.'),
            dict(type='text', text='يا لون عجبج؟'),
        ]))
        with patch.object(live.ImageObserver, '__call__', return_value=dict(status='matched', product_ids=['P1'])), \
             patch.object(m.ai_transport, 'post', side_effect=[denied, opening, denied, fit]) as model:
            self.incoming(10, '', attachments=[{'file_type': 'sticker', 'data_url': 'https://cdn.example/sticker.webp'}])
            self.work()
            self.incoming(11, '', **image)
            result = self.work()[1]
            self.assertEqual('continue', result.get('action'), result)
            opening = ' '.join(call.args[2][0]['text'] for call in self.transport.call_args_list)
            self.assertIn('15 ألف', opening)
            self.assertIn('25 ألف', opening)
            self.transport.reset_mock()
            self.incoming(12, 'الوزن ٩٥')
            result = self.work()[1]
            self.assertEqual('continue', result.get('action'), result)
        reply = ' '.join(call.args[2][0]['text'] for call in self.transport.call_args_list)
        self.assertIn('أكو قياس يناسبج', reply)
        self.assertIn('لون', reply)
        self.assertNotIn('شكد الوزن', reply)
        context = json.loads(model.call_args.kwargs['json']['messages'][1]['content'])
        self.assertEqual('P1', context['verified_photo_product_id'])
        self.assertEqual(95, context['progress']['fit']['weight_kg'])
        with m.app.app_context():
            db = m.get_db()
            state = live.Store(db).snapshot(live.scope_for(live.route(db, self.sender)))['data']
            self.assertEqual(95, float(state['cart'][0].get('weight_kg') or state['cart'][0]['size']))
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])

    def test_outage_after_verified_photo_does_not_send_saved_text(self):
        denied = self.model_response(None, status=402)
        self.incoming(10, '', attachments=[{'file_type': 'image', 'data_url': 'https://cdn.example/verified.jpg'}])
        with patch.object(live.ImageObserver, '__call__', return_value=dict(status='matched', product_ids=['P1'])), \
             patch.object(m.ai_transport, 'post', return_value=denied):
            result = self.work()[1]
        self.assertTrue(result.get('needs_human'), result)
        self.transport.assert_not_called()

    def test_agreement_to_see_alternatives_sends_available_product_images(self):
        products = m.load_products_from_file()
        products[0].update(stock='غير متوفر', product_type='tops')
        products.extend([
            dict(product_id='P2', store_id='khuyoot', product_name='قميص بديل', product_type='tops',
                 price='15000', stock='متوفر', status='active'),
            dict(product_id='P3', store_id='khuyoot', product_name='قميص نافد', product_type='tops',
                 price='15000', stock='غير متوفر', status='active'),
        ])
        def variants(product):
            return [{'url': f"https://catalog.test/product_image/{product['product_id']}.jpg", 'color': ''}]

        decision = self.model_response(dict(intent='continue'))
        offer = self.model_response(dict(parts=[dict(type='text', text='تحبين أدزلج صور موديلات ثانية؟')]))
        with patch.object(m, 'PUBLIC_URL', 'https://catalog.test'), \
             patch.object(m, 'product_image_variants', side_effect=variants), \
             patch.object(m.ai_transport, 'post', side_effect=[decision, offer]) as model:
            self.incoming(10, 'السوت موجود؟')
            self.assertEqual('continue', self.work()[1]['action'])
            self.transport.reset_mock()
            self.incoming(11, 'دزي')
            self.assertEqual('continue', self.work()[1]['action'])
        self.assertEqual(2, model.call_count)
        self.assertEqual(['https://catalog.test/product_image/P2.jpg'],
                         [call.args[2][0]['url'] for call in self.transport.call_args_list])

    def test_recent_staff_sold_out_reply_prevents_booking_against_conflicting_catalog(self):
        decision = self.model_response(dict(intent='continue'))
        reply = self.model_response(dict(parts=[dict(type='text', text='يا قياس تحبين؟')]))
        self.incoming(10, 'هلا')
        with patch.object(m.ai_transport, 'post', side_effect=[decision, reply]):
            self.assertEqual('continue', self.work()[1]['action'])
        with m.app.app_context():
            db = m.get_db()
            token = m._current_store_id.set('khuyoot')
            try:
                m.save_message(db, self.sender, 'outgoing', 'text', 'ولا قطعة بقت خلصت الكمية',
                               raw_payload=dict(sender_type='staff', _delivery_status='accepted'))
            finally:
                m._current_store_id.reset(token)
        self.incoming(11, f'أريد ماروني قياس 44\n{self.phone}\n{self.address}')
        buy = self.model_response(self.model_plan(color='ماروني'))
        clarification = self.model_response(dict(parts=[
            dict(type='text', text='حبيبتي، أحتاج أتأكد من توفر هالموديل حتى أثبتلج الطلب.')]))
        with patch.object(m.ai_transport, 'post', side_effect=[buy, clarification]) as model:
            self.assertEqual('continue', self.work()[1]['action'])
        context = json.loads(model.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertTrue(context['stock_conflict'])
        self.assertIsNone(context['next_step']['field'])
        with m.app.app_context():
            self.assertEqual(0, m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_new_photo_replaces_rejected_model_and_keeps_phone_for_fit_and_checkout(self):
        products = m.load_products_from_file()
        products[0].update(product_name='سوت فرو اختبار', fabric='فرو')
        products.append(dict(product_id='P2', store_id='khuyoot', product_name='دشداشة اختبار',
                             fabric='قديفة ثقيل', colors='ماروني، أخضر', price='25000',
                             sizes='من وزن 55 إلى 95 كيلو', fit_notes='قصتها واسعة',
                             status='active', stock='متوفر', stock_quantity=3))
        image = dict(attachments=[{'file_type': 'image', 'data_url': 'https://cdn.example/new-dress.jpg'}])
        drafts = [
            self.model_response(dict(intent='buy', changes=[
                dict(values=dict(product_id='P1', size='44', color='ماروني'))])),
            self.model_response(dict(parts=[dict(type='text', text='باقي بس عنوانج حبيبتي حتى أثبتلج.')])),
            self.model_response(dict(intent='continue', answer=dict(facts=[dict(product_id='P1', field='fabric')]))),
            self.model_response(dict(parts=[dict(type='text', text='خامة الفرو عيني. شكد وزنج؟')])),
            self.model_response(dict(parts=[dict(type='text', text='إي حبيبتي، قماشها قديفة ثقيل. شكد وزنج؟')])),
            self.model_response(dict(intent='continue')),
            self.model_response(dict(parts=[
                dict(type='text', text='إي حبيبتي قياسج متوفر، وقصتها واسعة.'),
                dict(type='text', text='تحبين أحجزلج هاللون؟')
            ])),
        ]
        with patch.object(live.ImageObserver, '__call__', return_value=dict(
                status='matched', product_ids=['P2'], observed_color='ماروني')), \
             patch.object(m.ai_transport, 'post', side_effect=drafts) as model:
            self.incoming(10, f'أريد ماروني قياس 44\nرقمي {self.phone}')
            self.assertEqual('continue', self.work()[1]['action'])
            self.incoming(11, 'لا مو تراك\nهذا الموديل والون، شنو قماشه؟', **image)
            self.assertEqual('continue', self.work()[1]['action'])
            self.incoming(12, 'وزني 80')
            self.assertEqual('continue', self.work()[1]['action'])
        photo_context = json.loads(model.call_args_list[2].kwargs['json']['messages'][-1]['content'])
        self.assertEqual('P2', photo_context['reference']['product_id'])
        self.assertEqual([], photo_context['state']['cart'])
        self.assertEqual(self.phone, photo_context['state']['contact']['phone'])
        retry = json.loads(model.call_args_list[4].kwargs['json']['messages'][-1]['content'])
        self.assertIn('reply.fabric', retry['validation_feedback'])
        fit_context = json.loads(model.call_args_list[-1].kwargs['json']['messages'][-1]['content'])
        self.assertEqual({'stock': 'available', 'limited_stock': True, 'roomy_cut': True},
                         fit_context['value_facts']['fit_availability'])
        self.assertEqual('purchase_intent', fit_context['next_step']['field'])
        sent = [call.args[2][0]['text'] for call in self.transport.call_args_list]
        self.assertFalse(any('فرو' in part for part in sent))
        self.assertTrue(any('قديفة' in part for part in sent))
        with m.app.app_context():
            db = m.get_db()
            state = live.Store(db).snapshot(live.scope_for(live.route(db, self.sender)))['data']
            self.assertEqual(('P2', 'ماروني', '80'), tuple(state['cart'][0][key] for key in ('product_id', 'color', 'size')))
            self.assertEqual(self.phone, state['contact']['phone'])
            self.assertEqual('P2', m.get_active_product_binding(db, self.sender)['product_id'])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_previously_accepted_reply_prevents_generation_for_the_same_incoming(self):
        self.incoming(10, 'أثبتلج؟')
        with m.app.app_context():
            db = m.get_db()
            incoming = db.execute("SELECT id FROM messages WHERE sender_id=? AND direction='incoming'",
                                  (self.sender,)).fetchone()[0]
            token = m._current_store_id.set('khuyoot')
            try:
                m.save_message(db, self.sender, 'outgoing', 'text', 'دزيلي رقم الموبايل حبيبتي.',
                               raw_payload=dict(sender_type='ai', _reply_to_message_id=incoming,
                                                _delivery_status='accepted'))
            finally:
                m._current_store_id.reset(token)
        with patch.object(m.ai_transport, 'post') as model:
            self.assertEqual('already_answered', self.work()[1]['reason'])
        model.assert_not_called()
        self.transport.assert_not_called()

    def test_staff_reply_and_product_link_leave_ai_ready_for_the_next_question(self):
        self.incoming(10, 'شنو القماش؟')
        with m.app.app_context():
            m.set_customer_ai_enabled(m.get_db(), self.sender, False, reason='sales_runtime_review')
        headers = {'X-CSRF-Token': 'test-csrf'}
        base = f'/api/conversations/{self.sender}'
        with patch.object(m, 'send_text_via_manychat_detailed', return_value=dict(ok=True, status='accepted')), \
             patch.object(m.ai_transport, 'post') as model:
            sent = self.client.post(base + '/send?store_id=khuyoot', json={'text': 'قماشه قطني.'}, headers=headers)
            self.assertEqual(200, sent.status_code, sent.get_json())
            self.assertTrue(sent.get_json()['ai_enabled'])
            linked = self.client.post(base + '/link_product?store_id=khuyoot',
                                      json={'product_id': 'P1', 'async': True}, headers=headers)
            self.assertEqual(202, linked.status_code, linked.get_json())
            self.assertEqual('already_answered', self.work()[1]['reason'])
            kind, result = self.work()
            self.assertEqual('link', kind)
            self.assertTrue(result['ai_resumed'])
            self.assertEqual({'sent': False, 'reason': 'already_answered'}, result['auto_reply'])
            model.assert_not_called()
        self.transport.assert_not_called()

        self.incoming(11, 'شكد سعره؟')
        plan = self.model_response(dict(intent='continue'))
        wording = self.model_response(dict(parts=[dict(type='text', text='سعره {{unit_price}}. شكد قياس تلبسين؟')]))
        with patch.object(m.ai_transport, 'post', side_effect=[plan, wording]) as model:
            self.assertEqual('continue', self.work()[1]['action'])
        context = json.loads(model.call_args_list[0].kwargs['json']['messages'][-1]['content'])
        self.assertEqual('شكد سعره؟', context['event']['text'])
        self.assertIn(dict(actor='staff', text='قماشه قطني.'), context['recent_messages'])
        self.assertIn('15 ألف', self.transport.call_args_list[0].args[2][0]['text'])

    def test_staff_reply_preserves_an_explicit_pause(self):
        self.incoming(10, 'شكد سعره؟')
        with m.app.app_context():
            m.set_customer_ai_enabled(m.get_db(), self.sender, False)
        with patch.object(m, 'send_text_via_manychat_detailed', return_value=dict(ok=True, status='accepted')):
            response = self.client.post(f'/api/conversations/{self.sender}/send?store_id=khuyoot',
                                        json={'text': 'سعره 15 ألف.'}, headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(200, response.status_code, response.get_json())
        self.assertFalse(response.get_json()['ai_enabled'])

    def test_new_question_arriving_during_staff_send_is_not_marked_answered(self):
        self.incoming(10, 'شنو القماش؟')

        def deliver_staff(*args, **kwargs):
            # The customer webhook arrives with its own Flask request and database connection.
            with ThreadPoolExecutor(max_workers=1) as worker:
                worker.submit(self.incoming, 11, 'شكد سعره؟').result(timeout=10)
            return dict(ok=True, status='accepted')

        with patch.object(m, 'send_text_via_manychat_detailed', side_effect=deliver_staff):
            response = self.client.post(f'/api/conversations/{self.sender}/send?store_id=khuyoot',
                                        json={'text': 'قماشه قطني.'}, headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(200, response.status_code, response.get_json())
        plan = self.model_response(dict(intent='continue'))
        wording = self.model_response(dict(parts=[dict(type='text', text='سعره {{unit_price}}. شكد قياس تلبسين؟')]))
        with patch.object(m.ai_transport, 'post', side_effect=[plan, wording]) as model:
            self.assertEqual('conversation_changed', self.work()[1]['reason'])
            self.assertEqual('continue', self.work()[1]['action'])
        context = json.loads(model.call_args_list[0].kwargs['json']['messages'][-1]['content'])
        self.assertEqual('شكد سعره؟', context['event']['text'])
        self.assertIn(dict(actor='staff', text='قماشه قطني.'), context['recent_messages'])
        self.assertIn('15 ألف', self.transport.call_args_list[0].args[2][0]['text'])

    def test_signed_burst_books_last_choice_and_records_confirmation_and_fulfillment_once(self):
        for mid, text in enumerate(('أريد ماروني قياس 44', self.phone, 'لا بدلي الماروني أزرق', self.address), 10):
            self.incoming(mid, text)
        self.incoming(13, self.address)
        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan())) as model:
            for _ in range(3):
                self.assertEqual((live.JOB_KIND, {'ok': True, 'reason': 'conversation_changed'}), self.work())
            kind, result = self.work()
        self.assertEqual((live.JOB_KIND, 'booked'), (kind, result.get('action')), result)
        self.assertEqual(1, model.call_count)
        request = json.loads(model.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertIn(self.phone, request['event']['text'])
        self.assertIn('بدلي الماروني أزرق', request['event']['text'])
        self.assertEqual('P1', request['reference']['product_id'])
        self.transport.assert_called_once()
        self.assertEqual(CONFIRMATION, self.transport.call_args.args[2][0]['text'])
        self.notify.assert_called_once()
        with m.app.app_context():
            db = m.get_db()
            order = db.execute('SELECT store_id,color,size,customer_name,phone FROM orders').fetchone()
            self.assertEqual(('khuyoot', 'أزرق', '44', 'Test Customer', self.phone), tuple(order))
            self.assertEqual(1, db.execute('SELECT COUNT(*) FROM menger_deliveries').fetchone()[0])
            self.assertEqual(4, db.execute("SELECT COUNT(*) FROM messages WHERE direction='incoming'").fetchone()[0])
            outgoing = db.execute("SELECT text,raw_payload FROM messages WHERE direction='outgoing'").fetchall()
            self.assertEqual(1, len(outgoing))
            self.assertEqual(CONFIRMATION, outgoing[0]['text'])
            self.assertEqual('accepted', json.loads(outgoing[0]['raw_payload'])['_delivery_status'])
            db.execute("UPDATE ai_jobs SET status='running' WHERE kind=? AND result LIKE '%booked%'", (live.JOB_KIND,))
            db.commit()
            m.ai_jobs.recover_interrupted(db)
        self.assertEqual('already_handled', self.work()[1]['reason'])
        self.transport.assert_called_once()
        self.notify.assert_called_once()

    def test_sol_fails_once_and_marks_the_incoming_message_without_sending(self):
        self.incoming(10, 'شكد سعره؟')
        failure = self.model_response(None, status=503)
        with patch.object(m.ai_transport, 'post', return_value=failure) as post:
            kind, result = self.work()
        self.assertEqual(live.JOB_KIND, kind)
        self.assertTrue(result.get('needs_human'), result)
        models = [call.kwargs['json']['model'] for call in post.call_args_list]
        self.assertEqual([m.MAIN_MODEL, BACKUP_MODEL], models)
        self.assertTrue(all(call.kwargs['max_attempts'] == 1 for call in post.call_args_list))
        self.transport.assert_not_called()
        with m.app.app_context():
            db = m.get_db()
            incoming_id = m.latest_incoming_message(db, self.sender)['id']
            self.assertFalse(m.is_customer_ai_enabled(db, self.sender))
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])
        for suffix in ('', f'&after_id={incoming_id}'):
            response = self.client.get(f'/api/conversations/{self.sender}/messages?store_id=khuyoot{suffix}')
            self.assertEqual({str(incoming_id): 'failed'}, response.get_json()['consultations'])

    def test_sol_guides_rewrite_and_only_the_primary_text_is_sent(self):
        self.incoming(10, 'شكد سعره؟')
        guidance = 'اذكري سعر القطعة المثبت بالكتالوج واطلبي فقط الخطوة المحددة.'
        responses = [
            self.model_response(dict(intent='continue')),
            self.model_response(None, status=503),
            self.model_response(dict(decision='assist', reason='writer unavailable', guidance=guidance, changes=[])),
            self.model_response({'parts': [dict(type='text', text='سعره {{unit_price}} حبيبتي، شكد قياس تلبسين؟')]}),
        ]
        with patch.object(m.ai_transport, 'post', side_effect=responses) as post:
            self.assertEqual('continue', self.work()[1]['action'])
        calls = [call.kwargs['json'] for call in post.call_args_list]
        self.assertEqual([m.MAIN_MODEL, m.MAIN_MODEL, BACKUP_MODEL, m.MAIN_MODEL],
                         [call['model'] for call in calls])
        self.assertEqual('writer', json.loads(calls[2]['messages'][-1]['content'])['recovery']['stage'])
        self.assertEqual(guidance, json.loads(calls[3]['messages'][-1]['content'])['consultant_guidance']['guidance'])
        sent = [call.args[2][0]['text'] for call in self.transport.call_args_list]
        self.assertIn('15 ألف', ' '.join(sent))
        self.assertNotIn(guidance, ' '.join(sent))
        data = self.client.get(f'/api/conversations/{self.sender}/messages?store_id=khuyoot').get_json()
        incoming_id = next(message['id'] for message in data['messages'] if message['direction'] == 'incoming')
        self.assertEqual({str(incoming_id): 'returned'}, data['consultations'])

    def test_primary_outage_after_sol_advice_requires_review_without_booking(self):
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')
        unavailable = self.model_response(None, status=503)
        advice = self.model_response(dict(decision='assist', reason='purchase is clear',
                                          guidance='اعتمدي الأزرق قياس 44.', changes=self.model_plan()['changes']))
        with patch.object(m.ai_transport, 'post', side_effect=[unavailable, advice, unavailable]) as post:
            self.assertTrue(self.work()[1]['needs_human'])
        self.assertEqual([m.MAIN_MODEL, BACKUP_MODEL, m.MAIN_MODEL],
                         [call.kwargs['json']['model'] for call in post.call_args_list])
        self.transport.assert_not_called()
        self.notify.assert_not_called()
        with m.app.app_context():
            db = m.get_db()
            route = live.route(db, self.sender)
            state = live.Store(db).snapshot(live.scope_for(route))['data']
            self.assertEqual([], state['cart'])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])

    def test_link_and_send_resumes_the_unanswered_caption_after_manual_image_binding(self):
        self.payload['conversation']['additional_attributes'] = {}
        self.incoming(10, 'شكد سعره؟', attachments=[dict(file_type='image', data_url='https://cdn.example/new.jpg')])
        with patch.object(live.ImageObserver, '__call__', return_value=dict(status='unavailable')):
            self.assertTrue(self.work()[1]['needs_human'])
        self.transport.assert_not_called()
        endpoint = f'/api/conversations/{self.sender}/link_product?store_id=khuyoot'
        response = self.client.post(endpoint, json={'product_id': 'P1', 'async': True},
                                    headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(202, response.status_code, response.get_json())
        plan = self.model_response(dict(intent='continue'))
        wording = self.model_response(dict(parts=[dict(type='text', text='سعره {{unit_price}}. شكد قياس تلبسين؟')]))
        with patch.object(m, 'teach_verified_product_image'), patch.object(live.ImageObserver, '__call__') as vision, \
             patch.object(m.ai_transport, 'post', side_effect=[plan, wording]) as post:
            kind, result = self.work()
        self.assertEqual('link', kind)
        self.assertTrue(result['auto_reply']['sent'], result)
        vision.assert_not_called()
        context = json.loads(post.call_args_list[0].kwargs['json']['messages'][-1]['content'])
        self.assertEqual('P1', context['state']['focus_product_id'])
        self.assertEqual('شكد سعره؟', context['event']['text'])

    def test_conversation_link_resolves_pending_album_for_followup_without_training_each_image(self):
        self.payload['conversation']['additional_attributes'] = {}
        album = [dict(file_type='image', data_url=f'https://cdn.example/color-{i}.jpg') for i in range(4)]
        clarify = self.model_response(dict(parts=[dict(type='text', text='يا صورة تقصدين حتى أعرف لج سعرها؟')]))
        with patch.object(live.ImageObserver, '__call__', return_value=dict(status='ambiguous')), \
             patch.object(m.ai_transport, 'post', return_value=clarify):
            self.incoming(10, '', attachments=album)
            self.assertEqual('media_unresolved', self.work()[1]['action'])
        self.incoming(11, 'جكد سعر\nوالتوصيل')
        with m.app.app_context():
            db = m.get_db()
            live.staff_product(db, self.sender, 'P1')
            state = live.Store(db).snapshot(live.scope_for(live.route(db, self.sender)))['data']
            self.assertEqual((None, None), live.Runtime.pending_photo(state))
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM manual_image_links').fetchone()[0])
        wording = self.model_response(dict(parts=[
            dict(type='text', text='سعرها {{unit_price}} والتوصيل {{delivery_fee}}.'),
            dict(type='text', text='شنو قياسج حتى أشيّك لج المتوفر؟'),
        ]))
        decision = self.model_response(dict(intent='continue'))
        with patch.object(m.ai_transport, 'post', side_effect=[decision, wording]) as provider, \
             patch.object(live.ImageObserver, '__call__') as vision:
            result = self.work()[1]
        self.assertEqual('continue', result.get('action'), result)
        vision.assert_not_called()
        context = json.loads(provider.call_args.kwargs['json']['messages'][1]['content'])
        self.assertEqual('P1', context['state']['focus_product_id'])
        self.assertEqual(15000, context['amounts']['unit_price'])

    def test_resume_preserves_the_committed_size_after_writer_failure(self):
        self.incoming(10, 'اريد ماروني قياس 44')
        plan = self.model_response(dict(intent='buy', changes=[dict(values=dict(product_id='P1', size='44', color='ماروني'))]))
        failure = self.model_response(None, status=503)
        with patch.object(m.ai_transport, 'post', side_effect=[plan, failure, failure]):
            self.assertTrue(self.work()[1]['needs_human'])
        response = self.client.post(f'/api/conversations/{self.sender}/ai?store_id=khuyoot', json={'enabled': True},
                                    headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(202, response.status_code, response.get_json())
        wording = self.model_response(dict(parts=[dict(type='text', text='دزيلي رقم الموبايل والعنوان حتى أثبتلج.')]))
        with patch.object(m.ai_transport, 'post', return_value=wording) as post:
            kind, result = self.work()
        self.assertEqual('resume', kind)
        self.assertTrue(result['auto_reply']['sent'], result)
        post.assert_called_once()
        context = json.loads(post.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertEqual(('44', 'ماروني'), tuple(context['state']['cart'][0][key] for key in ('size','color')))
        self.assertNotIn('قياس', self.transport.call_args.args[2][0]['text'])

    def test_skirt_photo_request_uses_latest_length_correction_and_previous_colors(self):
        products = [dict(product_id=pid, product_name='تنورة', product_type='skirts', store_id='khuyoot',
                         status='active', stock='متوفر', colors='أسود، ماروني', description=f'طولها {length} سم',
                         image_url=f'https://catalog.test/product_image/{pid}.jpg')
                    for pid, length in (('P1', 90), ('P2', 80))]

        def variants(product):
            return [dict(url=f'https://catalog.test/product_image/{product["product_id"]}-{color}.jpg', color=color)
                    for color in ('أسود', 'ماروني')]

        def color_urls(product, color):
            return [item['url'] for item in variants(product) if item['color'] == color]

        self.payload['conversation']['additional_attributes'] = {}
        for mid, text in enumerate(('السلام عليكم تنورة متوفر', 'بلا زحمة دزي صورتهم', 'اريد أسود وماروني',
                                    'تسعيني', 'مال دوام', 'مو قياس الموديل تسعيني قصدي',
                                    'ماعندي موديل معين', 'عندج صورتهن لولا'), 10):
            self.incoming(mid, text)
        with patch.object(m, 'PUBLIC_URL', 'https://catalog.test'), \
             patch.object(m, 'load_products_from_file', return_value=products), \
             patch.object(m, 'product_image_variants', side_effect=variants), \
             patch.object(m, 'select_product_image_urls', side_effect=color_urls), \
             patch.object(m.ai_transport, 'post') as post:
            for _ in range(7):
                self.assertEqual('conversation_changed', self.work()[1]['reason'])
            self.assertEqual('continue', self.work()[1]['action'])
        post.assert_not_called()
        parts = [call.args[2][0] for call in self.transport.call_args_list]
        self.assertEqual([('P1', 'أسود'), ('P1', 'ماروني')], [(part['product_id'], part['color']) for part in parts])
        with m.app.app_context():
            self.assertEqual(0, m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_interrupted_worker_sends_committed_booking_without_creating_another_order(self):
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')
        apply = live.Store.apply

        def interrupt_before_turn_marker(store, scope, key, version, command, handler):
            if key.endswith(':done'):
                raise SystemExit('synthetic worker interruption')
            return apply(store, scope, key, version, command, handler)

        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan())) as model:
            with patch.object(live.Store, 'apply', interrupt_before_turn_marker), self.assertRaises(SystemExit):
                self.work()
            self.transport.assert_not_called()
            self.notify.assert_not_called()
            with m.app.app_context():
                db = m.get_db()
                self.assertEqual(1, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
                self.assertEqual(1, db.execute('SELECT COUNT(*) FROM menger_deliveries').fetchone()[0])
                m.ai_jobs.recover_interrupted(db)
            self.assertEqual('booked', self.work()[1]['action'])
            self.assertEqual(1, model.call_count)
        self.transport.assert_called_once()
        self.notify.assert_called_once()
        with m.app.app_context():
            self.assertEqual(1, m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_nightly_job_runs_two_models_and_exposes_store_report_without_sending(self):
        from account_app import sales_intelligence
        day = sales_intelligence.BAGHDAD
        from datetime import datetime
        now = datetime.now(day).replace(hour=23, minute=30, second=0, microsecond=0)
        with m.app.app_context():
            db = m.get_db()
            db.execute("INSERT INTO messages(store_id,sender_id,direction,message_type,text,created_at) VALUES(?,?,'incoming','text',?,?)",
                       ('khuyoot', self.sender, 'السعر غالي، اكو عرض؟', now.replace(hour=20).isoformat()))
            db.commit()
            sales_intelligence.schedule(db, 'khuyoot', now)
        report = dict(findings=[dict(example=1, observation='الزبونة سألت عن العرض')], tactics=['lead_with_offer'])
        with patch.object(m.ai_transport, 'post', side_effect=[self.model_response(report), self.model_response(report)]) as provider:
            kind, result = self.work()
        self.assertEqual(sales_intelligence.JOB_KIND, kind)
        self.assertEqual('done', result['status'])
        self.assertEqual(2, provider.call_count)
        self.assertEqual(BACKUP_MODEL, provider.call_args.kwargs['json']['model'])
        self.transport.assert_not_called()
        evaluation = self.client.get('/api/evaluation?store_id=khuyoot').get_json()
        self.assertEqual('done', evaluation['nightly_analyses']['conversations']['report']['status'])
        self.assertIsNone(evaluation['nightly_analyses']['products']['report'])
        other = self.client.get('/api/evaluation?store_id=default').get_json()
        self.assertIsNone(other['nightly_analyses']['conversations']['report'])

    def test_evaluation_controls_scope_rules_and_run_products_manually(self):
        root = '/api/evaluation/analysis'
        store = '?store_id=khuyoot'
        headers = {'X-CSRF-Token': 'test-csrf'}
        page = self.client.get('/evaluation' + store).get_data(as_text=True)
        self.assertEqual(2, page.count('data-analysis-toggle='))
        self.assertEqual(1, page.count('data-analysis-run'))
        self.assertEqual(403, self.client.put(root + '/conversations' + store, json={}).status_code)
        response = self.client.put(root + '/conversations' + store, headers=headers,
                                   json={'setting': 'conversations', 'enabled': False})
        self.assertEqual(200, response.status_code)
        controls = response.json['nightly_analyses']
        self.assertFalse(controls['conversations']['enabled'])
        self.assertTrue(controls['conversations']['apply_learned_rules'])
        response = self.client.put(root + '/conversations' + store, headers=headers,
                                   json={'setting': 'learned_rules', 'enabled': False})
        self.assertFalse(response.json['nightly_analyses']['conversations']['apply_learned_rules'])

        rules_url = root + '/products/rules'
        added = self.client.post(rules_url + store, headers=headers, json={'text': 'ركّز على قطع التصفية'})
        rule = added.json['nightly_analyses']['products']['rules'][0]
        target = f"{rules_url}/{rule['id']}"
        self.assertEqual(404, self.client.delete(target + '?store_id=default', headers=headers).status_code)
        self.assertEqual(404, self.client.delete(target.replace('/products/', '/conversations/') + store, headers=headers).status_code)
        edited = self.client.put(target + store, headers=headers, json={'text': 'راجع القياسات المعروضة'})
        self.assertEqual('راجع القياسات المعروضة', edited.json['nightly_analyses']['products']['rules'][0]['text'])
        queued = self.client.post(root + '/products/run' + store, headers=headers, json={})
        self.assertTrue(queued.json['nightly_analyses']['products']['busy'])
        report = {'clearance': [{'product_id': 'P1', 'suggestion': 'اعرض القياسات المتوفرة'}]}
        def respond(*args, **kwargs):
            body = json.loads(kwargs['json']['messages'][1]['content'])
            self.assertEqual({'products', 'metrics'}, set(body['evidence']))
            self.assertEqual(['راجع القياسات المعروضة'], body['analysis_rules'])
            return self.model_response(report)
        with patch.object(m.ai_transport, 'post', side_effect=respond) as provider:
            self.assertEqual('done', self.work()[1]['status'])
        self.assertEqual(2, provider.call_count)
        latest = self.client.get(root + store).json['nightly_analyses']
        self.assertFalse(latest['products']['busy'])
        self.assertEqual(report, latest['products']['report']['review'])
        self.assertIsNone(latest['conversations']['report'])
        switched = self.client.put(target + store, headers=headers, json={'enabled': False})
        self.assertFalse(switched.json['nightly_analyses']['products']['rules'][0]['enabled'])
        self.assertEqual(200, self.client.delete(target + store, headers=headers).status_code)
        self.assertTrue(self.client.get(root + '?store_id=default').json['nightly_analyses']['conversations']['enabled'])
        self.transport.assert_not_called()

    def test_confirmed_order_suggests_clearance_only_when_asked_and_sends_right_photo(self):
        self.start_patch(patch.object(m, 'PUBLIC_URL', 'https://catalog.test'))
        self.start_patch(patch.object(m, 'product_image_variants', side_effect=lambda product: [
            dict(url=product['image_url'], color='')] if product.get('image_url') else []))
        products = m.load_products_from_file()
        products[0].update(product_type='tops', audience='women')
        products.append(dict(products[0], product_id='P2', product_name='بلوزة تجربة',
            image_url=m.PUBLIC_URL + '/product_image/test-clearance.jpg', ad_ids=[],
            clearance=True, discount_iqd=2000, reply_category='بلوزة'))
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')
        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan())):
            self.assertEqual('booked', self.work()[1]['action'])
        self.transport.assert_called_once()
        self.assertEqual(CONFIRMATION, self.transport.call_args.args[2][0]['text'])

        self.incoming(11, 'اكو موديل ثاني يجي على قياسي؟')
        replies = iter([self.model_response(dict(intent='continue')),
            self.model_response(dict(parts=['إي عيني، أكو بلوزة تصفية تناسب قياسج. تحبين أدزلج صورتها؟']))])
        def respond(*args, **kwargs):
            context = json.loads(kwargs['json']['messages'][-1]['content'])
            self.assertFalse(context.get('validation_feedback'), context.get('validation_feedback'))
            return next(replies)
        with patch.object(m.ai_transport, 'post', side_effect=respond) as provider:
            self.assertFalse(self.work()[1].get('needs_human'))
        writer_context = json.loads(provider.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertEqual(['P2'], [item['product_id'] for item in writer_context['catalog_memory']['matches']])
        self.incoming(12, 'اي دزيها')
        with patch.object(m.ai_transport, 'post') as provider:
            self.assertFalse(self.work()[1].get('needs_human'))
            provider.assert_not_called()
        with m.app.app_context():
            db = m.get_db()
            self.assertEqual(1, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
            last_photo = db.execute("SELECT image_url FROM messages WHERE direction='outgoing' ORDER BY id DESC LIMIT 1").fetchone()[0]
            self.assertTrue(last_photo.endswith('test-clearance.jpg'))

    def test_price_objection_proposes_one_cheaper_product_without_changing_cart(self):
        self.start_patch(patch.object(m, 'PUBLIC_URL', 'https://catalog.test'))
        self.start_patch(patch.object(m, 'product_image_variants', side_effect=lambda product: [
            dict(url=product['image_url'], color='')] if product.get('image_url') else []))
        products = m.load_products_from_file()
        products[0].update(product_type='tops', audience='women')
        products.append(dict(products[0], product_id='P2', product_name='بلوزة تجربة',
            image_url=m.PUBLIC_URL + '/product_image/alternative.jpg', ad_ids=[], price='13000'))
        self.incoming(10, 'لأعيني غالي')
        decision = self.model_response(dict(intent='continue', changes=[dict(values=dict(product_id='P2', size='44'))]))
        written = self.model_response(dict(parts=[dict(type='text', text='أفهمج عيني، عندي خيار أوفر بـ{{alternative_price}}. تحبين تشوفين صورته؟')]))
        with patch.object(m.ai_transport, 'post', side_effect=[decision, written]) as provider:
            self.assertFalse(self.work()[1].get('needs_human'))
        context = json.loads(provider.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertEqual('price_hesitation', context['catalog_memory']['opportunity'])
        self.assertEqual({'alternative_price': 13000}, context['amounts'])
        with m.app.app_context():
            db = m.get_db()
            scope = live.scope_for(live.route(db, self.sender))
            from account_app import sales_intelligence
            self.assertTrue(sales_intelligence.already_suggested(db, scope, None))
            self.assertFalse(live.Store(db).snapshot(scope)['data']['cart'])
            self.assertEqual(0, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_sol_guides_booking_and_uncertain_confirmation_is_never_resent(self):
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')
        self.transport.return_value = {'ok': False, 'status': 'transport_error', 'status_code': 0}
        advice = dict(decision='assist', reason='test', guidance='احفظي الاختيار الأخير وتابعي الحجز.',
                      changes=self.model_plan()['changes'])
        with patch.object(m.ai_transport, 'post', side_effect=[
            self.model_response(None, status=503), self.model_response(advice), self.model_response(self.model_plan()),
        ]) as post:
            kind, result = self.work()
        self.assertEqual(live.JOB_KIND, kind)
        self.assertTrue(result.get('needs_human'), result)
        self.assertEqual([m.MAIN_MODEL, BACKUP_MODEL, m.MAIN_MODEL],
                         [call.kwargs['json']['model'] for call in post.call_args_list])
        consult_payload = post.call_args_list[1].kwargs['json']['messages'][-1]['content']
        self.assertNotIn(self.phone, consult_payload)
        self.assertNotIn(self.address, consult_payload)
        self.transport.assert_called_once()
        with m.app.app_context():
            db = m.get_db()
            order = db.execute('SELECT id,phone FROM orders').fetchone()
            self.assertEqual(self.phone, order['phone'])
            self.assertEqual('unknown', db.execute("SELECT status FROM sales_v2_order_tasks WHERE kind='confirmation'").fetchone()[0])
        response = self.client.post(f'/api/orders/{order["id"]}/resend_confirmation?store_id=khuyoot',
                                    headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(409, response.status_code, response.get_json())
        self.transport.assert_called_once()

    def test_store_switch_supersedes_inflight_booking_even_if_enabled_again(self):
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')

        def toggle(*args, **kwargs):
            with m.app.app_context():
                db = m.get_db()
                m.set_store_ai_enabled(db, 'khuyoot', False)
                m.set_store_ai_enabled(db, 'khuyoot', True)
            return self.model_response(self.model_plan())

        with patch.object(m.ai_transport, 'post', side_effect=toggle) as post:
            self.assertEqual('conversation_changed', self.work()[1]['reason'])
        self.assertEqual(1, post.call_count)
        self.transport.assert_not_called()
        with m.app.app_context():
            self.assertEqual(0, m.get_db().execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_identity_in_burst_stops_before_provider_and_conversation_switch_resumes_new_messages(self):
        self.incoming(10, 'إنتِ AI؟')
        self.incoming(11, 'زين شكد سعره؟')
        with patch.object(m.ai_transport, 'post') as post:
            self.work()
            self.assertTrue(self.work()[1]['needs_human'])
            self.incoming(12, 'متوفر؟')
            self.assertEqual('ai_disabled', self.work()[1]['reason'])
        post.assert_not_called()
        self.transport.assert_not_called()
        response = self.client.post(f'/api/conversations/{self.sender}/ai?store_id=khuyoot',
                                    json={'enabled': True}, headers={'X-CSRF-Token': 'test-csrf'})
        self.assertEqual(202, response.status_code, response.get_json())
        with m.app.app_context():
            db = m.get_db()
            self.assertTrue(m.is_customer_ai_enabled(db, self.sender))
            self.assertEqual('active', live.Store(db).snapshot(live.scope_for(live.route(db, self.sender)))['data']['control_mode'])
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])

    def test_new_message_during_identity_handoff_reuses_the_review(self):
        self.incoming(10, 'أريد موظفة')
        handoff = live.Entrypoint.handoff_service

        def add_new_input(entry, scope, event_id, reason):
            result = handoff(entry, scope, event_id, reason)
            self.incoming(11, 'شكد السعر؟')
            return result

        with patch.object(live.Entrypoint, 'handoff_service', add_new_input), patch.object(m.ai_transport, 'post') as model:
            self.assertEqual('conversation_changed', self.work()[1]['reason'])
            self.assertEqual('customer_requested_human', self.work()[1]['reason'])
        model.assert_not_called()
        self.transport.assert_not_called()
        with m.app.app_context():
            self.assertEqual(1, m.get_db().execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])

    def test_preexisting_conversation_keeps_legacy_queue_after_activation(self):
        with patch.dict(os.environ, SALES_RUNTIME_KHUYOOT_PERCENT='0'):
            self.incoming(10, 'هلو')
        self.incoming(11, 'شكد السعر؟')
        with m.app.app_context():
            db = m.get_db()
            self.assertIsNone(live.route(db, self.sender))
            self.assertEqual(['webhook', 'webhook'], [row[0] for row in db.execute('SELECT kind FROM ai_jobs ORDER BY created_at')])

    def test_full_rollout_books_in_all_five_stores_through_their_existing_provider(self):
        account = 'a' * 24
        chatwoot_stores = ['default', 'khuyoot', 'al-fatena', 'golden-threads']
        stores = chatwoot_stores + ['baraah-kids']
        provider_map = {f'1:{index}': store for index, store in enumerate(chatwoot_stores, 2)}
        self.start_patch(patch.dict(os.environ, SALES_RUNTIME_MODE='all', CHATWOOT_INBOX_STORES=json.dumps(provider_map),
            ZERNIO_BARAAH_KIDS_ENABLED='1', ZERNIO_API_KEY='synthetic-zernio',
            ZERNIO_WEBHOOK_SECRET='synthetic-zernio-secret',
            ZERNIO_ACCOUNTS=json.dumps({account: {'store_id': 'baraah-kids', 'platform': 'facebook'}})))
        product = m.load_products_from_file()[0]
        self.start_patch(patch.object(m, 'load_products_from_file', side_effect=lambda store_id=None, **kwargs: [
            dict(product, store_id=store_id or m.current_store_id())]))
        zernio = self.start_patch(patch.object(m.zernio, 'send',
            return_value={'ok': True, 'status_code': 200, 'provider_id': 'synthetic-zn-receipt'}))
        text = f'أريد أزرق قياس 44، {self.phone}، {self.address}'
        for index, store in enumerate(chatwoot_stores, 2):
            conversation = dict(self.payload['conversation'], id=index + 10)
            response = self.post(dict(self.payload, id=index + 20, inbox={'id': index}, conversation=conversation, content=text))
            self.assertEqual(200, response.status_code, (store, response.get_json()))
        stamp = m.now_baghdad_iso()
        body = json.dumps(dict(event='message.received', id='synthetic-zn-event', timestamp=stamp,
            account={'id':account,'platform':'facebook'}, conversation={'id':'fixture-zn','participantId':'fixture-parent'},
            message=dict(id='fixture-message',direction='incoming',platform='facebook',text=text),
            referral={'ad_id':'1234'})).encode()
        signature = hmac.new(b'synthetic-zernio-secret', body, hashlib.sha256).hexdigest()
        response = self.client.post('/zernio/webhook/baraah-kids', data=body, content_type='application/json',
                                    headers={'X-Zernio-Signature':signature})
        self.assertEqual(200, response.status_code, response.get_json())
        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan())) as model:
            for store in stores:
                kind, result = self.work()
                self.assertEqual((live.JOB_KIND,'booked'), (kind,result.get('action')), (store,result))
        self.assertEqual(5, model.call_count)
        self.assertEqual(4, self.transport.call_count)
        zernio.assert_called_once()
        self.assertIn('confirmation', zernio.call_args.kwargs['operation_key'])
        self.assertEqual(5, self.notify.call_count)
        with m.app.app_context():
            db = m.get_db()
            routes = dict(db.execute('SELECT store_id,channel FROM sales_v2_live_routes'))
            self.assertEqual({**dict.fromkeys(chatwoot_stores,'chatwoot'),'baraah-kids':'zernio'}, routes)
            self.assertEqual(set(stores), {row[0] for row in db.execute('SELECT store_id FROM orders')})
            self.assertEqual(set(stores) - {'golden-threads'},
                             {row[0] for row in db.execute('SELECT local_store FROM menger_deliveries')})
        status = self.client.get('/health').get_json()['sales_runtime']
        self.assertEqual('all', status['mode'])
        self.assertTrue(all(store['active'] and store['percent'] == 100 for store in status['stores']))

    def seed_legacy_messages(self, last_reply, meta=None, *, later_reply=None):
        with m.app.app_context():
            token = m._current_store_id.set('khuyoot')
            try:
                db = m.get_db()
                m.get_or_create_customer(db, self.sender, '1', 'instagram')
                m.save_message(db, self.sender, 'incoming', 'text', 'هذا القميص شتوي؟')
                m.save_message(db, self.sender, 'outgoing', 'text', last_reply, raw_payload=meta or {})
                if later_reply:
                    m.save_message(db, self.sender, 'incoming', 'text', 'شوكت يوصلني الطلب؟')
                    m.save_message(db, self.sender, 'outgoing', 'text', later_reply)
            finally:
                m._current_store_id.reset(token)

    def test_settled_legacy_thread_is_adopted_with_history_as_context_only(self):
        self.start_patch(patch.dict(os.environ, SALES_RUNTIME_MODE='all'))
        self.seed_legacy_messages('إي حبيبتي، قماشه شتوي.')
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')
        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan())) as model:
            self.assertEqual('booked', self.work()[1]['action'])
        context = json.loads(model.call_args.kwargs['json']['messages'][-1]['content'])
        self.assertEqual('هذا القميص شتوي؟', context['recent_messages'][0]['text'])
        with m.app.app_context():
            db = m.get_db()
            self.assertEqual(1, db.execute("SELECT COUNT(*) FROM sales_v2_events WHERE kind='message'").fetchone()[0])
            self.assertEqual(1, db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_active_legacy_checkout_retains_its_draft_during_full_rollout(self):
        self.start_patch(patch.dict(os.environ, SALES_RUNTIME_MODE='all'))
        self.seed_legacy_messages('باقي العنوان حتى أثبتلج', {'checkout_draft':{'product_id':'P1'}},
                                  later_reply='يوصلج خلال 24 ساعة إلى يومين.')
        self.incoming(10, self.address)
        with m.app.app_context():
            db = m.get_db()
            self.assertIsNone(live.route(db, self.sender))
            self.assertEqual('webhook', db.execute('SELECT kind FROM ai_jobs').fetchone()[0])

    def test_returning_buyer_can_place_new_order_after_a_delivered_one(self):
        self.start_patch(patch.dict(os.environ, SALES_RUNTIME_MODE='all'))
        self.incoming(10, f'أريد أزرق قياس 44، {self.phone}، {self.address}')
        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan())):
            self.assertEqual('booked', self.work()[1]['action'])
        with m.app.app_context():
            db = m.get_db()
            db.execute("UPDATE orders SET status='delivered'")
            db.commit()
        self.incoming(11, f'أريد ماروني قياس 44، {self.phone}، {self.address}')
        with patch.object(m.ai_transport, 'post', return_value=self.model_response(self.model_plan(color='ماروني'))):
            self.assertEqual('booked', self.work()[1]['action'])
        with m.app.app_context():
            self.assertEqual([('أزرق','delivered'),('ماروني','new')],
                             [tuple(row) for row in m.get_db().execute('SELECT color,status FROM orders ORDER BY id')])
