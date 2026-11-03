"""Synthetic regressions for the November 1 last-50 conversation shapes."""
import json
import unittest
from unittest.mock import patch

from account_app import sales_context, sales_playbook, sales_voice, reply_layout, owner_sales, product_links
from account_app.tests import test_checkout_regressions as base
from account_app.tests import test_checkout_completion_oct25 as pipeline


class Last50MemoryTests(unittest.TestCase):
    def test_accepting_one_offered_color_survives_contact_bubbles(self):
        context = sales_context.advance({}, [
            dict(direction='incoming', text='قياس ٤٦ وزني ٧٧ كيلو'),
            dict(direction='outgoing', text='اللون بالصورة جوزي، تحبين نكمل على الجوزي؟'),
            dict(direction='incoming', text='نكمل'),
            dict(direction='outgoing', text='دزيلي رقم الموبايل والعنوان'),
            dict(direction='incoming', text='بغداد حي الاختبار')])
        selected = sales_playbook.customer_selection(context)
        self.assertEqual((selected['size'], selected['color']), ('46', 'جوزي'))

    def test_accepting_a_list_does_not_invent_one_color(self):
        context = sales_context.advance({}, [dict(direction='outgoing', text='أسود لو وردي؟'),
                                            dict(direction='incoming', text='تمام')])
        self.assertIsNone(sales_playbook.customer_selection(context)['color'])

    def test_selected_44_is_not_replaced_by_weight_70(self):
        context = sales_context.advance({}, [dict(direction='incoming', text='قياس 42'),
            dict(direction='outgoing', text='تحبين نثبت حجز؟'),
            dict(direction='incoming', text='خلي 44'), dict(direction='incoming', text='وزني 70')])
        self.assertEqual(sales_playbook.customer_selection(context)['size'], '44')

    def test_photo_color_belongs_to_its_product_not_the_next_one(self):
        def photo(pid, color):
            return dict(direction='incoming', text='', message_type='image',
                raw_payload=json.dumps({'_catalog_selections':[dict(product_id=pid, observed_color=color)]}))
        context = sales_context.advance({}, [photo('A', 'جوزي'),
            dict(direction='incoming', text='قياس 46'), photo('B', 'وردي')])
        selected = sales_playbook.customer_selection(context)
        self.assertEqual(selected['color'], 'وردي')
        self.assertIsNone(selected['size'])

    def test_fabric_sizes_and_colors_are_all_answered(self):
        product = dict(product_id='P', product_name='قميص', fabric='قطن', sizes='38 إلى 52',
                       colors='سماوي', price='13000', stock='متوفر')
        result = sales_voice.answer('شنو القماش والقياسات والالوان؟', product, {}, {}, [], {})
        for fact in ('قطن', '38', '52', 'سماوي'):
            self.assertIn(fact, result['reply'])
        self.assertNotIn('يا لون عجبج', result['reply'])

    def test_long_human_sales_answer_is_split_without_losing_facts(self):
        text = 'القماش قديفة ثقيل بتطريز على الصدر والردن. ' + \
            'سعر القطعة 15 ألف والتوصيل 5 آلاف، المجموع 20 ألف. ' + \
            'الألوان المتوفرة ماروني وأحمر وأخضر وأصفر وخردلي. شنو اللون اللي يعجبج؟'
        result = reply_layout.plan_sales_bubbles({'_human_sales_voice':True}, text)
        self.assertGreater(len(result['reply_parts']), 1)
        self.assertEqual(reply_layout.compact(' '.join(result['reply_parts'])), text)

    def test_absolute_fit_promise_is_removed(self):
        self.assertNotIn('ضيق أبد', owner_sales.conversational_wording('قياس 50 متوفر وما يطلع ضيق أبد.'))

    def test_alternatives_use_known_115_kg_without_a_new_size_question(self):
        source = dict(product_id='A', product_name='قميص', category='tops', sizes='38 إلى 52، من 55 إلى 100 كيلو',
                      colors='سماوي', price='13000', stock='متوفر', status='active', store_id='kh')
        alternative = dict(source, product_id='B', sizes='من 100 كيلو إلى 125 كيلو', image_url='https://images.test/b.jpg')
        found = product_links.catalog_alternatives(source, [source, alternative], weight=115)
        self.assertEqual([p['product_id'] for p in found], ['B'])


class Last50PipelineTests(unittest.TestCase):
    setUpClass = classmethod(base.CheckoutRegressionTests.setUpClass.__func__)
    tearDownClass = classmethod(base.CheckoutRegressionTests.tearDownClass.__func__)
    setUp = base.CheckoutRegressionTests.setUp
    tearDown = base.CheckoutRegressionTests.tearDown
    data = base.CheckoutRegressionTests.data
    process = pipeline.CheckoutCompletionTests.process

    def save(self, direction, text, **kwargs):
        return self.m.save_message(self.db, self.sender, direction, kwargs.get('kind', 'text'), text,
                                   kwargs.get('image'), None, None, kwargs.get('payload', {}))

    def link(self, colors='اسود'):
        self.product = dict(base.CATALOG[0], colors=colors)
        for name in ('load_products_from_file', 'load_active_products'):
            mocked = patch.object(self.m, name, return_value=[self.product])
            mocked.start(); self.patches.append(mocked)
        self.m.bind_customer_to_product(self.db, self.sender, self.product, source='image_recognition')

    def remember_photo(self, color='جوزي'):
        url = 'https://images.test/chosen.jpg'
        self.save('incoming', '', kind='image', image=url)
        self.m.customer_image_positions(self.db, self.sender, [dict(image_index=1, product_found=True, product_id='F1',
            image_url=url, observed_color=color)])

    def order(self):
        return self.db.execute('SELECT * FROM orders WHERE sender_id=?', (self.sender,)).fetchone()

    def test_photo_size_phone_then_address_book_without_reasking(self):
        self.link('جوزي ماروني')
        self.remember_photo()
        self.save('incoming', 'قياس ٤٦')
        self.save('outgoing', 'اللون بالصورة جوزي، تحبين نكمل على الجوزي؟')
        self.save('incoming', 'نكمل')
        self.save('outgoing', 'دزيلي رقم الموبايل والعنوان حتى أثبت الحجز')
        result, model = self.process('07701234567')
        model.assert_not_called()
        self.assertNotIn('لون', result['reply'])
        result, model = self.process('بغداد حي الاختبار')
        model.assert_not_called()
        self.assertIn('تم تثبيت الطلب', result['reply'])
        self.assertIn(self.m.DEFAULT_ORDER_CONFIRMATION_TEXT, result['reply'])
        items = json.loads(self.order()['order_items'])
        self.assertEqual((items[0]['color'], items[0]['size']), ('جوزي', '46'))

    def test_full_contacts_book_known_choice_without_prior_contact_request(self):
        self.link()
        self.save('incoming', 'أريد الأسود قياس 44')
        self.save('outgoing', 'الأسود متوفر بقياس 44.')
        result, model = self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called()
        self.assertIn('تم تثبيت الطلب', result['reply'])
        self.assertIsNotNone(self.order())

    def test_repeated_verified_photo_keeps_a_two_line_saved_cart(self):
        self.link()
        other = base.CATALOG[2]
        for name in ('load_products_from_file', 'load_active_products'):
            mocked = patch.object(self.m, name, return_value=[self.product, other])
            mocked.start(); self.patches.append(mocked)
        self.m.complete_customer_product_link(self.db, self.sender, other, 'image_recognition', source='image_recognition', preserve_existing=True)
        self.save('outgoing', 'دزيلي الرقم والعنوان حتى أثبت القطعتين', payload={'checkout_draft':self.data(True)})
        self.remember_photo('اسود')
        result, model = self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called()
        self.assertIn('تم تثبيت الطلب', result['reply'])
        self.assertEqual(len(json.loads(self.order()['order_items'])), 2)

    def client(self):
        client = self.m.app.test_client()
        with client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        return client

    def test_manual_free_text_and_missing_contacts_are_preserved(self):
        data = dict(items=[dict(product_name='قطعة يدوية', size='قياس خاص', weight='80-90',
                               color='لون حسب الصورة', quantity=25, unit_price=12000)])
        with patch.object(self.m, 'send_text_to_facebook', return_value=True):
            response = self.client().post(f'/api/conversations/{self.sender}/create_order', json=data)
        self.assertEqual(response.status_code, 200, response.get_json())
        items = json.loads(self.order()['order_items'])
        for key, value in data['items'][0].items():
            self.assertEqual(items[0][key], value)

    def test_confirmation_failure_can_be_retried_without_rebooking(self):
        with patch.object(self.m, 'send_text_to_facebook', return_value=False):
            result = self.client().post(f'/api/conversations/{self.sender}/create_order', json=self.data()).get_json()
        self.assertFalse(result['confirmation_sent'])
        row = self.db.execute("SELECT * FROM messages WHERE sender_id=? AND direction='outgoing' ORDER BY id DESC LIMIT 1", (self.sender,)).fetchone()
        self.assertEqual(json.loads(row['raw_payload'])['_delivery_status'], 'unconfirmed')
        with patch.object(self.m, 'send_text_to_facebook', return_value=True) as send:
            endpoint = f"/api/orders/{result['order_id']}/resend_confirmation?store_id=al-fatena"
            self.assertTrue(self.client().post(endpoint).get_json()['confirmation_sent'])
            self.assertTrue(self.client().post(endpoint).get_json()['confirmation_sent'])
        send.assert_called_once()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 1)

    def test_unknown_manual_price_saves_but_is_not_shipped_as_a_free_item(self):
        with patch.object(self.m, 'send_text_to_facebook', return_value=True):
            result = self.client().post(f'/api/conversations/{self.sender}/create_order',
                json=dict(items=[dict(product_name='قطعة خارج الكتالوج', quantity=1)])).get_json()
        self.assertTrue(result['ok']); self.assertTrue(result['price_pending'])
        row = self.db.execute('SELECT status FROM menger_deliveries WHERE order_id=?', (result['order_id'],)).fetchone()
        if row: self.assertEqual(row['status'], 'review_required')

    def test_unconfirmed_automatic_receipt_is_recovered_without_duplicate_order(self):
        self.link()
        self.save('incoming', 'ثبتي الأسود قياس 44')
        result, _ = self.process('07701234567 بغداد حي الاختبار')
        self.assertIn(self.m.DEFAULT_ORDER_CONFIRMATION_TEXT, result['reply'])
        row = self.db.execute("SELECT id,raw_payload FROM messages WHERE sender_id=? AND direction='outgoing' ORDER BY id DESC LIMIT 1", (self.sender,)).fetchone()
        meta = json.loads(row['raw_payload']); meta['_delivery_status'] = 'unconfirmed'
        self.db.execute('UPDATE messages SET raw_payload=? WHERE id=?', (json.dumps(meta), row['id'])); self.db.commit()
        cart = {'order': self.data()}
        with patch.object(self.m, 'send_order_to_telegram') as telegram:
            created, reply = self.m.create_order_if_valid(self.db, self.sender, cart, self.product)
        self.assertTrue(created); self.assertIn(self.m.DEFAULT_ORDER_CONFIRMATION_TEXT, reply)
        self.assertEqual(cart['_created_order_id'], self.order()['id'])
        telegram.assert_not_called()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?', (self.sender,)).fetchone()[0], 1)

    def test_confirmation_retry_cannot_send_another_stores_order(self):
        with patch.object(self.m, 'send_text_to_facebook', return_value=True):
            result = self.client().post(f'/api/conversations/{self.sender}/create_order', json=self.data()).get_json()
        with patch.object(self.m, 'send_text_to_facebook') as send:
            response = self.client().post(f"/api/orders/{result['order_id']}/resend_confirmation?store_id=baraah-kids")
        self.assertEqual(response.status_code, 404); send.assert_not_called()

    def test_manual_order_without_contacts_is_still_deduplicated(self):
        data = dict(items=[dict(product_name='قطعة يدوية', color='لون الصورة', quantity=1, unit_price=12000)])
        with patch.object(self.m, 'send_text_to_facebook', return_value=True) as send:
            first = self.client().post(f'/api/conversations/{self.sender}/create_order', json=data).get_json()
            second = self.client().post(f'/api/conversations/{self.sender}/create_order', json=data).get_json()
        self.assertTrue(first['ok']); self.assertTrue(second['duplicate'])
        send.assert_called_once()
        self.assertEqual(first['order_id'], second['existing_order_id'])

    def test_link_flow_keeps_failed_confirmation_for_retry(self):
        self.link()
        self.save('incoming', 'أريد الأسود قياس 44')
        self.save('outgoing', 'دزيلي الرقم والعنوان حتى أثبت الحجز')
        self.save('incoming', '07701234567 بغداد حي الاختبار')
        with patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'send_webhook_result_to_facebook', return_value=False), \
             patch.object(self.m, '_call_main_ai_once') as model:
            result = self.m.auto_reply_after_product_link(self.db, self.sender, self.product)
        self.assertFalse(result['sent']); model.assert_not_called()
        row = self.db.execute("SELECT raw_payload FROM messages WHERE sender_id=? AND direction='outgoing' ORDER BY id DESC LIMIT 1", (self.sender,)).fetchone()
        meta = json.loads(row['raw_payload'])
        self.assertEqual(meta['_delivery_status'], 'unconfirmed')
        self.assertEqual(meta['_order_confirmation_id'], self.order()['id'])

    def test_interrupted_confirmation_attempt_can_be_retried_after_lease_expires(self):
        with patch.object(self.m, 'send_text_to_facebook', return_value=False):
            result = self.client().post(f'/api/conversations/{self.sender}/create_order', json=self.data()).get_json()
        row = self.db.execute("SELECT id,raw_payload FROM messages WHERE sender_id=? AND direction='outgoing' ORDER BY id DESC LIMIT 1", (self.sender,)).fetchone()
        meta = json.loads(row['raw_payload']); meta.update(_delivery_status='pending', _confirmation_attempt_at=0)
        self.db.execute('UPDATE messages SET raw_payload=? WHERE id=?', (json.dumps(meta), row['id'])); self.db.commit()
        with patch.object(self.m, 'send_text_to_facebook', return_value=True) as send:
            response = self.client().post(f"/api/orders/{result['order_id']}/resend_confirmation?store_id=al-fatena").get_json()
        self.assertTrue(response['confirmation_sent']); send.assert_called_once()

    def test_both_photo_models_book_from_common_age_without_a_saved_cart(self):
        self.m._current_store_id.reset(self.token)
        self.token = self.m._current_store_id.set('baraah-kids')
        self.db.execute('UPDATE customers SET store_id=? WHERE sender_id=?', ('baraah-kids', self.sender))
        self.db.commit(); self.ev['store_id'] = 'baraah-kids'
        self.link('اسود وردي')
        self.product['product_name'] = 'فستان اختبار أطفال'
        self.product['sizes'] = 'من 2 إلى 7 سنوات'
        self.m.bind_customer_to_product(self.db, self.sender, self.product, source='image_recognition')
        other = dict(self.product, product_id='F2', product_name='طقم اختبار أطفال', colors='وردي كريمي')
        for name in ('load_products_from_file', 'load_active_products'):
            mocked = patch.object(self.m, name, return_value=[self.product, other])
            mocked.start(); self.patches.append(mocked)
        self.m.complete_customer_product_link(self.db, self.sender, other, 'image_recognition', source='image_recognition', preserve_existing=True)
        self.remember_photo('اسود')
        url = 'https://images.test/second.jpg'
        self.save('incoming', '', kind='image', image=url)
        self.m.customer_image_positions(self.db, self.sender, [dict(image_index=1, product_found=True,
            product_id='F2', image_url=url, observed_color='وردي')])
        self.save('incoming', 'نفس لون الصور قياس 6 سنوات أريدهم اثنينهم')
        self.save('outgoing', 'دزيلي الرقم والعنوان حتى أثبت الحجز')
        result, model = self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called(); self.assertIn(self.m.DEFAULT_ORDER_CONFIRMATION_TEXT, result['reply'])
        items = json.loads(self.order()['order_items'])
        self.assertEqual({(i['product_id'], i['color'], i['size']) for i in items},
                         {('F1', 'أسود', '6 سنوات'), ('F2', 'وردي', '6 سنوات')})

    def test_multiple_photos_without_buying_both_are_not_a_two_piece_cart(self):
        self.link()
        other = dict(self.product, product_id='F2', product_name='قطعة اختبار ثانية')
        context = sales_context.advance({}, [dict(direction='incoming', text='قياس 44 أسود')])
        candidate = self.m.resume_selected_multi_checkout(self.db, dict(self.ev, text='07701234567 بغداد حي الاختبار'),
            {}, [self.product, other], context)
        self.assertIsNone(candidate); self.assertIsNone(self.order())

    def test_multi_checkout_does_not_reduce_two_of_each_to_one_each(self):
        self.link()
        context = sales_context.advance({}, [dict(direction='incoming', text='أريدهم اثنينهم قطعتين من كل موديل قياس 44',
                                                  created_at=self.m.now_baghdad_iso())])
        candidate = self.m.resume_selected_multi_checkout(self.db, self.ev, {},
            [self.product, dict(self.product, product_id='F2')], context)
        self.assertIsNone(candidate); self.assertIsNone(self.order())
