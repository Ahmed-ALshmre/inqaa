"""New dress photos and corrections must not inherit a booked set's identity."""
import json
import unittest
from unittest.mock import patch

from account_app import product_discussion
from account_app.tests import test_audit_orders as fixtures

SET = dict(product_id='P010', product_name='طقم بناتي', category='طقم', price='13000',
           stock='متوفر', status='active', colors='اسود', sizes='2 إلى 7 سنوات',
           image_url='https://images.test/set.jpg')
DRESS = dict(product_id='P017', product_name='فستان الاميرات', category='فستان', price='12000',
             stock='متوفر', status='active', colors='اخضر وردي', sizes='2 إلى 7 سنوات',
             image_url='https://images.test/dress.jpg')


class CorrectionTypeTests(unittest.TestCase):
    def test_reported_corrections_request_a_dress_without_guessing_its_catalog_id(self):
        for text in ['اللي دزيتيليا طقم مو نفنوف حبيبتي',
                     'والله هذا نفنوف بي خرطه من الوسط كامل مو قطعتين',
                     'مو طقم اريد نفنوف']:
            self.assertEqual(product_discussion.correction_kind(text), 'dress')
        self.assertEqual(product_discussion.correction_kind('هذا طقم مو نفنوف'), 'set')
        self.assertEqual(product_discussion.correction_kind('إذا مو نفس الصورة أرجعه'), '')


class PostOrderProductTests(unittest.TestCase):
    def setUp(self):
        fixtures.AuditFixTests.setUp(self)
        self.token = self.m._current_store_id.set('default')
        fixtures.AuditFixTests.customer(self, 'discussion')
        self.sender = 'discussion'
        self.products = [SET, DRESS]
        self.patches = [patch.object(self.m, 'load_products_from_file', return_value=self.products),
                        patch.object(self.m, 'load_active_products', return_value=self.products),
                        patch.object(self.m, 'is_store_feature_enabled', return_value=False),
                        patch.object(self.m, 'send_telegram_message', return_value=False),
                        patch.object(self.m, 'send_telegram_photo', return_value=False)]
        for item in self.patches: item.start()
        self.m.complete_customer_product_link(self.db, self.sender, SET, 'manual', source='manual_admin')
        items = json.dumps([dict(product_id='P010', product_name=SET['product_name'],
                                color='اسود', size='2.5 سنة', quantity=1)], ensure_ascii=False)
        self.db.execute("INSERT INTO orders(sender_id,store_id,product_id,product_name,color,size,phone,province,address,"
                        "status,order_items,product_total,delivery_fee,total_amount,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,'new',?,13000,5000,18000,?)",
                        (self.sender, 'default', 'P010', SET['product_name'], 'اسود', '2.5 سنة',
                         '07701234567', 'بغداد', 'حي تجريبي', items, self.m.now_baghdad_iso()))
        self.db.commit()
        self.order = self.m.get_latest_customer_order(self.db, self.sender)
        self.snapshot = dict(self.order)
        self.ev = dict(sender_id=self.sender, page_id='page', platform='facebook', store_id='default',
                       text='', image_url=None, attachments=[], ref=None, ad_id=None)

    def tearDown(self):
        for item in reversed(self.patches): item.stop()
        self.m._current_store_id.reset(self.token)
        fixtures.AuditFixTests.tearDown(self)

    def assert_order_unchanged(self):
        self.assertEqual(dict(self.m.get_latest_customer_order(self.db, self.sender)), self.snapshot)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 1)

    def handle(self, text='', image=None, reply='هذا فستان الاميرات، متوفر بالأخضر والوردي.'):
        event = dict(self.ev, text=text, image_url=image)
        self.m.save_message(self.db, self.sender, 'incoming', 'image' if image else 'text',
                            text, image, None, None, {})
        with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply=reply, create_order=False, order={})) as model:
            result = self.m.handle_post_order_message(self.db, event, {}, self.m.load_history(self.db, self.sender),
                self.products, self.m.load_customer_products(self.db, self.sender), self.order)
        self.assert_order_unchanged()
        return result, model

    def test_new_photo_becomes_discussion_focus_without_changing_order_or_asking_replace(self):
        with patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=True, product_id='P017')) as vision:
            result, model = self.handle('إذا موجود من هل النفنوف اخضر اسود خلي اشوفه', 'https://images.test/customer-dress.jpg')
        vision.assert_called_once()
        self.assertIn('فستان الاميرات', result['reply'])
        self.assertNotIn('طقم', result['reply'])
        self.assertEqual(model.call_args.args[5]['product_id'], 'P017')
        self.assertEqual(model.call_args.args[0]['_post_order']['product_id'], 'P010')
        self.assertIsNone(self.m.pending_product_choice(self.db, self.sender))
        self.assertEqual(self.m.load_customer_products(self.db, self.sender)[0]['product_id'], 'P017')

    def test_webhook_analyzes_new_photo_even_when_an_order_already_exists(self):
        event = dict(self.ev, text='يعني هذا ماكو منه؟', image_url='https://images.test/customer-dress.jpg',
                     timestamp=0, postback=None, quick_reply=None, referral_source=None, referral_type=None)
        with patch.object(self.m, 'extract_facebook_event', return_value=event), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), \
             patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=True, product_id='P017')) as vision, \
             patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='هذا فستان الاميرات، متوفر بالأخضر والوردي.', create_order=False, order={})) as model:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        vision.assert_called_once()
        self.assertEqual(model.call_args.args[5]['product_id'], 'P017')
        self.assertIn('فستان الاميرات', result['reply'])
        self.assert_order_unchanged()

    def test_followup_keeps_new_photo_and_shipment_question_uses_saved_order(self):
        with patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=True, product_id='P017')) as vision:
            self.handle(image='https://images.test/customer-dress.jpg')
            result, model = self.handle('شنو الالوان الموجودة منه؟')
            self.assertEqual(model.call_args.args[5]['product_id'], 'P017')
            self.assertEqual(vision.call_count, 1)
        with patch.object(self.m, 'call_main_ai', return_value=dict(reply='طلبج مسجل، موعد الوصول يحتاج تأكيد.')) as model:
            self.m.handle_post_order_message(self.db, dict(self.ev, text='شوكت يوصل طلبي؟'), {}, [],
                self.products, self.m.load_customer_products(self.db, self.sender), self.order)
        self.assertEqual(model.call_args.args[5]['product_id'], 'P010')
        self.assert_order_unchanged()

    def test_correction_rechecks_archived_photo_instead_of_reusing_wrong_cached_set(self):
        with patch.object(self.m, 'match_customer_image_with_catalog', side_effect=[
                dict(product_found=True, product_id='P010'), dict(product_found=True, product_id='P017')]) as vision:
            self.handle(image='https://images.test/customer-dress.jpg', reply='الطقم الأسود متوفر.')
            result, model = self.handle('اللي دزيتيليا طقم مو نفنوف حبيبتي')
        self.assertEqual(vision.call_count, 2)
        self.assertEqual([p['product_id'] for p in vision.call_args.args[1]], ['P017'])
        self.assertEqual(model.call_args.args[5]['product_id'], 'P017')
        self.assertIn('فستان', result['reply'])

    def test_unrecognized_photo_and_followup_never_fall_back_to_old_set(self):
        with patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=False)):
            result, model = self.handle(image='https://images.test/unknown-dress.jpg')
        model.assert_not_called()
        self.assertEqual(result['reply'], '')
        self.assertTrue(result['meta']['needs_human'])
        result, model = self.handle('يعني هذا ماكو منه؟')
        model.assert_not_called()
        self.assertEqual(result['reply'], '')
        review = self.db.execute('SELECT image_url FROM human_reviews WHERE id=?', (result['meta']['human_review_id'],)).fetchone()
        self.assertEqual(review['image_url'], 'https://images.test/unknown-dress.jpg')

    def test_service_failure_does_not_relabel_old_set_as_customer_photo(self):
        with patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=False, service_error=True, error_code='billing')):
            result, model = self.handle(image='https://images.test/unavailable-dress.jpg')
        model.assert_not_called()
        self.assertEqual(result['reply_parts'], [])
        self.assertTrue(result['meta']['needs_human'])

    def test_type_correction_without_identifiable_photo_does_not_guess_a_dress(self):
        result, model = self.handle('والله هذا نفنوف كامل مو قطعتين')
        model.assert_not_called()
        self.assertEqual(result['reply'], '')
        positions = self.m.customer_image_positions(self.db, self.sender)
        self.assertFalse(positions[0]['product_found'])

    def test_manual_identification_can_resolve_unknown_discussion(self):
        self.handle('والله هذا نفنوف كامل مو قطعتين')
        event, matched, selected, analysis = self.m.post_order_product_focus(self.db,
            dict(self.ev, text='والله هذا نفنوف كامل مو قطعتين', _manual_product_id='P017'),
            self.products, [DRESS])
        self.assertEqual(matched['product_id'], 'P017')
        self.assertFalse(analysis['unmatched_customer_image'])
        self.assertEqual([p['product_id'] for p in selected], ['P017'])
        self.assert_order_unchanged()

    def test_different_named_product_becomes_discussion_focus(self):
        result, model = self.handle('فستان الاميرات شنو الوانه؟')
        self.assertEqual(model.call_args.args[5]['product_id'], 'P017')
        self.assertIn('فستان', result['reply'])

    def test_sold_out_photo_retains_its_identity(self):
        self.products[1] = dict(DRESS, stock='نفذ')
        with patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=True, product_id='P017')):
            result, model = self.handle(image='https://images.test/sold-out-dress.jpg', reply='فستان الاميرات مو متوفر حالياً.')
        self.assertEqual(model.call_args.args[5]['product_id'], 'P017')
        self.assertIn('مو متوفر', result['reply'])

    def test_reply_cannot_call_confirmed_dress_a_set(self):
        with patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=True, product_id='P017')):
            result, model = self.handle(image='https://images.test/customer-dress.jpg',
                reply='اي هذا الموديل طقم قطعتين، مو نفنوف مفرد.')
        self.assertEqual(result['reply'], '')
        self.assertTrue(result['meta']['needs_human'])

    def test_checker_retry_cannot_restore_wrong_set_identity(self):
        self.m.customer_image_positions(self.db, self.sender, [dict(image_index=1,
            _post_order_discussion=True, product_found=True, product_id='P017')])
        with patch.object(self.m, 'is_store_feature_enabled', return_value=True), \
             patch.object(self.m, 'call_main_ai', side_effect=[dict(reply='فستان الاميرات متوفر.'),
                                                             dict(reply='هذا الموديل طقم قطعتين.')]), \
             patch.object(self.m, 'check_reply', side_effect=[dict(approved=False, problem='صياغة'), dict(approved=True)]):
            result = self.m.handle_post_order_message(self.db, dict(self.ev, text='يعني هذا ماكو منه؟'),
                {}, [], self.products, [DRESS], self.order)
        self.assertEqual(result['reply'], '')
        self.assertTrue(result['meta']['needs_human'])
        self.assert_order_unchanged()

    def test_recognition_keeps_pending_delivery_review_open(self):
        review = self.m.create_human_review(self.db, self.ev, 'متابعة توصيل الطلب', notify_telegram=False)
        with patch.object(self.m, 'match_customer_image_with_catalog', return_value=dict(product_found=True, product_id='P017')):
            self.handle(image='https://images.test/customer-dress.jpg')
        self.assertEqual(self.db.execute('SELECT status FROM human_reviews WHERE id=?', (review,)).fetchone()[0], 'pending')

    def test_conditional_return_does_not_dispute_current_product_identity(self):
        with patch.object(self.m, '_match_single_product') as vision:
            event, matched, selected, result = self.m.post_order_product_focus(self.db,
                dict(self.ev, text='إذا مو نفس الصورة أرجعه بيد المندوب'), self.products, [SET])
        vision.assert_not_called()
        self.assertIsNone(selected)
        self.assertIsNone(result)
        self.assert_order_unchanged()
