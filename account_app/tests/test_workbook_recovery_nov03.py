"""Replay the failure shapes found in the November 3 workbook/database audit."""
import json
from unittest import TestCase
from unittest.mock import patch

from account_app import sales_answers, sales_context, sales_playbook, sales_recovery
from account_app.tests import test_last50_checkout_nov01 as fixtures


class RecoveryFacts(TestCase):
    def test_two_piece_offer_is_quoted_once(self):
        product = dict(product_id='K', price='12000', offer='قطعتين ب 20 الف')
        reply = ' '.join(sales_playbook.price_preview(product, [('', 5000)], 2))
        self.assertEqual(reply.count('20,000'), 1)
        self.assertIn('25,000', reply)

    def test_available_age_question_is_not_just_stock(self):
        self.assertIn('sizes', sales_answers.topics('وشكد عمره المتوفر؟'))
        self.assertNotIn('sizes', sales_answers.topics('عمره سنة ونص'))

    def test_inspection_policy_does_not_require_a_known_product(self):
        self.assertEqual(sales_recovery.policy_question('اكدر افتح القطعة وافحصها قبل ما ادفع؟'), 'inspection')
        for text in ('استلمت والقياس غلط اريد افحصه', 'القماش بيه عيب افحصه قبل الدفع؟'):
            self.assertNotEqual(sales_recovery.policy_question(text), 'inspection')

    def test_saved_cart_total_uses_receipt_and_does_not_swallow_changes(self):
        order = dict(total_amount=25000, product_total=20000, delivery_fee=5000)
        self.assertIn('25', sales_playbook.saved_order_total('يعني مجموعهن ويا التوصيل شكد؟', order))
        for text in ('اذا اخذ ثلاثة شكد المجموع؟', 'الغيه شكد الحساب؟', 'شكد المجموع وشوكت يوصل؟'):
            self.assertFalse(sales_playbook.saved_order_total(text, order))
        self.assertFalse(sales_playbook.saved_order_total('شكد الحساب؟', dict(order, total_amount=26000)))


class RecoveryCheckout(TestCase):
    setUpClass = classmethod(fixtures.Last50PipelineTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.Last50PipelineTests.tearDownClass.__func__)
    setUp, tearDown = fixtures.Last50PipelineTests.setUp, fixtures.Last50PipelineTests.tearDown
    link, save, process = fixtures.Last50PipelineTests.link, fixtures.Last50PipelineTests.save, fixtures.Last50PipelineTests.process
    remember_photo, order = fixtures.Last50PipelineTests.remember_photo, fixtures.Last50PipelineTests.order
    data = fixtures.Last50PipelineTests.data

    def hold(self):
        self.link('اسود ماروني')
        self.remember_photo('')
        self.save('incoming', 'أريد هذا قياس 44')
        self.save('outgoing', 'دزيلي الرقم والعنوان حتى أثبتلج')
        result, _ = self.process('07701234567 بغداد حي الاختبار')
        self.assertEqual(self.order()['status'], self.m.COLOR_REVIEW_STATUS, result)
        return self.order()

    def test_held_order_cannot_be_dismissed_or_resumed_without_color(self):
        held = self.hold()
        with self.m.app.test_request_context(method='POST', json={}):
            from flask import session
            session['dashboard_authenticated'] = True
            response, status = self.m.api_mark_reviewed(self.sender)
        self.assertEqual(status, 409)
        self.assertEqual(response.get_json()['held_order_ids'], [held['id']])
        self.assertTrue(self.m.has_pending_human_review(self.db, self.sender))
        self.m.close_human_attention(self.db)
        self.assertTrue(self.m.has_pending_human_review(self.db, self.sender))

    def test_linking_product_does_not_resolve_held_order_color(self):
        self.hold()
        self.m.complete_customer_product_link(self.db, self.sender, self.product, 'manual_admin', resume_ai=False)
        self.assertTrue(self.m.has_pending_human_review(self.db, self.sender))
        self.assertEqual(self.order()['color'], '')

    def test_staff_silent_link_also_keeps_color_review_pending(self):
        self.hold()
        with self.m.app.test_request_context(method='POST', json={'product_id':'F1', 'silent':True, 'resume_ai':False}):
            from flask import session
            session['dashboard_authenticated'] = True
            response = self.m.api_link_product(self.sender)
        self.assertTrue(response.get_json()['ok'])
        self.assertTrue(self.m.has_pending_human_review(self.db, self.sender))

    def test_startup_repairs_closed_review_once_without_changing_order(self):
        held = dict(self.hold())
        self.db.execute("UPDATE human_reviews SET status='reviewed' WHERE sender_id=?", (self.sender,))
        self.db.commit()
        self.m.set_customer_ai_enabled(self.db, self.sender, False, reason='manual')
        self.m.restore_held_color_reviews(self.db)
        self.m.restore_held_color_reviews(self.db)
        self.db.commit()
        self.assertEqual(dict(self.order()), held)
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, self.sender))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE sender_id=? AND status='pending'", (self.sender,)).fetchone()[0], 1)

    def test_full_inspection_receipt_and_owner_custom_receipt_are_preserved(self):
        with patch.object(self.m, 'get_store_settings', return_value={'inspection_message': self.m.DEFAULT_INSPECTION_POLICY_TEXT}):
            self.assertEqual(self.m.get_order_confirmation_text(self.db), self.m.DEFAULT_ORDER_CONFIRMATION_TEXT)
        custom = 'تم تثبيت الطلب. يرجى فحص الطلب بحضور المندوب. أهم شي تسلمون، توصيلنا مجاني.'
        with patch.object(self.m, 'get_store_settings', return_value={'inspection_message': custom}):
            self.assertEqual(self.m.get_order_confirmation_text(self.db), custom)

    def test_unavailable_previous_model_does_not_block_new_photo(self):
        old = dict(product_id='OLD', product_name='تراك قديم', stock='نفذ', status='active')
        new = dict(product_id='NEW', product_name='فستان', colors='وردي', sizes='1 إلى 3 سنوات', stock='متوفر', status='active')
        self.m.bind_customer_to_product(self.db, self.sender, old, source='image_recognition')
        self.m.product_choice_table(self.db)
        self.db.execute('INSERT INTO customer_product_choices VALUES(?,?,?,?)',
                        (self.sender, '["OLD"]', '["NEW"]', self.m.now_baghdad_iso()))
        self.db.commit()
        self.assertTrue(self.m.discard_unavailable_previous_choice(self.db, self.sender, [old, new]))
        self.assertIsNone(self.m.pending_product_choice(self.db, self.sender))
        active = [r[0] for r in self.db.execute("SELECT product_id FROM customer_product_interests WHERE sender_id=? AND status='active'", (self.sender,))]
        self.assertEqual(active, ['NEW'])

    def multi(self, text='الاثنين سوا بنفس التوصيل عيني', ages=('عمر سنة ونص',)):
        products = [dict(product_id='A', product_name='فستان', price='10000', sizes='1 إلى 3 سنوات', colors='وردي', stock='متوفر', status='active'),
                    dict(product_id='B', product_name='بدلة', price='9000', sizes='1 إلى 3 سنوات', colors='جوزي', stock='متوفر', status='active')]
        now = self.m.now_baghdad_iso()
        evidence = [dict(text=age, product_id=None, created_at=now) for age in ages]
        evidence.append(dict(text=text, product_id=None, created_at=now))
        self.ev['text'] = '07701234567 بغداد حي الاختبار'
        return self.m.resume_selected_multi_checkout(self.db, self.ev, {}, products, {'evidence': evidence}), products

    def test_both_models_share_explicit_age_and_book_without_choose_one(self):
        self.assertTrue(sales_context.context_evidence('الاثنين سوا بنفس التوصيل عيني', []))
        result, _ = self.multi()
        self.assertIsNotNone(result)
        items = result['order']['items']
        self.assertEqual([(i['product_id'], i['size'], i['color']) for i in items], [('A', self.m.product_variants.age_text(1.5), 'وردي'), ('B', self.m.product_variants.age_text(1.5), 'جوزي')])

    def test_both_does_not_guess_age_allocation_or_hypothetical_purchase(self):
        for text, ages in [('الاثنين سوا', ('عمر سنة وعمر سنتين',)),
                           ('اذا اخذ الاثنين سوا شكد يصير؟', ('عمر سنة ونص',)),
                           ('لا مو الاثنين سوا خلي بس الاول', ('عمر سنة ونص',))]:
            with self.subTest(text=text):
                self.assertIsNone(self.multi(text, ages)[0])

    def test_per_model_ages_do_not_leak_into_the_other_model(self):
        _, products = self.multi()
        now = self.m.now_baghdad_iso()
        context = {'evidence': [dict(text='عمر سنة', product_id='A', created_at=now),
                                dict(text='عمر سنتين', product_id='B', created_at=now),
                                dict(text='الاثنين سوا', product_id=None, created_at=now)]}
        result = self.m.resume_selected_multi_checkout(self.db, self.ev, {}, products, context)
        self.assertEqual([i['size'] for i in result['order']['items']],
                         [self.m.product_variants.age_text(1), self.m.product_variants.age_text(2)])
        context['evidence'].append(dict(text='لا خلي بس الاول', product_id=None, created_at=now))
        self.assertIsNone(self.m.resume_selected_multi_checkout(self.db, self.ev, {}, products, context))

    def test_local_price_surface_does_not_repeat_offer_or_add_fabric(self):
        self.link()
        product = dict(self.product, price='12000', offer='عرض خاص القطعة ب 12 الف قطعتين ب 20 الف', fabric='تركي شتوي')
        result = self.m.transparent_price_reply(dict(self.ev, text='شكد سعره؟'), product, {}, [product], [])
        self.assertEqual(result['reply'].count('12'), 1)
        self.assertEqual(result['reply'].count('20'), 1)
        self.assertNotIn('تركي', result['reply'])
