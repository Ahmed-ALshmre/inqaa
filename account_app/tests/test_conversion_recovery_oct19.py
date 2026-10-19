import copy
import unittest
from unittest.mock import patch

from account_app import sales_recovery
from account_app.checkout import contact_fields, invalid_shipping_address
from account_app.tests import test_audit_orders as fixtures


CATALOG = [dict(product_id='A', product_name='فستان جوزي', colors='جوزي', sizes='44',
                stock='متوفر', status='active', price='10000'),
           dict(product_id='B', product_name='دشداشة حوامل', colors='وردي', sizes='44',
                stock='متوفر', status='active', price='15000')]


class ConversionRecoveryTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def test_dialect_shipping_address_with_phone_is_saved_without_asking_again(self):
        self.customer('address')
        text = 'ناصريه منصوريه قرب وكيله رسميه رقم الهاتف ٠٧٧٠١٢٣٤٥٦٧'
        contact = self.m.capture_customer_contact(self.db, 'address', text)
        self.assertEqual(contact['province'], 'ذي قار')
        self.assertEqual(contact['phone'], '07701234567')
        self.assertIn('منصوريه', contact['address'])
        for text, city in [('العماره حي الحسين', 'ميسان'), ('سليمانيه قرب الجامع', 'السليمانية'),
                           ('كربلا حي الحر', 'كربلاء')]:
            self.assertEqual(contact_fields(text)['province'], city)
        self.assertNotIn('address', contact_fields('شكد توصيل الناصريه؟'))
        self.assertNotIn('address', contact_fields('ناصريه'))
        self.assertTrue(invalid_shipping_address('ناصريه'))

    def test_checkout_keeps_answer_to_inspection_question_and_only_requests_missing_contact(self):
        question = 'ثبتي\nزين افحصها كدام المندوب؟\nوقياسها عفيه ركزيلي ع موضوع القياس'
        answer = 'تكدرين تفحصين القطعة بحضور المندوب قبل الدفع.\nبس دزيلي رقم الموبايل والمحافظة والعنوان.'
        notice = 'حتى أثبت الطلب أحتاج رقم الهاتف والمحافظة والعنوان الكامل 🌸'
        reply = sales_recovery.checkout_reply(answer, notice, question)
        self.assertIn('تفحصين القطعة', reply)
        self.assertIn(notice, reply)
        self.assertNotIn('بس دزيلي', reply)
        self.assertEqual(reply.count('رقم'), 1)

    def test_checkout_does_not_keep_a_false_order_confirmation(self):
        reply = sales_recovery.checkout_reply('تم تثبيت طلبج.\nالتوصيل خلال يومين.',
                                               'باقي نحدد القياس.', 'شوكت يوصل؟')
        self.assertIn('التوصيل خلال يومين', reply)
        self.assertNotIn('تم تثبيت', reply)
        self.assertIn('باقي نحدد', reply)
        self.assertEqual(sales_recovery.checkout_reply('أي كلام', 'ناقص الهاتف', '07701234567'), 'ناقص الهاتف')

    def test_photo_dispute_is_not_a_new_catalog_request_or_a_return_condition(self):
        for text in ['هذا مو نفس الصورا الي ادزلج ياها', 'اني ادزلج موديل انتي ادزلي غير موديل',
                     'الصورة مو المطلوب']:
            self.assertTrue(sales_recovery.disputed_product_photo(text), text)
        for text in ['أريد غير موديل', 'غيري اللون', 'إذا مو نفس الصورة أرجعه ويا المندوب',
                     'اذا مو نفس الصوره ما ادفع', 'ممكن صورة ثانية']:
            self.assertFalse(sales_recovery.disputed_product_photo(text), text)

    def test_disputed_photo_rejects_only_current_link_and_creates_one_review(self):
        self.customer('dispute')
        for product in CATALOG:
            self.m.complete_customer_product_link(self.db, 'dispute', product, 'image_recognition',
                                                 source='image_recognition', preserve_existing=True)
        ev = dict(sender_id='dispute', text='هذا مو نفس الصورا الي ادزلج ياها', page_id='', platform='facebook')
        with patch.object(self.m, 'classify_contextual_selection') as classifier, \
             patch.object(self.m, 'load_products_from_file', return_value=CATALOG):
            first = self.m.resolve_product_choice(self.db, ev, CATALOG)
            second = self.m.resolve_product_choice(self.db, ev, CATALOG)
        classifier.assert_not_called()
        self.assertTrue(first['meta']['photo_identity_disputed'])
        self.assertEqual(first['meta']['human_review_id'], second['meta']['human_review_id'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0], 1)
        active = self.db.execute("SELECT product_id FROM customer_product_interests WHERE sender_id='dispute' AND status='active'").fetchall()
        self.assertEqual([p['product_id'] for p in active], ['A'])
        self.assertNotIn('أرسلي صورة', first['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_missing_one_catalog_item_preserves_known_lines_and_routes_that_item_to_staff(self):
        self.customer('missing')
        order = dict(phone='07701234567', province='بغداد', address='بغداد حي اور', items=[
            dict(product_id='A', product_name='فستان جوزي', color='جوزي', size='44', quantity=1),
            dict(product_id='', product_name='الفستان الأسود', color='اسود', size='44', quantity=1)])
        result = dict(create_order=True, order=copy.deepcopy(order))
        with patch.object(self.m, 'load_products_from_file', return_value=CATALOG):
            created, reply = self.m.create_order_if_valid(self.db, 'missing', result, None)
            self.m.create_order_if_valid(self.db, 'missing', result, None)
        self.assertFalse(created)
        self.assertIn('الفستان الأسود', reply)
        self.assertNotIn('دزيلي صورة', reply)
        self.assertEqual(result['order']['items'], order['items'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0], 1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
