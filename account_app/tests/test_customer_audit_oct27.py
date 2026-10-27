"""Synthetic regressions for the Oct 27 audit; no private chats or live calls."""
import unittest
from unittest.mock import patch

from account_app import owner_sales, sales_answers, sales_decision, sales_context, sales_playbook, product_albums, product_links
from account_app.checkout import unresolved_condition, unresolved_purchase_terms, child_cart_error, has_shipping_landmark
from account_app.pricing import money, quote
from account_app.reply_layout import plan_sales_bubbles, compact
from account_app.tests import test_audit_orders as fixtures


PRODUCT = dict(product_id='AUDIT', product_name='فستان دانتيل', price='16000',
               sizes='38 إلى 52', colors='اسود', stock='متوفر', status='active', fabric='قطن')


class AuditPolicyTests(unittest.TestCase):
    def test_delivery_questions_are_not_objections(self):
        for text in ('شكد التوصيل؟', 'التوصيل مجاني؟', 'اريد توصيل لبغداد',
                     'انتو كاتبين ١٦ يعني التوصيل مجاني', 'السعر يشمل التوصيل؟'):
            with self.subTest(text=text):
                self.assertFalse(owner_sales.delivery_objection(text))
                self.assertFalse(sales_decision.price_mismatch(text))
                reply = owner_sales.reply({'text': text}, PRODUCT, {}, {}, [])
                self.assertNotIn('مو إلنا', (reply or {}).get('reply', ''))

    def test_courier_explanation_only_for_an_actual_objection(self):
        for text in ('التوصيل غالي', 'خلي التوصيل مجاني', 'اريد التوصيل عليكم'):
            self.assertTrue(owner_sales.delivery_objection(text), text)
            result = owner_sales.reply({'text': text}, PRODUCT, {}, {}, [])
            self.assertIn('شركة التوصيل', result['reply'])
        text = 'التوصيل 5 آلاف. أجور التوصيل لشركة التوصيل مو إلنا. شكد قياس تلبسين؟'
        filtered = owner_sales.delivery_wording(text, 'شكد التوصيل؟')
        self.assertIn('5 آلاف', filtered)
        self.assertNotIn('شركة التوصيل', filtered)

    def test_courier_return_policy_does_not_latch_a_discount_condition(self):
        text = 'إذا يرجع الفستان ماانطي التوصيل'
        self.assertEqual(unresolved_purchase_terms(text), '')
        self.assertIsNone(unresolved_condition([{'direction': 'incoming', 'text': text}], 'نعم ثبتي'))
        self.assertTrue(unresolved_purchase_terms('إذا السعر 10 آلاف والتوصيل مجاني ثبتي'))
        result = owner_sales.reply({'text': text}, PRODUCT, {}, {}, [])
        self.assertIn('مجاناً', result['reply'])
        self.assertNotIn('تخفيض', result['reply'])

    def test_graduation_request_does_not_match_buttons_inside_adkum(self):
        text = 'متوفر عدكم فستان تخرج'
        self.assertNotIn('closure', sales_answers.topics(text))
        self.assertNotIn('closure', sales_decision.advance_details([], 'incoming', text))
        self.assertIn('closure', sales_answers.topics('بيه دكم لو سحاب؟'))

    def test_explicit_current_price_is_used_for_all_cart_lines(self):
        product = dict(PRODUCT, price='13000 بدل 15 الف')
        result = quote([dict(product_id='AUDIT', quantity=2)], [product], 5000)
        self.assertEqual((result['product_total'], result['delivery_fee']), (26000, 5000))
        self.assertEqual(money('١٣ الف بدل ١٥ الف'), 13000)
        for ambiguous in ('13000 15000', '13 أو 15 ألف', '-13000 بدل 15000'):
            with self.assertRaises(ValueError):
                money(ambiguous)

    def test_long_sales_reply_has_complete_short_bubbles_and_no_emoji_only_bubble(self):
        reply = ('هذا الفستان متوفر بالأسود وقياساته من 38 إلى 52.\n'
                 'القماش قطن وخفيف.\n🌸\n'
                 'السعر 16 ألف والتوصيل 5 آلاف والمجموع 21 ألف.\nشكد قياس تلبسين؟')
        plan = plan_sales_bubbles({}, reply)
        self.assertTrue(2 <= len(plan['reply_parts']) <= 4)
        self.assertEqual(compact(' '.join(plan['reply_parts'])), compact(reply))
        self.assertTrue(all(any(c.isalnum() for c in part) for part in plan['reply_parts']))

    def test_age_color_and_size_survive_a_long_conversation_and_corrections(self):
        messages = [dict(id=1, direction='incoming', text='قياسي 44 اللون اسود'),
                    dict(id=2, direction='incoming', text='متوفر لعمر 5 سنوات؟')]
        messages += [dict(id=i, direction='incoming', text=f'ميزانيتي {20000+i}') for i in range(3, 46)]
        state = sales_context.advance({}, messages)
        chosen = sales_playbook.customer_selection(state)
        self.assertEqual({key: chosen[key] for key in ('size', 'age', 'color')}, dict(size='44', age=5, color='أسود'))
        state = sales_context.advance(state, [dict(id=46, direction='incoming', text='قياسي 46')])
        self.assertEqual(sales_playbook.customer_selection(state)['size'], '46')

    def test_three_photo_captions_keep_their_own_ages(self):
        products = [dict(PRODUCT, product_id=str(i), sizes=sizes)
                    for i, sizes in enumerate(('من 2 إلى 6 سنوات', 'من 1 إلى 4 سنوات', 'من 1 إلى 4 سنوات'))]
        images = [dict(product_found=True, product_id=str(i), question=f'عمر {age}')
                  for i, age in enumerate((5, 6, 3))]
        result = product_albums.response(images, products, 'عمر 5\nعمر 6\nوهذا عمر 3')
        self.assertEqual(len(result['reply_parts']), 3)
        self.assertIn('5', result['reply_parts'][0])
        self.assertNotIn('قياس 6', result['reply_parts'][0])
        self.assertRegex(result['reply_parts'][1], r'عمر 6.{0,15}مو (?:موجود|ضمن)')
        self.assertRegex(result['reply_parts'][2], r'عمر 3.{0,15}(?:موجود|متوفر)')

    def test_no_match_is_not_sold_out_but_known_sold_out_is_reported(self):
        unknown = [dict(product_found=False, image_url='https://images.test/unknown.png')]
        self.assertEqual(product_albums.response(unknown, [PRODUCT])['reply'], '')
        self.assertTrue(product_albums.availability_error('الصورة الأولى مو متوفر', None, [PRODUCT], unknown))
        known = [dict(product_found=True, product_id='AUDIT')]
        self.assertIn('مو متوفر', product_albums.response(known, [dict(PRODUCT, stock='نفذ')])['reply'])

    def test_conflicting_colors_are_not_offered_as_verified_stock(self):
        product = dict(PRODUCT, colors='اسود وردي كريمي', notes='قماش شتوي باقي بس اسود')
        self.assertTrue(product_albums.color_conflict(product))
        self.assertEqual(product_albums.stock_state(product), 'unknown')
        self.assertFalse(product_albums.color_conflict(dict(product, colors='اسود')))

    def test_two_children_cannot_be_priced_and_booked_as_one(self):
        product = dict(PRODUCT, sizes='من 2 إلى 6 سنوات')
        first = dict(product_id='AUDIT', quantity=1, size='2 سنة')
        messages = ['اريد لعمر 2 سنوات ولعمر 4 سنوات اللون اسود']
        self.assertTrue(child_cart_error([first], messages, [product]))
        self.assertEqual(child_cart_error([first, dict(first, size='4 سنة')], messages, [product]), '')
        self.assertEqual(child_cart_error([first], messages + ['بس قطعة وحده لعمر 2'], [product]), '')

    def test_gender_is_consistent_after_greeting_and_after_booking(self):
        male = owner_sales.surface('تدللين عيني، اختيارج حلو. دزي اللون اللي يعجبج ويناسبج.', 'male')
        for word in ('تدللين', 'اختيارج', 'دزي', 'يعجبج', 'يناسبج'):
            self.assertNotIn(word, male)
        female = owner_sales.surface('تدلل، شنو القياس اللي يناسبك؟', 'female')
        self.assertIn('يناسبج', female)
        self.assertNotIn('يناسبك', female)

    def test_urgent_courier_question_uses_order_service_priority(self):
        self.assertEqual(owner_sales.human_reason('جاوبي المندوب بسرعه', post_order=True),
                         'post_order_delivery_status')

    def test_broad_area_is_not_a_landmark(self):
        for address in ('بغداد المشتل', 'واسط مركز مدينة الكوت', 'بغداد حي اور'):
            self.assertFalse(has_shipping_landmark(address), address)
        self.assertTrue(has_shipping_landmark('بغداد المشتل قرب جامع الاختبار'))
        self.assertTrue(has_shipping_landmark('بغداد محلة 123 زقاق 4 دار 5'))

    def test_alternatives_search_past_rejected_wrong_age_and_wrong_fabric(self):
        source = dict(PRODUCT, product_id='old', sizes='من 2 إلى 6 سنوات', category='غير مصنف', image_url='old.jpg')
        choices = [dict(source, product_id='rejected', sizes='من 8 إلى 12 سنوات'),
                   dict(source, product_id='young'),
                   dict(source, product_id='summer', sizes='من 8 إلى 12 سنوات', fabric='لينن صيفي'),
                   dict(source, product_id='winter', sizes='من 8 إلى 12 سنوات', fabric='قطن شتوي ثقيل')]
        found = product_links.catalog_alternatives(source, choices, age_years=10, color='أسود',
                                                   request='اريد غير موديل بقماش اثخن', excluded=['rejected'])
        self.assertEqual([p['product_id'] for p in found], ['winter'])


class AuditPipelineTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def test_long_single_bubble_hint_does_not_override_sales_sections(self):
        text = ('الفستان متوفر بالأسود، وقماشه قطن مريح وناعم. '
                'السعر 16 ألف والتوصيل 5 آلاف والمجموع 21 ألف. '
                'الفحص بوجود المندوب قبل الدفع وإذا ما ناسبج يرجع مجاناً دون دفع أجور توصيل. '
                'القياسات المتوفرة من 38 إلى 52 حسب جدول هذا الموديل. شكد قياس تلبسين؟')
        result, reply = self.m.format_customer_reply({'reply': text, '_single_message': True}, text)
        plan = plan_sales_bubbles(result, reply)
        self.assertGreater(len(plan['reply_parts']), 1)
        self.assertEqual(compact(' '.join(plan['reply_parts'])), compact(text))

    def test_children_store_delivery_quote_does_not_add_an_objection_defense(self):
        token = self.m._current_store_id.set('baraah-kids')
        self.addCleanup(self.m._current_store_id.reset, token)
        parts, missing = self.m.store_policy_parts(['delivery'], 'شكد التوصيل؟')
        self.assertTrue(parts)
        self.assertFalse(missing)
        self.assertNotIn('مو إلنا', ' '.join(parts))
        self.assertNotIn('شركة التوصيل', ' '.join(parts))

    def test_catalog_matching_uses_labelled_product_photos_when_legacy_vision_is_disabled(self):
        token = self.m._current_store_id.set('al-fatena')
        self.addCleanup(self.m._current_store_id.reset, token)
        with patch.object(self.m, 'build_product_vision_candidates', return_value=[PRODUCT]), \
             patch.object(self.m, 'is_store_feature_enabled', side_effect=lambda key, default: key == 'catalog_match_enabled'), \
             patch.object(self.m, 'confirm_with_vision', return_value={'product_found': True, 'product_id': 'AUDIT'}) as vision:
            found = self.m._match_customer_image_with_catalog_once('https://images.test/customer.png', [PRODUCT])
        self.assertTrue(found['product_found'])
        self.assertEqual(vision.call_args.kwargs['catalog_model'], self.m.CATALOG_MATCH_MODEL)

    def test_unknown_image_creates_review_and_does_not_repeat_receipt(self):
        self.customer('audit-review')
        ev = dict(sender_id='audit-review', text='', page_id='test', platform='facebook',
                  image_url='https://images.test/unknown.png', attachments=[], ref=None, ad_id=None,
                  timestamp=0, postback=None, quick_reply=None, referral_source=None, referral_type=None)
        with patch.object(self.m, 'extract_facebook_event', return_value=ev), \
             patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'load_active_products', return_value=[PRODUCT]), \
             patch.object(self.m, 'match_product', return_value=(None, None, {})):
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertEqual(result['reply'], '')
        self.assertTrue(self.m.has_pending_human_review(self.db, 'audit-review'))
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, 'audit-review'))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)

    def test_quick_reply_persists_and_delivers_the_same_split_plan(self):
        self.customer('audit-layout')
        ev = dict(sender_id='audit-layout', text='السعر والفحص؟', page_id='test', platform='facebook')
        reply = ('الفستان متوفر بالأسود وقماشه قطن. السعر 16 ألف والتوصيل 5 آلاف والمجموع 21 ألف. '
                 'الفحص بوجود المندوب قبل الدفع وإذا ما ناسبج يرجع مجاناً. شكد قياس تلبسين؟')
        result = self.m.saved_checkout_reply(self.db, ev, reply, {})
        self.assertGreater(len(result['reply_parts']), 1)
        saved = [row[0] for row in self.db.execute("SELECT text FROM messages WHERE direction='outgoing' ORDER BY id")]
        self.assertEqual(saved, result['reply_parts'])
        with patch.object(self.m, 'send_text_to_facebook', return_value=True) as send:
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual([call.args[1] for call in send.call_args_list], saved)

    def test_burst_captions_remain_associated_with_each_image(self):
        self.customer('audit-photos')
        for image, caption in [('one', 'عمر 5'), ('two', 'عمر 6'), ('three', 'وهذا عمر 3')]:
            self.m.save_message(self.db, 'audit-photos', 'incoming', 'image', '', f'https://images.test/{image}.png', None, None, {})
            last = self.m.save_message(self.db, 'audit-photos', 'incoming', 'text', caption, None, None, None, {})
        ev = self.m.collect_unanswered_event(self.db, dict(sender_id='audit-photos', text='وهذا عمر 3'), last)
        self.assertEqual(ev['_image_questions']['https://images.test/one.png'], 'عمر 5')
        self.assertEqual(ev['_image_questions']['https://images.test/two.png'], 'عمر 6')
        self.assertEqual(ev['_image_questions']['https://images.test/three.png'], 'وهذا عمر 3')

    def test_phone_and_address_in_separate_bubbles_are_retained(self):
        self.customer('audit-contact')
        for text in ('اني من بغداد المشتل', 'قرب جامع الاختبار', '07701234567'):
            self.m.capture_customer_contact(self.db, 'audit-contact', text)
        customer = dict(self.db.execute("SELECT * FROM customers WHERE sender_id='audit-contact'").fetchone())
        self.assertEqual(customer['phone'], '07701234567')
        self.assertIn('المشتل', customer['address'])
        self.assertIn('جامع الاختبار', customer['address'])
        self.assertEqual(sales_context.reasks_contact('من يا محافظة؟', customer), 'province')

    def test_catalog_price_conflict_is_handed_off_without_internal_error_or_order(self):
        self.customer('audit-price')
        product = dict(PRODUCT, price='13000 15000')
        result = {'order': dict(phone='07701234567', province='بغداد', address='المشتل قرب جامع الاختبار',
                               items=[dict(product_id='AUDIT', size='44', color='اسود', quantity=1)])}
        with patch.object(self.m, 'load_products_from_file', return_value=[product]):
            created, reply = self.m.create_order_if_valid(self.db, 'audit-price', result, product, cart_confirmed=True)
        self.assertFalse(created)
        self.assertIn('المختص', reply)
        self.assertNotIn('سعر المنتج', reply)
        self.assertTrue(result['_checkout_human_review_id'])
        self.assertTrue(self.m.has_pending_human_review(self.db, 'audit-price'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_stale_cart_cannot_book_a_color_when_catalog_notes_conflict(self):
        self.customer('audit-color')
        product = dict(PRODUCT, colors='اسود وردي', notes='باقي بس اسود')
        result = {'order': dict(phone='07701234567', province='بغداد', address='المشتل قرب جامع الاختبار',
                               items=[dict(product_id='AUDIT', size='44', color='وردي', quantity=1)])}
        with patch.object(self.m, 'load_products_from_file', return_value=[product]):
            created, reply = self.m.create_order_if_valid(self.db, 'audit-color', result, product, cart_confirmed=True)
        self.assertFalse(created)
        self.assertIn('المختص', reply)
        self.assertTrue(self.m.has_pending_human_review(self.db, 'audit-color'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)

    def test_mixed_color_price_and_payment_question_keeps_all_answers(self):
        self.customer('audit-mixed')
        ev = dict(sender_id='audit-mixed', text='اكو لون ابيض وشكد السعر وشلون الدفع؟')
        with patch.object(self.m, '_call_main_ai_once', return_value={'reply': 'الدفع عند الاستلام.', 'create_order': False}):
            result = self.m.call_main_ai(ev, 'text', {}, [], [PRODUCT], PRODUCT, None, '', [])
        self.assertIn('16', result['reply'])
        self.assertIn('استلام', result['reply'])
        self.assertRegex(result['reply'], 'أسود|اسود')
        self.assertFalse(result['create_order'])

    def test_landmark_is_requested_without_losing_saved_contact_or_cart(self):
        self.customer('audit-landmark')
        self.m.capture_customer_contact(self.db, 'audit-landmark', '07701234567 بغداد المشتل')
        result = {'order': dict(items=[dict(product_id='AUDIT', size='44', color='اسود', quantity=1)])}
        with patch.object(self.m, 'load_products_from_file', return_value=[PRODUCT]):
            created, reply = self.m.create_order_if_valid(self.db, 'audit-landmark', result, PRODUCT, cart_confirmed=True)
        self.assertFalse(created)
        self.assertIn('نقطة دالة', reply)
        self.assertNotIn('رقم', reply)
        self.m.capture_customer_contact(self.db, 'audit-landmark', 'جامع الاختبار')
        contact = self.m.checkout_customer_data(self.db, 'audit-landmark', result['order'])
        self.assertTrue(has_shipping_landmark(contact['address']))
        self.assertIn('المشتل', contact['address'])
        self.assertEqual(contact['phone'], '07701234567')

    def test_accepted_alternatives_send_matching_photos_without_asking_again(self):
        self.customer('audit-alternatives')
        source = dict(PRODUCT, product_id='old', sizes='من 2 إلى 6 سنوات', category='غير مصنف', image_url='old.jpg')
        target = dict(source, product_id='new', sizes='من 8 إلى 12 سنوات', image_url='new.jpg')
        self.m.save_message(self.db, 'audit-alternatives', 'incoming', 'text', 'اريد لعمر 10', None, None, None, {})
        self.m.save_message(self.db, 'audit-alternatives', 'outgoing', 'text', 'تحبين أقترحلج موديل ثاني؟', None, None, None, {})
        last = self.m.save_message(self.db, 'audit-alternatives', 'incoming', 'text', 'نعم', None, None, None, {})
        ev = dict(sender_id='audit-alternatives', text='نعم', page_id='test', platform='facebook')
        with patch.object(self.m, 'product_image_urls', side_effect=lambda p: ['https://images.test/' + p['image_url']]):
            result = self.m.requested_catalog_alternatives(self.db, ev, [source, target], {}, [source], last)
        self.assertIsNotNone(result)
        self.assertIn('https://images.test/new.jpg', result.get('image_urls', []))
        self.assertNotIn('https://images.test/old.jpg', result.get('image_urls', []))
        self.assertNotIn('العمر المطلوب', result['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 0)
