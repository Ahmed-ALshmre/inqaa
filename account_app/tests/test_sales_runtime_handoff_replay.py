"""Redacted replays of the 30 open reviews in Khuyoot and Lamsa, 2026-11-16."""
import json
import unittest
from unittest.mock import Mock

from account_app import conversation_quality, inbound_voice, reply_layout, variant_inventory
from account_app.sales_runtime import Scope
from account_app.sales_runtime.contracts import DecisionUnavailable
from account_app.sales_runtime.copy_guard import validate_fulfillment
from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.tests import test_sales_runtime_linking as linking


class OfflineWriter:
    structured_sales = True

    def __call__(self, context):
        raise DecisionUnavailable('writer_provider_http_402')


class HandoffReplayTests(unittest.TestCase):
    setUp = linking.LinkingTests.setUp
    tearDown = linking.LinkingTests.tearDown

    def replay(self, store='khuyoot', *, facts=None, fee=5000):
        self.sid = store
        self.scope = Scope(store, 'chatwoot', 'audit', 'replay')
        product = dict(self.products[0], store_id=store, ad_ids=['ad-audit'], sizes='38 إلى 48',
                       colors='أسود، ماروني', fabric='دبل إيطالي', variant_stock=[],
                       product_type='skirts', price='15000', image_url='https://catalog.test/first.jpg')
        product.update(facts or {})
        self.products = [product, dict(product, product_id='P3', ad_ids=[], clearance=True,
                                       image_url='https://catalog.test/third.jpg')]
        variant_inventory.init(self.db)
        self.db.commit()
        self.backup = Mock()
        self.backup.advise.side_effect = DecisionUnavailable('backup_provider_http_503')
        self.decide = Mock(side_effect=DecisionUnavailable('decision_provider_http_402'))
        self.entry = Entrypoint(
            self.db, lambda _: self.products, lambda *_: fee, self.decide,
            backup=self.backup, writer=OfflineWriter(),
            media=lambda scope, kind, refs, products: (
                dict(status='matched', product_ids=['P1']) if refs == ['catalog-image'] else
                dict(status='unsupported') if kind == 'file' else dict(status='unknown')),
            product_photos=lambda product, colors: ([dict(type='image', url=product['image_url'],
                product_id=product['product_id'])] if product.get('image_url') else []))
        self.entry.migrate()
        self.mid = 0

    def send(self, text='', *, kind='text', refs=None, ad=True, **kwargs):
        self.mid += 1
        result = self.entry.receive(store_id=self.sid, channel='chatwoot', account_id='audit',
            sender_id='replay', message_id=str(self.mid), text=text, media_kind=kind, media_ref=refs,
            ad={'ad_id': 'ad-audit'} if ad else None, **kwargs)
        outbox = []
        while part := self.entry.store.claim_reply(self.scope):
            outbox.append(part['payload'])
            self.entry.store.settle_reply(self.scope, part['id'], 'accepted', f'receipt-{part["id"]}')
        return result, outbox

    def active(self):
        self.assertEqual('active', self.entry.store.snapshot(self.scope)['data']['control_mode'])
        self.assertEqual(0, self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])

    def text(self, outbox):
        return ' '.join(part['text'] for part in outbox if part['type'] == 'text')

    def booked(self):
        result, _ = self.send('أريد ماروني قياس 44\n07701234567\nبغداد حي التجربة قرب المدرسة')
        self.assertEqual('booked', result['action'], result)
        return result['order_id']

    def photo_checkout_models(self):
        self.decide.side_effect = None
        self.decide.return_value = dict(intent='continue')
        self.entry.responses.writer = lambda context: [dict(type='text', text=(
            'المجموع {total} دينار، يناسبج هالحساب؟' if context['question_field'] == 'price.accept'
            else 'من عيوني، أكمل وياج على القطع اللي اخترتيها.'
        ))]

    def test_khuyoot_one_line_checkout_with_eta(self):
        self.replay(facts=dict(sizes='38 إلى 52'))
        policies = lambda _: {'delivery_time': 'من 24 ساعة إلى يومين', 'delivery_fee': {'صلاح الدين': 5000}}
        self.entry.runtime.policy_loader = self.entry.responses.policy_loader = policies
        self.send('بيش الكيلوت؟')
        result, parts = self.send('لون اسود قياس 46 صلاح الدين حي التجربة قرب المدرسة 07701234567 كم يوم يوصل الطلب')
        self.assertEqual('booked', result['action'], result)
        self.assertIn('24 ساعة إلى يومين', self.text(parts))
        items = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])
        self.assertEqual(('أسود', '46'), (items[0]['color'], items[0]['size']))
        state = self.entry.store.snapshot(self.scope)['data']
        self.assertEqual('صلاح الدين حي التجربة قرب المدرسة', state['contact']['address'])
        self.active()

    def test_khuyoot_checkout_prefers_corrected_size_over_old_draft(self):
        self.replay(facts=dict(sizes='38 إلى 52'))
        self.send('اريد اسود قياس 46')
        self.decide.side_effect = None
        self.decide.return_value = dict(intent='continue', changes=[dict(values=dict(product_id='P1', size='46'))])
        result, _ = self.send('تمام افحص بس لو 48 احسن\n07701234567\nبغداد حي التجربة قرب المدرسة')
        self.assertEqual('booked', result['action'], result)
        items = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])
        self.assertEqual('48', items[0]['size'])

    def test_khuyoot_customer_asks_for_color_photos_by_noun(self):
        self.replay()
        self.send('شنو ألوانه؟')
        _, parts = self.send('زين ممكن صورة للألوان يعني حتى اعرف فاتح لو طوخ')
        self.assertEqual('image', parts[0]['type'], parts)
        self.assertEqual('P1', parts[0]['product_id'])
        self.active()

    def test_khuyoot_late_order_tracking_is_not_an_eta_promise(self):
        self.replay()
        self.booked()
        result, parts = self.send('من الاحد وصيت وماكو يوصلني اليوم لو لا')
        self.assertEqual('order_tracking', result['reason'])
        self.assertEqual([], parts)

    def test_khuyoot_unknown_photo_answers_freight_without_using_the_old_product_price(self):
        self.replay()
        policies = lambda _: {'delivery_time': 'من 24 ساعة إلى يومين', 'delivery_fee': {'بغداد': 5000}}
        self.entry.runtime.policy_loader = self.entry.responses.policy_loader = policies
        self.send('بيش الكيلوت؟')
        self.send(kind='image', refs=['unknown-item'], ad=False)
        result, parts = self.send('اخذ ثلاث قطع منكم اشكد يسوي ويه توصيل هم؟', ad=False)
        self.assertNotEqual('human', result['action'], result)
        self.assertEqual('photo-quantity', parts[-1]['question_stage'])
        self.assertIn('5 آلاف', self.text(parts))
        self.assertNotIn('15 ألف', self.text(parts))
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        self.active()

    def test_khuyoot_quantity_enquiry_uses_tier_and_flat_shipping_without_booking(self):
        self.replay(facts=dict(product_type='trousers'))
        self.entry.responses.policy_loader = lambda _: {'delivery_fee': {'بغداد': 5000, 'صلاح الدين': 5000}}
        self.send('How men denar')
        self.decide.side_effect = None
        self.decide.return_value = dict(intent='continue', changes=[dict(values=dict(product_id='P1', quantity=3))])
        result, parts = self.send('اخذ ثلاث قطع منكم اشكد يسوي ويه توصيل هم؟')
        self.assertNotEqual('booked', result['action'], result)
        self.assertIn('50 ألف', self.text(parts))
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        self.active()

    def test_khuyoot_contact_without_a_selected_color_does_not_choose_black(self):
        self.replay(facts=dict(sizes='38 إلى 52'))
        self.decide.side_effect = None
        self.decide.return_value = dict(intent='continue', changes=[dict(values=dict(product_id='P1', size='52'))])
        self.send('يوجد قياس 52؟')
        self.decide.side_effect = DecisionUnavailable('decision_provider_http_402')
        result, parts = self.send('07701234567\nبغداد حي التجربة قرب المدرسة')
        self.assertNotEqual('booked', result['action'], result)
        self.assertNotIn('يوصلج الكيلوت', self.text(parts))
        self.assertTrue(any(part.get('question_field', '').endswith('.color') for part in parts), parts)
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        result, _ = self.send('الماروني')
        self.assertEqual('booked', result['action'], result)
        item = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])[0]
        self.assertEqual(('ماروني', '52'), (item['color'], item['size']))
        self.active()

    def test_khuyoot_switch_back_to_named_shirt_after_uncertain_skirt_photo(self):
        self.replay(facts=dict(product_type='tops', product_name='قميص التجربة',
                               sizes='38 إلى 52', colors='سماوي فاتح، أسود'))
        self.send('بيش القميص؟')
        self.send(kind='image', refs=['unknown-skirt'], ad=False)
        result, parts = self.send('ثبت لي حجز على القميص واريد القياس 50 واللون السماوي الفاتح', ad=False)
        self.assertNotEqual('human', result['action'], result)
        state = self.entry.store.snapshot(self.scope)['data']
        self.assertEqual('P1', state['cart'][0]['product_id'])
        self.assertEqual('50', state['cart'][0]['size'])
        self.assertTrue(any(part.get('question_field') == 'contact.details' for part in parts), parts)
        self.active()

    def test_khuyoot_reply_keeps_delivery_intro_with_answer(self):
        layout = reply_layout.plan_sales_bubbles({'_human_sales_voice': True, '_short_sales_bubbles': True},
            'وبالنسبة للتوصيل لتكريت، يوصلج خلال 24 ساعة إلى يومين. تحبين تشوفين الألوان؟')
        self.assertIn('وبالنسبة للتوصيل لتكريت، يوصلج', layout['reply_parts'][0])
        for reply, proof in (
            ('دا أتابع ويه شركة التوصيل', {}),
            ('يوصلج الكيلوت الأسود قياس 52', {}),
            ('التوصيل يومين لـ3 أيام', {'delivery_time': 'من 24 ساعة إلى يومين'}),
        ):
            with self.subTest(reply=reply), self.assertRaises(ValueError):
                validate_fulfillment([dict(type='text', text=reply)], proof)

    def test_four_photos_of_one_product_keep_their_own_sizes_until_booking(self):
        self.replay(facts=dict(sizes='من 60 إلى 130 كيلو', colors='أسود، ماروني، أزرق، رصاصي'))
        self.photo_checkout_models()
        self.entry.media = lambda *_: dict(status='matched', product_ids=['P1'], positions=[
            dict(product_id='P1', color=color, fingerprint=str(index))
            for index, color in enumerate(('أسود', 'ماروني', 'أزرق', 'رصاصي'))
        ])
        self.send('هذا 5x\n3x\n3x\nوهذا 5x', kind='image', refs=['one', 'two', 'three', 'four'],
                  photo_sizes=['5x', '3x', '3x', '5x'])
        self.send('مو دزيت الك صوره للالوان والقياس')
        self.send('رقمي 07701234567')
        result, _ = self.send('بغداد حي التجربة قرب المدرسة')
        self.assertEqual('booked', result['action'], result)
        items = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])
        self.assertEqual([('أسود', '5XL'), ('ماروني', '3XL'), ('أزرق', '3XL'), ('رصاصي', '5XL')],
                         [(item['color'], item['size']) for item in items])
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        self.active()

    def test_mismatched_photo_offer_waits_for_an_accurate_quote(self):
        self.replay(facts=dict(sizes='3XL 5XL'))
        self.photo_checkout_models()
        self.entry.media = lambda *_: dict(status='matched', product_ids=['P1'], positions=[
            dict(product_id='P1', color='أسود'), dict(product_id='P1', color='ماروني'),
        ])
        self.send('3x\n5x', kind='image', refs=['one', 'two'], photo_sizes=['3x', '5x'])
        self.send('يعني هسه فلوسهن ٣٠ الف والتوصيل مجانا مو حسب كلامكم')
        self.send('اريد هاية القطعتين')
        self.send('رقمي 07701234567')
        _, out = self.send('بغداد حي التجربة قرب المدرسة')
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        self.assertEqual(35000, next(part['quote_total'] for part in out if part.get('question_field') == 'price.accept'))
        result, _ = self.send('اي تمام')
        self.assertEqual('booked', result['action'], result)
        self.active()

    def test_photo_fabric_question_uses_saved_staff_specification(self):
        self.replay(facts=dict(product_type='sets', fabric='فرو', sizes='من 60 إلى 130 كيلو',
                               _staff_guidance=[dict(topic='fabric_feel', guidance='ثخين ملمسه ناعم')]))
        self.entry.media = lambda *_: dict(status='ambiguous', product_ids=['P1'], latest_match='P1')
        _, out = self.send('بس سؤال\nالقماش ثخين لو خفيف؟', kind='image', refs=['other', 'identified'], ad=False)
        self.assertIn('ثخين ملمسه ناعم', self.text(out))
        self.active()
        result, _ = self.send('شكد عرضه بالسنتيم؟', ad=False)
        self.assertEqual('human', result['action'])
        self.assertEqual('width', self.db.execute('SELECT topic FROM product_questions').fetchone()[0])

    def test_photo_pair_asks_only_for_missing_size_and_resumes_after_contact(self):
        self.replay(facts=dict(sizes='3XL 5XL'))
        self.photo_checkout_models()
        self.entry.media = lambda *_: dict(status='matched', product_ids=['P1'], positions=[
            dict(product_id='P1', color='أسود'), dict(product_id='P1', color='ماروني'),
        ])
        def write(context):
            if context.get('photo_order_gap'):
                self.assertEqual('photo.selection.size', context['question_field'])
                self.assertEqual('ماروني', context['photo_order_gap']['color'])
                return [dict(type='text', text='باقي بس قياس الماروني، شنو تحبين؟')]
            return [dict(type='text', text='صار عيوني.')]
        self.entry.responses.writer = write
        _, out = self.send('اريد هاية القطعتين', kind='image', refs=['one', 'two'], photo_sizes=['3x', ''])
        self.assertEqual('size', out[-1]['photo_order_gap']['field'])
        _, out = self.send('رقمي 07701234567\nبغداد حي التجربة قرب المدرسة')
        self.assertEqual([], out)
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        result, _ = self.send('5x')
        self.assertEqual('booked', result['action'], result)
        items = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])
        self.assertEqual([('أسود', '3XL'), ('ماروني', '5XL')], [(item['color'], item['size']) for item in items])

    def test_k01_skirt_photo_and_fabric_question(self):
        self.replay()
        self.send(kind='image', refs=['catalog-image'], ad=False)
        _, out = self.send('هاي تنورة\nنوع قماش شنو', ad=False)
        self.assertIn('دبل إيطالي', self.text(out))
        self.active()

    def test_k02_season_without_a_catalog_fact_goes_to_missing_data(self):
        self.replay(facts=dict(fabric='باربي', description=''))
        result, _ = self.send('هذا بهاري', kind='image', refs=['catalog-image'])
        self.assertEqual('human', result['action'])
        self.assertEqual('season', self.db.execute('SELECT topic FROM product_questions').fetchone()[0])

    def test_photo_season_question_uses_the_saved_winter_checkbox(self):
        self.replay(facts=dict(product_type='trousers', fabric='دبل ايطالي', description='',
                               occasions=['everyday', 'outing', 'winter']))
        result, out = self.send('هذا الكيلوت شتوي', kind='image', refs=['catalog-image'], ad=False)
        self.assertEqual('continue', result['action'], result)
        self.assertIn('شتوي', self.text(out))
        self.active()

    def test_k03_today_for_an_existing_order_is_not_a_general_delivery_quote(self):
        self.replay()
        self.booked()
        result, out = self.send('حبي اليوم يوصل الطلب 😢')
        self.assertEqual('order_tracking', result['reason'])
        self.assertEqual([], out)

    def test_k04_photo_picks_up_the_previous_questions_about_weight_and_stretch(self):
        self.replay(facts=dict(sizes='من 55 إلى 100 كيلو', stretch='إي، بي ليكرا وينطي باللبس'))
        self.send('الاسود يلبس ياوزن\nينطي باللبس', ad=False)
        _, out = self.send('تمام', kind='image', refs=['catalog-image'], ad=False)
        self.assertIn('ليكرا', self.text(out))
        self.assertIn('100 كيلو', self.text(out))
        self.active()

    def test_k05_empty_shared_post_does_not_pause_the_conversation(self):
        self.replay()
        for _ in range(4):
            result, out = self.send(ad=False)
            self.assertEqual(('skipped', []), (result['action'], out))
        self.active()

    def test_k06_black_in_size_42_is_answered_without_reasking_size(self):
        self.replay()
        _, out = self.send('متوفر لون اسود قياس ٤٢')
        self.assertIn('42 موجود', self.text(out))
        self.assertNotIn('؟', self.text(out))
        self.active()

    def test_k07_repeated_confirmation_does_not_create_or_change_an_order(self):
        self.replay()
        order_id = self.booked()
        result, out = self.send('نعم ثبتي الطلب والقياس')
        self.assertEqual('order_status', result['action'])
        self.assertIn('مسجل', self.text(out))
        self.assertEqual([(order_id,)], [tuple(row) for row in self.db.execute('SELECT id FROM orders')])
        self.active()

    def test_k08_cancellation_after_fulfillment_needs_the_delivery_service(self):
        self.replay()
        order_id = self.booked()
        self.db.execute("UPDATE sales_v2_order_tasks SET status='accepted' WHERE kind='fulfillment'")
        self.db.commit()
        result, _ = self.send('تاخر ألغيه\nالغي الطلب')
        self.assertEqual('order_cancellation_requires_fulfillment_review', result['reason'])
        self.assertEqual('new', self.db.execute('SELECT status FROM orders WHERE id=?', (order_id,)).fetchone()[0])

    def test_k09_unintelligible_audio_has_no_fabricated_transcript(self):
        clips = [dict(message_id=1, index=0)]
        self.assertIsNone(inbound_voice.transcripts({'voice': {'clear': False}}, clips))

    def test_k10_delivery_delay_is_understood_as_after_sales(self):
        self.replay()
        self.booked()
        result, out = self.send('خوم اضل انتظر')
        self.assertEqual('order_tracking', result['reason'])
        self.assertEqual([], out)

    def test_l01_unresolved_image_and_sold_out_acknowledgment_leave_ai_active(self):
        self.replay('default', facts=dict(stock='غير متوفر'))
        self.send(kind='image', refs=['unmatched'], ad=False)
        self.send('تمام', ad=False)
        self.active()

    def test_l02_unreadable_video_keeps_multiple_weights_out_of_a_wrong_cart(self):
        self.replay('default')
        _, out = self.send('نفس هذا\nاريد قماش قديفة مخمل\nخلي واحد وزن ٧٠ واحد ٦٠',
                           kind='file', ad=False)
        self.assertIn('رابط', self.text(out))
        self.assertEqual([], self.entry.store.snapshot(self.scope)['data']['cart'])
        self.active()

    def test_l03_shirt_length_is_read_from_catalogue(self):
        self.replay('default', facts=dict(product_type='tops', description='طوله 90 سم'))
        _, out = self.send('سلام عليكم شكد طول القميص')
        self.assertIn('90', self.text(out))
        self.active()

    def test_l04_browsing_can_continue_even_when_previous_photo_was_unresolved(self):
        self.replay('default')
        self.send(kind='image', refs=['unmatched'], ad=False)
        _, out = self.send('هذا\nوشنو اكو مديلات\nعدكم\nعلمود اختار\nبعد', ad=False)
        self.assertTrue(any(part['type'] == 'image' for part in out), out)
        self.active()

    def test_l05_no_belt_fit_uses_documented_cut(self):
        self.replay('default', facts=dict(fit_notes='تجي واسعة باللبس وبدون حزام'))
        _, out = self.send('اذا ما به حزام\nيكون ضيق في اللبس')
        self.assertIn('واسعة', self.text(out))
        self.active()

    def test_l06_shorthand_offer_and_free_delivery_are_both_answered(self):
        self.replay('default', facts=dict(offer='قطعتين بـ25 ألف'), fee=0)
        _, out = self.send('هي ٢ ب ٢٥ ؟\nوالتوصيل مجاني ؟')
        self.assertIn('25 ألف', self.text(out))
        self.assertIn('مجاناً', self.text(out))
        self.active()

    def test_l07_catalog_image_request_is_a_real_attachment(self):
        self.replay('default', facts=dict(product_type='sets', sizes='2XL، 3XL'))
        _, out = self.send('السلام عليكم ممكن صور الصوت قياس 3اكس', kind='image', refs=['catalog-image'])
        self.assertTrue(any(part['type'] == 'image' for part in out), out)
        self.active()

    def test_l08_material_question_checks_the_catalogue(self):
        self.replay('default', facts=dict(fabric='قديفة شتوي'))
        _, out = self.send('السلام عليكم قماشهن قديفه')
        self.assertIn('قديفة', self.text(out))
        self.active()

    def test_l09_price_negotiation_does_not_turn_into_a_booking(self):
        self.replay('default')
        _, out = self.send('القطعتين ٢٠ وي التوصيل اذا يصير انريد ٣ قطع\nاذا على عشره انريد ٣ قطع')
        self.assertTrue(out)
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        self.active()

    def test_l10_multi_product_quote_does_not_guess_from_an_unmatched_image(self):
        self.replay('default')
        self.send('أريد ثلاث قطع من هذا النوع وقطعة من الروب بكم؟',
                  kind='image', refs=['unmatched'], ad=False)
        result, out = self.send('100کیلو\nهل يمكنكِ تصويرها بنفسكِ وإرسال الصور إليّ؟', ad=False)
        self.assertIn('رابط', self.text(out), result)
        self.assertEqual([], self.entry.store.snapshot(self.scope)['data']['cart'])
        self.active()

    def test_l11_multiple_image_retries_stop_silently_with_a_visible_review(self):
        self.replay('default')
        for index in range(3):
            _, out = self.send('هاي شنو قياساته' if index == 0 else 'ً',
                               kind='image', refs=[f'foreign-{index}'], ad=False)
            if index == 2:
                self.assertEqual([], out)
        self.assertEqual('human', self.entry.store.snapshot(self.scope)['data']['control_mode'])
        self.assertEqual(1, self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0])

    def test_l12_corrected_photo_unlocks_the_size_question(self):
        self.replay('default', facts=dict(sizes='من 65 إلى 115 كيلو'))
        self.send(kind='image', refs=['unmatched'], ad=False)
        self.send('فدوه العمرج قياس خاص اكو', ad=False)
        result, out = self.send('قياس خاص اكو', kind='image', refs=['catalog-image'], ad=False)
        self.assertIn('115 كيلو', self.text(out), result)
        self.active()

    def test_l13_named_color_updates_the_choice_without_reasking_it(self):
        self.replay('default')
        self.send('قياس 44')
        _, out = self.send('ماروني')
        self.assertIn('ماروني', self.text(out))
        self.assertNotIn('يا لون', self.text(out))
        self.assertEqual('ماروني', self.entry.store.snapshot(self.scope)['data']['cart'][0]['color'])
        self.active()

    def test_l14_discount_delivery_window_and_payment_do_not_get_lost(self):
        self.replay('default')
        _, out = self.send('هل يوجد خصم إضافي؟\nكم يوم لتوصيل الطلبية؟\nما هي طريقة الدفع؟')
        self.assertIn('يومين', self.text(out))
        self.assertIn('الدفع عند الاستلام', self.text(out))
        self.active()

    def test_l15_missing_wash_behavior_creates_a_product_question(self):
        self.replay('default', facts=dict(care='', faq='', description=''))
        result, _ = self.send('شنو نوع القماش\nيشيل بالغسل ؟', kind='image', refs=['catalog-image'])
        self.assertEqual('human', result['action'])
        self.assertEqual('care', self.db.execute('SELECT topic FROM product_questions').fetchone()[0])

    def test_l16_replying_to_a_request_for_a_photo_with_an_empty_share_keeps_context(self):
        self.replay('default')
        self.send('نوع القماش', ad=False)
        result, _ = self.send(ad=False)
        self.assertEqual('skipped', result['action'])
        self.active()

    def test_l17_conditional_cancellation_does_not_cancel_early(self):
        self.replay('default')
        self.booked()
        result, _ = self.send('حبيبتي ماوصل الطلب\nاذا مايوصل باجر الغي الحجز')
        self.assertEqual('order_tracking', result['reason'])
        self.assertEqual('new', self.db.execute('SELECT status FROM orders').fetchone()[0])

    def test_l18_missing_delivery_is_not_a_new_purchase(self):
        self.replay('default')
        self.booked()
        result, _ = self.send('سلام عليكم\nلحد الان ماوصل الطلب')
        self.assertEqual('order_tracking', result['reason'])
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_l19_reviewer_cannot_override_an_explicitly_paused_conversation(self):
        self.replay('default')
        result, out = self.send('؟؟', ai_enabled=False)
        self.assertEqual(('skipped', []), (result['action'], out))
        self.decide.assert_not_called()

    def test_l20_a_real_change_is_not_treated_as_confirmation_of_a_saved_order(self):
        self.replay('default')
        self.booked()
        self.assertFalse(conversation_quality.booking_status_question('ثبتي الطلب وبدلي اللون أسود'))
        self.decide.side_effect = None
        self.decide.return_value = dict(intent='buy', changes=[
            dict(values=dict(product_id='P1', color='أسود', size='44'))])
        result, _ = self.send('ثبتي الطلب وبدلي اللون أسود')
        self.assertEqual('booked_cart_requires_order_change_service', result['reason'])
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
