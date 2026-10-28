"""Owner-approved conversations, transport failures and real SQLite handoffs."""
import unittest
from unittest.mock import patch

from account_app import owner_sales, product_variants, sales_playbook, sales_answers, reply_layout
from account_app.tests import test_sales_playbook as reply_fixtures
from account_app.tests import test_post_order_service as service_fixtures
from account_app.tests import test_delivery_acceptance_oct24 as delivery_fixtures

PRODUCT = dict(product_id='A', product_name='سوت حوامل', category='سوت نسائي',
               sizes='من 70 الى 130 كيلو', colors='اسود', price='16000', stock='متوفر', status='active')


class OwnerPolicyTests(unittest.TestCase):
    def test_warm_female_addresses_rotate_but_male_unknown_never_receive_them(self):
        outputs = {owner_sales.surface('متوفر، من يا محافظة؟', 'female', seed=str(i)) for i in range(20)}
        self.assertGreater(len(outputs), 2)
        for gender in ('male', '', 'unknown'):
            reply = owner_sales.surface('حبيبتي عمري روحي حياتي عيني متوفر، شكد قياس تلبسين؟', gender)
            self.assertFalse(owner_sales.AFFECTION.search(reply), reply)
            self.assertIn('تلبس؟' if gender == 'male' else 'القياس المطلوب؟', reply)
        self.assertEqual(len(owner_sales.EMOJI.findall(owner_sales.surface('متوفر 🌷 🌸 ❤️ ✨', 'female'))), 2)

    def test_names_are_address_hints_and_explicit_choice_wins(self):
        for name in ('زينب علي', 'مريم احمد', 'هدى', 'أم محمد', 'UM Tahani', 'Huda_', 'سارة حسين'):
            self.assertEqual(owner_sales.address_gender(name), 'female', name)
        for name in ('محمد علي', 'أحمد', 'حيدر النعماني', 'أمجد حسين', 'أبو مريم', 'Kareem Mohamed'):
            self.assertEqual(owner_sales.address_gender(name), 'male', name)
        for name in ('نور', 'صفاء', 'حساب خاص', 'متجر مريم', 'Blue River', ''):
            self.assertEqual(owner_sales.address_gender(name), '', name)
        self.assertEqual(owner_sales.address_gender('زينب', 'male'), 'male')
        self.assertEqual(owner_sales.address_gender('محمد', 'female'), 'female')

    def test_size_labels_are_owner_stock_not_guessed_weight_and_exclude_children(self):
        for label in ('S', 'M', 'L', 'XL', 'XXL', '3XL', '4XL', '5XL'):
            self.assertTrue(product_variants.assess(PRODUCT, 'قياس ' + label)[0]['supported'])
            self.assertEqual(product_variants.order_choice_error(PRODUCT, 'size', label), '')
        child = dict(PRODUCT, product_name='فستان اطفال', sizes='2 إلى 7 سنوات')
        self.assertFalse(product_variants.assess(child, 'XXL')[0]['supported'])
        self.assertEqual(product_variants.normalize_size_words('اريد اكسين'), 'اريد XXL')
        self.assertNotIn('وزن', owner_sales.reply({'text':'اكسين موجود؟'}, PRODUCT, {}, {}, [])['reply'])

    def test_size_range_and_latest_correction_are_retained(self):
        context = {'evidence':[{'text':'قياسي 50،52'}]}
        self.assertEqual(sales_playbook.customer_selection(context)['size'], '50-52')
        context = {'evidence':[{'text':'قياسي 44'}, {'text':'لا خلي قياس 48'}]}
        self.assertEqual(sales_playbook.customer_selection(context)['size'], '48')
        selected = sales_playbook.customer_selection({'evidence':[{'text':'اريد اكسين اسود'}]})
        self.assertEqual((selected['size'], selected['color']), ('XXL', 'أسود'))
        self.assertEqual(sales_playbook.customer_selection({'evidence':[{'text':'قياس XXL موجود؟'}]})['size'], 'XXL')

    def test_fit_preference_is_not_a_missing_width_or_an_extra_question(self):
        self.assertNotIn('width', sales_answers.topics('واسع'))
        self.assertIn('width', sales_answers.topics('عرضه بالسنتيمتر؟'))
        result = owner_sales.reply({'text':'واسع'}, PRODUCT, {'size':'XXL'}, {}, [])
        self.assertIn('فيت', result['reply'])
        self.assertNotIn('مضبوط لو واسع', sales_playbook.price_next_step(PRODUCT, {}, history=[{'direction':'outgoing','text':'شكد قياس تلبسين؟'}]))
        self.assertNotIn('مضبوط لو واسع', owner_sales.surface('متوفر. تحبينه مضبوط لو واسع؟', 'female'))

    def test_handoff_intents_do_not_include_conditional_inspection_or_ordinary_arrival(self):
        for text in ('اريد رقم المتجر', 'اريد موظفة', 'رقمكم حتى اتصل'):
            self.assertTrue(owner_sales.human_reason(text))
        for text in ('بدلي اللون للأسود', 'ضيفيلي قطعة ثانية', 'اريد الغي الطلب', 'حجزت من يومين ما وصل'):
            self.assertTrue(owner_sales.human_reason(text, post_order=True))
        self.assertEqual(owner_sales.human_reason('اذا مو نفس الصورة ارجعه؟', post_order=True), '')
        self.assertEqual(owner_sales.human_reason('شوكت يوصل؟', post_order=True), '')

    def test_payment_and_photo_absence_use_inspection_without_a_video_offer(self):
        for text in ('الدفع مقدم لو عند الاستلام؟', 'اخاف يختلف عن الصورة', 'اكو صور اضافية؟'):
            result = owner_sales.reply({'text':text}, PRODUCT, {}, {}, [], photo_count=1)
            self.assertIn('المندوب', result['reply'])
            self.assertIn('قبل الدفع', result['reply'])
            self.assertNotIn('فيديو', result['reply'])

    def test_partial_acceptance_is_restricted_for_bundles_and_quantity_offers(self):
        text = 'اكدر اخذ وحدة وارجع الثانية؟'
        for product in (dict(PRODUCT, price='بكج 3 قطع ب 25000'), dict(PRODUCT, offer='قطعتين ب 27 الف')):
            self.assertIn('كامل', owner_sales.reply({'text':text}, product, {}, {}, [])['reply'])
        self.assertIn('بسعر المفرد', owner_sales.reply({'text':text}, PRODUCT, {}, {}, [])['reply'])

    def test_layout_supports_one_to_four_without_losing_approved_facts(self):
        short='إي متوفر، من يا محافظة؟'
        plan=reply_layout.plan_sales_bubbles({},short)
        self.assertEqual(plan['reply_parts'],[short])
        self.assertFalse(plan['needs_rewrite'])
        parts=['الصورة الأولى، الفستان متوفر بـ16 ألف.', 'الصورة الثانية، السوت مو متوفر. تحبين غيره؟']
        reply='\n\n'.join(parts)
        plan=reply_layout.plan_sales_bubbles({'_ordered_album':True,'reply_parts':parts},reply)
        self.assertEqual(len(plan['reply_parts']),2)
        self.assertEqual(' '.join(plan['reply_parts']).split(), reply.split())
        self.assertEqual(reply_layout.MAX_PARTS,4)


class OwnerReplyPipelineTests(unittest.TestCase):
    setUp=reply_fixtures.ReviewedReplyPipelineTests.setUp
    tearDown=reply_fixtures.ReviewedReplyPipelineTests.tearDown
    customer=reply_fixtures.ReviewedReplyPipelineTests.customer
    ask=reply_fixtures.ReviewedReplyPipelineTests.ask

    def test_xxl_answers_without_model_weight_or_size_reask(self):
        result, model=self.ask('قياس XXL موجود؟', PRODUCT, {'gender':'female'})
        model.assert_not_called()
        self.assertIn('متوفر',result['reply'])
        self.assertIn('محافظة',result['reply'])
        self.assertNotIn('وزن',result['reply'])
        self.assertNotIn('قياس تلبسين',result['reply'])

    def test_unknown_detail_remains_silent_and_does_not_leak_placeholder(self):
        result,model=self.ask('شنو سعره وبيه بطانة؟', PRODUCT)
        model.assert_not_called()
        self.assertTrue(result['requires_human'])
        self.assertEqual(result['reply'],'')

    def test_pricing_dispute_remains_silent_for_real_review(self):
        result,model=self.ask('بالاعلان كاتبين 10 ليش هسه 16؟',PRODUCT)
        model.assert_not_called()
        self.assertTrue(result['requires_human'])
        self.assertEqual(result['reply'],'')

    def test_saved_name_controls_wording_without_persisting_inferred_gender(self):
        self.customer('name-owner')
        self.db.execute('UPDATE customers SET name=? WHERE sender_id=?', ('زينب احمد', 'name-owner'))
        self.db.commit()
        result=self.m.owner_result_surface({'reply':'متوفر، من يا محافظة؟'}, {'sender_id':'name-owner','text':'السعر؟'})
        self.assertTrue(owner_sales.AFFECTION.search(result['reply']))
        self.assertFalse(self.db.execute('SELECT gender FROM customers WHERE sender_id=?', ('name-owner',)).fetchone()[0])
        correction=self.m.owner_result_surface({'reply':'حبيبتي متوفر'}, {'sender_id':'name-owner','text':'اني رجل'})
        self.assertFalse(owner_sales.AFFECTION.search(correction['reply']))

    def test_forbidden_placeholder_is_silent_and_requests_staff(self):
        for text in ('وصل المرفق عيني', 'هذه التفاصيل تحتاج تأكيداً من المتجر', 'هسه يفتح يمج الألبوم'):
            result = self.m.owner_result_surface({'reply': text}, {'text':'السعر؟'}, {'gender':'female'})
            self.assertEqual(result['reply'], '')
            self.assertTrue(result['requires_human'])

    def test_bare_price_reply_for_a_man_cannot_use_female_address(self):
        result,model=self.ask('السعر؟',PRODUCT,{'gender':'male'})
        model.assert_not_called()
        self.assertFalse(owner_sales.AFFECTION.search(result['reply']))
        self.assertNotIn('تلبسين',result['reply'])


class OwnerPostOrderTests(unittest.TestCase):
    setUpClass = classmethod(service_fixtures.PostOrderServiceTests.setUpClass.__func__)
    tearDownClass = classmethod(service_fixtures.PostOrderServiceTests.tearDownClass.__func__)
    setUp = service_fixtures.PostOrderServiceTests.setUp
    tearDown = service_fixtures.PostOrderServiceTests.tearDown
    handle = service_fixtures.PostOrderServiceTests.handle
    # Inherit fixture setup, not the obsolete assertions for silent action notices.
    def test_owner_actions_create_review_without_changing_or_duplicating_order(self):
        original=dict(self.order)
        for text in ('بدلي اللون للأسود','ضيفيلي قطعة ثانية','اريد الغي الطلب','حجزت من يومين ما وصل'):
            result,calls=self.handle({'reply':'must not be used'},text=text)
            self.assertEqual(calls,0)
            self.assertTrue(result['meta']['human_review_id'])
            self.assertIn('المختص',result['reply'])
            self.assertEqual(self.m.get_latest_customer_order(self.db,self.sender),original)
            self.assertEqual(self.db.execute('SELECT count(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],1)


class OwnerImageRetryTests(unittest.TestCase):
    setUp=delivery_fixtures.DeliveryAcceptanceOct24Tests.setUp
    tearDown=delivery_fixtures.DeliveryAcceptanceOct24Tests.tearDown
    prepare_result=delivery_fixtures.DeliveryAcceptanceOct24Tests.prepare_result
    response=staticmethod(delivery_fixtures.DeliveryAcceptanceOct24Tests.response)

    def test_rejected_image_retries_once_then_success_does_not_create_review(self):
        result,_=self.prepare_result(image=True)
        def image(*args):
            count=image.calls;image.calls+=1
            self.m._last_transport_result.set({'status_code':400 if count==0 else 200})
            return count==1
        image.calls=0
        with patch.object(self.m,'send_text_to_facebook',return_value=True),patch.object(self.m,'send_image_to_facebook',side_effect=image) as send:
            self.assertTrue(self.m.send_webhook_result_to_facebook(result))
        self.assertEqual(send.call_count,2)
        self.assertFalse(result.get('meta',{}).get('needs_human'))

    def test_second_rejection_creates_review_and_timeout_never_replays_uncertain_image(self):
        for status,expected in ((400,2),(0,1)):
            result,_=self.prepare_result(image=True)
            def image(*args):
                self.m._last_transport_result.set({'status_code':status})
                return False
            with patch.object(self.m,'send_text_to_facebook',return_value=True),patch.object(self.m,'send_image_to_facebook',side_effect=image) as send,patch.object(self.m,'send_telegram_message'),patch.object(self.m,'send_telegram_photo'):
                self.assertFalse(self.m.send_webhook_result_to_facebook(result))
            self.assertEqual(send.call_count,expected)
            self.assertTrue(result['meta']['human_review_id'])
            # Separate conversations keep the delivery ledger independent.
            self.sender+='-next'
            self.m.get_or_create_customer(self.db,self.sender,'synthetic-page','facebook')


class OwnerAdvertisementPipelineTests(unittest.TestCase):
    setUp=reply_fixtures.ReviewedReplyPipelineTests.setUp
    tearDown=reply_fixtures.ReviewedReplyPipelineTests.tearDown
    customer=reply_fixtures.ReviewedReplyPipelineTests.customer

    def test_failed_image_analysis_keeps_explicit_ad_identity_and_answers_price(self):
        self.customer('ad-owner')
        product=dict(PRODUCT, product_name='فستان دانتيل', ad_id='owner-ad', image_url='https://images.test/dress.jpg')
        ev=dict(sender_id='ad-owner',page_id='page',platform='facebook',text='',
                image_url='https://images.test/customer.jpg',attachments=[{'type':'image','url':'https://images.test/customer.jpg'}],
                ref=None,ad_id='owner-ad',postback=None,quick_reply=None,timestamp=0,referral_source=None,referral_type=None)
        with patch.object(self.m,'extract_facebook_event',return_value=ev), \
             patch.object(self.m,'load_active_products',return_value=[product]), \
             patch.object(self.m,'load_products_from_file',return_value=[product]), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'is_customer_ai_enabled',return_value=True), \
             patch.object(self.m,'get_auto_product_settings',return_value={'enabled':False}), \
             patch.object(self.m,'match_product',return_value=(None,None,{'service_error':True})), \
             patch.object(self.m,'send_webhook_result_to_facebook',return_value=True) as send, \
             patch.object(self.m,'schedule_followup_if_needed'), \
             patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertTrue(result['meta']['catalog_match'])
        reply=send.call_args.args[0]['reply']
        self.assertIn('16 ألف',reply)
        self.assertFalse(self.m.reply_reasks_known_product(reply))
        self.assertNotIn('تأكيد',reply)
        model.assert_not_called()
        self.assertEqual(self.m.customer_image_positions(self.db,'ad-owner')[0]['product_id'],'A')

    def test_store_phone_request_creates_real_review_without_exposing_a_number(self):
        self.customer('phone-owner')
        ev=dict(sender_id='phone-owner',page_id='page',platform='facebook',text='اريد رقم المتجر',
                image_url=None,attachments=[],ref=None,ad_id=None,postback=None,quick_reply=None,
                timestamp=0,referral_source=None,referral_type=None)
        with patch.object(self.m,'extract_facebook_event',return_value=ev), \
             patch.object(self.m,'is_ai_enabled',return_value=True), \
             patch.object(self.m,'is_customer_ai_enabled',return_value=True), \
             patch.object(self.m,'send_telegram_message'), \
             patch.object(self.m,'send_telegram_photo'), \
             patch.object(self.m,'_call_main_ai_once') as model:
            result=self.m.process_webhook(self.db,{},use_debounce=False)
        self.assertTrue(result['meta']['human_review_id'])
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0],0)
        self.assertIn('المختص',result['reply'])
        model.assert_not_called()
