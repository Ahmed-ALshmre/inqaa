"""Regressions for the owner's Iraqi sales workbook."""
import json
from unittest import TestCase

from account_app import product_questions, sales_recovery, sales_answers, sales_context, sales_strategy, sales_voice
from account_app.checkout import contact_fields
from account_app.tests import test_last50_checkout_nov01 as checkout


class WorkbookQuestions(TestCase):
    def test_policy_questions_in_an_opening_or_later_turn(self):
        for question, intent in [('هل يمكنني الدفع عند الاستلام؟','payment'),
                                 ('يصير أدفع للمندوب من أستلم؟','payment'),
                                 ('كم تبلغ تكلفة التوصيل إلى محافظتي؟','delivery')]:
            with self.subTest(question=question):
                self.assertEqual(sales_recovery.policy_question(question), intent)

    def test_eta_does_not_turn_hypothetical_destination_into_an_address(self):
        self.assertNotIn('address', contact_fields('إذا بغداد حي الاختبار، وشوكت يوصلني؟'))
        self.assertEqual(contact_fields('بغداد حي الربيع، يم مدرسة الحي. بعد تحتاجين شي؟')['address'],
                         'بغداد حي الربيع، يم مدرسة الحي')

    def test_permission_to_send_photos_is_scoped_to_previous_offer(self):
        self.assertEqual(sales_context.continuation('إي دزيهن', {'last_staff_turn':[{'text':'أدزلج صور الألوان؟'}]})['intent'],'browse')
        self.assertIsNone(sales_context.continuation('إي دزيهن', {'last_staff_turn':[{'text':'أثبتلج القطعتين وأرسل الطلب؟'}]}))

    def test_shirt_length_is_read_from_its_own_catalog_record(self):
        for length in (80,90):
            product={'notes':f'طويل طوله {length}'}
            self.assertIn(str(length),sales_strategy.measurement_fact(product,'length'))
            self.assertNotIn('length',product_questions.missing('طول القميص اشكد؟',product))
        self.assertFalse(sales_strategy.measurement_fact({'notes':'قياس 50'},'length'))

    def test_mixing_two_child_colors_needs_a_valid_offer_and_available_choices(self):
        p=dict(product_id='K',product_name='تراك',stock='متوفر',status='active',colors='أخضر، برتقالي',
               sizes='من 2 الى 7 سنوات',price='12000',offer='قطعتين ب 20 الف')
        q='أريد أخضر لعمر خمس سنوات وبرتقالي لعمر ستة. يصير أخلطهن بالعرض؟'
        self.assertIn('اللونين',sales_voice.mixed_child_offer(q,p)['reply'])
        for change in ({'colors':'أخضر'},{'offer':'قطعتين ب 20 الف بشرط نفس اللون'},{'sizes':'من 2 إلى 3 سنوات'}):
            self.assertIsNone(sales_voice.mixed_child_offer(q,dict(p,**change)))

    def test_owner_child_detail_exceptions_are_specific_to_the_question(self):
        p=dict(sizes='من 2 إلى 7 سنوات',stretch='بيه ليكرا')
        policy='إلكم حق الفحص بوجود المندوب قبل الدفع'
        self.assertIn('ما عندي طوله',sales_voice.child_detail_reply('عندج طول البنطرون حتى أقيسه؟',p,policy))
        self.assertIsNone(sales_voice.child_detail_reply('عندج طول البنطرون؟',p,'ممنوع الفحص عند الاستلام'))
        self.assertIn('ما أگدر أضمن',sales_voice.child_detail_reply('بيه ليكرا؟ هو قطن 100%؟',p,policy))
        self.assertIsNone(sales_voice.child_detail_reply('بيه ليكرا وهل هو قطن 100% وآمن للأكزيما؟',p,policy))

    def test_phone_first_does_not_ask_for_size_and_address_together(self):
        answer='حتى أكملج الطلب، شكد قياس تلبسين؟ وبأي محافظة وعنوان تحبين يوصلج؟'
        self.assertEqual(sales_voice.phone_first_step(answer,'07701234567'),'حتى أكملج الطلب، شكد قياس تلبسين؟')
        self.assertEqual(sales_voice.phone_first_step('شكد قياس تلبسين؟\nودزيلي المحافظة والعنوان بالتفصيل.','07701234567'),'شكد قياس تلبسين؟')
        self.assertEqual(sales_voice.phone_first_step('وين عنوانج؟','07701234567'),'وين عنوانج؟')

    def test_flare_question_requires_verified_cut(self):
        for text in ('کلوش؟', 'يعني تتوسع من جوه؟', 'أقصد وسعها من الأسفل'):
            self.assertIn('cut',sales_answers.topics(text))
            self.assertIn('cut',product_questions.missing(text,{'product_id':'D'}))


class WorkbookCheckout(TestCase):
    setUpClass = classmethod(checkout.Last50PipelineTests.setUpClass.__func__)
    tearDownClass = classmethod(checkout.Last50PipelineTests.tearDownClass.__func__)
    for_testing = checkout.Last50PipelineTests
    setUp, tearDown = for_testing.setUp, for_testing.tearDown
    link, save, process = for_testing.link, for_testing.save, for_testing.process
    remember_photo, order = for_testing.remember_photo, for_testing.order
    data = for_testing.data

    def test_verified_model_unknown_color_contacts_book_for_review(self):
        self.link('اسود ماروني')
        self.remember_photo('')
        self.save('incoming','أريد هذا قياس 44')
        self.save('outgoing','دزيلي الرقم والعنوان حتى أثبتلج')
        result, _ = self.process('07701234567 بغداد حي الاختبار')
        order = self.order()
        self.assertIsNotNone(order,result.get('reply'))
        self.assertEqual(order['status'],self.m.COLOR_REVIEW_STATUS)
        self.assertEqual(json.loads(order['order_items'])[0]['color'],'')
        self.assertIn('تم تثبيت',result['reply'])
        self.assertTrue(self.db.execute('SELECT 1 FROM human_reviews WHERE sender_id=? AND status=\'pending\'',(self.sender,)).fetchone())
        self.assertFalse(self.db.execute('SELECT 1 FROM menger_deliveries WHERE order_id=?',(order['id'],)).fetchone())

    def test_contact_completion_for_one_known_model_holds_unselected_color(self):
        self.link('أسود ماروني')
        self.save('incoming','أريد هذا قياس 44')
        self.save('outgoing','دزيلي الرقم والعنوان حتى أثبتلج')
        self.process('07701234567')
        reply,_=self.process('بغداد حي الاختبار')
        self.assertIsNotNone(self.order(),reply.get('reply'))
        self.assertEqual(self.order()['status'],self.m.COLOR_REVIEW_STATUS)
        self.assertFalse(json.loads(self.order()['order_items'])[0]['color'])

    def test_pending_color_requires_identified_photo_of_same_model(self):
        self.link('اسود ماروني')
        order={'order':self.data()}
        order['order']['items'][0]['color']=''
        result,error=self.m.create_order_if_valid(self.db,self.sender,order,self.product)
        self.assertFalse(result)
        self.assertIn('لون',error)

    def test_confirmation_and_arrival_question_preserve_both(self):
        self.link()
        self.save('incoming','أريد الأسود قياس 44')
        self.save('outgoing','دزيلي الرقم والعنوان')
        result,_=self.process('07701234567 بغداد حي الاختبار، وشوكت يوصلني؟')
        self.assertIsNotNone(self.order(),result.get('reply'))
        self.assertIn('تم تثبيت',result['reply'])
        self.assertIn(self.m.get_delivery_settings(self.db)['delivery_time'],result['reply'])

    def test_color_correction_then_contacts_and_eta(self):
        self.link('أزرق ماروني')
        for message in ('أريد دشداشة ماروني قياس 44','لا لحظة، خليها أزرق بدل الماروني. ثبتي وحدة',
                        '07701234567','بغداد حي الاختبار قرب مدرسة التجربة، وشوكت يوصلني؟'):
            result,_=self.process(message)
        self.assertIsNotNone(self.order(),result.get('reply'))
        item=json.loads(self.order()['order_items'])[0]
        self.assertEqual((item['color'],item['size']),('أزرق','44'))
        self.assertNotIn('شوكت', self.order()['address'])
        self.assertIn(self.m.get_delivery_settings(self.db)['delivery_time'],result['reply'])

    def test_staff_color_releases_held_order_once(self):
        self.link('اسود ماروني')
        self.remember_photo('')
        order=self.data()
        order['items'][0]['color']=''
        self.m.create_order_if_valid(self.db,self.sender,{'order':order},self.product)
        held=self.order()
        with self.m.app.test_request_context(method='PATCH',json={'color':'ماروني'}):
            from flask import session
            session['dashboard_authenticated']=True
            reply=self.m.api_update_order(held['id'])
            self.assertTrue(reply.get_json()['ok'])
            self.m.api_update_order(held['id'])
        self.assertEqual(self.order()['status'],'new')
        self.assertEqual(self.order()['color'],'ماروني')
        self.assertFalse(self.m.has_pending_human_review(self.db,self.sender))

    def test_no_unexecuted_customer_booking_claim(self):
        self.assertTrue(self.m.claims_order_created('هسه كملت كل التفاصيل وثبتت طلبج'))
        self.assertFalse(self.m.claims_order_created('ما ثبتت طلبج'))
