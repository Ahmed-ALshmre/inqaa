"""Observed size questions must not turn unknown women shoppers into men."""
import json
import re
import unittest
from unittest.mock import patch

from account_app import owner_sales
from account_app.tests import test_sales_playbook as fixtures


class AddressEvidenceTests(unittest.TestCase):
    def test_womens_store_default_is_wording_only_and_other_stores_stay_unknown(self):
        for store in ('default', 'al-fatena', 'khuyoot'):
            for name in ('', 'حساب خاص', 'نور', 'Blue River'):
                self.assertEqual(owner_sales.address_gender(name, store_id=store), 'female')
        for store in ('baraah-kids', 'golden-threads', 'new-store', ''):
            self.assertEqual(owner_sales.address_gender('Blue River', store_id=store), '')

    def test_explicit_correction_beats_name_store_and_old_profile(self):
        self.assertEqual(owner_sales.address_gender('Hiba', 'female', text='اني رجل واشتري لزوجتي', store_id='al-fatena'), 'male')
        self.assertEqual(owner_sales.address_gender('محمد', 'male', text='مو رجل اني بنية', store_id='default'), 'female')
        for text, gender in [('خاطبني بصيغة المؤنث', 'female'), ('خاطبيني كمذكر', 'male'), ('أنا أنثى', 'female')]:
            self.assertEqual(owner_sales.explicit_gender(text), gender)
        self.assertEqual(owner_sales.address_gender('Hiba', 'male', store_id='default'), 'male')

    def test_latin_names_prefixes_and_fathers_name(self):
        for name in ('its. heba', 'Hiba Ali', 'Heba Ahmed', 'Hajar Ali', 'مريم محمد'):
            self.assertEqual(owner_sales.address_gender(name), 'female', name)
        self.assertEqual(owner_sales.address_gender('Ahmed Hiba'), 'male')

    def test_self_description_survives_followups_but_recipient_and_agent_do_not_count(self):
        for text in ('اني محتارة بين لونين', 'أنا لابسة قياس 44', 'اني خايفه ما يجي قدي'):
            self.assertEqual(owner_sales.address_gender('محمد', text=text), 'female')
            self.assertEqual(owner_sales.address_gender('محمد', history=[{'direction':'incoming','text':text}], text='شكد التوصيل؟'), 'female')
            self.assertEqual(owner_sales.explicit_gender(text), '')
        for text in ('اريد فستان لبنتي', 'زوجتي محتارة', 'حبيبتي عندج قياس 44؟', 'انتي موظفة؟'):
            self.assertEqual(owner_sales.address_gender('', text=text, store_id='baraah-kids'), '')
        self.assertEqual(owner_sales.address_gender('', history=[{'role':'assistant','content':'اني بنت'}]), '')
        self.assertEqual(owner_sales.address_gender('', history=[{'role':'user','content':'اني رجل'}], store_id='default'), 'male')

    def test_actual_size_and_offer_phrases_are_feminine_without_mixed_suffixes(self):
        for original in ('شكد قياس تلبس؟', 'شكد تلبس قياس؟', 'شكد تلبس؟', 'شنو قياسك؟', 'تحب تشوفه؟', 'تحب تشوفين؟'):
            text = owner_sales.surface(original, 'female', warm=False)
            self.assertFalse(re.search(r'(?<!\w)(?:تلبس|تحب|تشوف|تشوفه|قياسك)(?!\w)', text), text)
        self.assertIn('تحبين تشوفينه', owner_sales.surface('تحب تشوفه؟', 'female', warm=False))

    def test_unknown_is_not_masculine_and_receipt_facts_are_untouched(self):
        self.assertEqual(owner_sales.surface('شكد قياس تلبسين؟', '', warm=False), 'شنو القياس المطلوب؟')
        self.assertEqual(owner_sales.surface('شكد تلبس قياس؟', '', warm=False), 'شنو القياس المطلوب؟')
        self.assertEqual(owner_sales.surface('تحب تشوفه؟', '', warm=False), 'أعرضه؟')
        neutral=owner_sales.surface('هلا بيج عيني، شنو حاب تشوف من الموديلات؟', '', warm=False)
        for word in ('بيج','حاب','تشوف'):
            self.assertNotIn(word,neutral)
        receipt='تم تثبيت الطلب، السعر 16,000 والتوصيل 5,000 والمجموع 21,000. قياس 44، اللون بيج.'
        self.assertEqual(owner_sales.surface(receipt, 'female'), receipt)
        facts='اللون بيج، القياس 44، السعر 16,000.'
        for gender in ('female', 'male', ''):
            self.assertIn(facts, owner_sales.surface(facts, gender, warm=False))

    def test_address_does_not_turn_agents_offer_into_customer_instruction(self):
        text='أرسلها بعد ما أتأكد من توفرها.'
        self.assertEqual(owner_sales.surface(text,'female',warm=False),text)


class AddressPipelineTests(unittest.TestCase):
    setUp = fixtures.ReviewedReplyPipelineTests.setUp
    tearDown = fixtures.ReviewedReplyPipelineTests.tearDown
    customer = fixtures.ReviewedReplyPipelineTests.customer
    ask = fixtures.ReviewedReplyPipelineTests.ask

    def test_local_price_reply_defaults_female_without_persisting_gender(self):
        result, model = self.ask('شكد سعره؟', sender='address-local')
        model.assert_not_called()
        self.assertIn('تلبسين', result['reply'])
        self.assertFalse(owner_sales.AFFECTION.search(result['reply']))
        self.assertFalse(self.db.execute('SELECT gender FROM customers WHERE sender_id=?', ('address-local',)).fetchone()[0])

    def test_male_buyer_in_womens_store_is_asked_for_requested_size_not_his_own(self):
        result, model=self.ask('شكد سعره؟',customer={'gender':'male'},sender='address-male')
        model.assert_not_called()
        self.assertIn('شنو القياس المطلوب؟',result['reply'])
        self.assertNotIn('تلبس',result['reply'])

    def test_db_evidence_survives_turns_and_explicit_male_is_respected(self):
        self.customer('address-history')
        self.db.execute('UPDATE customers SET name=? WHERE sender_id=?', ('محمد', 'address-history'))
        self.m.save_message(self.db, 'address-history', 'incoming', 'text', 'اني محتارة', None, None, None, {})
        self.assertEqual(self.m._customer_gender_from_db(self.db, 'address-history'), 'female')
        self.m.update_customer_intelligence(self.db, 'address-history', 'اني رجل واشتري لزوجتي')
        self.assertEqual(self.m._customer_gender_from_db(self.db, 'address-history'), 'male')
        corrected=self.m.owner_result_surface({'reply':'شكد قياس تلبس؟'}, {'sender_id':'address-history','text':'اني بنت'})
        self.assertIn('تلبسين', corrected['reply'])

    def test_reply_parts_use_one_address_and_keep_price_delivery_and_size(self):
        parts=['سعره 16 ألف، التوصيل 5 آلاف والمجموع 21 ألف.', 'تحب تشوفه؟ قياسك 44 متوفر.']
        result=self.m.owner_result_surface({'reply':'\n\n'.join(parts), 'reply_parts':parts}, {'text':'سعره؟'}, {'name':'its. heba'})
        self.assertEqual(len(result['reply_parts']), 2)
        self.assertIn('تحبين تشوفينه',result['reply'])
        for fact in ('16 ألف','5 آلاف','21 ألف','44'):
            self.assertIn(fact,result['reply'])

    def test_first_reply_prompt_uses_effective_female_address(self):
        self.customer('address-first')
        with patch.object(self.m.ai_transport,'post') as post:
            post.return_value.json.return_value={'choices':[{'message':{'content':json.dumps({'reply':'شكد قياس تلبسين؟'})},'finish_reason':'stop'}]}
            reply, gender=self.m.generate_first_message_reply(self.db, {'sender_id':'address-first','text':'سلام'}, [], '', [])
        self.assertEqual(gender,'female')
        self.assertIn('صيغة خطاب الزبون: مؤنث', post.call_args.kwargs['json']['messages'][0]['content'])

