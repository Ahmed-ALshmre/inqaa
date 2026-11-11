"""Staff explanations, isolated reply routing, and the first-visit question queue."""
import json
import sqlite3
import unittest
from unittest.mock import patch
import requests

from account_app import product_questions as questions
from account_app.tests import test_product_questions_nov01 as fixtures
from account_app.tests import test_staff_access as staff_fixtures


class QuestionGuidanceTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.ProductQuestionsTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ProductQuestionsTests.tearDownClass.__func__)
    setUp = fixtures.ProductQuestionsTests.setUp
    tearDown = fixtures.ProductQuestionsTests.tearDown
    process = fixtures.ProductQuestionsTests.process
    save = fixtures.ProductQuestionsTests.save
    count = fixtures.ProductQuestionsTests.count
    owner = fixtures.ProductQuestionsTests.owner

    def capture(self, topic='stretch', question='بيه ليكرا؟', store='baraah-kids', product=None, sender=None):
        return questions.capture(self.db, store, sender or self.sender, product or self.product,
            question, [topic], None, self.m.now_baghdad_iso())[0]

    def teach(self, topic='stretch', question='بيه ليكرا؟', text=None):
        identity = self.capture(topic, question)
        guide = text or 'وضحي لها أن قماش هذا المنتج بيه ليكرا خفيفة، واسألي عن العمر إذا بعده مو معروف.'
        questions.save_guidance(self.db, 'baraah-kids', identity, guide, 'موظفة اختبار', self.m.now_baghdad_iso())
        return identity, guide

    def test_migration_preserves_legacy_answers_and_can_run_twice(self):
        db = sqlite3.connect(':memory:');db.row_factory = sqlite3.Row
        questions.init(db)
        db.execute('ALTER TABLE product_questions DROP COLUMN guidance')
        db.execute("INSERT INTO product_questions(store_id,product_id,topic,question,question_key,answer,status,created_at,updated_at) VALUES('s','p','fabric','q','k','قطن','answered','t','t')")
        questions.init(db);questions.init(db)
        row = dict(db.execute('SELECT * FROM product_questions').fetchone())
        self.assertEqual((row['answer'], row['guidance']), ('قطن', ''))
        db.close()

    def test_explanation_does_not_become_a_product_field_faq_or_exact_reply(self):
        _, guide = self.teach()
        original = dict(self.product)
        learned = questions.apply(self.db, 'baraah-kids', [original])[0]
        self.assertEqual(learned.get('stretch'), original.get('stretch'))
        self.assertEqual(learned.get('price'), original.get('price'))
        self.assertNotIn(guide, learned.get('faq') or '')
        self.assertFalse(questions.exact_answer(learned, 'بيه ليكرا؟'))
        self.assertNotIn('question', learned['_staff_guidance'][0])
        self.assertNotIn('sender_id', learned['_staff_guidance'][0])
        self.assertEqual(questions.apply(self.db, 'baraah-kids', [learned])[0], learned)

    def test_legacy_fact_is_preserved_when_adding_explanation(self):
        identity = self.capture()
        questions.save_answer(self.db, 'baraah-kids', identity, 'بيه ليكرا', 'الموظفة', self.m.now_baghdad_iso())
        questions.save_guidance(self.db, 'baraah-kids', identity, 'جاوبي عن الليكرا باختصار، ولا تعيدين سؤال العمر المعروف.', 'الموظفة', self.m.now_baghdad_iso())
        learned = questions.apply(self.db, 'baraah-kids', [self.product])[0]
        self.assertEqual(learned['stretch'], 'بيه ليكرا')
        self.assertEqual(len(learned['_staff_guidance']), 1)

    def test_only_relevant_product_store_and_topic_receive_guidance(self):
        self.teach()
        learned, other = questions.apply(self.db, 'baraah-kids', [self.product, dict(self.product, product_id='K2')])
        self.assertTrue(questions.guidance_for(learned, 'بي مطاط؟'))
        self.assertFalse(questions.guidance_for(learned, 'شكد السعر؟'))
        self.assertFalse(questions.guidance_for(other, 'بيه ليكرا؟'))
        self.assertFalse(questions.apply(self.db, 'default', [self.product])[0].get('_staff_guidance'))

    def test_mixed_question_still_reviews_other_unknown_facts(self):
        self.teach()
        learned = questions.apply(self.db, 'baraah-kids', [dict(self.product, lining='')])[0]
        self.assertEqual(questions.missing('بيه ليكرا؟', learned), [])
        self.assertIn('lining', questions.missing('بيه ليكرا وبيه بطانة؟', learned))
        self.assertNotIn('stretch', questions.missing('بيه ليكرا وبيه بطانة؟', learned))

    def test_explanations_cannot_replace_an_invalid_canonical_price(self):
        self.teach('price', 'شكد سعره؟', 'وضحي أن السعر مناسب، وكملي اختيار اللون بدون إطالة.')
        learned = questions.apply(self.db, 'baraah-kids', [dict(self.product, price='غير محدد')])[0]
        self.assertIn('price', questions.missing('شكد سعره؟', learned))

    def test_general_details_require_same_question_not_unrelated_question(self):
        self.teach('details', 'شنو بلد المنشأ؟', 'اشرحي أن بلد المنشأ لهذا المنتج تركيا، بدون إعادة سرد باقي المواصفات.')
        learned = questions.apply(self.db, 'baraah-kids', [self.product])[0]
        self.assertTrue(questions.guidance_for(learned, 'شنو بلد المنشأ؟'))
        self.assertEqual(questions.missing('شنو بلد المنشأ؟', learned), [])
        self.assertFalse(questions.guidance_for(learned, 'كم قطعة بالطقم؟'))

    def test_exact_details_teaching_can_explain_the_contents_of_a_set(self):
        question = 'هو قطعتين؟'
        self.teach('details', question, 'اشرحي لها أن هذا الطقم يتكون من قطعتين، قميص وبنطرون، بدون إعادة اسم الموديل.')
        learned = questions.apply(self.db, 'baraah-kids', [self.product])[0]
        self.assertEqual(questions.missing(question, learned), [])
        self.assertTrue(questions.missing('بيه بطانة؟', learned))

    def test_service_guidance_is_sender_and_question_scoped(self):
        self.teach('service', 'وين صار طلبي؟', 'اطلبي رقم الطلب إذا مو معروف، ولا توعدينها بموعد تسليم غير مؤكد.')
        learned = questions.apply(self.db, 'baraah-kids', [self.product], sender=self.sender)[0]
        self.assertTrue(questions.guidance_for(learned, 'وين صار طلبي؟'))
        self.assertFalse(questions.guidance_for(learned, 'يوصل باجر؟'))
        self.assertFalse(questions.apply(self.db, 'baraah-kids', [self.product], sender='other')[0].get('_staff_guidance'))
        self.assertFalse(questions.apply(self.db, 'baraah-kids', [self.product])[0].get('_staff_guidance'))

    def test_guidance_reaches_provider_and_reply_is_contextual_not_verbatim(self):
        identity, guide = self.teach()
        generated = dict(reply='إي حبيبتي، بيه ليكرا خفيفة. شكد العمر المطلوب؟', create_order=False, order={})
        with patch.object(self.m, '_call_main_ai_once', return_value=generated) as provider:
            result = self.m.call_main_ai(dict(self.ev, text='بيه ليكرا؟'), 'text', {}, [],
                [self.product], self.product, None, '', [], customer_products=[self.product])
        self.assertTrue(provider.called, result)
        self.assertFalse(result.get('requires_human'), result)
        self.assertIn('ليكرا', result['reply'])
        self.assertNotIn('وضحي', result['reply'])
        self.assertNotEqual(result['reply'], guide)
        self.assertEqual(self.count(), 0)

    def test_actual_provider_prompt_carries_explanation_and_non_copy_rule(self):
        _, guide = self.teach()
        learned = questions.apply(self.db, 'baraah-kids', [self.product])[0]
        response = requests.Response();response.status_code = 200
        response._content = json.dumps({'choices': [{'message': {'content': json.dumps(
            dict(reply='إي عيوني، بيه ليكرا خفيفة.', create_order=False, order={}))}}]}).encode()
        with patch.object(self.m, 'OPENROUTER_KEY', 'offline-test-only'), patch.object(self.m.ai_transport, 'post', return_value=response) as provider:
            self.m._call_main_ai_once(dict(self.ev, text='بيه ليكرا؟'), 'text', {}, [], [learned], learned, None, '', [])
        system = provider.call_args.kwargs['json']['messages'][0]['content']
        self.assertIn(guide, system)
        self.assertIn('لا تنقلي الشرح حرفياً', system)
        self.assertIn('أسعار وخصومات', system)

    def test_feel_season_care_and_known_fabric_use_guidance_instead_of_old_shortcuts(self):
        cases = [
            ('fabric_feel', 'ثخين لو خفيف؟', 'وضحي لها أن هذا المنتج ثخين، وإذا العمر مو معروف اسألي عنه.', 'ثخين حبيبتي، شكد العمر المطلوب؟'),
            ('season', 'شتوي لو صيفي؟', 'اشرحي لها أن هذا المنتج شتوي، وتابعي اختيار العمر الناقص.', 'شتوي حبيبتي، شكد العمر المطلوب؟'),
            ('care', 'ينغسل بالغسالة؟', 'فهميها أن غسل هذا المنتج على البارد بدون عصر قوي.', 'ينغسل عالبارد بدون عصر قوي حبيبتي.'),
            ('fabric', 'شنو قماشه؟', 'جاوبي عن قماش القطن المسجّل باختصار، بدون سرد المواصفات.', 'قماشه قطن حبيبتي، شكد العمر المطلوب؟'),
        ]
        self.product['fabric'] = 'قطن'
        for topic, question, explanation, reply in cases:
            with self.subTest(topic=topic):
                self.teach(topic, question, explanation)
                with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply=reply, create_order=False, order={})) as provider:
                    result = self.m.call_main_ai(dict(self.ev, text=question), 'text', {}, [], [self.product], self.product, None, '', [])
                self.assertTrue(provider.called, result)
                self.assertFalse(result.get('requires_human'), result)
                self.assertIn(reply.split()[0], result['reply'])
                self.assertNotEqual(result['reply'], explanation)

    def test_webhook_uses_teaching_and_does_not_create_a_new_review(self):
        self.teach()
        event = dict(self.ev, text='بيه ليكرا؟')
        generated = dict(reply='إي عيوني، بيه ليكرا خفيفة. شكد العمر المطلوب؟', create_order=False, order={})
        with patch.object(self.m, 'extract_facebook_event', return_value=event), patch.object(self.m, 'is_ai_enabled', return_value=True), \
             patch.object(self.m, 'is_customer_ai_enabled', return_value=True), patch.object(self.m, 'is_store_feature_enabled', return_value=False), \
             patch.object(self.m, '_call_main_ai_once', return_value=generated) as provider:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        self.assertTrue(provider.called, result)
        self.assertIn('ليكرا', result['reply'])
        self.assertFalse(result.get('meta', {}).get('human_review_id'), result)
        self.assertEqual(self.count(), 0)

    def test_a_model_that_copies_the_explanation_is_retried_then_silently_stopped(self):
        _, guide = self.teach()
        consultation = requests.Response();consultation.status_code = 200
        consultation._content = json.dumps({'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({
            'decision': 'assist', 'guidance': 'لا تنقلي شرح الموظفة؛ صيغي جواباً عن الليكرا فقط حسب المعلومة التي أكّدتها.',
            'reason': 'الشرح ليس رسالة للزبون.', 'focus_product_id': None})}}]}).encode()
        with patch.object(self.m.ai_transport, 'post', return_value=consultation), \
             patch.object(self.m, '_call_main_ai_once', return_value=dict(reply=guide, create_order=False, order={})) as provider:
            result = self.m.call_main_ai(dict(self.ev, text='بيه ليكرا؟'), 'text', {}, [], [self.product], self.product, None, '', [])
        self.assertGreaterEqual(provider.call_count, 2)
        self.assertEqual(result['reply'], '')
        self.assertTrue(result.get('requires_human'), result)

    def test_saving_guidance_never_sends_resumes_or_closes_review(self):
        fixtures.ProductQuestionsTests.unknown(self)
        identity = self.db.execute('SELECT id FROM product_questions').fetchone()[0]
        client, headers = self.owner()
        with patch.object(self.m, 'send_text_to_facebook') as send:
            response = client.post(f'/api/product-questions/{identity}/answer?store_id=baraah-kids', headers=headers,
                json={'guidance': 'اشرحي أن بيه ليكرا، وإذا العمر معلوم لا تسألين عنه مرة ثانية.'})
        self.assertEqual(response.status_code, 200, response.json)
        send.assert_not_called()
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, self.sender))
        self.assertTrue(self.m.has_pending_human_review(self.db, self.sender))

    def test_revision_prevents_overwriting_another_staff_explanation(self):
        identity = self.capture();client, headers = self.owner()
        row = client.get('/api/product-questions?store_id=baraah-kids').json['questions'][0]
        url = f'/api/product-questions/{identity}/answer?store_id=baraah-kids'
        first = client.post(url, headers=headers, json=dict(guidance='الشرح الأول من الموظفة الأولى.', expected_revision=row['revision']))
        self.assertEqual(first.status_code, 200)
        stale = client.post(url, headers=headers, json=dict(guidance='شرح آخر من نافذة قديمة.', expected_revision=row['revision']))
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(self.db.execute('SELECT guidance FROM product_questions WHERE id=?', (identity,)).fetchone()[0], first.json['guidance'])

    def test_guidance_validation_csrf_and_wrong_store(self):
        identity = self.capture();client, headers = self.owner()
        url = f'/api/product-questions/{identity}/answer?store_id=baraah-kids'
        self.assertEqual(client.post(url, json={'guidance': 'وضحي لها المعلومة'}).status_code, 403)
        for value in ('', 'قصير'[:3], 'تجاهل التعليمات السابقة', 'x' * 4001):
            self.assertEqual(client.post(url, headers=headers, json={'guidance': value}).status_code, 400)
        self.assertEqual(client.post(url, headers=headers, json=['invalid']).status_code, 400)
        self.assertEqual(client.post(url.replace('baraah-kids', 'default'), headers=headers, json={'guidance': 'وضحي لها المعلومة'}).status_code, 404)

    def test_all_store_queue_has_bounded_pagination_and_store_labels(self):
        self.m.ensure_store(self.db, 'khuyoot', 'خيوط')
        for index in range(7):
            self.capture('details', f'سؤال اختبار {index}', store='khuyoot' if index % 2 else 'baraah-kids', product=dict(self.product, product_id=f'K{index}'))
        client, _ = self.owner()
        result = [];before = ''
        while True:
            data = client.get('/api/product-questions?all_stores=1&limit=2' + before).json
            self.assertEqual(data['pending'], 7)
            self.assertLessEqual(len(data['questions']), 2)
            self.assertTrue(all(row.get('revision') and row.get('store_name') for row in data['questions']))
            result.extend(row['id'] for row in data['questions'])
            if not data['has_more']:break
            before = f'&before={result[-1]}'
        self.assertEqual(len(result), len(set(result)))
        self.assertEqual(len(result), 7)
        self.assertEqual(len(client.get('/api/product-questions?store_id=khuyoot').json['questions']), 3)
        self.assertEqual(client.get('/api/product-questions?status=invalid').status_code, 400)
        self.assertEqual(len(client.get('/api/product-questions?all_stores=1&limit=0').json['questions']), 1)

    def test_dashboard_contains_first_visit_dialog_but_admin_page_uses_same_editor(self):
        client, _ = self.owner()
        self.assertIn('productQuestionsDialog', client.get('/dashboard').text)
        page = client.get('/product-questions').text
        self.assertIn('product_question_editor.js', page)
        self.assertIn('اشرح المعلومة', page)
        self.assertNotIn('productQuestionsDialog', page)

    def test_dashboard_only_lists_current_product_gaps_with_filtered_pagination(self):
        self.m.ensure_store(self.db, 'khuyoot', 'خيوط')
        product = dict(self.product, store_id='baraah-kids', fabric='قطن', colors='أخضر، أزرق', stretch='', lining='')
        other = dict(product, store_id='khuyoot', stretch='بيه ليكرا')
        archived = dict(product, product_id='OLD', status='inactive')
        missing_stretch = self.capture(product=product)
        mixed = 'شنو قماشه؟\nعنواني حي الاختبار\nبيه بطانة؟'
        missing_lining = self.capture('details', mixed, product=product)
        missing_length = self.capture('length', 'شكد طول القطعة؟', store='khuyoot', product=other)
        self.capture(product=other, store='khuyoot')
        self.capture(product=archived)
        self.capture('fabric', 'شنو قماشه؟', product=product)
        self.capture('details', 'شنو الألوان المتوفرة؟', product=product)
        self.capture('details', 'أريد الأسود.', product=product)
        self.capture('price', 'هل يوجد خصم إضافي؟', product=product)
        self.capture('service', 'وين صار طلبي؟', product=product)
        self.capture('payment', 'شلون الدفع؟', product=product)
        client, headers = self.owner()

        with patch.object(self.m, 'load_products_from_file', return_value=[product, other, archived]):
            url = '/api/product-questions?needs_attention=1&all_stores=1&limit=1'
            first = client.get(url).json
            self.assertEqual(first['pending'], 3)
            self.assertTrue(first['has_more'])
            self.assertEqual(first['questions'][0]['id'], missing_length)
            second = client.get(f'{url}&before={missing_length}').json
            row = second['questions'][0]
            self.assertEqual((row['id'], row['question'], row['gap_label']), (missing_lining, 'بيه بطانة؟', 'البطانة'))
            self.assertEqual(second['pending'], 3)
            self.assertTrue(second['has_more'])
            scoped = client.get('/api/product-questions?needs_attention=1&store_id=baraah-kids').json
            self.assertEqual([row['id'] for row in scoped['questions']], [missing_lining, missing_stretch])

            archive = client.get('/api/product-questions?all_stores=1').json
            self.assertEqual(archive['pending'], 11)
            self.assertEqual(next(item['question'] for item in archive['questions'] if item['id'] == missing_lining), mixed)
            saved = client.post(f'/api/product-questions/{missing_lining}/answer?store_id=baraah-kids', headers=headers,
                json={'guidance': 'وضحي أن القطعة بيها بطانة قطن.', 'expected_revision': row['revision']})
            self.assertEqual(saved.status_code, 200, saved.json)
            product['stretch'] = 'بيه ليكرا'
            remaining = client.get('/api/product-questions?needs_attention=1&store_id=baraah-kids').json
            self.assertEqual((remaining['pending'], remaining['questions']), (0, []))


class QuestionGuidancePermissions(unittest.TestCase):
    setUp = staff_fixtures.StaffAccessTests.setUp
    tearDown = staff_fixtures.StaffAccessTests.tearDown
    employee = staff_fixtures.StaffAccessTests.employee

    def test_reply_staff_can_teach_but_price_requires_product_permission(self):
        from account_app import app as m
        db = m.get_db();product = dict(product_id='T', product_name='طقم اختبار')
        stretch = questions.capture(db, 'default', 'test', product, 'بيه ليكرا؟', ['stretch'], None, m.now_baghdad_iso())[0]
        price = questions.capture(db, 'default', 'test', product, 'شكد السعر؟', ['price'], None, m.now_baghdad_iso())[0]
        self.employee()
        self.assertEqual(self.client.get('/api/product-questions?all_stores=1').status_code, 200)
        self.assertEqual(self.client.post(f'/api/product-questions/{stretch}/answer', headers=self.headers,
            json={'guidance': 'جاوبي عن الليكرا باختصار.'}).status_code, 200)
        self.assertEqual(self.client.post(f'/api/product-questions/{price}/answer', headers=self.headers,
            json={'guidance': 'وضحي لها السعر المسجّل.'}).status_code, 403)
        db.execute('UPDATE staff_accounts SET permissions=? WHERE id=?', ('[]', self.id));db.commit()
        self.assertEqual(self.client.get('/api/product-questions?all_stores=1').status_code, 403)
        self.assertNotIn('productQuestionsDialog', self.client.get('/dashboard').text)
