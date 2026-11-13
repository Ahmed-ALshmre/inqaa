"""A bounded Sol consultation assists the existing main model, never replaces it."""
import json
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch
import requests

from account_app import reply_consultant as consultant, ai_transport
from account_app.tests import test_product_questions_nov01 as fixtures


def answer(decision='assist', guidance='راجعي اللون المصحح إلى الأسود والقياس المعروف، وأجيبي عن سؤالها دون طلب الصورة مجدداً.', **overrides):
    return dict(decision=decision, guidance=guidance, reason='السجل يحتوي المعلومات المطلوبة.', focus_product_id=None, **overrides)


def response(value=None, status=200, finish='stop'):
    result = requests.Response();result.status_code = status
    if status >= 400:
        result._content = b'{"error":{"message":"provider-private-details"}}'
    else:
        result._content = json.dumps({'choices': [{'finish_reason': finish,
            'message': {'content': json.dumps(value or answer(), ensure_ascii=False)}}],
            'usage': {'prompt_tokens': 120, 'completion_tokens': 80, 'cost': .0001}}).encode()
    return result


class ConsultantClientTests(unittest.TestCase):
    def data(self):
        return dict(store_id='default', ev={'sender_id': 'private-sender', 'text': 'مو وردي، أسود قياس44'},
            customer={'phone': '07701234567', 'address': 'عنوان خاص', 'province': 'بغداد'},
            history=[{'direction': 'incoming', 'text': 'رقمي07701234567 وأسود قياس44', 'image_url': 'https://private.test/image.jpg'}],
            products=[dict(product_id='A', store_id='default', price='12000', colors='أسود', sizes='44', image_url='https://private.test/product.jpg'),
                      dict(product_id='B', store_id='khuyoot', price='1')],
            matched_product=dict(product_id='A', store_id='default', price='12000', colors='أسود'),
            memory={'cart': {'items': [{'product_id': 'A', 'color': 'أسود', 'size': '44'}]}, 'phone': '07701234567'})

    def test_exact_model_schema_and_no_direct_customer_reply_or_side_effects(self):
        provider = Mock(return_value=response());record = Mock()
        with consultant.scope(True, **self.data()) as state:
            plan = consultant.advise(state, {'reply': 'دزيلي الصورة'}, ['lost_context'], key='synthetic', endpoint='https://provider.test', post=provider, record=record)
            self.assertEqual(plan['decision'], 'assist')
            self.assertEqual(state.status, 'advised')
            self.assertTrue(state.attempted)
        request = provider.call_args.kwargs
        self.assertEqual(request['json']['model'], 'openai/gpt-6.1-sol')
        self.assertEqual(request['max_attempts'], 1)
        self.assertEqual(request['budget_seconds'], 40)
        self.assertTrue(request['json']['response_format']['json_schema']['strict'])
        self.assertNotIn('tools', request['json'])
        self.assertNotIn('reply', plan)
        record.assert_called_once()

    def test_payload_is_store_scoped_text_only_and_redacts_phone_urls_and_keys(self):
        data = self.data();data['history'][0]['text'] += ' sk-or-v1-' + 'a' * 40
        value = consultant.snapshot(data, {'reply': 'دزيلي الصورة', 'order': {'phone': '07701234567'}}, ['lost_context'])
        raw = json.dumps(value, ensure_ascii=False)
        self.assertNotIn('07701234567', raw)
        self.assertNotIn('https://', raw)
        self.assertNotIn('private-sender', raw)
        self.assertNotIn('sk-or-v1-', raw)
        self.assertNotIn('عنوان خاص', raw)
        self.assertEqual([p['product_id'] for p in value['products']], ['A'])
        self.assertEqual(value['memory']['cart']['items'][0]['size'], '44')
        self.assertTrue(value['known_contact']['phone_saved'])

    def test_latest_correction_is_kept_when_history_is_bounded(self):
        data = self.data();data['history'] = [{'direction': 'incoming', 'text': str(i) + 'س' * 1700} for i in range(100)]
        data['history'][-1]['text'] = 'لا، آخر اختيار أزرق قياس46'
        value = consultant.snapshot(data, {}, ['lost_context'])
        self.assertLessEqual(len(value['history']), 32)
        self.assertEqual(value['history'][-1]['text'], 'لا، آخر اختيار أزرق قياس46')

    def test_adviser_handoff_timeout_auth_billing_and_malformed_output_fail_closed(self):
        variants = [response(answer(decision='needs_human', guidance='')), response(status=401), response(status=402),
                    response(status=503), response({'decision': 'assist'}), response(answer(), finish='length')]
        for value in variants:
            with self.subTest(value=value.status_code):
                provider = Mock(return_value=value)
                with consultant.scope(True, **self.data()) as state:
                    self.assertIsNone(consultant.advise(state, {}, ['hard'], key='synthetic', endpoint='p', post=provider))
                    self.assertIn(state.status, {'failed', 'needs_human'})
                    self.assertNotIn('private', str(state.metadata()))
                provider.assert_called_once()
        with consultant.scope(True, **self.data()) as state:
            self.assertIsNone(consultant.advise(state, {}, [], key='synthetic', endpoint='p', post=Mock(side_effect=requests.Timeout)))
            self.assertEqual(state.status, 'failed')

    def test_unknown_product_focus_and_unauthorized_action_fields_are_rejected(self):
        for plan in [dict(answer(), focus_product_id='not-in-catalog'), dict(answer(), create_order=True),
                     dict(answer(), guidance=''), dict(answer(), reason=123)]:
            with consultant.scope(True, **self.data()) as state:
                self.assertIsNone(consultant.advise(state, {}, [], key='synthetic', endpoint='p', post=Mock(return_value=response(plan))))

    def test_only_one_consultation_and_no_context_leak_between_turns(self):
        provider = Mock(return_value=response())
        with consultant.scope(True, **self.data()) as state:
            consultant.advise(state, {}, [], key='synthetic', endpoint='p', post=provider)
            self.assertIsNone(consultant.advise(state, {}, [], key='synthetic', endpoint='p', post=provider))
        self.assertIsNone(consultant.current());provider.assert_called_once()
        with consultant.scope(True, **self.data()) as following:
            self.assertFalse(following.attempted)

    def test_parallel_stores_have_independent_consultation_state(self):
        def one(store):
            with consultant.scope(True, store_id=store) as state:
                state.attempted = store == 'A'
                return consultant.current().data['store_id'], state.attempted
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(one, ['A', 'B'])), [('A', True), ('B', False)])
        self.assertIsNone(consultant.current())

    def test_oversized_context_fails_before_spending_on_a_request(self):
        data = self.data()
        data['matched_product'] = {field: 'x' * 1400 for field in consultant.PRODUCT_FIELDS}
        data['matched_product'].update(product_id='A', store_id='default')
        data['customer_products'] = [dict(data['matched_product'], product_id=str(index)) for index in range(8)]
        provider = Mock()
        with consultant.scope(True, **data) as state:
            self.assertIsNone(consultant.advise(state, {}, [], key='synthetic', endpoint='p', post=provider))
            self.assertEqual(state.status, 'failed')
        provider.assert_not_called()

    def test_bounded_transport_does_not_retry_consultant_or_forward_internal_keywords(self):
        with patch.object(ai_transport.requests, 'post', return_value=response(status=503)) as post, patch.object(ai_transport.time, 'sleep') as sleep:
            ai_transport.post('https://provider.test', json={}, timeout=30, max_attempts=1, budget_seconds=40)
        post.assert_called_once();sleep.assert_not_called()
        self.assertNotIn('max_attempts', post.call_args.kwargs)
        self.assertNotIn('budget_seconds', post.call_args.kwargs)
        self.assertLessEqual(sum(post.call_args.kwargs['timeout']), 40)

    def test_offline_runner_only_mocks_the_exact_consultant_endpoint(self):
        from account_app.tests.run_isolated import offline_request
        with self.assertRaises(RuntimeError):
            offline_request(url='https://customer-message.test', json={'model': consultant.MODEL})
        with self.assertRaises(RuntimeError):
            offline_request(url='https://openrouter.ai/api/v1/chat/completions', json={'model': 'a-main-model'})
        payload = {'model': consultant.MODEL, 'messages': [{'content': json.dumps({'unsuccessful_answer': {'missing_topics': ['stretch']}})}]}
        value = offline_request(url='https://openrouter.ai/api/v1/chat/completions', json=payload)
        self.assertEqual(json.loads(value.json()['choices'][0]['message']['content'])['decision'], 'needs_human')


class ConsultantPipelineTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.ProductQuestionsTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ProductQuestionsTests.tearDownClass.__func__)
    tearDown = fixtures.ProductQuestionsTests.tearDown
    process = fixtures.ProductQuestionsTests.process
    save = fixtures.ProductQuestionsTests.save
    count = fixtures.ProductQuestionsTests.count

    def setUp(self):
        fixtures.ProductQuestionsTests.setUp(self)
        self.flag = patch.dict(os.environ, {'REPLY_CONSULTANT_ENABLED': '1'});self.flag.start()
        self.addCleanup(self.flag.stop)
        self.key = patch.object(self.m, 'OPENROUTER_KEY', 'synthetic-key');self.key.start();self.addCleanup(self.key.stop)
        self.product.update(fabric='قطن', colors='أسود', sizes='من 2 إلى 6 سنوات')
        self.db.execute('DELETE FROM human_reviews');self.db.commit()

    def ask(self, text, history=None, **kwargs):
        with patch.object(self.m, 'factual_customer_request', return_value=None):
            return self.m.call_main_ai(dict(self.ev, text=text), 'text', {}, history or [],
                [self.product], self.product, None, '', [], customer_products=[self.product], **kwargs)

    def test_normal_catalog_reply_never_calls_consultant(self):
        with patch.object(self.m.ai_transport, 'post') as advisor:
            result = self.ask('شنو قماشه؟')
        advisor.assert_not_called();self.assertIn('قطن', result['reply'])
        self.assertNotIn('_reply_consultation', result)

    def test_model_handoff_gets_one_advice_then_main_model_rewrites(self):
        good = dict(reply='التوصيل لبغداد5 آلاف عيوني.', requires_human=False, create_order=False, order={})
        # Delivery facts are normally local; this narrow fixture exercises the
        # same provider path used by genuinely non-local difficult questions.
        with patch.object(self.m, 'factual_customer_request', return_value=None), \
             patch.object(self.m, '_call_main_ai_once', side_effect=[dict(reply='', requires_human=True), good]) as main, \
             patch.object(self.m.ai_transport, 'post', return_value=response()) as advisor:
            result = self.ask('شكد التوصيل؟')
        advisor.assert_called_once();self.assertEqual(main.call_count, 2)
        self.assertEqual(result['_reply_consultation']['status'], 'recovered', result)
        self.assertIn('استشارة داخلية', main.call_args.kwargs['fix_instruction'])
        self.assertTrue(main.call_args.args[0]['_consultation_retry'])
        self.assertFalse(result.get('requires_human'), result)
        self.assertFalse(self.m.has_pending_human_review(self.db, self.sender))
        self.assertTrue(self.m.is_customer_ai_enabled(self.db, self.sender))

    def test_advisor_failure_pauses_and_records_one_real_human_review_silently(self):
        with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='', requires_human=True)) as main, \
             patch.object(self.m.ai_transport, 'post', return_value=response(status=503)) as advisor:
            result = self.ask('شكد التوصيل؟')
        main.assert_called_once();advisor.assert_called_once()
        self.assertEqual(result['reply'], '')
        self.assertTrue(result.get('requires_human'), result)
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, self.sender))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0], 1)
        self.assertEqual(self.count(), 0)

    def test_full_rollout_uses_sol_once_when_legacy_checkout_still_owns_turn(self):
        with patch.dict(os.environ, SALES_RUNTIME_MODE='all'), \
             patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='', requires_human=True)), \
             patch.object(self.m.ai_transport, 'post', return_value=response(status=503)) as advisor:
            result = self.ask('شكد التوصيل؟')
        advisor.assert_called_once()
        self.assertEqual('openai/gpt-6.1-sol', advisor.call_args.kwargs['json']['model'])
        self.assertEqual(1, advisor.call_args.kwargs['max_attempts'])
        self.assertEqual('openai/gpt-6.1-sol', result['_reply_consultation']['model'])
        self.assertTrue(result['requires_human'])
        self.assertEqual('', result['reply'])

    def test_if_main_still_fails_after_advice_there_is_no_third_attempt(self):
        with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='', requires_human=True)) as main, \
             patch.object(self.m.ai_transport, 'post', return_value=response()) as advisor:
            result = self.ask('شكد التوصيل؟')
        self.assertEqual(main.call_count, 2);advisor.assert_called_once()
        self.assertEqual(result['reply'], '')
        self.assertEqual(result['_reply_consultation']['status'], 'failed_validation')
        self.assertTrue(self.m.has_pending_human_review(self.db, self.sender))

    def test_unknown_fact_is_consulted_before_creating_question_and_review(self):
        with patch.object(self.m.ai_transport, 'post', return_value=response(answer(decision='needs_human', guidance=''))) as advisor, \
             patch.object(self.m, '_call_main_ai_once') as main:
            result = self.ask('بيه ليكرا؟')
        advisor.assert_called_once();main.assert_not_called()
        self.assertEqual(result['reply'], '')
        self.assertTrue(result.get('_product_question_review_id'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM product_questions').fetchone()[0], 1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0], 1)
        self.assertFalse(self.m.is_customer_ai_enabled(self.db, self.sender))

    def test_unknown_fact_consultation_has_no_early_pause_or_review(self):
        def consult(*args, **kwargs):
            self.assertTrue(self.m.is_customer_ai_enabled(self.db, self.sender))
            self.assertFalse(self.m.has_pending_human_review(self.db, self.sender))
            return response(answer(decision='needs_human', guidance=''))
        with patch.object(self.m.ai_transport, 'post', side_effect=consult):
            self.ask('بيه ليكرا؟')

    def test_user_request_for_employee_is_immediate_without_any_model(self):
        with patch.object(self.m.ai_transport, 'post') as advisor, patch.object(self.m, '_call_main_ai_once') as main:
            result = self.ask('اريد أحچي ويا موظفة')
        advisor.assert_not_called();main.assert_not_called()
        self.assertEqual(result['reply'], '')
        self.assertTrue(result.get('_customer_handoff_review_id'))

    def test_advice_cannot_bypass_missing_fact_or_invent_stretch(self):
        with patch.object(self.m.ai_transport, 'post', return_value=response(answer(guidance='جاوبي عن الليكرا باختصار.'))) as advisor, \
             patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='إي عيوني، بيه ليكرا.', create_order=False, order={})) as main:
            result = self.ask('بيه ليكرا؟')
        advisor.assert_called_once();main.assert_called_once()
        self.assertEqual(result['reply'], '', result)
        self.assertTrue(result.get('_product_question_review_id'))

    def test_main_model_settings_and_vision_settings_never_change(self):
        self.m.set_store_setting(self.db, 'ai_main_model', 'google/gemini-3.8-flash', 'baraah-kids')
        self.m.set_store_setting(self.db, 'ai_catalog_match_model', 'google/gemini-2.5-flash', 'baraah-kids')
        before = tuple(self.m.get_ai_model(self.db, key, '') for key in ('main_model', 'catalog_match_model', 'checker_model', 'improve_model'))
        with patch.object(self.m.ai_transport, 'post', return_value=response(answer(decision='needs_human', guidance=''))):
            self.ask('بيه ليكرا؟')
        after = tuple(self.m.get_ai_model(self.db, key, '') for key in ('main_model', 'catalog_match_model', 'checker_model', 'improve_model'))
        self.assertEqual(before, after)

    def test_disabled_feature_uses_existing_primary_retry_and_unknown_fact_queue(self):
        with patch.dict(os.environ, {'REPLY_CONSULTANT_ENABLED': '0'}), patch.object(self.m.ai_transport, 'post') as advisor:
            result = self.ask('بيه ليكرا؟')
        advisor.assert_not_called();self.assertTrue(result.get('_product_question_review_id'))

    def test_billing_failure_does_not_trigger_another_paid_provider_attempt(self):
        with patch.object(self.m, '_call_main_ai_once', return_value=dict(reply='', failed=True, failure_reason='provider_http_402')) as main, \
             patch.object(self.m.ai_transport, 'post') as advisor:
            result = self.ask('شكد التوصيل؟')
        advisor.assert_not_called();main.assert_called_once()
        self.assertTrue(result.get('failure_reason') == 'provider_http_402' or 'التوصيل' in result.get('reply', ''))

    def test_paused_store_does_not_call_any_reply_or_consultation_model(self):
        with patch.object(self.m, 'extract_facebook_event', return_value=dict(self.ev, text='سؤال صعب')), \
             patch.object(self.m, 'is_ai_enabled', return_value=False), \
             patch.object(self.m, '_call_main_ai_once') as main, patch.object(self.m.ai_transport, 'post') as advisor:
            result = self.m.process_webhook(self.db, {}, use_debounce=False)
        main.assert_not_called();advisor.assert_not_called()
        self.assertEqual(result['reply'], '')

    def test_internal_advice_cannot_be_sent_as_the_customer_reply(self):
        plan = answer()
        with patch.object(self.m, '_call_main_ai_once', side_effect=[dict(reply='', requires_human=True),
                dict(reply=plan['guidance'], create_order=False, order={})]) as main, \
             patch.object(self.m.ai_transport, 'post', return_value=response(plan)) as advisor:
            result = self.ask('شكد التوصيل؟')
        self.assertEqual(main.call_count, 2);advisor.assert_called_once()
        self.assertEqual(result['reply'], '')
        self.assertTrue(result.get('requires_human'))

    def test_consulted_wording_survives_later_surface_passes(self):
        text = 'قماشه قطن عيوني، تحبين تشوفين الألوان؟'
        result = self.m.owner_result_surface(dict(reply=text, create_order=False, order={}, _consulted_reply=True),
            dict(self.ev, text='شنو قماشه؟', _reply_product=self.product), {}, [])
        self.assertIn('الألوان', result['reply'])

    def test_primary_wire_after_consultation_keeps_the_configured_main_model(self):
        self.m.set_store_setting(self.db, 'ai_main_model', 'google/gemini-3.8-flash', 'baraah-kids')
        original = self.m._call_main_ai_once
        def first_then_actual(*args, **kwargs):
            if not args[0].get('_consultation_retry'):
                return dict(reply='', requires_human=True)
            return original(*args, **kwargs)
        main_response = response(dict(reply='التوصيل5 آلاف عيوني.', create_order=False, order={}, requires_human=False))
        def provider(*args, **kwargs):
            if kwargs['json']['model'] == consultant.MODEL:
                return response()
            self.assertEqual(kwargs['json']['model'], 'google/gemini-3.8-flash')
            self.assertEqual(kwargs['max_attempts'], 1)
            self.assertEqual(kwargs['budget_seconds'], 45)
            self.assertIn('استشارة داخلية', kwargs['json']['messages'][-1]['content'])
            return main_response
        with patch.object(self.m, '_call_main_ai_once', side_effect=first_then_actual), \
             patch.object(self.m.ai_transport, 'post', side_effect=provider) as wire:
            result = self.ask('شكد التوصيل؟')
        self.assertEqual([call.kwargs['json']['model'] for call in wire.call_args_list], [consultant.MODEL, 'google/gemini-3.8-flash'])
        self.assertFalse(result.get('requires_human'), result)
