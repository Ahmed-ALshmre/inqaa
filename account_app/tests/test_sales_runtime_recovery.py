"""Adversarial decisions exercise the exact boundary used by real models."""
import json
import unittest
from unittest.mock import Mock

from account_app.tests import test_sales_runtime_linking as linking
from account_app.sales_runtime.runtime import Runtime
from account_app.sales_runtime.contracts import clean_plan, DECISION_SCHEMA
from account_app.sales_runtime.gateway import DecisionGateway
from account_app.sales_runtime import Conflict


class RecoveryTests(unittest.TestCase):
    setUp=linking.LinkingTests.setUp
    tearDown=linking.LinkingTests.tearDown
    event=linking.LinkingTests.event
    version=linking.LinkingTests.version

    def test_system_assigns_id_even_if_legacy_model_returned_number(self):
        self.plan=dict(intent='continue',changes=[dict(line_id=1,values=dict(product_id='P1',color='ماروني',size='44'))])
        self.runtime.turn(self.scope,self.event('ماروني 44'))
        line=self.store.snapshot(self.scope)['data']['cart'][0]
        self.assertTrue(line['line_id'].startswith('line_'))
        self.assertIsInstance(line['line_id'],str)

    def test_second_command_failure_rolls_back_first_and_evidence(self):
        self.plan=dict(intent='continue',changes=[dict(new_line=True,values=dict(product_id='P1',size='44')),
                                                dict(new_line=True,values=dict(product_id='P9'))])
        event=self.event('قميص وقطعة ثانية')
        version=self.version()
        with self.assertRaises(Conflict):self.runtime.turn(self.scope,event)
        self.assertEqual([],self.store.snapshot(self.scope)['data']['cart'])
        self.assertEqual(version,self.version())
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM sales_v2_evidence').fetchone()[0])

    def test_failed_contact_does_not_leave_partial_selection(self):
        self.plan=dict(intent='continue',changes=[dict(values=dict(product_id='P1',size='44'))],
                       contact=dict(phone='07701234567'))
        event=self.event('قميص 44')
        with self.assertRaisesRegex(Conflict,'phone_not_in_source'):
            self.runtime.turn(self.scope,event)
        self.assertEqual([],self.store.snapshot(self.scope)['data']['cart'])

    def test_repair_runs_outside_transaction_then_commits_once(self):
        test=self
        class RepairingModel:
            def __call__(self,context):
                return dict(intent='continue',changes=[dict(values=dict(product_id='P1',size='99'))])
            def repair(self,context,previous,error):
                test.assertFalse(test.db.in_transaction)
                test.assertEqual([],test.store.snapshot(test.scope)['data']['cart'])
                self.feedback=error
                return dict(intent='continue',changes=[dict(values=dict(product_id='P1',size='44'))])
        model=RepairingModel()
        runtime=Runtime(self.store,self.loader,self.orders,model)
        event=self.event('قياس 44')
        outcome=runtime.turn(self.scope,event)
        self.assertEqual('continue',outcome['action'])
        self.assertTrue(model.feedback)
        self.assertEqual('44',self.store.snapshot(self.scope)['data']['cart'][0]['size'])
        self.assertEqual(outcome,runtime.turn(self.scope,event))
        self.assertEqual(1,len(self.store.snapshot(self.scope)['data']['cart']))

    def test_repair_limit_does_not_hide_bad_tools(self):
        class BadModel:
            calls=0
            def __call__(self,context):return dict(intent='continue',changes=[dict(values=dict(product_id='P9'))])
            def repair(self,context,previous,error):
                self.calls+=1
                return self(context)
        model=BadModel();runtime=Runtime(self.store,self.loader,self.orders,model)
        with self.assertRaises(Conflict):runtime.turn(self.scope,self.event('قطعة'))
        self.assertEqual(1,model.calls)
        self.assertEqual([],self.store.snapshot(self.scope)['data']['cart'])

    def test_new_message_during_repair_cancels_stale_change(self):
        test=self
        class Model:
            def __call__(self,context):return dict(intent='continue',changes=[dict(values=dict(product_id='P9'))])
            def repair(self,context,previous,error):
                test.event('غيرت رايي')
                return dict(intent='continue',changes=[dict(values=dict(product_id='P1',size='44'))])
        with self.assertRaisesRegex(Conflict,'state_version_changed'):
            Runtime(self.store,self.loader,self.orders,Model()).turn(self.scope,self.event('قميص'))
        self.assertEqual([],self.store.snapshot(self.scope)['data']['cart'])

    def test_two_same_product_lines_require_unambiguous_reference(self):
        self.plan=dict(intent='continue',changes=[dict(new_line=True,values=dict(product_id='P1',color=c,size='44')) for c in ('ماروني','أزرق')])
        self.runtime.turn(self.scope,self.event('ماروني وازرق 44'))
        before=self.store.snapshot(self.scope)['data']['cart']
        self.plan=dict(intent='continue',changes=[dict(values=dict(product_id='P1',size='40'))])
        with self.assertRaisesRegex(Conflict,'ambiguous_line_reference'):
            self.runtime.turn(self.scope,self.event('غيري القياس'))
        self.assertEqual(before,self.store.snapshot(self.scope)['data']['cart'])

    def test_shared_contract_rejects_numeric_size_price_and_boolean_quantity(self):
        for values in [dict(size=44),dict(price=1),dict(quantity=True),dict(age_months=1.5)]:
            with self.subTest(values=values),self.assertRaises(ValueError):
                clean_plan(dict(intent='continue',changes=[dict(values=values)]))
        plan=clean_plan(dict(intent='continue',changes=[dict(line_ref=None,new_line=False,remove=False,
                        values=dict(product_id='P1',size=None))],contact=dict(phone=None,address=None)))
        self.assertEqual({'product_id':'P1'},plan['changes'][0]['values'])
        self.assertEqual({},plan['contact'])

    def test_native_schema_and_feedback_contain_exact_validation_error(self):
        bad=dict(intent='continue',changes=[dict(values=dict(quantity=True))])
        good=dict(intent='continue',changes=[])
        responses=[]
        for plan in (bad,good):
            response=Mock(status_code=200)
            response.json.return_value={'choices':[{'message':{'content':json.dumps(plan)}}]}
            responses.append(response)
        post=Mock(side_effect=responses)
        self.assertEqual('continue',DecisionGateway('test-model','fake',post)({})['intent'])
        request=post.call_args.kwargs['json']
        self.assertEqual(DECISION_SCHEMA,request['response_format']['json_schema']['schema'])
        self.assertTrue(request['provider']['require_parameters'])
        self.assertIn('quantity',request['messages'][-1]['content'])

    def test_context_includes_previous_messages_and_accepted_replies(self):
        first=self.event('قياس 44')
        self.runtime.turn(self.scope,first)
        self.store.enqueue_reply(self.scope,'history',self.version(),[dict(type='text',text='إي متوفر')])
        part=self.store.claim_reply(self.scope);self.store.settle_reply(self.scope,part['id'],'accepted','x')
        self.runtime.turn(self.scope,self.event('اريد الماروني'))
        context=self.decide.call_args.args[0]
        self.assertEqual('قياس 44',context['recent_messages'][0]['text'])
        self.assertEqual('إي متوفر',context['recent_replies'][0]['text'])

    def test_model_budget_survives_duplicate_retry_and_database_reopen(self):
        import sqlite3
        from account_app.sales_runtime import Store
        decide=Mock(side_effect=ValueError('bad format'))
        runtime=Runtime(self.store,self.loader,self.orders,decide)
        event=self.event('مرحبا')
        for attempt in range(2):
            with self.assertRaisesRegex(ValueError,'bad format'):runtime.turn(self.scope,event)
        self.db.close();self.db=sqlite3.connect(self.path);self.store=Store(self.db)
        with self.assertRaisesRegex(Conflict,'decision_budget_exhausted'):
            Runtime(self.store,self.loader,self.orders,decide).turn(self.scope,event)
        self.assertEqual(2,decide.call_count)
        self.assertEqual(2,self.db.execute('SELECT COUNT(*) FROM sales_v2_model_attempts').fetchone()[0])

    def test_sizeless_bag_question_is_known_fact_not_handoff(self):
        from account_app.sales_runtime.response import Responses
        self.plan=dict(intent='continue',changes=[dict(values=dict(product_id='B1',color='أسود'))],
                       answer=dict(facts=[dict(product_id='B1',field='sizes')]))
        event=self.event('تحتاجين وزني لو قياسي؟')
        outcome=self.runtime.turn(self.scope,event)
        self.assertEqual([],outcome['readiness']['conditions'])
        self.assertEqual([],self.store.snapshot(self.scope)['data']['conditions'])
        writer=Mock(return_value=[dict(type='text',text='لا حبيبتي ما تحتاج قياس')])
        self.assertEqual('queued',Responses(self.store,self.loader,writer).prepare(self.scope,event,outcome)['action'])
        self.assertEqual(dict(product_id='B1',field='size_system',value='none'),writer.call_args.args[0]['facts'][0])

    def test_known_ad_has_next_step_and_repeat_question_is_suppressed(self):
        from account_app.sales_runtime.response import Responses
        self.plan=dict(intent='continue',answer=dict(facts=[dict(product_id='P1',field='fabric')]))
        self.products[0]['fabric']='قديفة'
        event=self.store.append(self.scope,'ad-message',kind='message',actor='customer',
                                payload=dict(text='شنو القماش',ad={'ad_id':'120251717777850095'}))['id']
        outcome=self.runtime.turn(self.scope,event)
        writer=Mock(side_effect=[[dict(type='text',text='قديفة حبيبتي شكد قياسج؟')],
                                 [dict(type='text',text='قديفة شتوي حبيبتي')]])
        responses=Responses(self.store,self.loader,writer)
        responses.prepare(self.scope,event,outcome)
        self.assertEqual('product.P1.size',writer.call_args.args[0]['question_field'])
        part=self.store.claim_reply(self.scope);self.store.settle_reply(self.scope,part['id'],'accepted','size-ask')
        event=self.event('قماشه ثخين؟')
        outcome=self.runtime.turn(self.scope,event)
        responses.prepare(self.scope,event,outcome)
        self.assertIsNone(writer.call_args.args[0]['question_field'])

    def test_exhausted_contract_becomes_one_visible_review_not_uncaught_error(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        from account_app.sales_runtime.contracts import DecisionRejected
        decide=Mock(side_effect=DecisionRejected('decision_invalid_after_retry'))
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,decide)
        kwargs=dict(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='fault',message_id='m',text='متوفر؟')
        outcome=entry.receive(**kwargs)
        self.assertEqual('needs_review',outcome['action'])
        self.assertEqual('',outcome['reply'])
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])
        entry.receive(**kwargs)
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])

    def test_writer_outage_can_resume_saved_turn_without_staling_it(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        from account_app.sales_runtime.contracts import DecisionUnavailable
        writer=Mock(side_effect=[DecisionUnavailable('writer_provider_http_402'),[dict(type='text',text='هلا بيج')]])
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide,writer=writer)
        kwargs=dict(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='writer-fault',message_id='m',text='هلا')
        self.assertEqual('service_failure',entry.receive(**kwargs)['action'])
        self.assertEqual('queued',entry.receive(**kwargs)['reply']['action'])
        self.decide.assert_called_once()
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM sales_v2_incidents').fetchone()[0])

    def test_obsolete_failure_does_not_pause_newer_message(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        from account_app.sales_runtime.contracts import DecisionRejected
        scope=self.scope
        def decide(context):
            self.event('رسالة أحدث')
            raise DecisionRejected('decision_invalid_after_retry')
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,decide)
        result=entry.receive(store_id=scope.store,channel=scope.channel,account_id=scope.account,
                             sender_id=scope.sender,message_id='old',text='رسالة قديمة')
        self.assertEqual('superseded',result['action'])
        self.assertEqual('active',self.store.snapshot(scope)['data']['control_mode'])

    def test_writer_cannot_smuggle_repeated_phone_request_without_question_mark(self):
        from account_app.sales_runtime.copy_guard import validate_copy
        with self.assertRaisesRegex(ValueError,'contact.phone'):
            validate_copy([dict(type='text',text='بدلته للازرق بس دزيلي رقم الموبايل')],None)
        validate_copy([dict(type='text',text='صار عيوني بدلته للأزرق')],None)

    def test_writer_repairs_copy_without_reexecuting_selection(self):
        from account_app.sales_runtime.response import Responses
        test=self
        class Writer:
            repairs=0
            def __call__(self,context):return [dict(type='text',text='بس دزيلي رقم الموبايل')]
            def repair(self,context,previous,error):
                self.repairs+=1
                test.assertIn('contact.phone',error)
                return [dict(type='text',text='صار عيوني')]
        event=self.event('اهلا')
        outcome=self.runtime.turn(self.scope,event)
        writer=Writer()
        result=Responses(self.store,self.loader,writer).prepare(self.scope,event,outcome)
        self.assertEqual('queued',result['action'])
        self.assertEqual(1,writer.repairs)
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM sales_v2_outbox').fetchone()[0])


if __name__=='__main__':unittest.main()
