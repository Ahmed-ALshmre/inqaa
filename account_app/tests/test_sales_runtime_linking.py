"""Offline end-to-end tool routing using the application's canonical order schema."""
import json
from pathlib import Path
import re
import unittest
from unittest.mock import Mock

from account_app.tests import test_sales_runtime_foundation as foundation
from account_app.sales_runtime import Conflict, Scope
from account_app.sales_runtime.cart import Catalog
from account_app.sales_runtime.orders import Orders, migrate_tasks, CONFIRMATION
from account_app.sales_runtime.references import References
from account_app.sales_runtime.runtime import Runtime
from account_app.sales_runtime.gateway import DecisionGateway


class LinkingTests(unittest.TestCase):
    def setUp(self):
        foundation.RuntimeFoundationTests.setUp(self)
        # Read the CREATE statement from production rather than a parallel v2 orders table.
        source = (Path(__file__).parents[1] / 'app.py').read_text(encoding='utf8')
        ddl = re.search(r'CREATE TABLE IF NOT EXISTS orders \(.*?\);', source, re.S)[0]
        self.db.executescript(ddl)
        self.db.executescript(re.search(r'CREATE TABLE IF NOT EXISTS human_reviews \(.*?\);',source,re.S)[0])
        self.db.execute('ALTER TABLE human_reviews ADD COLUMN store_id TEXT')
        for name, typ in [('store_id', 'TEXT'), ('product_total', 'INTEGER'),
                          ('delivery_fee', 'INTEGER'), ('total_amount', 'INTEGER')]:
            self.db.execute(f'ALTER TABLE orders ADD COLUMN {name} {typ}')
        self.db.commit()
        migrate_tasks(self.db)
        self.products[0].update(stock='متوفر', ad_ids=['120251717777850095'])
        self.products[1].update(stock='متوفر')
        self.loader = lambda sid: Catalog(sid, self.products)
        self.orders = Orders(self.store, self.loader, lambda sid, province: 5000)
        self.plan = dict(intent='continue')
        self.decide = Mock(side_effect=lambda context: self.plan)
        self.runtime = Runtime(self.store, self.loader, self.orders, self.decide)

    tearDown = foundation.RuntimeFoundationTests.tearDown
    event = foundation.RuntimeFoundationTests.event
    version = foundation.RuntimeFoundationTests.version
    add = foundation.RuntimeFoundationTests.add

    def run_turn(self, text, **plan):
        self.plan = dict(intent='continue', **plan)
        event = self.event(text)
        return event, self.runtime.turn(self.scope, event)

    def select(self):
        return self.run_turn('ماروني قياس 44', changes=[dict(line_id='one',
                         values=dict(product_id='P1', color='ماروني', size='44'))])

    def prompt(self):
        self.store.enqueue_reply(self.scope, 'contacts', self.version(),
            [dict(type='text', purpose='checkout_contacts', text='دزيلي الرقم والعنوان حتى أثبتلج الحجز')])
        part = self.store.claim_reply(self.scope)
        self.store.settle_reply(self.scope, part['id'], 'accepted', 'remote-1')
        return self.runtime.accept_checkout_prompt(self.scope, 'ask', self.version(), part['id'])

    def test_direct_purchase_saves_actual_orders_and_tasks(self):
        self.select()
        event, result = self.run_turn('حجزيلي 07701234567 بغداد حي المنصور قرب المدرسة',
            contact=dict(phone='07701234567', address='بغداد حي المنصور قرب المدرسة'))
        self.assertEqual('booked', result['action'])
        row = self.db.execute('SELECT order_items,total_amount,store_id FROM orders').fetchone()
        self.assertEqual(('ماروني', '44'), tuple(json.loads(row[0])[0][f] for f in ('color','size')))
        self.assertEqual((17000, 'khuyoot'), row[1:])
        self.assertEqual(2, self.db.execute('SELECT COUNT(*) FROM sales_v2_order_tasks').fetchone()[0])
        confirmation = self.db.execute("SELECT payload FROM sales_v2_order_tasks WHERE kind='confirmation'").fetchone()[0]
        self.assertEqual(CONFIRMATION, json.loads(confirmation)['text'])
        calls = self.decide.call_count
        self.assertEqual(result, self.runtime.turn(self.scope, event))
        self.assertEqual(calls, self.decide.call_count)

    def test_scattered_contacts_after_accepted_prompt_book_without_extra_question(self):
        self.select(); self.prompt()
        self.run_turn('07701234567', contact=dict(phone='07701234567'))
        self.run_turn('بدلي الماروني ازرق', changes=[dict(line_id='one', values={'color':'أزرق'})])
        _, result = self.run_turn('بغداد حي المنصور قرب المدرسة', contact=dict(address='بغداد حي المنصور قرب المدرسة'))
        self.assertEqual('booked', result['action'])
        row = self.db.execute('SELECT order_items,phone FROM orders').fetchone()
        self.assertEqual('أزرق', json.loads(row[0])[0]['color'])
        self.assertEqual('07701234567', row[1])

    def test_price_inquiry_with_contacts_does_not_book(self):
        self.select()
        _, result = self.run_turn('07701234567 بغداد حي المنصور قرب المدرسة، واصلات شكد؟',
            contact=dict(phone='07701234567', address='بغداد حي المنصور قرب المدرسة'))
        self.assertEqual('continue', result['action'])
        self.assertIn('purchase_intent', result['readiness']['missing_fields'])

    def test_condition_blocks_until_resolved(self):
        self.select(); self.prompt()
        self.plan = dict(intent='conditional')
        self.runtime.turn(self.scope, self.event('اذا يوصل الخميس حجزيلي'))
        _, result = self.run_turn('07701234567 بغداد حي المنصور قرب المدرسة',
            contact=dict(phone='07701234567', address='بغداد حي المنصور قرب المدرسة'))
        self.assertEqual('continue', result['action'])
        self.assertTrue(result['readiness']['conditions'])

    def test_new_event_during_model_call_cannot_mutate_cart(self):
        event = self.event('ماروني 44')
        def decide(context):
            self.event('لا اريده ازرق')
            return dict(intent='continue', changes=[dict(line_id='one', values={'product_id':'P1'})])
        self.runtime.decide = decide
        with self.assertRaisesRegex(Conflict, 'state_version_changed'):
            self.runtime.turn(self.scope, event)
        self.assertEqual([], self.store.snapshot(self.scope)['data']['cart'])

    def test_model_cannot_choose_price_or_invent_contact(self):
        with self.assertRaises(ValueError):
            self.run_turn('ماروني', changes=[dict(line_id='one', values={'product_id':'P1','price':1})])
        with self.assertRaisesRegex(Conflict, 'phone_not_in_source'):
            self.run_turn('اهلا', contact={'phone':'07701234567'})

    def test_new_key_does_not_duplicate_booked_cart(self):
        self.test_direct_purchase_saves_actual_orders_and_tasks()
        result = self.orders.commit(self.scope, 'another-key', self.version())
        self.assertTrue(result['result']['already_booked'])
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        event = self.event('بدلي اللون')
        with self.assertRaisesRegex(Conflict, 'booked_cart_requires_order_change'):
            self.cart.change(self.scope, 'after', self.version(), event,
                             line_id='one', values={'color':'أزرق'})

    def test_atomic_rollback_includes_order_and_state(self):
        self.db.executescript("CREATE TRIGGER fail_task BEFORE INSERT ON sales_v2_order_tasks BEGIN SELECT RAISE(ABORT,'failure'); END;")
        self.select()
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            self.run_turn('حجزيلي 07701234567 بغداد حي المنصور قرب المدرسة',
                contact=dict(phone='07701234567', address='بغداد حي المنصور قرب المدرسة'))
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        self.assertNotIn('order_id', self.store.snapshot(self.scope)['data'])

    def test_catalog_reloaded_before_commit(self):
        self.select()
        def change_catalog(context):
            self.products[0]['stock'] = 'نفذ'
            return dict(intent='buy', contact=dict(phone='07701234567',address='بغداد حي المنصور قرب المدرسة'))
        self.runtime.decide = change_catalog
        with self.assertRaisesRegex(Conflict, 'checkout_stock_not_confirmed'):
            self.runtime.turn(self.scope, self.event('حجزيلي 07701234567 بغداد حي المنصور قرب المدرسة'))

    def test_ad_known_unknown_ambiguous_and_cross_store(self):
        refs = References(self.store, self.loader('khuyoot'))
        self.assertEqual('P1', refs.resolve(self.scope, ad={'ad_id':'120251717777850095'})['product_id'])
        self.assertEqual('unknown', refs.resolve(self.scope, ad={'ad_id':'123'})['status'])
        self.products[1]['ad_ids'] = ['120251717777850095']
        self.assertEqual('ambiguous', References(self.store,self.loader('khuyoot')).resolve(
            self.scope,ad={'ad_id':'120251717777850095'})['status'])
        with self.assertRaises(Conflict):
            refs.resolve(Scope('other','chatwoot','a','s'),ad={'ad_id':'123'})

    def test_registered_product_link_resolves_before_an_unmapped_referral(self):
        link = 'https://catalog.example.test/robes?item=one'
        self.products[0]['ref'] = link
        refs = References(self.store, self.loader('khuyoot'))
        self.assertEqual(dict(status='known',source='product_link',product_id='P1'),
                         refs.resolve(self.scope, ad={'ad_id':'unmapped'}, text=f'أريد هذا {link}#photo'))
        self.assertEqual('P1',refs.resolve(self.scope,ad={'ad_id':'unmapped','source_url':link})['product_id'])
        self.assertEqual('unknown',refs.resolve(self.scope,text=link.replace('item=one','item=two'))['status'])
        self.products[1]['ref'] = link
        self.assertEqual('ambiguous',References(self.store,self.loader('khuyoot')).resolve(
            self.scope,text=link)['status'])

    def test_ordinal_reference_requires_accepted_images(self):
        self.event('صور')
        choices = [dict(product_id='P1', color='ماروني'),dict(product_id='P1',color='أزرق')]
        self.store.enqueue_reply(self.scope,'photos',self.version(),[dict(type='image',**c) for c in choices])
        ids = []
        for i in range(2):
            part = self.store.claim_reply(self.scope); ids.append(part['id'])
            self.store.settle_reply(self.scope, part['id'], 'accepted', str(i))
        refs = References(self.store,self.loader('khuyoot'))
        refs.record_accepted(self.scope,'present',self.version(),'album-1',ids,choices)
        self.assertEqual('أزرق',refs.resolve(self.scope,presentation='album-1',position=2)['color'])
        with self.assertRaises(Conflict):
            refs.resolve(self.scope,presentation='album-1',position=3)

    def test_failed_image_cannot_be_presented(self):
        self.event('صورة')
        self.store.enqueue_reply(self.scope,'photo',self.version(),[dict(type='image',product_id='P1')])
        part=self.store.claim_reply(self.scope)
        self.store.settle_reply(self.scope,part['id'],'unknown')
        with self.assertRaises(Conflict):
            References(self.store,self.loader('khuyoot')).record_accepted(self.scope,'p',self.version(),
                'album',[part['id']],[dict(product_id='P1')])

    def test_gateway_limits_invalid_output_and_does_not_retry_billing(self):
        response=Mock(status_code=200)
        response.json.return_value={'choices':[{'message':{'content':'invalid'}}]}
        post=Mock(return_value=response)
        with self.assertRaisesRegex(ValueError,'decision_invalid_after_retry'):
            DecisionGateway('configured-model','fake-key',post)({})
        self.assertEqual(2,post.call_count)
        post.reset_mock();response.status_code=402
        with self.assertRaisesRegex(RuntimeError,'http_402'):
            DecisionGateway('configured-model','fake-key',post)({})
        self.assertEqual(1,post.call_count)

    def test_confirmation_timeout_is_not_resent(self):
        from account_app.sales_runtime.delivery import deliver_confirmation
        self.test_direct_purchase_saves_actual_orders_and_tasks()
        task=self.db.execute("SELECT id FROM sales_v2_order_tasks WHERE kind='confirmation'").fetchone()[0]
        send=Mock(side_effect=TimeoutError())
        self.assertEqual('unknown',deliver_confirmation(self.store,self.scope,task,send,lambda s:True)['status'])
        self.assertEqual('unknown',deliver_confirmation(self.store,self.scope,task,send,lambda s:True)['status'])
        self.assertEqual(1,send.call_count)

    def test_confirmation_accepted_receipt_and_disabled_store(self):
        from account_app.sales_runtime.delivery import deliver_confirmation
        self.test_direct_purchase_saves_actual_orders_and_tasks()
        task=self.db.execute("SELECT id FROM sales_v2_order_tasks WHERE kind='confirmation'").fetchone()[0]
        send=Mock(return_value=dict(accepted=True,provider_id='remote-77'))
        self.assertEqual('paused',deliver_confirmation(self.store,self.scope,task,send,lambda s:False)['status'])
        send.assert_not_called()
        self.assertEqual('accepted',deliver_confirmation(self.store,self.scope,task,send,lambda s:True)['status'])
        self.assertEqual('remote-77',self.db.execute('SELECT provider_id FROM sales_v2_order_tasks WHERE id=?',(task,)).fetchone()[0])

    def test_fulfillment_transfer_is_local_atomic_and_replayable(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        from unittest.mock import patch
        self.test_direct_purchase_saves_actual_orders_and_tasks()
        entry=Entrypoint(self.db,lambda sid:self.products,lambda sid,p:5000,self.decide)
        task=self.db.execute("SELECT id FROM sales_v2_order_tasks WHERE kind='fulfillment'").fetchone()[0]
        with patch('account_app.menger.enqueue') as enqueue:
            result=entry.prepare_fulfillment(self.scope,task)
            self.assertFalse(result['replayed'])
            self.assertTrue(entry.prepare_fulfillment(self.scope,task)['replayed'])
            enqueue.assert_called_once()

    def test_sticker_does_not_enter_media_adapter_and_staff_takes_over(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        entry=Entrypoint(self.db,lambda sid:self.products,lambda sid,p:5000,self.decide)
        args=dict(store_id='khuyoot',channel='zernio',account_id='a',sender_id='test-zn')
        self.assertEqual('continue',entry.receive(**args,message_id='sticker',media_kind='sticker')['action'])
        self.assertEqual('needs_media_adapter',entry.receive(**args,message_id='image',media_kind='image')['action'])
        entry.receive(**args,message_id='staff',text='هلا',actor='staff')
        self.assertEqual('human',entry.receive(**args,message_id='next',text='مرحبا')['action'])

    def test_raw_condition_overrides_incorrect_model_buy(self):
        self.select();self.prompt()
        self.plan=dict(intent='buy',contact=dict(phone='07701234567',address='بغداد حي المنصور قرب المدرسة'))
        result=self.runtime.turn(self.scope,self.event('اذا الخميس حجزيلي 07701234567 بغداد حي المنصور قرب المدرسة'))
        self.assertEqual('continue',result['action'])
        self.assertTrue(result['readiness']['conditions'])

    def test_writer_receives_grounded_facts_and_drops_known_question(self):
        from account_app.sales_runtime.response import Responses
        self.products[0]['fabric']='قديفة شتوي'
        self.plan=dict(intent='continue',changes=[dict(line_id='one',values=dict(product_id='P1',color='ماروني',size='44'))],
                       answer=dict(facts=[dict(product_id='P1',field='fabric')],question_field='cart.one.size'))
        event=self.event('قماشه شنو؟ ماروني 44')
        outcome=self.runtime.turn(self.scope,event)
        writer=Mock(return_value=[dict(type='text',text='إي حبيبتي قماشه قديفة شتوي')])
        responses=Responses(self.store,self.loader,writer)
        self.assertEqual('queued',responses.prepare(self.scope,event,outcome)['action'])
        context=writer.call_args.args[0]
        self.assertIsNone(context['question_field'])
        self.assertEqual('قديفة شتوي',context['facts'][0]['value'])
        responses.prepare(self.scope,event,outcome)
        writer.assert_called_once()

    def test_missing_fact_does_not_generate_customer_reply(self):
        from account_app.sales_runtime.response import Responses
        self.select()
        self.plan=dict(intent='continue',answer=dict(facts=[dict(product_id='P1',field='fabric')]))
        event=self.event('بي ليكرا؟')
        outcome=self.runtime.turn(self.scope,event)
        writer=Mock()
        self.assertEqual('missing_fact',Responses(self.store,self.loader,writer).prepare(self.scope,event,outcome)['action'])
        writer.assert_not_called()

    def test_image_match_enters_same_decision_state_without_auto_selection(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        media=Mock(return_value=dict(status='matched',product_ids=['P1']))
        entry=Entrypoint(self.db,lambda sid:self.products,lambda sid,p:5000,self.decide,media=media)
        args=dict(store_id='khuyoot',channel='zernio',account_id='a',sender_id='test-photo',
                  message_id='image',media_kind='image',media_ref='catalog-test-image')
        result=entry.receive(**args)
        self.assertEqual('continue',result['action'])
        context=self.decide.call_args.args[0]
        self.assertEqual(['P1'],next(iter(context['state']['media_observations'].values()))['product_ids'])
        self.assertEqual([],context['state']['cart'])
        entry.receive(**args)
        media.assert_called_once()

    def test_wrong_store_media_match_rejected_and_sticker_skips_recognizer(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        media=Mock(return_value=dict(status='matched',product_ids=['P9']))
        entry=Entrypoint(self.db,lambda sid:self.products,lambda sid,p:5000,self.decide,media=media)
        args=dict(store_id='khuyoot',channel='zernio',account_id='a',sender_id='photo')
        entry.receive(**args,message_id='sticker',media_kind='sticker')
        media.assert_not_called()
        with self.assertRaises(Conflict):
            entry.receive(**args,message_id='image',media_kind='image',media_ref='test')

    def test_confirmation_crash_recovery_and_cancelled_order(self):
        from account_app.sales_runtime.delivery import deliver_confirmation,recover_confirmations
        self.test_direct_purchase_saves_actual_orders_and_tasks()
        task=self.db.execute("SELECT id FROM sales_v2_order_tasks WHERE kind='confirmation'").fetchone()[0]
        self.db.execute("UPDATE sales_v2_order_tasks SET status='sending' WHERE id=?",(task,));self.db.commit()
        recover_confirmations(self.store)
        send=Mock()
        self.assertEqual('unknown',deliver_confirmation(self.store,self.scope,task,send,lambda s:True)['status'])
        send.assert_not_called()

    def test_completed_turn_and_confirmation_survive_reopen(self):
        import sqlite3
        from account_app.sales_runtime import Store
        self.test_direct_purchase_saves_actual_orders_and_tasks()
        event=self.db.execute("SELECT MAX(id) FROM sales_v2_events").fetchone()[0]
        self.db.close();self.db=sqlite3.connect(self.path);self.store=Store(self.db)
        runtime=Runtime(self.store,self.loader,Orders(self.store,self.loader,lambda s,p:5000),Mock(side_effect=AssertionError('must not call model')))
        self.assertEqual('booked',runtime.turn(self.scope,event)['action'])
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_actual_menger_queue_accepts_committed_payload_without_network(self):
        import sqlite3
        from account_app.sales_runtime.entrypoint import Entrypoint
        self.test_direct_purchase_saves_actual_orders_and_tasks()
        self.db.row_factory=sqlite3.Row
        task=self.db.execute("SELECT id FROM sales_v2_order_tasks WHERE kind='fulfillment'").fetchone()[0]
        entry=Entrypoint(self.db,lambda sid:self.products,lambda sid,p:5000,self.decide)
        entry.prepare_fulfillment(self.scope,task)
        queued=json.loads(self.db.execute('SELECT payload FROM menger_deliveries').fetchone()[0])
        self.assertEqual(17000,queued['total_price'])
        self.assertEqual('ماروني',queued['items'][0]['color'])
        self.assertEqual('ai',queued['attribution']['booking_actor'])

    def test_staff_order_edit_blocks_old_fulfillment_task(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        self.test_direct_purchase_saves_actual_orders_and_tasks()
        self.db.execute("UPDATE orders SET address='عنوان جديد'");self.db.commit()
        task=self.db.execute("SELECT id FROM sales_v2_order_tasks WHERE kind='fulfillment'").fetchone()[0]
        entry=Entrypoint(self.db,lambda sid:self.products,lambda sid,p:5000,self.decide)
        with self.assertRaisesRegex(Conflict,'order_changed_before_fulfillment'):
            entry.prepare_fulfillment(self.scope,task)

    def test_quantity_limit_is_aggregated_across_cart_lines(self):
        self.products[0]['stock_quantity']=1
        self.select()
        self.run_turn('واحدة ازرق همين',changes=[dict(new_line=True,values=dict(product_id='P1',color='أزرق',size='44'))])
        with self.assertRaisesRegex(Conflict,'checkout_quantity_exceeds_stock'):
            self.run_turn('حجزيلي 07701234567 بغداد حي المنصور قرب المدرسة',
                contact=dict(phone='07701234567',address='بغداد حي المنصور قرب المدرسة'))

    def test_unknown_fact_creates_silent_existing_review_and_question(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        source=(Path(__file__).parents[1]/'app.py').read_text(encoding='utf8')
        self.db.executescript(re.search(r'CREATE TABLE IF NOT EXISTS human_reviews \(.*?\);',source,re.S)[0])
        self.db.commit()
        self.plan=dict(intent='continue',answer=dict(facts=[dict(product_id='P1',field='fabric')]))
        writer=Mock()
        backup=Mock()
        backup.decide.return_value=dict(intent='continue',answer=dict(facts=[dict(product_id='P1',field='fabric')]))
        entry=Entrypoint(self.db,lambda sid:self.products,lambda s,p:5000,self.decide,writer=writer,backup=backup)
        entry.migrate()
        args=dict(store_id='khuyoot',channel='chatwoot',account_id='a',sender_id='missing',
                  ad={'ad_id':'120251717777850095'})
        result=entry.receive(**args,message_id='m',text='شنو القماش؟')
        self.assertEqual('human',result['action']);self.assertEqual('',result['reply'])
        writer.assert_not_called()
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM product_questions').fetchone()[0])
        self.assertEqual('human',entry.receive(**args,message_id='m',text='شنو القماش؟')['action'])
        backup.decide.assert_called_once()
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])


if __name__ == '__main__':
    unittest.main()
