"""Offline end-to-end tool routing using the application's canonical order schema."""
import json
from pathlib import Path
import re
import unittest
from unittest.mock import Mock, patch

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

    def test_variant_last_piece_is_atomic_and_cancel_returns_only_its_stock(self):
        from account_app import variant_inventory
        from account_app.sales_runtime.entrypoint import Entrypoint
        entry = Entrypoint(self.db, lambda _: self.products, lambda *_: 5000, self.decide)
        variant_inventory.init(self.db)
        rows = [dict(color='ماروني', size='44', quantity=1), dict(color='أزرق', size='44', quantity=3)]
        variant_inventory.replace(self.db, self.scope.store, 'P1', rows)
        self.db.commit()
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account)
        self.plan = dict(intent='buy', changes=[dict(values=dict(product_id='P1', color='ماروني', size='44'))])
        message = 'احجزيلي ماروني قياس 44\nرقمي 07701234567\nبغداد حي المنصور قرب المدرسة'
        first = entry.receive(**target, sender_id='variant-first', message_id='select', text=message)
        self.assertEqual('booked', first['action'], first)
        self.assertEqual(first['order_id'],
                         entry.receive(**target, sender_id='variant-first', message_id='select', text=message)['order_id'])
        second = entry.receive(**target, sender_id='variant-second', message_id='select', text=message)
        self.assertEqual('continue', second['action'])
        self.assertEqual('ماروني', second['variant_stock_feedback']['requested']['color'])
        self.assertEqual(1, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        self.assertEqual([3, 0], [row['quantity'] for row in variant_inventory.rows_by_product(
            self.db, self.scope.store)['P1']])
        cancelled = entry.receive(**target, sender_id='variant-first', message_id='cancel', text='الغي الطلب')
        self.assertEqual('cancelled', cancelled['action'])
        self.assertEqual([3, 1], [row['quantity'] for row in variant_inventory.rows_by_product(
            self.db, self.scope.store)['P1']])

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

    def test_observer_uses_scoped_archive_when_the_attachment_link_expires(self):
        from account_app.sales_runtime.preview import ImageObserver
        image = 'data:image/png;base64,dGVzdA=='
        url = 'https://cdn.example/expired.png'
        fetch = Mock(return_value=None)
        match = Mock(return_value=dict(product_found=True, product_id='P1'))
        archived = Mock(return_value=image)
        observer = ImageObserver(fetch, match, archived)
        with patch('account_app.sales_runtime.preview.inbound_media.trusted_source', return_value=True):
            result = observer(self.scope, 'image', [url], self.products)
        self.assertEqual(dict(status='matched', product_ids=['P1']), result)
        archived.assert_called_once_with(self.scope, url)
        match.assert_called_once_with(image, self.products)
        fetch.assert_not_called()

    def test_five_shared_post_previews_match_once_per_distinct_image(self):
        from account_app.sales_runtime.preview import ImageObserver
        urls = [f'https://cdn.example/preview-{index}.png' for index in range(5)]
        image = (b'preview', 'image', '', 'image/png')
        different = (b'different-preview', 'image', '', 'image/png')
        fetch = Mock(side_effect=[image, image, different, image, image])
        match = Mock(return_value=dict(product_found=True, product_id='P1'))
        observer = ImageObserver(fetch, match)
        with patch('account_app.sales_runtime.preview.inbound_media.trusted_source', return_value=True):
            self.assertEqual(dict(status='matched', product_ids=['P1']), observer(self.scope, 'image', urls, self.products))
            self.assertEqual(2, match.call_count)
            fetch.side_effect = [image, different]
            match.side_effect = [dict(product_found=True, product_id='P1'), dict(product_found=True, product_id='B1')]
            self.assertEqual(dict(status='ambiguous', product_ids=['B1', 'P1'], verified_pair=True),
                             observer(self.scope, 'image', urls[:2], self.products))

    def test_a_failed_attachment_does_not_hide_the_verified_image_in_the_batch(self):
        from account_app.sales_runtime.preview import ImageObserver, media_input
        url = 'https://cdn.example/picture.png'
        self.assertEqual(('image', [url]), media_input([
            dict(type='image', payload={'url': url}), dict(type='video')]))
        attachment = (b'synthetic-image', 'image', '', 'image/png')
        fetch = Mock(side_effect=[None, attachment])
        match = Mock(return_value=dict(product_found=True, product_id='P1'))
        observer = ImageObserver(fetch, match)
        with patch('account_app.sales_runtime.preview.inbound_media.trusted_source', return_value=True):
            result = observer(self.scope, 'image', ['https://cdn.example/failed.png', url], self.products)
        self.assertEqual(dict(status='ambiguous', product_ids=['P1'], latest_match='P1'), result)
        match.assert_called_once()

    def test_album_comparison_recognizes_one_product_across_distinct_colors(self):
        from account_app.sales_runtime.preview import ImageObserver
        urls = [f'https://cdn.example/dress-{index}.jpg' for index in range(4)]
        fetch = Mock(side_effect=[(f'color-{i}'.encode(), 'image', '', 'image/jpeg') for i in range(4)])
        match = Mock(side_effect=[dict(product_found=False), dict(product_found=True, product_id='P1'),
                                  dict(product_found=False), dict(product_found=False)])
        album = Mock(return_value=dict(product_found=True, product_id='P1'))
        observer = ImageObserver(fetch, match, match_album=album)
        with patch('account_app.sales_runtime.preview.inbound_media.trusted_source', return_value=True):
            self.assertEqual(dict(status='matched', product_ids=['P1']), observer(self.scope, 'image', urls, self.products))
        images, products = album.call_args.args
        self.assertEqual(4, len(images))
        self.assertEqual(self.products, products)

    def test_colloquial_price_and_delivery_use_verified_amounts_without_a_cart(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        self.products[0].update(price='12000', offer='قطعتين ب 20000')
        writer = Mock(return_value=[dict(type='text',
            text='سعر القطعة {{unit_price}} والقطعتين بالعرض {{offer_total}}. والتوصيل {{delivery_fee}}.')])
        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, p: 5000, self.decide,
                           media=lambda *args: dict(status='ambiguous', product_ids=['P1'], latest_match='P1'),
                           writer=writer, policy_loader=lambda _sid: {'delivery_fee': {'بغداد': 5000, 'أخرى': 5000}})
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                      sender_id=self.scope.sender)
        result = entry.receive(**target, message_id='price', media_kind='image', media_ref=['one', 'two'],
                               text='جكد سعر\nوالتوصيل')
        self.assertEqual('continue', result['action'], result)
        context = writer.call_args.args[0]
        self.assertEqual({'unit_price': 12000, 'offer_total': 20000, 'delivery_fee': 5000}, context['amounts'])
        self.assertEqual('P1', context['verified_photo_product_id'])
        self.assertEqual([], entry.store.snapshot(self.scope)['data']['cart'])

    def test_wholesale_question_on_known_photo_records_only_the_missing_wholesale_fact(self):
        from sqlite3 import Row
        from account_app import product_questions
        from account_app.sales_runtime.entrypoint import Entrypoint
        from account_app.sales_runtime.facts import product_fact
        writer = Mock()
        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, p: 5000, self.decide,
                           media=lambda *args: dict(status='matched', product_ids=['P1']), writer=writer)
        entry.migrate()
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                      sender_id=self.scope.sender)
        result = entry.receive(**target, message_id='wholesale', media_kind='image', media_ref=['one'],
                               text='نص درزن بيش')
        self.assertEqual('human', result['action'], result)
        writer.assert_not_called()
        self.db.row_factory = Row
        row = self.db.execute('SELECT id,topic FROM product_questions').fetchone()
        self.assertEqual('wholesale', row['topic'])
        self.assertEqual([], entry.store.snapshot(self.scope)['data']['cart'])
        product_questions.save_answer(self.db, self.scope.store, row['id'], 'نص درزن 6 قطع بـ48000',
                                      'اختبار', '2026-11-14T12:00:00+03:00')
        product = entry.load_catalog(self.scope.store).get('P1')
        self.assertEqual('نص درزن 6 قطع بـ48000', product_fact(product, 'wholesale')['value'])
        self.assertEqual('12000', product['price'])

    def test_price_question_uses_last_verified_photo_in_an_incomplete_batch(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        self.products[0].update(price='15000')
        media = Mock(return_value=dict(status='ambiguous', product_ids=['P1'], latest_match='P1'))
        writer = Mock(return_value=[dict(type='text', text='القطعة اللي بآخر صورة بـ{{unit_price}}.')])
        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, p: 5000, self.decide,
                           media=media, writer=writer)
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                      sender_id=self.scope.sender)
        result = entry.receive(**target, message_id='batch', media_kind='image', media_ref=['old', 'recent'],
                               text='وهذا شكد؟')
        self.assertEqual('continue', result['action'], result)
        self.assertEqual('القطعة اللي بآخر صورة بـ15 ألف.', entry.store.claim_reply(self.scope)['payload']['text'])
        self.assertEqual('P1', writer.call_args.args[0]['verified_photo_product_id'])
        self.assertEqual([], entry.store.snapshot(self.scope)['data']['cart'])

    def test_photo_of_different_product_answers_its_price_and_preserves_checkout(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        other = dict(self.products[0], product_id='P2', price='27000', ad_ids=[])
        self.products.append(other)
        self.run_turn('أريد ماروني قياس 44', changes=[
            dict(values=dict(product_id='P1', color='ماروني', size='44'))])
        original = self.store.snapshot(self.scope)['data']['cart']
        self.plan = dict(intent='buy', changes=[dict(line_ref=original[0]['line_id'],
                         values=dict(product_id='P2', color='أزرق', size='44'))])
        writer = Mock(side_effect=[
            [dict(type='text', text='سعر هاي القطعة {{unit_price}}.')],
            [dict(type='text', text='باقي بس العنوان عيني حتى أثبتلج الطلب.')],
        ])
        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, p: 5000, self.decide,
                           media=lambda *args: dict(status='matched', product_ids=['P2']), writer=writer)
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                      sender_id=self.scope.sender)
        photo = entry.receive(**target, message_id='another-photo', media_kind='image',
                              media_ref=['photo'], text='وهذا شكد؟')
        self.assertEqual('continue', photo['action'], photo)
        self.assertEqual('P2', self.decide.call_args.args[0]['reference']['product_id'])
        context = writer.call_args.args[0]
        self.assertEqual(27000, context['amounts']['unit_price'])
        self.assertIsNone(context['next_step']['field'])
        self.assertEqual(original, self.store.snapshot(self.scope)['data']['cart'])
        self.assertIsNone(self.db.execute('SELECT id FROM orders').fetchone())
        phone = entry.receive(**target, message_id='phone', text='رقمي 07701234567')
        self.assertEqual('continue', phone['action'], phone)
        booked = entry.receive(**target, message_id='address', text='بغداد حي المنصور قرب المدرسة')
        self.assertEqual('booked', booked['action'], booked)
        order = self.db.execute('SELECT order_items FROM orders').fetchone()
        self.assertEqual(['P1'], [line['product_id'] for line in json.loads(order[0])])

    def test_buying_both_photos_requires_two_verified_products_in_the_cart(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        other = dict(self.products[0], product_id='P2', ad_ids=[])
        self.products.append(other)
        media = Mock(side_effect=[dict(status='matched', product_ids=['P1']),
                                  dict(status='matched', product_ids=['P2'])])
        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, p: 5000, self.decide, media=media)
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                      sender_id=self.scope.sender)
        for mid in ('one', 'two'):
            entry.receive(**target, message_id=mid, media_kind='image', media_ref=[mid])
        self.plan = dict(intent='buy', changes=[dict(values=dict(product_id='P2', quantity=2, size='44'))])
        with self.assertRaisesRegex(Conflict, 'photo_pair_requires_both_verified_products'):
            entry.receive(**target, message_id='purchase', text='عيوني اريد هذن ثنينهن')
        self.assertEqual(['P1', 'P2'], self.decide.call_args.args[0]['requested_photo_products'])
        self.assertFalse(self.store.snapshot(self.scope)['data']['cart'])
        self.assertIsNone(self.db.execute('SELECT id FROM orders').fetchone())

    def test_buying_both_verified_images_in_one_message_can_book_with_saved_contact(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        self.products.append(dict(self.products[0], product_id='P2', ad_ids=[]))
        media = Mock(return_value=dict(status='ambiguous', product_ids=['P1', 'P2'], verified_pair=True))
        entry = Entrypoint(self.db, lambda sid: self.products, lambda sid, p: 5000, self.decide, media=media)
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                      sender_id=self.scope.sender)
        self.plan = dict(intent='buy', changes=[dict(values=dict(product_id=pid, size='44', color='ماروني'))
                                                for pid in ('P1', 'P2')])
        result = entry.receive(**target, message_id='pair', media_kind='image', media_ref=['one', 'two'],
                               text='اريد هذني الثنين\nرقمي 07701234567\nبغداد حي المنصور قرب المدرسة')
        self.assertEqual('booked', result['action'], result)
        result_rows = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])
        self.assertCountEqual(['P1', 'P2'], [row['product_id'] for row in result_rows])

    def test_red_caption_on_verified_maroon_photo_preserves_size_and_books_once(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        writer = Mock(side_effect=[
            [dict(type='text', text='إي عيني متوفر، دزيلي رقم الموبايل والعنوان حتى أثبتلج.')],
            [dict(type='text', text='حبيبتي، باقي بس رقم الموبايل حتى أثبتلج.')],
        ])
        media = Mock(return_value=dict(status='matched', product_ids=['P1'], observed_color='ماروني'))
        entry = Entrypoint(self.db, lambda store: self.products, lambda store, province: 5000,
                           self.decide, media=media, writer=writer)
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                      sender_id=self.scope.sender)
        self.plan = dict(intent='buy', changes=[dict(values=dict(product_id='P1', color='أحمر', size='38'))])
        entry.receive(**target, message_id='caption', text='أريد الأحمر قياس ٤٤', media_kind='image', media_ref='fixture')
        self.assertEqual('ماروني', self.decide.call_args.args[0]['reference']['available_photo_color'])
        self.assertEqual('ماروني', writer.call_args.args[0]['verified_photo_choice']['color'])
        self.assertEqual('contact.details', writer.call_args.args[0]['question_field'])
        self.plan = dict(intent='continue')
        address = entry.receive(**target, message_id='district', text='بغداد حي الاختبار قرب المدرسة')
        self.assertEqual('queued', address['reply']['action'])
        self.assertEqual('contact.phone', writer.call_args.args[0]['question_field'])
        number = dict(message_id='phone', text='+٩٦٤ ٧٧٠\u200f١٢٣ ٤٥٦٧')
        self.assertEqual('booked', entry.receive(**target, **number)['action'])
        self.assertEqual('booked', entry.receive(**target, **number)['action'])
        self.assertEqual([('ماروني', '44', '07701234567')], [tuple(row) for row in self.db.execute(
            'SELECT color,size,phone FROM orders')])

    def test_adjacent_photo_availability_uses_maroon_without_replacing_confirmed_blue(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        self.run_turn('أريد أزرق قياس 44', changes=[dict(values=dict(product_id='P1', color='أزرق', size='44'))])
        media = Mock(return_value=dict(status='matched', product_ids=['P1'], observed_color='ماروني'))
        entry = Entrypoint(self.db, lambda store: self.products, lambda store, province: 5000, self.decide, media=media)
        target = dict(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                      sender_id=self.scope.sender)
        self.plan = dict(intent='continue')
        entry.receive(**target, message_id='image', media_kind='image', media_ref='fixture')
        self.plan = dict(intent='continue', changes=[dict(values=dict(product_id='P1', color='أحمر'))])
        result = entry.receive(**target, message_id='availability', text='الأحمر متوفر؟')
        self.assertEqual('ماروني', result['photo_choice']['color'])
        self.assertEqual('أزرق', self.store.snapshot(self.scope)['data']['cart'][0]['color'])
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_photo_reference_does_not_override_an_unrelated_color(self):
        from account_app.sales_runtime.entrypoint import Entrypoint
        self.plan = dict(intent='buy', changes=[dict(values=dict(product_id='P1', color='أزرق', size='44'))])
        media = Mock(return_value=dict(status='matched', product_ids=['P1'], observed_color='ماروني'))
        entry = Entrypoint(self.db, lambda store: self.products, lambda store, province: 5000, self.decide, media=media)
        result = entry.receive(store_id='khuyoot', channel='zernio', account_id='a', sender_id='different-color',
                               message_id='caption', text='أريد الأزرق قياس 44', media_kind='image', media_ref='fixture')
        self.assertIsNone(result['photo_choice'])
        state = self.store.snapshot(Scope('khuyoot', 'zernio', 'a', 'different-color'))['data']
        self.assertEqual('أزرق', state['cart'][0]['color'])

    def test_customer_size_correction_wins_even_if_old_size_is_in_stock(self):
        self.select()
        self.run_turn('لا قياسي ٤٠', changes=[dict(values=dict(product_id='P1', size='44'))])
        self.assertEqual('40', self.store.snapshot(self.scope)['data']['cart'][0]['size'])
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

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
        backup.advise.return_value=dict(guidance='راجعي قماش القطعة المختارة.', reason='test', changes=[])
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
        backup.advise.assert_called_once()
        self.assertEqual(1,self.db.execute('SELECT COUNT(*) FROM human_reviews').fetchone()[0])


if __name__ == '__main__':
    unittest.main()
