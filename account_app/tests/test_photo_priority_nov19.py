"""Photos remain evidence, not guesses; burst timing uses a controlled clock."""
import json
import sqlite3
import unittest
from unittest.mock import Mock, patch

from account_app import ai_jobs, sales_progress
from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.tests import test_sales_runtime_linking as linking


class PhotoPriorityTests(unittest.TestCase):
    setUp = linking.LinkingTests.setUp
    tearDown = linking.LinkingTests.tearDown
    event = linking.LinkingTests.event
    version = linking.LinkingTests.version
    select = linking.LinkingTests.select
    run_turn = linking.LinkingTests.run_turn

    def entry(self, observation, writer):
        self.products.append(dict(self.products[0], product_id='P2', fabric='دبل إيطالي'))
        self.products[0]['fabric'] = 'قديفة'
        return Entrypoint(self.db, lambda _: self.products, lambda *_: 5000, self.decide,
                          writer=writer, media=lambda *_: observation)

    def receive(self, entry, **kwargs):
        return entry.receive(store_id=self.scope.store, channel=self.scope.channel, account_id=self.scope.account,
                             sender_id=self.scope.sender, message_id=str(self.version()), **kwargs)

    def test_two_verified_models_keep_the_latest_factual_reference_and_the_old_cart(self):
        self.select()
        writer = Mock(return_value=[dict(type='text', text='قماشه دبل إيطالي.')])
        entry = self.entry(dict(status='ambiguous', product_ids=['P1','P2'], verified_pair=True,
            positions=[dict(product_id='P1'),dict(product_id='P2')], latest_match='P2'), writer)
        self.decide.reset_mock()
        result = self.receive(entry, text='شنو قماشها؟', media_kind='image', media_ref=['image-1','image-2'])
        self.assertEqual('continue', result['action'])
        context = writer.call_args.args[0]
        self.assertEqual('دبل إيطالي', context['product_facts']['fabric'])
        self.assertEqual('P2', context['verified_photo_product_id'])
        self.decide.assert_not_called()
        self.assertEqual(['P1'], [line['product_id'] for line in entry.store.snapshot(self.scope)['data']['cart']])
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])

    def test_image_arriving_after_our_photo_request_answers_the_pending_fabric_question(self):
        self.event('شنو قماشها؟')
        self.event('دزيلي صورة الموديل حتى أشوفه', actor='staff')
        writer = Mock(return_value=[dict(type='text', text='قماشه دبل إيطالي.')])
        entry = self.entry(dict(status='ambiguous', product_ids=['P1','P2'],
            positions=[dict(product_id='P1'),dict(product_id='P2')], latest_match='P2'), writer)
        result = self.receive(entry, media_kind='image', media_ref=['image-1','image-2'])
        self.assertEqual('continue', result['action'])
        context = writer.call_args.args[0]
        self.assertEqual('شنو قماشها؟', context['customer_message'])
        self.assertEqual(['fabric'], context['requested_fields'])
        self.assertEqual('دبل إيطالي', context['product_facts']['fabric'])

    def test_unresolved_last_photo_cannot_reuse_the_previous_known_identity(self):
        entry = self.entry(dict(status='ambiguous', product_ids=['P1','P2'],
            positions=[dict(product_id='P1'),dict(product_id='P2'),{}], latest_match='P2'),
            Mock(return_value=[dict(type='text', text='ممكن صورة القطعة المقصودة وحدها؟')]))
        self.receive(entry, media_kind='image', media_ref=['image-1','image-2','unknown-image'])
        observations = entry.store.snapshot(self.scope)['data']['media_observations']
        self.assertNotIn('latest_match', list(observations.values())[-1])

    def test_one_verified_photo_needs_a_writer_but_no_second_identity_decision(self):
        writer = Mock(return_value=[dict(type='text', text='سعره {{unit_price}}.')])
        entry = self.entry(dict(status='matched', product_ids=['P1']), writer)
        self.receive(entry, media_kind='image', media_ref=['image-1'])
        self.decide.assert_not_called()
        self.assertEqual(1, writer.call_count)
        self.assertEqual([], entry.store.snapshot(self.scope)['data']['cart'])

    def test_single_matched_photo_recovers_the_question_after_staff_asked_for_a_photo(self):
        self.event('شنو قماشها؟')
        self.event('دزيلي صورة الموديل حتى أشوفه', actor='staff')
        writer = Mock(return_value=[dict(type='text', text='قماشه قديفة.')])
        entry = self.entry(dict(status='matched', product_ids=['P1']), writer)
        self.receive(entry, media_kind='image', media_ref=['image-1'])
        context = writer.call_args.args[0]
        self.assertEqual('شنو قماشها؟', context['customer_message'])
        self.assertEqual('قديفة', context['product_facts']['fabric'])
        self.assertNotIn('price', context['product_facts'])

    def test_new_photo_does_not_repeat_a_fabric_question_already_answered(self):
        self.event('شنو قماشها؟')
        self.event('قماشه قديفة عيوني', actor='staff')
        writer = Mock(return_value=[dict(type='text', text='سعره {{unit_price}}.')])
        entry = self.entry(dict(status='matched', product_ids=['P1']), writer)
        self.receive(entry, media_kind='image', media_ref=['image-1'])
        self.assertNotEqual('شنو قماشها؟', writer.call_args.args[0]['customer_message'])

    def test_hidden_photo_identity_is_not_replaced_by_an_available_product(self):
        self.products[0]['status'] = 'hidden'
        writer = Mock(return_value=[dict(type='text', text='هالقطعة مو متوفرة هسه.')])
        entry = self.entry(dict(status='matched', product_ids=['P1']), writer)
        result = self.receive(entry, text='كم السعر', media_kind='image', media_ref=['old-photo'])
        self.assertEqual('continue', result['action'])
        self.assertEqual('P1', result['unavailable_product'])
        self.assertEqual([], entry.store.snapshot(self.scope)['data']['cart'])
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
        self.decide.assert_not_called()


class BurstTimingTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        ai_jobs.init_db(self.db)

    def tearDown(self):
        self.db.close()

    def test_twenty_seconds_after_latest_message_without_extra_old_jobs(self):
        for value, kind in [('صورة','image'),('شنو قماشه؟','text'),('07701234567','text'),('', 'audio')]:
            self.assertEqual(20, sales_progress.debounce_seconds(value,kind,35))
            self.assertEqual(0, sales_progress.debounce_seconds(value,kind,0))
        with patch.object(ai_jobs.time,'time',return_value=100):
            first = ai_jobs.enqueue(self.db,'khuyoot','customer','sales_runtime',dict(incoming_id=1),'1',delay=20)
        with patch.object(ai_jobs.time,'time',return_value=108):
            latest = ai_jobs.enqueue(self.db,'khuyoot','customer','sales_runtime',dict(incoming_id=2),'2',delay=20)
        with patch.object(ai_jobs.time,'time',return_value=120):
            self.assertIsNone(ai_jobs.claim(self.db,'worker'))
            ai_jobs.enqueue(self.db,'khuyoot','customer','sales_runtime',dict(incoming_id=1),'1',delay=20)
        self.assertEqual('done',self.db.execute('SELECT status FROM ai_jobs WHERE id=?',(first['id'],)).fetchone()[0])
        with patch.object(ai_jobs.time,'time',return_value=127.99):
            self.assertIsNone(ai_jobs.claim(self.db,'worker'))
        with patch.object(ai_jobs.time,'time',return_value=128):
            job = ai_jobs.claim(self.db,'worker')
        self.assertEqual(latest['id'],job['id'])
        self.assertEqual(2,json.loads(job['payload'])['incoming_id'])

    def test_coalescing_does_not_cancel_another_store_or_a_running_job(self):
        first = ai_jobs.enqueue(self.db,'khuyoot','customer','sales_runtime',{},'1')
        self.assertEqual(first['id'],ai_jobs.claim(self.db,'worker')['id'])
        other = ai_jobs.enqueue(self.db,'default','customer','sales_runtime',{},'1')
        ai_jobs.enqueue(self.db,'khuyoot','customer','sales_runtime',{},'2',delay=20)
        rows = dict(self.db.execute('SELECT id,status FROM ai_jobs'))
        self.assertEqual('running',rows[first['id']])
        self.assertEqual('queued',rows[other['id']])
