import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, Mock

from account_app import sales_engagement
import account_app.app as m


class EngagementTests(unittest.TestCase):
    def test_natural_address_preserves_product_values(self):
        text = "تدلل/ين، تفضل/ي القياس 38/40 والسعر 12,000"
        self.assertEqual(sales_engagement.natural_address(text), "تدلل، تفضل القياس 38/40 والسعر 12,000")

    def test_reply_parts_and_reply_use_same_natural_address(self):
        result = m.normalize_ai_reply_parts({'reply_parts': ['تدلل/ين', 'القياس 38/40']})
        self.assertNotIn('/ين', result['reply'])
        self.assertIn('38/40', result['reply'])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old_db = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'engagement.db')
        m.init_db()
        self.ctx = m.app.app_context(); self.ctx.push()
        self.token = m._current_store_id.set('default')
        self.db = m.get_db()
        self.now = datetime.now(m.BAGHDAD_TZ)
        self.sender = 'default::test-engagement'
        self.add_customer(self.sender, 'default')
        m.set_store_setting(self.db, 'followup_enabled', '1')
        self.incoming(self.sender, 'شكد السعر', self.now-timedelta(minutes=10))
        self.outgoing(self.sender)

    def tearDown(self):
        m._current_store_id.reset(self.token)
        self.ctx.pop(); m.DB_PATH = self.old_db; self.temp.cleanup()

    def add_customer(self, sender, store):
        self.db.execute("INSERT INTO customers(sender_id,store_id,lead_score) VALUES(?,?,80)", (sender,store))
        self.db.commit()

    def incoming(self, sender, text, when):
        self.db.execute("INSERT INTO messages(sender_id,store_id,direction,message_type,text,created_at) VALUES(?,?,'incoming','text',?,?)",
                        (sender,sender.split('::')[0],text,when.isoformat()))
        self.db.commit()

    def outgoing(self, sender):
        self.db.execute("INSERT INTO messages(sender_id,store_id,direction,message_type,text,created_at) VALUES(?,?,'outgoing','text','السعر موضح',?)", (sender,sender.split('::')[0],(self.now-timedelta(minutes=1)).isoformat()))
        self.db.commit()

    def queued(self, sender, status='pending', when=None):
        when = when or self.now-timedelta(minutes=5)
        cur = self.db.execute("INSERT INTO followups(sender_id,stage,message_text,status,scheduled_at,created_at,sent_at) VALUES(?,'price','متابعة',?,?,?,?)",
                              (sender,status,when.isoformat(),when.isoformat(),when.isoformat() if status=='sent' else None))
        self.db.commit(); return cur.lastrowid

    def test_ai_abstention_does_not_become_generic_booking_message(self):
        with patch.object(m,'generate_ai_followup_message',return_value=None):
            self.assertIsNone(m.build_followup_message(self.db,self.sender,'price'))
            self.assertIsNone(m.schedule_followup_if_needed(self.db,self.sender,'price'))
        self.assertEqual(self.db.execute('SELECT count(*) FROM followups').fetchone()[0],0)

    def test_explicit_empty_model_reply_is_respected(self):
        response=Mock();response.json.return_value={'choices':[{'message':{'content':json.dumps({'reply':'','silent_reason':'unknown'})}}]}
        with patch.object(m,'OPENROUTER_KEY','test'),patch.object(m.ai_transport,'post',return_value=response):
            self.assertIsNone(m.generate_ai_followup_message(self.db,self.sender,'price'))

    def test_provider_failure_does_not_send_scarcity_fallback(self):
        with patch.object(m,'OPENROUTER_KEY','test'),patch.object(m.ai_transport,'post',side_effect=RuntimeError('offline')):
            self.assertIsNone(m.generate_ai_followup_message(self.db,self.sender,'price'))
        self.assertIsNone(m._followup_fallback_message('price',{'product_name':'قطعة'}))

    def test_followup_template_is_scoped_to_customer_store(self):
        m.set_store_setting(self.db,'followup_message_template','default-only')
        other='khuyoot::test';self.add_customer(other,'khuyoot');self.incoming(other,'القياس 44 متوفر',self.now-timedelta(minutes=3));self.outgoing(other)
        token=m._current_store_id.set('khuyoot')
        try:
            m.set_store_setting(self.db,'followup_message_template','khuyoot-only')
            self.assertEqual(m.build_followup_message(self.db,other,'availability'),'khuyoot-only')
            self.assertIsNone(m.build_followup_message(self.db,self.sender,'price'))
        finally:m._current_store_id.reset(token)

    def test_only_one_followup_until_customer_replies(self):
        self.queued(self.sender,'sent')
        self.assertEqual(m._followup_block_reason(self.db,self.sender),'already_followed_up_without_reply')
        self.incoming(self.sender,'قياس 44',self.now-timedelta(minutes=1))
        self.outgoing(self.sender)
        self.assertIsNone(m._followup_block_reason(self.db,self.sender))

    def test_defer_even_if_next_bubble_is_courtesy(self):
        self.incoming(self.sender,'خليني افكر بعدين',self.now-timedelta(minutes=2))
        self.incoming(self.sender,'عيني',self.now-timedelta(minutes=1))
        self.assertEqual(m._followup_block_reason(self.db,self.sender),'customer_declined_or_deferred')
        self.assertIsNone(m.schedule_ai_review_followup(self.db,self.sender,'متابعة',90))

    def test_stale_or_future_incoming_is_blocked(self):
        for delta in (timedelta(hours=-24),timedelta(hours=1)):
            self.db.execute('UPDATE messages SET created_at=? WHERE sender_id=?',((self.now+delta).isoformat(),self.sender));self.db.commit()
            self.assertEqual(m._followup_block_reason(self.db,self.sender),'stale_customer_message')

    def test_human_pause_and_booked_customers_are_blocked(self):
        self.db.execute('INSERT INTO customer_ai_settings(sender_id,enabled) VALUES(?,0)',(self.sender,));self.db.commit()
        self.assertEqual(m._followup_block_reason(self.db,self.sender),'customer_ai_paused')
        self.db.execute('DELETE FROM customer_ai_settings WHERE sender_id=?',(self.sender,))
        self.db.execute('INSERT INTO orders(sender_id,status,created_at) VALUES(?,?,?)',(self.sender,'new',self.now.isoformat()));self.db.commit()
        self.assertEqual(m._followup_block_reason(self.db,self.sender),'existing_order')

    def test_sender_rechecks_deferral_and_never_sends(self):
        fid=self.queued(self.sender)
        self.incoming(self.sender,'ما اريد',self.now-timedelta(minutes=1))
        with patch.object(m,'send_reply_via_manychat') as send:
            result=m.send_due_followups(self.db)
            send.assert_not_called()
        self.assertEqual(result['skipped'],1)
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE id=?',(fid,)).fetchone()[0],'cancelled')

    def test_sender_keeps_other_store_queue_untouched(self):
        other='khuyoot::test';self.add_customer(other,'khuyoot');self.incoming(other,'سعر القطعة',self.now-timedelta(minutes=10));fid=self.queued(other)
        with patch.object(m,'send_reply_via_manychat') as send:
            self.assertEqual(m.send_due_followups(self.db)['sent'],0);send.assert_not_called()
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE id=?',(fid,)).fetchone()[0],'pending')

    def test_customer_reply_cancels_old_pending_followup(self):
        fid=self.queued(self.sender)
        self.incoming(self.sender,'قياس 44 متوفر',self.now-timedelta(minutes=1))
        with patch.object(m,'send_reply_via_manychat') as send:
            m.send_due_followups(self.db);send.assert_not_called()
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE id=?',(fid,)).fetchone()[0],'cancelled')

    def test_duplicate_pending_rows_only_send_once(self):
        self.queued(self.sender);self.queued(self.sender)
        with patch.object(m,'send_reply_via_manychat',return_value=True) as send:
            result=m.send_due_followups(self.db)
        self.assertEqual(send.call_count,1);self.assertEqual(result['sent'],1)

    def test_each_store_receives_correct_sales_profile(self):
        for store,phrase in [('default','لمسة ستور'),('khuyoot','خيوط'),('al-fatena','الفاتنة'),('golden-threads','الحد الأدنى'),('baraah-kids','ملابس أطفال')]:
            token=m._current_store_id.set(store)
            try:
                instructions,_=m.load_ai_config(self.db)
                self.assertIn(phrase,instructions)
                self.assertIn('السكوت لا يثبت',instructions)
            finally:m._current_store_id.reset(token)

    def test_unanswered_question_is_not_treated_as_customer_silence(self):
        self.incoming(self.sender,'كم يوم التوصيل',self.now-timedelta(seconds=20))
        self.assertEqual(m._followup_block_reason(self.db,self.sender),'customer_waiting_for_answer')

    def test_low_interest_cannot_bypass_reviewer_threshold(self):
        self.db.execute('UPDATE customers SET lead_score=5 WHERE sender_id=?',(self.sender,));self.db.commit()
        self.assertEqual(m._followup_block_reason(self.db,self.sender),'insufficient_interest')
        self.assertIsNone(m.schedule_followup_if_needed(self.db,self.sender,'price'))
