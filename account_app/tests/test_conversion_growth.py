import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import account_app.app as m
from account_app import conversion_growth as growth


class ConversionGrowthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'growth.db')
        m.init_db()
        self.ctx = m.app.app_context(); self.ctx.push()
        self.token = m._current_store_id.set('default')
        self.db = m.get_db()
        self.now = datetime.now(m.BAGHDAD_TZ).replace(hour=12, minute=0, second=0, microsecond=0)
        self.clock = patch.object(m, 'now_baghdad_iso', return_value=self.now.isoformat()); self.clock.start()
        self.sender = 'default::growth'
        self.db.execute("INSERT INTO customers(sender_id,store_id,lead_score) VALUES(?,'default',80)", (self.sender,))
        m.set_store_setting(self.db, 'followup_enabled', '1')
        self.msg('incoming', 'اريد قياس 44', -10)
        self.msg('outgoing', 'قياس 44 متوفر', -5)

    def tearDown(self):
        self.clock.stop(); m._current_store_id.reset(self.token); self.ctx.pop()
        m.DB_PATH = self.old; self.temp.cleanup()

    def msg(self, direction, text, minutes=0, payload=None, sender=None):
        cur = self.db.execute("INSERT INTO messages(sender_id,store_id,direction,message_type,text,raw_payload,created_at) VALUES(?,'default',?,'text',?,?,?)",
            (sender or self.sender, direction, text, json.dumps(payload or {}), (self.now+timedelta(minutes=minutes)).isoformat()))
        self.db.commit(); return cur.lastrowid

    def plan(self, settings=None):
        return growth.followup_plan(self.db, self.sender, self.now.isoformat(), settings or m.get_followup_settings(self.db))

    def due(self):
        fid = m.schedule_followup_if_needed(self.db,self.sender,'selection')
        self.db.execute('UPDATE followups SET scheduled_at=? WHERE id=?', (self.now.isoformat(),fid)); self.db.commit()
        return fid

    def test_selection_checkout_general_and_explicit_reminder(self):
        self.assertEqual(self.plan()['delay_minutes'],120)
        self.msg('incoming','ثبتي',1); self.msg('outgoing','رقم الهاتف؟',2)
        self.assertEqual(self.plan()['delay_minutes'],60)
        self.msg('incoming','شكد السعر',3); self.msg('outgoing','18000',4)
        self.assertEqual(self.plan()['delay_minutes'],240)
        self.msg('incoming','ذكريني بعد ٣ ساعات',5); self.msg('outgoing','حاضر',6)
        self.assertEqual(self.plan()['delay_minutes'],180)
        self.assertEqual(self.plan()['reason'],'requested_reminder')

    def test_quiet_hours_shift_and_do_not_escape_window(self):
        at = self.now.replace(hour=22)
        self.assertEqual(growth.during_contact_hours(at).hour,9)
        self.assertEqual(growth.during_contact_hours(at).day,(at+timedelta(days=1)).day)
        settings=m.get_followup_settings(self.db); settings.update(adaptive_timing=False,default_delay_minutes=1440)
        self.assertIsNone(self.plan(settings))

    def test_night_shift_and_round_the_clock(self):
        self.assertEqual(growth.during_contact_hours(self.now,21,9).hour,21)
        self.assertEqual(growth.during_contact_hours(self.now,9,9),self.now)

    def test_reminder_does_not_guess_tomorrow_or_override_rejection(self):
        self.assertIsNone(growth.reminder_minutes('ذكريني باجر'))
        self.assertIsNone(growth.reminder_minutes('لا تراسلني بعد ساعتين'))
        self.msg('incoming','مو هسه ذكريني بعد ساعتين',-2); self.msg('outgoing','حاضر',-1)
        self.assertIsNone(m._followup_block_reason(self.db,self.sender))

    def test_no_model_call_in_reply_path_and_one_pending_per_turn(self):
        with patch.object(m,'generate_ai_followup_message') as ai:
            fid=m.schedule_followup_if_needed(self.db,self.sender)
            self.assertEqual(fid,m.schedule_followup_if_needed(self.db,self.sender))
            ai.assert_not_called()
        self.assertEqual(self.db.execute("SELECT count(*) FROM followups WHERE status='pending'").fetchone()[0],1)

    def test_new_customer_turn_replaces_old_schedule(self):
        old=m.schedule_followup_if_needed(self.db,self.sender)
        self.msg('incoming','ثبتي',-2); self.msg('outgoing','رقم الهاتف؟',-1)
        new=m.schedule_followup_if_needed(self.db,self.sender)
        self.assertNotEqual(old,new)
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE id=?',(old,)).fetchone()[0],'cancelled')

    def test_abstention_at_send_is_cancelled_without_outreach(self):
        fid=self.due()
        with patch.object(m,'build_followup_message',return_value=None),patch.object(m,'send_reply_via_manychat') as send:
            m.send_due_followups(self.db); send.assert_not_called()
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE id=?',(fid,)).fetchone()[0],'cancelled')

    def test_send_uses_fresh_message_and_never_repeats(self):
        self.due()
        with patch.object(m,'build_followup_message',return_value='أوضحلك قياس القطعة؟'),patch.object(m,'send_reply_via_manychat',return_value=True) as send:
            self.assertEqual(m.send_due_followups(self.db)['sent'],1)
            self.assertEqual(m.send_due_followups(self.db)['sent'],0)
            send.assert_called_once()
        self.assertEqual(self.db.execute("SELECT count(*) FROM messages WHERE raw_payload LIKE '%automatic_followup%'").fetchone()[0],1)

    def test_reply_during_generation_aborts_send(self):
        self.due()
        def generate(*args):
            self.msg('incoming','ما اريد',0)
            return 'متابعة'
        with patch.object(m,'build_followup_message',side_effect=generate),patch.object(m,'send_reply_via_manychat') as send:
            m.send_due_followups(self.db); send.assert_not_called()

    def test_uncertain_delivery_never_retries(self):
        fid=self.due()
        with patch.object(m,'build_followup_message',return_value='متابعة'),patch.object(m,'send_reply_via_manychat',side_effect=TimeoutError) as send:
            m.send_due_followups(self.db); m.send_due_followups(self.db)
            send.assert_called_once()
        self.assertEqual(self.db.execute('SELECT status FROM followups WHERE id=?',(fid,)).fetchone()[0],'uncertain')

    def test_rollout_does_not_message_old_customers_or_reset_manual_disable(self):
        m.activate_conversion_plan(self.db)
        self.assertEqual(m._followup_block_reason(self.db,self.sender),'before_plan_activation')
        m.set_store_setting(self.db,'followup_enabled','0')
        m.activate_conversion_plan(self.db)
        self.assertFalse(m.get_followup_settings(self.db)['enabled'])

    def test_metrics_use_same_cohort_not_order_count(self):
        for sender in [self.sender,self.sender,'default::outside']:
            self.db.execute("INSERT INTO orders(sender_id,store_id,created_at,status) VALUES(?,'default',?,'new')",(sender,self.now.isoformat()))
        self.db.commit()
        result=growth.metrics(self.db,'default',self.now.isoformat())
        self.assertEqual((result['people'],result['buyers'],result['orders']),(1,1,3))
        self.assertEqual(result['conversion'],100)
        self.assertEqual(growth.metrics(self.db,'khuyoot',self.now.isoformat())['people'],0)

    def test_unlinked_catalog_cart_is_confirmed_not_blocked_forever(self):
        product=dict(product_id='F1',product_name='فستان',price='18000',colors='اسود',sizes='40',status='active')
        data=dict(phone='07701234567',province='بغداد',address='بغداد حي اور',items=[dict(product_id='F1',product_name='فستان',color='اسود',size='40',quantity=1)])
        with patch.object(m,'load_products_from_file',return_value=[product]):
            result={'order':data}
            created,reply=m.create_order_if_valid(self.db,self.sender,result,None)
        self.assertFalse(created)
        self.assertIn('هذه القطع المقترحة',reply)
        self.assertIn('_checkout_proposal',result)
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0],0)

    def test_cart_survives_empty_outgoing_payload_but_not_correction(self):
        product={'product_id':'F1','product_name':'فستان'}
        draft={'items':[dict(product_id='F1',product_name='فستان',color='اسود',size='40',quantity=1)]}
        self.msg('outgoing','رقم الهاتف؟',-4,{'checkout_draft':draft})
        self.msg('incoming','07701234567',-3)
        self.msg('outgoing','العنوان؟',-2,{'checkout_draft':{}})
        self.msg('incoming','بغداد حي اور',-1)
        with patch.object(m,'load_customer_products',return_value=[product]):
            result={'order':{}}
            m.restore_checkout_draft(self.db,{'sender_id':self.sender,'text':'بغداد حي اور'},result,product)
            self.assertEqual(result['order']['items'][0]['size'],'40')
            self.msg('incoming','بدل قياس 44',0)
            result={'order':{}}
            m.restore_checkout_draft(self.db,{'sender_id':self.sender,'text':'بغداد حي اور'},result,product)
            self.assertEqual(result['order'],{})

    def test_store_settings_api_and_cancel_are_isolated(self):
        self.due()
        self.db.execute("INSERT INTO customers(sender_id,store_id,lead_score) VALUES('khuyoot::other','khuyoot',80)")
        self.db.execute("INSERT INTO followups(sender_id,status,scheduled_at,created_at) VALUES('khuyoot::other','pending',?,?)",(self.now.isoformat(),self.now.isoformat()))
        self.db.commit()
        client=m.app.test_client()
        with client.session_transaction() as session: session['dashboard_authenticated']=True
        response=client.get('/api/settings/followup?store_id=default')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.get_json()['pending_count'],1)
        self.assertEqual(len(response.get_json()['queue']),1)
        response=client.post('/api/followups/cancel_pending?store_id=default')
        self.assertEqual(response.get_json()['cancelled'],1)
        self.assertEqual(self.db.execute("SELECT status FROM followups WHERE sender_id='khuyoot::other'").fetchone()[0],'pending')

    def test_rolling_daily_limit_survives_midnight(self):
        self.due()
        previous=self.now-timedelta(hours=13)
        self.db.execute("INSERT INTO followups(sender_id,status,created_at,sent_at) VALUES(?,'sent',?,?)",(self.sender,previous.isoformat(),previous.isoformat()))
        self.db.commit()
        with patch.object(m,'send_reply_via_manychat') as send:
            m.send_due_followups(self.db); send.assert_not_called()

    def test_product_removal_prevents_stale_offer(self):
        fid=m.schedule_followup_if_needed(self.db,self.sender,product={'product_id':'removed'})
        self.db.execute('UPDATE followups SET scheduled_at=? WHERE id=?',(self.now.isoformat(),fid)); self.db.commit()
        with patch.object(m,'load_customer_products',return_value=[]),patch.object(m,'send_reply_via_manychat') as send:
            m.send_due_followups(self.db); send.assert_not_called()
