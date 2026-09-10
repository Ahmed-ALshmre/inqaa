"""Regressions for the September 10 conversation audit and orders page."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from account_app.checkout import is_confirmation, contact_fields


class AuditFixTests(unittest.TestCase):
    def setUp(self):
        import account_app.app as m
        self.m = m
        self.temp = tempfile.TemporaryDirectory()
        self.old = m.DB_PATH
        m.DB_PATH = str(Path(self.temp.name) / 'audit.db')
        m.init_db()
        self.ctx = m.app.app_context(); self.ctx.push()
        self.db = m.get_db()
        self.client = m.app.test_client()
        with self.client.session_transaction() as session:
            session['dashboard_authenticated'] = True
        self.net = patch('requests.sessions.Session.request', side_effect=AssertionError('Unexpected network'))
        self.net.start()

    def tearDown(self):
        self.net.stop(); self.ctx.pop(); self.m.DB_PATH = self.old; self.temp.cleanup()

    def test_confirmation_only_never_accepts_changes_or_negation(self):
        for phrase in ['تمام ثبتي', 'يي عيني ثبتي', 'ثبتي الحجز يمعوده', 'نعم', 'اي حياتي']:
            self.assertTrue(is_confirmation(phrase), phrase)
        for phrase in ['لا تثبتين', 'تمام بس غيري القياس', 'اي مو اسود', 'تمام ثبتي؟', 'ثبتي باجر', 'مو موافقة']:
            self.assertFalse(is_confirmation(phrase), phrase)

    def test_basra_spelling_and_delivery_question(self):
        for phrase in ['البصره / القبله / حي القائم', 'البصرة / القبلة / حي القائم']:
            self.assertEqual(contact_fields(phrase)['province'], 'البصرة')
            self.assertIn('حي القائم', contact_fields(phrase)['address'])
        self.assertNotIn('address', contact_fields('شكد توصيل البصره'))

    def customer(self, sender, store='default'):
        self.db.execute("INSERT INTO customers(sender_id,store_id,first_seen_at) VALUES(?,?,?)",(sender,store,'2026-09-10T10:00:00+03:00'))
        self.db.commit()

    def test_manual_order_prices_quantity_shipping_and_resend_snapshot(self):
        self.customer('price')
        catalog=[dict(product_id='A',product_name='تنورة',price='١٢,٠٠٠',colors='اسود',sizes='38 إلى 52',status='active',stock='متوفر')]
        payload=dict(phone='07701234567',province='بغداد',address='بغداد حي اور',items=[dict(product_id='A',color='اسود',size='44',quantity=2)],total_amount=1)
        with patch.object(self.m,'load_products_from_file',return_value=catalog), patch.object(self.m,'delivery_fee_for_province',return_value=5000), patch.object(self.m,'send_order_to_telegram',return_value=True) as send, patch.object(self.m,'save_booking_to_file'), patch.object(self.m,'send_text_to_facebook',return_value=True):
            response=self.client.post('/api/conversations/price/create_order',json=payload)
            self.assertEqual(response.status_code,200,response.get_json())
            self.assertEqual(send.call_args.args[0]['total_amount'],29000)
            order=dict(self.db.execute('select * from orders').fetchone())
            self.assertEqual((order['product_total'],order['delivery_fee'],order['total_amount']),(24000,5000,29000))
            catalog[0]['price']='20000'
            response=self.client.post(f"/api/orders/{order['id']}/resend_telegram")
            self.assertEqual(response.status_code,200)
            self.assertEqual(send.call_args.args[0]['total_amount'],29000)
            self.assertIn('29000 مع التوصيل', self.m.format_order_for_telegram(send.call_args.args[0]))
            self.assertEqual(self.client.post('/api/conversations/price/create_order',json=payload).get_json()['duplicate'],True)
            self.assertEqual(self.db.execute('select count(*) from orders').fetchone()[0],1)

    def test_real_cart_confirmation_variants_and_correction_guard(self):
        catalog=[dict(product_id='A',product_name='تنورة',price='12000',colors='اسود',sizes='38 إلى 52',status='active',stock='متوفر')]
        phrases=['تمام ثبتي','يي عيني ثبتي','ثبتي الحجز يمعوده','اي بس غيري القياس']
        with patch.object(self.m,'load_products_from_file',return_value=catalog), patch.object(self.m,'delivery_fee_for_province',return_value=5000), patch.object(self.m,'send_order_to_telegram',return_value=True), patch.object(self.m,'save_booking_to_file'):
            for i, phrase in enumerate(phrases):
                sender='cart-'+str(i);self.customer(sender)
                event=dict(sender_id=sender,text=phrase)
                result={'order':dict(phone='07701234567',province='بغداد',address='حي اور',items=[dict(product_id='A',color='اسود',size='44',quantity=1),dict(product_id='A',color='اسود',size='48',quantity=1)])}
                _,reply=self.m.create_order_if_valid(self.db,sender,result,None)
                self.m.saved_checkout_reply(self.db,event,reply,{}, {'checkout_proposal':result['_checkout_proposal']})
                self.m.save_message(self.db,sender,'incoming','text',phrase,None,None,None,{})
                accepted=self.m.accept_checkout_proposal(self.db,event,[],catalog)
                order=self.db.execute('select total_amount from orders where sender_id=?',(sender,)).fetchone()
                if i < 3:
                    self.assertTrue(accepted['meta']['order_created'])
                    self.assertEqual(order['total_amount'],29000)
                    self.assertIsNone(self.m.accept_checkout_proposal(self.db,event,[],catalog))
                else:
                    self.assertIsNone(accepted);self.assertIsNone(order)

    def test_order_stats_include_all_rows_and_same_period_people(self):
        for sender in ['a','b','old','other']: self.customer(sender,'other' if sender=='other' else 'default')
        self.db.execute("update customers set first_seen_at='2020-01-01' where sender_id='old'")
        for sender,status,date,store in [('a','new','2026-09-10T23:59:59+03:00','default'),('a','new','2026-09-10T12:00:00+03:00','default'),('b','cancelled','2026-09-10T12:00:00+03:00','default'),('old','new','2020-01-01','default'),('other','new','2026-09-10','other')]:
            self.db.execute('insert into orders(sender_id,status,created_at,store_id,phone,product_id,address) values(?,?,?,?,?,?,?)',(sender,status,date,store,'07701234567','A','حي اور'))
        self.db.commit()
        url='/api/orders?date_from=2026-09-10&date_to=2026-09-10&store_id=default&limit=1'
        data=self.client.get(url).get_json()
        self.assertEqual((data['total'],data['new_count'],data['people_count'],data['ordering_people_count']),(3,2,2,1))
        self.assertEqual(data['conversion_rate'],50)
        self.assertTrue(data['has_more'])
        self.assertNotEqual(data['orders'][0]['id'],self.client.get(url+'&offset=1').get_json()['orders'][0]['id'])
        self.assertEqual(self.client.get(url+'&q=not-present').get_json()['orders'],[])
        self.assertEqual(self.client.get(url+'&q=not-present').get_json()['total'],3)
        self.assertEqual(self.client.get('/api/orders?date_from=bad').status_code,400)
        self.assertEqual(self.client.get('/api/orders?date_from=2026-09-11&date_to=2026-09-10').status_code,400)

    def test_legacy_price_unknown_requires_explicit_review(self):
        self.db.execute("insert into orders(sender_id,status) values('legacy','new')");self.db.commit()
        with patch.object(self.m,'send_order_to_telegram',return_value=True) as send:
            self.assertEqual(self.client.post('/api/orders/1/resend_telegram').status_code,409)
            send.assert_not_called()
            self.assertEqual(self.client.patch('/api/orders/1',json={'product_total':16000,'delivery_fee':0}).status_code,200)
            self.assertEqual(self.client.post('/api/orders/1/resend_telegram').status_code,200)
            self.assertEqual(send.call_args.args[0]['total_amount'],16000)
            self.assertEqual(self.client.patch('/api/orders/1',json={'product_total':-5,'delivery_fee':0}).status_code,400)

    def test_system_issues_separate_human_handoff_and_detect_checkout_loop(self):
        for sender in ['technical','human','loop','resolved']:
            self.customer(sender)
        for sender,reason in [('technical','exception:ValueError'),('human','AI متوقف (ai_disabled_for_conversation)')]:
            self.db.execute("insert into human_reviews(sender_id,status,reason) values(?,'pending',?)",(sender,reason))
        for sender in ['loop','resolved']:
            for i in range(3):
                self.db.execute("insert into messages(sender_id,direction,text,created_at) values(?,'outgoing',?,?)",(sender,'هذه القطع المقترحة للحجز، ولم يثبت الطلب بعد:',f'2026-09-10T10:0{i}:00+03:00'))
        self.db.execute("insert into orders(sender_id,created_at) values('resolved','2026-09-10T11:00:00+03:00')")
        self.db.commit()
        ids=lambda status:{r['sender_id'] for r in self.client.get('/api/conversations?status='+status).get_json()['conversations']}
        self.assertEqual(ids('system_issues'),{'technical','loop'})
        self.assertEqual(ids('problems'),{'human'})
