"""Deterministic regressions from the September 21 backup audit."""
import json
import unittest
from unittest.mock import patch
from account_app import conversation_quality as quality, order_actions
from account_app.tests import test_audit_orders as fixtures


class LanguageGuards(unittest.TestCase):
    def test_observed_colloquial_requests(self):
        self.assertEqual(quality.budget('دزي صور موديلات بسعر ١٥ الف'), 15000)
        self.assertEqual(quality.child_age('عمرها 6 اشهر'), .5)
        self.assertIsNone(quality.operational_request('يذا ماعجبني يرجه'))
        self.assertEqual(quality.operational_request('الغاء الطلب')['kind'], 'cancel')
        self.assertEqual(quality.operational_request('غيري القياس 40')['value'], '40')
        self.assertTrue(quality.unsupported_claim('بلغت شركة التوصيل حتى يستعجلون'))
        self.assertFalse(quality.wants_photos('اي التنوره دريت صورتها'))


class QualityIntegrationTests(unittest.TestCase):
    setUp = fixtures.AuditFixTests.setUp
    tearDown = fixtures.AuditFixTests.tearDown
    customer = fixtures.AuditFixTests.customer

    def test_photos_follow_requested_price_not_old_product(self):
        products = [dict(product_id=str(p),product_name='فستان '+str(p),price=str(p),stock='متوفر',status='active',image_url=f'https://images.test/{p}.jpg') for p in (15000,16000,18000)]
        result = self.m.factual_customer_request({'text':'دزي صور موديلات بسعر 15 الف'}, products, products[1])
        self.assertEqual(result['image_product_ids'], ['15000'])
        urls = self.m.requested_reply_images(result, products, products[1], 'دزي صور موديلات بسعر 15 الف', result['reply'])
        self.assertEqual(urls, ['https://images.test/15000.jpg'])
        missing = self.m.factual_customer_request({'text':'دزي صور موديلات بسعر 12 الف'}, products, products[1])
        self.assertTrue(missing['_suppress_product_images'])
        self.assertEqual(self.m.requested_reply_images(missing,products,products[1],'',missing['reply']), [])

    def test_change_requires_real_item_update_before_review_closes(self):
        self.customer('edit')
        catalog = [dict(product_id='A',product_name='فستان',price='16000',status='active',stock='متوفر')]
        payload = dict(phone='07701234567',province='بغداد',address='بغداد حي اور',items=[dict(product_id='A',quantity=1,color='اسود',size='38')])
        with patch.object(self.m,'load_products_from_file',return_value=catalog), patch.object(self.m,'send_order_to_telegram',return_value=True), patch.object(self.m,'save_booking_to_file'), patch.object(self.m,'send_text_to_facebook',return_value=True):
            response = self.client.post('/api/conversations/edit/create_order', json=payload)
            self.assertEqual(response.status_code,200,response.json)
            order = dict(self.db.execute('SELECT * FROM orders').fetchone())
            order_actions.record(self.db,'edit',order['id'],{'kind':'size','value':'40'},self.m.now_baghdad_iso())
            self.assertEqual(self.client.post('/api/conversations/edit/mark_reviewed',json={}).status_code,409)
            updated = self.client.patch(f"/api/orders/{order['id']}",json={'size':'40'})
            self.assertEqual(updated.status_code,200,updated.json)
            stored = json.loads(self.db.execute('SELECT order_items FROM orders').fetchone()[0])
            self.assertEqual(stored[0]['size'],'40')
            self.assertFalse(order_actions.pending(self.db,'edit'))
            self.assertEqual(self.client.post('/api/conversations/edit/mark_reviewed',json={}).status_code,200)

    def test_pending_action_survives_bulk_review_close(self):
        self.customer('cancel')
        order_actions.record(self.db,'cancel',99,{'kind':'cancel','value':'cancelled'},self.m.now_baghdad_iso())
        with patch.object(self.m,'send_telegram_message'):
            self.m.create_human_review(self.db,{'sender_id':'cancel'},'طلب إلغاء')
        self.m.close_human_attention(self.db)
        self.assertTrue(self.m.has_pending_human_review(self.db,'cancel'))

    def test_each_item_size_is_saved_without_changing_other_variant(self):
        self.customer('multi')
        catalog = [dict(product_id='A',product_name='فستان',price='16000',status='active',stock='متوفر')]
        payload = dict(phone='07701234567',province='بغداد',address='بغداد حي اور',items=[dict(product_id='A',quantity=1,color=c,size=s) for c,s in [('اسود','38'),('وردي','44')]])
        with patch.object(self.m,'load_products_from_file',return_value=catalog), patch.object(self.m,'send_order_to_telegram',return_value=True), patch.object(self.m,'save_booking_to_file'), patch.object(self.m,'send_text_to_facebook',return_value=True):
            response = self.client.post('/api/conversations/multi/create_order',json=payload)
            self.assertEqual(response.status_code,200,response.json)
            order = dict(self.db.execute('SELECT * FROM orders').fetchone())
            payload['items'][0]['size'] = '40'
            updated = self.client.patch(f"/api/orders/{order['id']}",json={'items':payload['items']})
            self.assertEqual(updated.status_code,200,updated.json)
            lines = updated.json['order']['items']
            self.assertEqual([(i['color'],i['size']) for i in lines],[('اسود','40'),('وردي','44')])
            exported = json.loads(self.db.execute('SELECT payload FROM menger_deliveries WHERE order_id=?',(order['id'],)).fetchone()[0])
            self.assertEqual([i['size'] for i in exported['items']],['40','44'])
