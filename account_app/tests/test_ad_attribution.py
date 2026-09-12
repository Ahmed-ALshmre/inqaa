import json
import sqlite3
import unittest
from account_app import ad_attribution as ad
from account_app.media import extract_media

class AdAttributionTests(unittest.TestCase):
    def test_chatwoot_conversation_referral_and_media_separation(self):
        payload={'conversation':{'additional_attributes':{'referral':{'source_type':'ad','source_id':'123','headline':'Dress','image_url':'https://cdn.example/ad.jpg','source_url':'https://fb.me/ad'}}}}
        result=ad.extract(payload)
        self.assertEqual(result['ad_id'],'123')
        self.assertEqual(result['headline'],'Dress')
        self.assertEqual(extract_media({'_ad_context':result}),[])

    def test_facebook_referral_creative(self):
        payload={'entry':[{'messaging':[{'referral':{'ad_id':'44','source':'ADS','ads_context_data':{'ad_title':'Title','photo_url':'https://cdn.example/image.jpg','post_id':'22'}}}]}]}
        result=ad.extract(payload)
        self.assertEqual(result['ad_id'],'44')
        self.assertEqual(result['post_id'],'22')
        self.assertIn('image_url',result)

    def test_plain_customer_message_not_ad(self):
        self.assertEqual(ad.extract({'content':'hello','source_id':'message123','attachments':[{'data_url':'https://cdn.example/customer.jpg'}]}),{})
        result=ad.extract({'ad_id':'22','image_url':'https://cdn.example/customer.jpg'})
        self.assertNotIn('image_url',result)

    def test_reject_unsafe_urls_and_unknown_secrets(self):
        result=ad.extract({'referral':{'ad_id':'1','image_url':'javascript:alert(1)','source_url':'https://user:password@example.com','api_token':'secret'}})
        self.assertEqual(result,{'ad_id':'1'})

    def test_persist_backfill_pagination_and_customer_isolation(self):
        db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
        ad.init_db(db)
        db.execute('CREATE TABLE messages(id INTEGER PRIMARY KEY,sender_id TEXT,direction TEXT,ad_id TEXT,ref TEXT,raw_payload TEXT)')
        db.execute("INSERT INTO messages VALUES(1,'alice','incoming','11',NULL,'{}')")
        self.assertEqual(ad.load(db,'alice')['ad_id'],'11')
        self.assertIsNone(ad.load(db,'bob'))
        ad.save(db,'alice',105,{},None,None)
        self.assertEqual(ad.load(db,'alice')['ad_id'],'11')
        ad.save(db,'alice',106,{'referral':{'ad_id':'12'}})
        self.assertEqual(ad.load(db,'alice')['ad_id'],'12')
        ad.save(db,'bob',107,{'referral':{'ad_id':'33'}})
        self.assertEqual(ad.load(db,'bob')['ad_id'],'33')
        ad.save(db,'bob',108,{'referral':{'ad_id':'33','image_url':'https://cdn.example/ad.jpg'}})
        ad.save(db,'bob',109,{'referral':{'ad_id':'33'}})
        self.assertIn('image_url',ad.load(db,'bob'))
        ad.save(db,'bob',110,{'referral':{'ad_id':'34'}})
        self.assertNotIn('image_url',ad.load(db,'bob'))
        db.close()
