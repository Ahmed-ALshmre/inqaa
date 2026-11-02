import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
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

    def test_video_snapshot_and_explicit_ad_source_survive_normalization(self):
        result = ad.extract({'conversation': {'metadata': {
            'meta_ad_id': '44', 'meta_ad_title': 'Ad title',
            'meta_ad_video_url': 'https://cdn.example/ad.mp4',
            'meta_ad_photo_url': 'https://cdn.example/preview.jpg',
            'meta_ad_source_url': 'https://example.com/ad',
        }}})
        self.assertEqual(result['video_url'], 'https://cdn.example/ad.mp4')
        self.assertEqual(result['image_url'], 'https://cdn.example/preview.jpg')
        self.assertEqual(result['source_url'], 'https://example.com/ad')
        self.assertEqual(extract_media({'_ad_context': result}), [])

    def test_customer_video_and_unsafe_ad_video_never_become_ad_creative(self):
        self.assertNotIn('video_url', ad.extract({'ad_id': '44', 'video_url': 'https://cdn.example/customer.mp4'}))
        self.assertEqual(ad.extract({'message': {'attachments': [
            {'type': 'video', 'url': 'https://cdn.example/customer.mp4'}]}}), {})
        for url in ('javascript:alert(1)', 'data:video/mp4;base64,x', 'https://user:secret@example.com/ad.mp4'):
            self.assertNotIn('video_url', ad.extract({'referral': {'ad_id': '44', 'video_url': url}}))
        self.assertEqual(ad.normalize({'referral_media_content_type': 'video/mp4',
            'referral_media_url': 'https://cdn.example/ad.mp4'})['video_url'], 'https://cdn.example/ad.mp4')

    @unittest.skipUnless(shutil.which('node'), 'Node.js is required for the dashboard renderer check')
    def test_dashboard_resets_video_source_and_rejects_unsafe_links(self):
        source = Path(__file__).parents[1] / 'static' / 'js' / 'dashboard.js'
        script = r'''
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync(process.argv[1], 'utf8');
const code = source.slice(source.indexOf('function renderConversationAdContext('), source.indexOf('function safeMediaUrl('));
const nodes = Object.fromEntries(['Context','Image','Link','Video','Title','Details'].map(name =>
  ['conversationAd'+name, {hidden: true, removeAttribute(name) {delete this[name];}}]));
const context = vm.createContext({URL, location: {origin:'https://store.example'},
  document: {getElementById: id => nodes[id]}});
vm.runInContext(code, context);
context.renderConversationAdContext({ad_id:'44',headline:'Winter ad',video_url:'https://cdn.example/ad.mp4',source_url:'https://example.com/post'});
assert.equal(nodes.conversationAdVideo.hidden, false);
assert.equal(nodes.conversationAdVideo.href, 'https://cdn.example/ad.mp4');
assert.equal(nodes.conversationAdLink.href, 'https://example.com/post');
assert.equal(nodes.conversationAdImage.hidden, true);
context.renderConversationAdContext({ad_id:'45',image_url:'https://cdn.example/new.jpg'});
assert.equal(nodes.conversationAdVideo.hidden, true);
assert.equal(nodes.conversationAdVideo.href, undefined);
assert.equal(nodes.conversationAdLink.href, undefined);
assert.equal(nodes.conversationAdImage.hidden, false);
for (const video_url of ['javascript:alert(1)','https://user:secret@example.com/video']) {
  context.renderConversationAdContext({ad_id:'46',video_url});
  assert.equal(nodes.conversationAdVideo.hidden, true);
}
context.renderConversationAdContext(null);
assert.equal(nodes.conversationAdContext.hidden, true);
assert.equal(nodes.conversationAdImage.src, undefined);
context.esc = value => String(value);
context.fmtDatetime = () => 'time';
vm.runInContext(source.slice(source.indexOf('function safeMediaUrl('), source.indexOf('function mediaLoaded(')), context);
vm.runInContext(source.slice(source.indexOf('function messageMarkup('), source.indexOf('const resolvingMedia =')), context);
const sticker = {direction:'incoming',media:[{type:'sticker',url:'https://cdn.example/thumb.png'}]};
const markup = context.messageMarkup(sticker);
assert.ok(markup.includes('<img '));
assert.ok(markup.includes('ملصق في المحادثة'));
assert.ok(!markup.includes('فتح المرفق'));
assert.equal(sticker.media[0].type, 'sticker');
'''
        result = subprocess.run(['node', '-e', script, str(source)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

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
