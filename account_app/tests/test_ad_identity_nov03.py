"""Ad aliases resolve catalog facts without vision or disclosure of failures."""
import io
import json
import unittest
from unittest.mock import patch

from account_app import ad_attribution as ads, app as m
from account_app.tests import test_zernio as fixtures
from account_app.tests import test_product_editor_links_oct24 as editor
from account_app.tests import test_zernio_flow as flow


class AdIdentityTests(unittest.TestCase):
    def test_string_id_roundtrip_and_separators(self):
        ids = ['120251751355980095', '120251751385940095']
        for value in [ids + ids, '\n'.join(ids), '، '.join(ids), ';'.join(ids)]:
            self.assertEqual(ads.normalize_product_ids(value, strict=True), ids)

    def test_bad_input_is_rejected_without_silent_numeric_rounding(self):
        for value in [123, [120251751355980095], [True], {}, ['https://example.com/123'], ['x y'], list(map(str, range(101)))]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                ads.normalize_product_ids(value, strict=True)

    def test_legacy_and_alias_identity_are_exact_and_store_scoped(self):
        product = dict(store_id='a', product_id='P1', ad_id='old', ad_ids=['new'], ref='campaign')
        catalog = [product, dict(product, store_id='b')]
        for context in [dict(ad_id='old'), dict(ad_id='new'), dict(ref='campaign')]:
            self.assertEqual(ads.product_candidates(catalog, context, 'a'), [product])
        for context in [dict(ad_id='ne'), dict(post_id='new'), dict(headline='new'), {}]:
            self.assertEqual(ads.product_candidates(catalog, context, 'a'), [])

    def test_conflicting_ad_and_ref_are_ambiguous(self):
        a = dict(product_id='A', ad_id='one')
        b = dict(product_id='B', ref='two')
        self.assertEqual(len(ads.product_candidates([a,b], dict(ad_id='one',ref='two'))), 2)


class AliasConversationTests(unittest.TestCase):
    setUp = fixtures.ZernioTests.setUp
    tearDown = fixtures.ZernioTests.tearDown
    post = fixtures.ZernioTests.post
    sender = fixtures.ZernioTests.sender
    incoming = fixtures.ZernioTests.incoming
    referral = fixtures.ZernioTests.referral
    process_latest = fixtures.ZernioTests.process_latest
    catalog = fixtures.ZernioTests.catalog

    def aliases(self):
        catalog = self.catalog()
        catalog[1].update(ad_id='original', ad_ids=['123','second'])
        return catalog

    def test_each_alias_directly_selects_real_price_without_vision(self):
        catalog = self.aliases()
        with patch.object(m, 'ad_creative_product', side_effect=AssertionError('Vision must not run')):
            for identifier in ['123','second','original']:
                self.payload['metadata']['referral']['ad_id'] = identifier
                self.referral()
                self.incoming('شكد سعره؟ ' + identifier)
                result = self.process_latest(catalog)
                self.assertRegex(result['reply'], r'16(?: ألف|,000)')
                self.assertNotRegex(result['reply'], 'الإعلان|للمراجعة|دزيلي صورة')

    def test_new_mapping_unblocks_prior_billing_review_before_retrying_vision(self):
        self.referral()
        self.incoming('ما هي مواصفات القطعة؟')
        with m.app.app_context():
            db = m.get_db()
            m.create_human_review(db, dict(sender_id=self.sender(), image_url='https://images.test/ad.jpg'),
                'Image analysis service failure: billing; ad product unresolved', notify_telegram=False)
        with patch.object(m, 'ad_creative_product', side_effect=AssertionError('No vision')), \
                patch.object(m, 'match_product', wraps=m.match_product) as match:
            result = self.process_latest(self.aliases())
            self.assertNotRegex(result['reply'], 'الإعلان|للمراجعة|مو موضح|دزيلي صورة')
        with m.app.app_context():
            db = m.get_db()
            self.assertEqual(db.execute("SELECT status FROM human_reviews").fetchone()[0], 'linked')
            self.assertEqual(db.execute("SELECT product_id FROM customer_product_interests WHERE status='active'").fetchone()[0], 'F2')

    def test_unknown_ad_hands_off_silently_and_does_not_link_default(self):
        self.referral()
        self.incoming('ما هي مواصفات القطعة؟')
        with patch.object(m, 'ad_creative_product', return_value=(None, {'service_error': True,'error_code':'billing'}, '')):
            result = self.process_latest(self.catalog()[:1])
        self.assertEqual(result['reply'], '')
        self.assertTrue(result['meta']['human_review_id'])
        with m.app.app_context():
            self.assertIsNone(m.get_active_product_binding(m.get_db(), self.sender()))

    def test_mapping_does_not_close_unrelated_staff_review_or_manual_pause(self):
        self.referral(); self.incoming('شكد سعره؟')
        with m.app.app_context():
            token = m._current_store_id.set('al-fatena')
            try:
                db=m.get_db(); ev=dict(sender_id=self.sender(),ad_id='123',text='شكد سعره؟')
                for reason in ['Image analysis service failure: billing; ad product unresolved', 'شكوى على المندوب']:
                    m.create_human_review(db,ev,reason,notify_telegram=False)
                m.set_customer_ai_enabled(db,self.sender(),False,reason='manual')
                self.assertTrue(m.recover_mapped_ad_review(db,ev,self.aliases()))
                self.assertFalse(m.is_customer_ai_enabled(db,self.sender()))
                self.assertEqual(db.execute("SELECT count(*) FROM human_reviews WHERE status='pending'").fetchone()[0],1)
            finally:m._current_store_id.reset(token)


class AliasEditorTests(unittest.TestCase):
    setUp = editor.ProductEditorLinkTests.setUp
    tearDown = editor.ProductEditorLinkTests.tearDown
    create = editor.ProductEditorLinkTests.create
    saved = editor.ProductEditorLinkTests.saved

    def test_create_update_and_clear_preserve_exact_ids(self):
        self.assertEqual(self.create('P1',ad_id='old',ad_ids=['120251751355980095','new','new']).status_code,201)
        self.assertEqual(self.saved()['P1']['ad_ids'],['120251751355980095','new'])
        result=self.client.put('/api/products/manage/P1',headers=self.headers,json={'price':'19000'})
        self.assertEqual(result.status_code,200)
        self.assertEqual(result.json['product']['ad_ids'],['120251751355980095','new'])
        result=self.client.put('/api/products/manage/P1',headers=self.headers,json={'ad_ids':[]})
        self.assertEqual(result.json['product']['ad_ids'],[])
        self.assertEqual(result.json['product']['ad_id'],'old')

    def test_catalog_import_roundtrips_aliases_without_rounding(self):
        imported=[dict(product_id='P1', product_name='طقم', ad_id='old',
                       ad_ids=['120251751355980095','120251751385940095'])]
        response=self.client.post('/api/import/products',headers=self.headers,
            data={'file':(io.BytesIO(json.dumps(imported).encode()),'products.json')})
        self.assertEqual(response.status_code,200,response.json)
        product=self.client.get('/api/products/manage').json['products'][0]
        self.assertEqual(product['ad_ids'], imported[0]['ad_ids'])
        self.assertEqual(ads.product_ids(product),['old',*imported[0]['ad_ids']])

    def test_duplicate_mapping_is_rejected_for_primary_or_alias_and_keeps_file(self):
        self.create('P1',ad_id='old',ad_ids=['new'])
        self.create('P2')
        before=self.catalog.read_bytes()
        for value in [dict(ad_id='new'),dict(ad_ids=['old']),dict(ad_ids=[120251751355980095])]:
            response=self.client.put('/api/products/manage/P2',headers=self.headers,json=value)
            self.assertEqual(response.status_code,400,response.json)
            self.assertEqual(self.catalog.read_bytes(),before)

    def test_import_rejects_numeric_and_conflicting_ids_without_changing_catalog(self):
        self.create('P1',ad_ids=['existing'])
        before=self.catalog.read_bytes()
        for imported in [[dict(product_id='P1',product_name='طقم',ad_ids=[120251751355980095])],
                         [dict(product_id='P1',product_name='طقم',ad_ids=['same']),
                          dict(product_id='P2',product_name='قميص',ad_id='same')]]:
            response=self.client.post('/api/import/products',headers=self.headers,
                data={'file':(io.BytesIO(json.dumps(imported).encode()),'products.json')})
            self.assertEqual(response.status_code,400,response.json)
            self.assertEqual(self.catalog.read_bytes(),before)


class AliasWorkerTests(unittest.TestCase):
    setUp = flow.ZernioFlowTests.setUp
    post = flow.ZernioFlowTests.post
    response = flow.ZernioFlowTests.response
    sender = flow.ZernioFlowTests.sender
    message = flow.ZernioFlowTests.message
    run_job = flow.ZernioFlowTests.run_job
    turn = flow.ZernioFlowTests.turn
    model_reply = flow.ZernioFlowTests.model_reply
    recognize = flow.ZernioFlowTests.recognize
    deliver = flow.ZernioFlowTests.deliver

    def test_alias_reply_and_followup_use_same_product_and_transport_without_vision(self):
        self.catalog[1].update(ad_id='original',ad_ids=['123'])
        result,sent=self.turn('شكد سعره؟',ad=True)
        self.assertTrue(result['ok'])
        self.assertTrue(any('16 ألف' in x.get('message','') for x in sent))
        _,sent=self.turn('دزي صورة الموديل')
        self.assertEqual([x['attachmentUrl'] for x in sent if x.get('attachmentType')=='image'],[self.catalog[1]['image_url']])
        self.vision.assert_not_called()

    def test_unknown_ad_then_saved_alias_recovers_on_customer_followup_without_vision(self):
        self.catalog[1]['ad_id']='original'
        _,sent=self.turn('ما هي مواصفات القطعة؟',ad=True)
        self.assertEqual(sent,[])
        self.catalog[1]['ad_ids']=['123']
        result,sent=self.turn('شكد سعره؟')
        self.assertTrue(result['ok'])
        self.assertRegex(' '.join(x.get('message','') for x in sent), r'16(?: ألف|,000)')
        self.assertFalse(any('الإعلان' in x.get('message','') for x in sent))
        self.vision.assert_not_called()

    def test_only_automatic_ad_pause_recovers_while_manual_and_other_work_stay_paused(self):
        for pause,extra,should_send in [('image_unresolved',False,True),('manual',False,False),
                                       ('customer_requested_human',False,False),('image_unresolved',True,False)]:
            with self.subTest(pause=pause,extra=extra):
                self.catalog[1]['ad_id']='original'; self.catalog[1]['ad_ids']=[]
                # New thread identity for each independent pause scenario.
                participant='person-'+pause+str(extra)
                self.payload['conversation'].update(id='thread-'+participant,participantId=participant)
                self.payload['message']['sender']['id']=participant
                job,payload=self.message('شكد سعره؟',ad=True)
                self.run_job(job)
                with m.app.app_context():
                    db=m.get_db()
                    sender=db.execute('SELECT sender_id FROM zernio_conversations WHERE participant_id=?',(participant,)).fetchone()[0]
                    m.set_customer_ai_enabled(db,sender,False,reason=pause)
                    if extra:m.create_human_review(db,dict(sender_id=sender),'شكوى على التوصيل',notify_telegram=False)
                self.catalog[1]['ad_ids']=['123']
                _,sent=self.turn('بعده متوفر؟')
                self.assertEqual(bool(sent),should_send)
        self.vision.assert_not_called()

    def test_explicit_different_product_wins_over_new_alias(self):
        self.catalog[1].update(ad_id='original',ad_ids=['123'])
        result,sent=self.turn('اريد تنورة سادة',ad=True)
        self.assertTrue(result['ok'])
        self.assertTrue(any('12' in x.get('message','') for x in sent))
        self.assertFalse(any('16' in x.get('message','') for x in sent))
        self.vision.assert_not_called()
