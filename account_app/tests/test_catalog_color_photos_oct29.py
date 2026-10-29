"""Color photo requests keep the known model and send an honest catalog album."""
import unittest
from unittest.mock import patch
from account_app.tests import test_checkout_regressions as fixtures
from account_app.tests import test_checkout_completion_oct25 as completion


class CatalogColorPhotoTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.CheckoutRegressionTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.CheckoutRegressionTests.tearDownClass.__func__)
    tearDown=fixtures.CheckoutRegressionTests.tearDown
    process=completion.CheckoutCompletionTests.process

    def setUp(self):
        fixtures.CheckoutRegressionTests.setUp(self)
        self.m._current_store_id.reset(self.token)
        self.token=self.m._current_store_id.set('baraah-kids')
        self.db.execute('UPDATE customers SET store_id=? WHERE sender_id=?',('baraah-kids',self.sender))
        self.db.commit();self.ev['store_id']='baraah-kids'
        self.urls=['https://images.test/princess-'+str(i)+'.jpg' for i in range(4)]
        self.product=dict(product_id='C1',product_name='فستان الاميرات',colors='تركوازي كريمي وردي',
                          sizes='من 2 الى 7 سنوات',price='15000',stock='متوفر',status='active',
                          image_url=self.urls,image_colors={})
        self.catalog=[self.product]
        for name in ('load_products_from_file','load_active_products'):
            p=patch.object(self.m,name,side_effect=lambda *a,**k:self.catalog);p.start();self.patches.append(p)
        self.m.bind_customer_to_product(self.db,self.sender,self.product,source='image_recognition')
        self.save('incoming','عمرها 4 سنوات')
        self.save('outgoing','فستان الاميرات متوفر بالتركوازي والكريمي والوردي، يا لون يعجبج؟')

    def save(self,direction,text):
        self.m.save_message(self.db,self.sender,direction,'text',text,None,None,None,{})

    def test_available_or_missing_color_sends_model_album_without_reasking_quantity(self):
        result,model=self.process('ممكن اشوف صور الوردي او الاحمر\nدزيلي صورهن عيني')
        model.assert_not_called()
        self.assertEqual(result['image_urls'],self.urls)
        self.assertIn('الأحمر مو متوفر',result['reply'])
        self.assertIn('صور الموديل',result['reply'])
        self.assertNotIn('قطعة من كل لون',result['reply'])
        self.assertNotIn('ما عندي صورة مطابقة',result['reply'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0],0)

    def test_labelled_requested_color_sends_only_that_color(self):
        self.product['image_colors']={self.urls[0]:'وردي',self.urls[1]:'كريمي',self.urls[2]:'تركوازي',self.urls[3]:'وردي'}
        result,model=self.process('اشوف الوردي او الاحمر')
        model.assert_not_called();self.assertEqual(result['image_urls'],[self.urls[0],self.urls[3]])
        self.assertIn('الأحمر مو متوفر',result['reply'])

    def test_only_unavailable_color_does_not_send_other_color_as_requested(self):
        result,model=self.process('ممكن اشوف صورة الاحمر')
        model.assert_not_called();self.assertFalse(result['send_image'])
        self.assertIn('الأحمر مو متوفر',result['reply']);self.assertIn('الوردي',result['reply'])

    def test_partial_labels_can_show_general_model_album_without_claiming_exact_color(self):
        self.product['image_colors']={self.urls[0]:'كريمي'}
        result,model=self.process('ممكن اشوف صور الوردي')
        model.assert_not_called();self.assertEqual(result['image_urls'],self.urls)
        self.assertIn('صورة منفصلة',result['reply']);self.assertIn('صور الموديل',result['reply'])

    def test_simple_followup_uses_known_album_after_old_photo_failure(self):
        self.save('outgoing','ما عندي صورة مطابقة جاهزة لهذا اللون حالياً.')
        result,model=self.process('دزيلي الصور عيني')
        model.assert_not_called();self.assertEqual(result['image_urls'],self.urls)

    def test_browsing_two_available_colors_does_not_request_purchase_quantity(self):
        result,model=self.process('اشوف صور الوردي والكريمي')
        model.assert_not_called();self.assertEqual(result['image_urls'],self.urls)
        self.assertNotIn('قطعة من كل لون',result['reply'])

    def test_strict_color_selector_stays_strict_outside_general_album(self):
        self.assertEqual(self.m.select_product_image_urls(self.product,'وردي'),[])
        result={'reply':'هاي صورة الوردي','image_product_ids':['C1'],'image_color':'وردي'}
        self.assertEqual(self.m.requested_reply_images(result,self.catalog,self.product,'اشوف الوردي',result['reply']),[])

    def test_mixed_fabric_question_does_not_enter_photo_only_shortcut(self):
        ev=dict(self.ev,text='شنو قماشه وممكن اشوف صور الوردي',_sales_context={})
        self.assertIsNone(self.m.offered_product_photo_reply(ev,self.catalog,self.product))

    def test_additional_real_photography_is_not_replaced_by_catalog_album(self):
        ev=dict(self.ev,text='ممكن صور حقيقية من المحل بالوردي',_sales_context={})
        self.assertIsNone(self.m.offered_product_photo_reply(ev,self.catalog,self.product))

    def test_missing_or_sold_out_album_is_not_promised(self):
        for change in ({'image_url':[]},{'stock':'نفذ'}):
            p=dict(self.product,**change)
            self.assertIsNone(self.m.offered_product_photo_reply(dict(self.ev,text='اشوف صور الوردي'),[p],p))

    def test_rejected_model_is_not_restored_by_a_short_photo_request(self):
        ev=dict(self.ev,text='اشوف صور الوردي',_sales_context={'offer_interrupted':True})
        self.assertIsNone(self.m.offered_product_photo_reply(ev,self.catalog,self.product))

    def test_identity_question_routes_to_human_review_per_owner_instruction(self):
        self.assertTrue(self.m.sales_playbook.human_requested('انتي مساعدة آلية لو موظفة حقيقية؟'))

    def offer_two_models(self):
        other=dict(self.product,product_id='C2',product_name='طقم بناتي',colors='أسود',
                   image_url=['https://images.test/suit.jpg'])
        self.catalog.append(other)
        self.save('incoming','فستان الاميرات والطقم')
        self.save('outgoing','فستان الاميرات متوفر بالوردي والكريمي. وطقم بناتي متوفر بالأسود.')
        self.save('outgoing','يا لون يعجبج بفستان الاميرات؟')

    def test_color_photo_answer_follows_last_question_among_two_offered_models(self):
        self.offer_two_models()
        result,model=self.process('ممكن اشوف صور الوردي او الاحمر\nدزيلي صورهن عيني')
        self.assertEqual(model.call_count,0,result);self.assertEqual(result.get('image_urls'),self.urls,result)
        self.assertIn('فستان الاميرات',result['reply']);self.assertNotIn('طقم بناتي',result['reply'])

    def test_explicit_all_photos_followup_keeps_both_offered_models(self):
        self.offer_two_models()
        result,model=self.process('دزيلي صورهن كلهن')
        model.assert_not_called()
        self.assertEqual(set(result.get('image_urls') or []),{self.urls[0],'https://images.test/suit.jpg'},result)
        self.assertIn('صورة لكل موديل',result['reply'])
        self.assertFalse(result.get('meta',{}).get('order_created'))

    def test_preview_does_not_claim_each_model_when_only_an_unavailable_color_has_a_photo(self):
        self.offer_two_models()
        other=self.catalog[1]
        other['image_colors']={other['image_url'][0]:'وردي'}  # Only black is currently sold.
        ev=dict(self.ev,text='دزيلي صورهن كلهن',_sales_context={
            'last_staff_turn':[{'text':'فستان الاميرات وطقم بناتي متوفرات، تحبين صورهن؟'}]})
        self.assertIsNone(self.m.offered_product_photo_reply(ev,self.catalog,self.product))
