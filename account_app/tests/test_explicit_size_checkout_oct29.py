"""A customer's garment size completes checkout without a mandatory body weight."""
import json
import unittest
from account_app import product_variants, sales_context, sales_playbook
from account_app.tests import test_weight_color_checkout_oct29 as fixtures


class SizeWordsTests(unittest.TestCase):
    def test_iraqi_plural_and_written_variants_are_sizes_not_quantities(self):
        for value,expected in [('ثلاث اكسات','3XL'),('ثلاثة اكس','3XL'),('ثلاثه اكس لارج','3XL'),
                               ('اربعة اكسات','4XL'),('خمسة اكسات','5XL'),('دبل اكس لارج','XXL')]:
            self.assertEqual(product_variants.requested(value)['sizes'],[expected],value)
        self.assertEqual(product_variants.requested('ثلاث قطع')['sizes'],[])

    def test_label_and_two_colors_in_one_bubble_are_all_retained(self):
        context=sales_context.advance({},[{'direction':'incoming','text':'ثبتي ثلاث اكسات ماروني ورمادي قطعة من كل لون'}])
        choice=sales_playbook.customer_selection(context)
        self.assertEqual(choice['size'],'3XL');self.assertEqual(len(choice['colors']),2)


class ExplicitSizeCheckoutTests(unittest.TestCase):
    setUpClass=classmethod(fixtures.WeightColorCheckoutTests.setUpClass.__func__)
    tearDownClass=classmethod(fixtures.WeightColorCheckoutTests.tearDownClass.__func__)
    setUp=fixtures.WeightColorCheckoutTests.setUp
    tearDown=fixtures.WeightColorCheckoutTests.tearDown
    save=fixtures.WeightColorCheckoutTests.save
    process=fixtures.WeightColorCheckoutTests.process
    order_count=fixtures.WeightColorCheckoutTests.order_count
    assert_confirmation=fixtures.WeightColorCheckoutTests.assert_confirmation

    def items(self):
        return json.loads(self.db.execute('SELECT order_items FROM orders WHERE sender_id=?',(self.sender,)).fetchone()[0])

    def test_final_iraqi_size_after_contacts_books_both_colors_once(self):
        self.save('incoming','وزني 80');self.save('incoming','ماروني ورمادي قطعة من كل لون')
        self.save('outgoing','دزيلي رقم الموبايل والمحافظة والعنوان')
        self.save('incoming','07701234567 بغداد حي الاختبار')
        self.m.capture_customer_contact(self.db,self.sender,'07701234567 بغداد حي الاختبار')
        self.save('outgoing','شكد قياس تلبسين؟')
        result,model=self.process('ثلاث اكسات')
        model.assert_not_called();self.assert_confirmation(result)
        items=self.items();self.assertEqual(len(items),2)
        self.assertTrue(all(i['size']=='3XL' and i['size_type']=='size' and not i.get('weight') for i in items))
        self.process('ثلاث اكسات');self.assertEqual(self.order_count(),1)

    def test_explicit_label_two_colors_and_contacts_need_no_weight(self):
        self.save('incoming','ثبتي ثلاث اكسات ماروني ورمادي قطعة من كل لون')
        self.save('outgoing','دزيلي رقم الموبايل والمحافظة والعنوان')
        result,model=self.process('07701234567 بغداد حي الاختبار')
        model.assert_not_called();self.assert_confirmation(result)
        self.assertEqual(len(self.items()),2)
        self.assertTrue(all(i['size']=='3XL' and not i.get('weight') for i in self.items()))

    def test_single_label_keeps_cart_while_collecting_split_contact(self):
        self.product['colors']='ماروني'
        self.save('incoming','اريد دبل اكس ماروني')
        self.save('outgoing','دزيلي رقم الموبايل والمحافظة والعنوان')
        result,model=self.process('07701234567')
        model.assert_not_called();self.assertNotIn('وزن',result['reply']);self.assertNotIn('قياس',result['reply'])
        result,model=self.process('بغداد حي الاختبار')
        model.assert_not_called();self.assert_confirmation(result)
        self.assertEqual(self.items()[0]['size'],'XXL')

    def test_real_inventory_does_not_expand_to_unsupported_size(self):
        self.product['sizes']='XL فقط، يناسب من 55 كيلو الى 90'
        self.save('incoming','ثبتي ثلاث اكسات ماروني')
        self.process('07701234567 بغداد حي الاختبار')
        self.assertEqual(self.order_count(),0)

    def test_final_size_does_not_bypass_unresolved_arrival_condition(self):
        self.save('incoming','ماروني ورمادي قطعة من كل لون')
        self.save('incoming','اذا يوصل باجر ثبتي')
        self.save('outgoing','ما أگدر أضمن الموعد.')
        self.save('incoming','07701234567 بغداد حي الاختبار')
        self.m.capture_customer_contact(self.db,self.sender,'07701234567 بغداد حي الاختبار')
        self.save('outgoing','شنو القياس المطلوب؟')
        self.process('ثلاث اكسات');self.assertEqual(self.order_count(),0)

    def test_final_size_does_not_guess_quantities_of_two_colors(self):
        self.save('incoming','الماروني والرمادي')
        self.save('outgoing','دزيلي رقم الموبايل والمحافظة والعنوان')
        self.save('incoming','07701234567 بغداد حي الاختبار')
        self.m.capture_customer_contact(self.db,self.sender,'07701234567 بغداد حي الاختبار')
        self.save('outgoing','شنو القياس المطلوب؟')
        result,model=self.process('ثلاث اكسات')
        model.assert_not_called();self.assertEqual(self.order_count(),0)
        self.assertIn('قطعة من كل لون',result['reply']);self.assertNotIn('وزن',result['reply'])

    def test_missing_choice_asks_size_instead_of_requiring_numeric_weight(self):
        result,model=self.process('اريد هذا الموديل')
        model.assert_not_called();self.assertIn('القياس المطلوب',result['reply'])
        self.assertNotIn('كيلو',result['reply']);self.assertNotIn('برقم واحد',result['reply'])
        self.assertEqual(self.order_count(),0)

    def test_explicit_label_does_not_turn_an_outside_weight_into_a_fit_claim(self):
        self.product['colors']='ماروني'
        result,_=self.process('وزني 120 وقياسي ثلاث اكسات')
        self.assertNotIn('هالوزن ضمن المجال',result['reply'])
        self.assertEqual(self.order_count(),0)
