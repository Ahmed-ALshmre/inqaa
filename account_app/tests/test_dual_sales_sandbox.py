import tempfile
import unittest
from pathlib import Path

from tools.dual_sales_sandbox import SalesSandbox


class SandboxPricingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        case = dict(id='test', products=[dict(product_id='T1',price='24000',colors='اسود نيلي',sizes='من 38 الى 48',stock_quantity='5',image_colors=['اسود','نيلي'])],
                    policy={'التوصيل_لبغداد':5000,'التوصيل_لبقية_المحافظات':5000})
        self.s = SalesSandbox(Path(self.temp.name)/'test.sqlite',[case],'CONFIRM')
        self.cart(quantity=2)

    def tearDown(self):
        self.temp.cleanup()

    def execute(self, name, **args):
        return self.s.execute(name,args,'test')

    def cart(self, quantity=2):
        return self.execute('update_cart',items=[dict(product_id='T1',color='اسود',size='44',quantity=quantity)],phone='07700000000',province='بغداد',address='حي الاختبار قرب المدرسة')

    def offer(self):
        self.s.latest['test']='سويهن بخمسين واصلات'
        return self.execute('negotiate_price',requested_total=50000,requested_discount=None,request_text=self.s.latest['test'])

    def accept(self, text='زين موافقة على 51'):
        self.s.latest['test']=text
        q=self.execute('quote_cart')
        return self.execute('accept_price',quote_fingerprint=q['quote_fingerprint'],evidence=text)

    def commit(self):
        return self.execute('commit_order',cart_version=self.execute('get_state')['cart_version'])

    def test_save_uses_net_price_and_persists_absolute_discount(self):
        self.assertEqual(self.offer()['total_amount'],51000)
        self.assertTrue(self.accept()['ok'])
        saved=self.commit()
        self.assertTrue(saved['ok'])
        self.assertEqual((saved['order']['product_total'],saved['order']['negotiated_discount'],saved['order']['total_amount']),(48000,2000,51000))
        self.assertTrue(self.commit()['already_saved'])

    def test_data_with_conditional_rejected_price_does_not_book(self):
        self.offer()
        self.assertFalse(self.accept('ما اخذهن الا بخمسين، ثبتي ورقمي07700000000')['ok'])
        self.assertEqual(self.commit()['error'],'price_acceptance_required')

    def test_implicit_contact_acceptance_only_after_quote_was_shown(self):
        q=self.offer()
        self.assertFalse(self.accept('07700000000')['ok'])
        self.s.mark_presented('test',q)
        self.assertTrue(self.accept('07700000000')['ok'])

    def test_contact_with_new_discount_request_is_not_implicit_acceptance(self):
        q=self.offer();self.s.mark_presented('test',q)
        self.assertFalse(self.accept('نزلي ألف ثاني وهذا رقمي07700000000')['ok'])
        self.assertEqual(self.commit()['error'],'price_acceptance_required')

    def test_discount_does_not_return_after_downgrade_without_new_request(self):
        self.offer();self.accept();self.cart(quantity=1)
        self.assertEqual(self.execute('quote_cart')['total_amount'],29000)
        self.assertEqual(self.commit()['error'],'price_acceptance_required')
        self.cart(quantity=2)
        self.assertEqual(self.execute('quote_cart')['total_amount'],53000)

    def test_new_quote_acceptance_is_needed_after_catalog_change(self):
        self.offer();self.accept()
        self.s.cases['test']['products'][0]['price']='25000'
        self.assertEqual(self.commit()['error'],'price_acceptance_required')

    def test_reopening_bargain_invalidates_old_acceptance(self):
        self.offer();self.accept()
        self.s.latest['test']='خليهن بخمسين شيلي الألف الباقي'
        q=self.execute('negotiate_price',requested_total=50000,requested_discount=None,request_text=self.s.latest['test'])
        self.assertEqual(q['total_amount'],51000)
        self.assertEqual(self.commit()['error'],'price_acceptance_required')

    def test_discount_request_evidence_required(self):
        self.s.latest['test']='شكد السعر؟'
        result=self.execute('negotiate_price',requested_total=None,requested_discount=2000,request_text='شكد السعر؟')
        self.assertEqual(result['error'],'customer_discount_request_required')

    def test_iraqi_discount_request_and_explicit_total_mismatch(self):
        self.s.latest['test']='نزلي بس ألف من حسابهن'
        q=self.execute('negotiate_price',requested_total=None,requested_discount=1000,request_text=self.s.latest['test'])
        self.assertEqual(q['total_amount'],52000)
        self.assertFalse(self.accept('تمام على51 ثبتي')['ok'])
        self.assertTrue(self.accept('تمام على52 ثبتي')['ok'])

    def test_quote_preview_does_not_change_choices_or_apply_cart_discount(self):
        self.offer()
        before=self.execute('get_state')
        q=self.execute('quote_product',product_id='T1',quantity=1,province='بغداد')
        self.assertEqual(q['total_amount'],29000)
        self.assertTrue(q['preview_only'])
        self.assertEqual(before,self.execute('get_state'))

    def test_preview_price_survives_variant_selection_and_data_reply(self):
        preview=self.execute('quote_product',product_id='T1',quantity=1,province='بغداد')
        self.s.mark_presented('test',preview)
        self.cart(quantity=1)
        self.assertTrue(self.execute('get_state')['price_presented'])
        self.assertTrue(self.accept('07700000000')['ok'])
        self.assertTrue(self.commit()['ok'])

    def test_same_price_variant_change_keeps_price_agreement(self):
        self.offer();self.accept()
        self.execute('update_cart',items=[dict(product_id='T1',color='نيلي',size='46',quantity=2)],phone=None,province=None,address=None)
        self.assertTrue(self.execute('get_state')['price_accepted'])
        self.assertTrue(self.commit()['ok'])

    def test_interpretation_survives_handoff_without_becoming_cart_truth(self):
        self.s.latest['test']='قياسي44 ومحتارة بين الأسود والجوزي وأريده للدوام'
        self.s.remember_understanding('test',dict(occasion='الدوام',preferences=['قياس44','أسود أو جوزي']))
        self.execute('handoff_to_staff',reason='سؤال قماش غير موثق',question='هل يشف؟')
        state=self.execute('get_state')
        self.assertEqual(state['conversation_understanding']['occasion'],'الدوام')
        self.assertEqual(state['conversation_understanding']['last_customer_message'],self.s.latest['test'])
        self.assertTrue(state['review_pending'])
        self.assertIsNone(state['order'])


if __name__=='__main__':unittest.main()
