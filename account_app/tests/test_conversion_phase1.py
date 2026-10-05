import unittest
from unittest.mock import patch
from account_app import sales_context, conversation_quality


class ConversionPhaseOneTests(unittest.TestCase):
    def test_discovery_does_not_loop_requesting_unknown_product_photo(self):
        from account_app import app
        products = [{'product_id': 'F1', 'product_name': 'فستان', 'price': '20000',
                     'stock': 'متوفر', 'status': 'active'}]
        with patch.object(app, '_call_main_ai_once', return_value={'reply': 'دزيلي صورة الموديل'}):
            result = app.call_main_ai({'text': 'اي موديل سعره عشرين اكو؟'}, 'text', {},
                                      [], products, None, None, '', [])
        self.assertIn('فستان', result['reply'])
        self.assertIn('20,000', result['reply'])
        self.assertFalse(result['create_order'])

    def test_discovery_budget_ranks_within_budget_before_expensive(self):
        products = [{'price': '25000'}, {'price': '10000'}, {'price': '20000'}]
        self.assertTrue(sales_context.browse_request('اي موديل سعره عشرين اكو؟'))
        self.assertEqual(sales_context.browse_candidates('اي موديل سعره عشرين اكو؟', products),
                         [products[2], products[1], products[0]])

    def test_arabic_digit_budget(self):
        products = [{'price': '30000'}, {'price': '15000'}]
        self.assertEqual(sales_context.browse_candidates('بحدود ٢٠ الف', products)[0], products[1])

    def test_correction_and_question_survive_history(self):
        state = sales_context.advance({}, [
            {'direction': 'outgoing', 'text': 'السوت متوفر'},
            {'id': 2, 'direction': 'incoming', 'text': 'لا اقصد الفستان'},
            {'id': 3, 'direction': 'incoming', 'text': 'الاكتاف مبطنة؟'}])
        self.assertEqual(state['corrections'][0]['message_id'], 2)
        self.assertEqual(state['latest_customer_turn']['message_id'], 3)
        self.assertEqual(state['version'], 3)

    def test_documented_new_measurements_accepted_unknown_rejected(self):
        self.assertEqual(conversation_quality.grounded_error('طوله 135 سم',
                         {'measurements': 'طول 135 سم'}, []), '')
        self.assertTrue(conversation_quality.grounded_error('طوله 135 سم', {'product_name': 'فستان'}, []))

    def test_product_roundtrip_preserves_new_fields_and_old_defaults(self):
        from account_app import app
        raw = {'product_id': 'P1', 'store_id': 'other', 'lining': 'مبطنة',
               'measurements': 'طول 135 سم', 'bundle_contents': 'قطعتان'}
        product = app._normalize_product(raw)
        self.assertEqual(product['store_id'], 'other')
        self.assertEqual(product['lining'], raw['lining'])
        self.assertEqual(product['measurements'], raw['measurements'])
        self.assertEqual(product['bundle_contents'], raw['bundle_contents'])
        self.assertEqual(product['faq'], '')
