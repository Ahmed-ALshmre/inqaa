"""Privacy and fixture isolation checks for offline customer-text replay tooling."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tools.evaluate_live_customer_dialogues import scrub,safe_product,initialize_test_db
from account_app.checkout import phone_number


class LiveDialogueQATests(unittest.TestCase):
    def test_identifiers_removed_but_weight_and_size_remain(self):
        row=dict(name='رؤى الدليمي',phone='07700987654',address='بغداد الكرادة قرب المدرسة',province='بغداد')
        text='اسمي رؤى الدليمي\nبغداد الكرادة قرب المدرسة\n07700987654\nوزني 130 وقياسي 60'
        result=scrub(text,row,1)
        self.assertNotIn('رؤى',result)
        self.assertNotIn('الكرادة',result)
        self.assertNotIn('07700987654',result)
        self.assertIn('130',result)
        self.assertIn('60',result)
        self.assertIsNotNone(phone_number(result))

    def test_invalid_phone_does_not_become_valid_test_contact(self):
        result=scrub('07790000',{},2)
        self.assertFalse(phone_number(result))

    def test_shipping_question_is_not_replaced_with_an_address(self):
        result=scrub('شكد التوصيل لبغداد',dict(province='بغداد'),3)
        self.assertIn('شكد',result)
        self.assertNotIn('مدرسة التجربة',result)

    def test_no_media_urls_in_product_snapshot(self):
        product=dict(product_id='P1',image_url='https://example.test/customer.jpg',
                     video_url='https://example.test/video.mp4',image_colors={'أزرق':'https://example.test/a.jpg'},
                     notes='تفاصيل https://example.test/private',price='12000')
        result=safe_product(product)
        self.assertNotIn('https://',str(result))
        self.assertEqual('12000',result['price'])

    def test_only_disposable_schema_is_initialized(self):
        with tempfile.TemporaryDirectory() as root:
            db=initialize_test_db(str(Path(root)/'test.db'))
            self.assertEqual(0,db.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
            columns={r[1] for r in db.execute('PRAGMA table_info(orders)')}
            self.assertIn('store_id',columns)
            self.assertIn('total_amount',columns)
            db.close()


if __name__=='__main__':unittest.main()
