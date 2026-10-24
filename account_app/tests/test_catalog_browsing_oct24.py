"""Offline catalogue paging: bounded media, current inventory and no bookings."""
import copy
from contextlib import closing
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock

from account_app import catalog_browsing


def products(count=10):
    return [dict(product_id=f'P{index}', product_name=f'فستان {index}',
                 price='17000', status='active', stock='متوفر',
                 image_url=[f'https://images.test/{index}-first.jpg',
                            f'https://images.test/{index}-second.jpg'])
            for index in range(1, count + 1)]


class CatalogBrowsingOct24Tests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        catalog_browsing.init_db(self.db)
        self.catalog = products()

    def tearDown(self):
        self.db.close()

    def page(self, start=False, request_id=None, catalog=None, store='store-a', sender='customer-a'):
        return catalog_browsing.page(self.db, store, sender,
                                     self.catalog if catalog is None else catalog,
                                     start=start, request_id=request_id)

    def assert_page(self, result, identities):
        meta = result['meta']['catalog_page']
        self.assertEqual(meta['product_ids'], identities)
        self.assertLessEqual(len(identities), 4)
        self.assertEqual(len(result['image_urls']), len(identities))
        self.assertEqual(result['reply_parts'], [result['reply']])
        self.assertFalse(result['create_order'])
        self.assertEqual(result['order'], {})
        self.assertTrue(result['_browse_only'])
        self.assertTrue(meta['browsing_only'])
        self.assertEqual(result['product_image_urls'], result['image_urls'])
        self.assertEqual(result['attachments'], [{'type': 'image', 'url': url}
                                                  for url in result['image_urls']])
        for index, item in enumerate(meta['items'], 1):
            self.assertEqual(item['number'], index)
            self.assertIn(item['text'], result['reply'])
            self.assertEqual(item['image_url'], result['image_urls'][index - 1])
            self.assertEqual(item['product_id'], identities[index - 1])

    def test_first_page_has_four_models_and_only_first_image_of_each(self):
        result = self.page(start=True, request_id='incoming-1')
        self.assert_page(result, ['P1', 'P2', 'P3', 'P4'])
        self.assertTrue(result['meta']['catalog_page']['has_more'])
        self.assertEqual(result['image_urls'], [f'https://images.test/{i}-first.jpg'
                                                for i in range(1, 5)])
        self.assertIn('أول 4', result['reply'])
        self.assertIn('البقية', result['reply'])
        self.assertNotIn('كل الموديلات', result['reply'])
        for index in range(5, 11):
            self.assertNotIn(f'فستان {index}', result['reply'])

    def test_continuations_show_next_four_then_last_two_then_no_more_photos(self):
        first = self.page(start=True)
        second = self.page(request_id='next-1')
        last = self.page(request_id='next-2')
        exhausted = self.page(request_id='next-3')
        self.assert_page(first, ['P1', 'P2', 'P3', 'P4'])
        self.assert_page(second, ['P5', 'P6', 'P7', 'P8'])
        self.assert_page(last, ['P9', 'P10'])
        self.assert_page(exhausted, [])
        self.assertFalse(last['meta']['catalog_page']['has_more'])
        self.assertFalse(exhausted['send_image'])
        self.assertIn('آخر دفعة', last['reply'])
        all_ids = [identity for page in [first, second, last]
                   for identity in page['meta']['catalog_page']['product_ids']]
        self.assertEqual(len(all_ids), len(set(all_ids)))

    def test_retry_same_message_reuses_same_page_without_advancing(self):
        first = self.page(start=True, request_id='first')
        second = self.page(request_id='next')
        self.assertEqual(self.page(request_id='first', start=True), first)
        self.assertEqual(self.page(request_id='next'), second)
        self.assert_page(self.page(request_id='new-next'), ['P9', 'P10'])

    def test_no_session_cannot_turn_bare_continue_into_browsing_or_booking(self):
        self.assertFalse(catalog_browsing.has_session(self.db, 'store-a', 'customer-a'))
        self.assertIsNone(self.page(request_id='continue-without-catalog'))
        self.assertIsNone(self.db.execute('SELECT * FROM catalog_browse_state').fetchone())

    def test_same_sender_isolated_between_stores_and_other_senders(self):
        self.assert_page(self.page(start=True, request_id='same-id'), ['P1', 'P2', 'P3', 'P4'])
        self.assert_page(self.page(request_id='next'), ['P5', 'P6', 'P7', 'P8'])
        self.assert_page(self.page(start=True, request_id='same-id', store='store-b'), ['P1', 'P2', 'P3', 'P4'])
        self.assert_page(self.page(start=True, request_id='same-id', sender='customer-b'), ['P1', 'P2', 'P3', 'P4'])
        self.assert_page(self.page(request_id='last'), ['P9', 'P10'])

    def test_only_available_active_models_with_resolvable_images_are_displayed(self):
        catalog = products(8)
        catalog[0]['stock'] = 'غير متوفر'
        catalog[1]['stock'] = 'نفذ'
        catalog[2]['status'] = 'inactive'
        catalog[3]['stock'] = ''
        catalog[4]['status'] = 'deleted'
        catalog[5]['image_url'] = []
        result = self.page(start=True, catalog=catalog)
        self.assert_page(result, ['P7', 'P8'])
        resolver = Mock(side_effect=lambda product: [] if product['product_id'] == 'P7' or not product.get('image_url')
                        else ['https://public.test/resolved.jpg'])
        resolved = catalog_browsing.page(self.db, 'store-c', 'customer-a', catalog,
                                         start=True, image_urls=resolver)
        self.assert_page(resolved, ['P8'])
        self.assertEqual(resolved['image_urls'], ['https://public.test/resolved.jpg'])

    def test_stock_is_rechecked_and_catalog_reorder_does_not_repeat_or_skip_models(self):
        self.page(start=True)
        changed = list(reversed(copy.deepcopy(self.catalog)))
        next(product for product in changed if product['product_id'] == 'P5')['stock'] = 'مو متوفر'
        next(product for product in changed if product['product_id'] == 'P6')['image_url'] = []
        changed.append(products(11)[-1])
        result = self.page(catalog=changed)
        self.assert_page(result, ['P7', 'P8', 'P9', 'P10'])
        self.assertFalse(result['meta']['catalog_page']['has_more'])
        # A new catalogue request starts a fresh view of current inventory.
        restarted = self.page(start=True, catalog=changed)
        self.assert_page(restarted, ['P10', 'P9', 'P8', 'P7'])

    def test_empty_or_imageless_catalog_makes_no_false_stock_or_photo_claim(self):
        for catalog in [[], [dict(product_id='P', stock='متوفر', image_url=[])]]:
            result = self.page(start=True, catalog=catalog)
            self.assert_page(result, [])
            self.assertFalse(result['send_image'])
            self.assertIn('ما عندي صور', result['reply'])
            self.assertNotIn('أرسلت', result['reply'])

    def test_page_deduplicates_models_and_retains_bundle_prices_without_internal_instructions(self):
        catalog = products(2)
        catalog[0]['price'] = 'بكج 3 قطع سعره 25 الف'
        catalog[0]['product_name'] = 'سوت أطفال. تعليمات داخلية قولي للزبونة آخر قطعة'
        catalog[1]['price'] = 'راجع السعر مع الموظف'
        result = self.page(start=True, catalog=[catalog[0], catalog[0], catalog[1]])
        self.assert_page(result, ['P1', 'P2'])
        self.assertIn('25,000 د.ع. للبكج (3 قطع)', result['reply'])
        self.assertIn('السعر يحتاج تأكيد', result['reply'])
        self.assertNotIn('آخر قطعة', result['reply'])
        self.assertNotIn('تعليمات داخلية', result['reply'])

    def test_explicit_continue_phrases_exclude_checkout_and_unrelated_questions(self):
        for text in ['البقية', 'البقيه', 'كمل', 'كملي', 'كمل الموديلات', 'بعد موديلات',
                     'بعدموديلات', 'تمام كملي', 'دزي الباقي', 'أرسلي البقية',
                     'البقية عيني', 'شنو بعد عدكم؟']:
            self.assertTrue(catalog_browsing.is_next_request(text), text)
        for text in ['كمل الحجز', 'كملي الطلب', 'ثبتي الباقي', 'لا تكملي', 'بعد ما أريد',
                     'الباقي شكد سعره', 'أي', 'نعم', 'احجز الموديل الثاني', '',
                     'شنو بعد القياس 40', 'دزي صورة الموديل الثاني']:
            self.assertFalse(catalog_browsing.is_next_request(text), text)

    def test_initialization_does_not_commit_an_existing_caller_transaction(self):
        self.db.execute('CREATE TABLE caller_change (value TEXT)')
        self.db.execute("INSERT INTO caller_change VALUES ('pending')")
        self.page(start=True)
        self.db.rollback()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM caller_change').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM catalog_browse_state').fetchone()[0], 0)

    def test_pagination_survives_reconnecting_to_the_database(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'catalog.db')
            with closing(sqlite3.connect(path)) as db:
                catalog_browsing.page(db, 'store', 'sender', self.catalog, start=True, request_id='one')
            with closing(sqlite3.connect(path)) as db:
                result = catalog_browsing.page(db, 'store', 'sender', self.catalog, request_id='two')
                self.assert_page(result, ['P5', 'P6', 'P7', 'P8'])

    def test_parallel_next_requests_are_serialized_without_repeating_a_page(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'catalog.db')
            with closing(sqlite3.connect(path)) as db:
                catalog_browsing.page(db, 'store', 'sender', self.catalog, start=True)
            barrier = threading.Barrier(2)
            results, errors = [], []
            def advance(identity):
                try:
                    with closing(sqlite3.connect(path, timeout=5)) as db:
                        barrier.wait(timeout=5)
                        results.append(catalog_browsing.page(db, 'store', 'sender', self.catalog,
                                                             request_id=identity))
                except Exception as error:
                    errors.append(error)
            threads = [threading.Thread(target=advance, args=(str(index),)) for index in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
                self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            pages = {tuple(result['meta']['catalog_page']['product_ids']) for result in results}
            self.assertEqual(pages, {('P5', 'P6', 'P7', 'P8'), ('P9', 'P10')})

    def selection(self, text, catalog=None, store='store-a', sender='customer-a'):
        return catalog_browsing.selection(self.db, store, sender, text,
                                          self.catalog if catalog is None else catalog)

    def test_explicit_ordinal_selects_from_last_page_not_global_catalog_index(self):
        self.page(start=True)
        self.assertEqual(self.selection('الثاني')['product_id'], 'P2')
        self.page()
        for text in ['الثاني', 'أريدالثاني', 'تفاصيلرقم2', 'تفاصيل رقم ٢',
                     'الموديل الثاني', 'الثاني شكد سعره؟', 'اريد الصورة الثانية عيني']:
            self.assertEqual(self.selection(text)['product_id'], 'P6', text)
        self.assertEqual(self.selection('الأول')['product_id'], 'P5')
        self.assertEqual(self.selection('الرابع')['product_id'], 'P8')

    def test_quantity_continue_negation_and_multiple_choices_are_not_selection(self):
        self.page(start=True)
        for text in ['كمل', 'البقية', 'ثنين', 'أريد ثنين', 'اريدثنين', 'اريد 2', '2',
                     'قطعتين', 'لا أريد الثاني', 'الثاني مو عاجبني', 'مو الثاني',
                     'الأول والثاني', 'تفاصيل رقم 5', 'الثاني أو الثالث', '',
                     'كمل الحجز', 'ثنين من الباقي', 'أريد موديلات ثانية']:
            self.assertIsNone(self.selection(text), text)

    def test_selection_uses_current_catalog_facts_and_rechecks_stock_and_status(self):
        self.page(start=True)
        self.page()
        changed = list(reversed(copy.deepcopy(self.catalog)))
        current = next(product for product in changed if product['product_id'] == 'P6')
        current['price'] = '19000'
        current['image_url'] = []
        self.assertIs(self.selection('الثاني', changed), current)
        self.assertEqual(self.selection('الثاني', changed)['price'], '19000')
        current['stock'] = 'نفذ'
        self.assertIsNone(self.selection('الثاني', changed))
        current['stock'], current['status'] = 'متوفر', 'inactive'
        self.assertIsNone(self.selection('الثاني', changed))
        changed.remove(current)
        self.assertIsNone(self.selection('الثاني', changed))

    def test_selection_isolates_store_and_sender_without_creating_booking_state(self):
        self.assertIsNone(self.selection('الثاني'))
        self.page(start=True)
        self.page(start=True, store='store-b', catalog=list(reversed(self.catalog)))
        self.assertEqual(self.selection('الثاني')['product_id'], 'P2')
        self.assertEqual(self.selection('الثاني', store='store-b')['product_id'], 'P9')
        self.assertIsNone(self.selection('الثاني', sender='another-customer'))
        self.assertIsNone(self.selection('الثاني', store='another-store'))
        before = self.db.execute('SELECT * FROM catalog_browse_state').fetchall()
        self.selection('أريد الثاني')
        self.assertEqual(self.db.execute('SELECT * FROM catalog_browse_state').fetchall(), before)
        names = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn('orders', names)
        self.assertNotIn('customer_product_interests', names)

    def test_last_page_ordinal_cannot_leak_from_previous_page_and_exhaustion_retains_visible_page(self):
        self.page(start=True)
        self.page()
        self.page()
        self.assertEqual(self.selection('الثاني')['product_id'], 'P10')
        self.assertIsNone(self.selection('الرابع'))
        self.page()  # Exhausted: no new images; the last visible page still applies.
        self.assertEqual(self.selection('الثاني')['product_id'], 'P10')

    def test_cached_old_page_retry_does_not_replace_current_selection_page(self):
        self.page(start=True, request_id='old')
        self.page(request_id='new')
        self.page(start=True, request_id='old')
        self.assertEqual(self.selection('الثاني')['product_id'], 'P6')

    def test_existing_schema_is_migrated_without_losing_cursor_or_guessing_old_selection(self):
        with closing(sqlite3.connect(':memory:')) as db:
            db.execute('''CREATE TABLE catalog_browse_state (
                store_id TEXT,sender_id TEXT,product_ids_json TEXT,cursor INTEGER,
                page_number INTEGER,updated_at REAL,PRIMARY KEY(store_id,sender_id))''')
            db.execute("INSERT INTO catalog_browse_state VALUES('store-a','customer-a','[]',4,1,1)")
            catalog_browsing.init_db(db)
            row = db.execute('SELECT cursor,last_page_json FROM catalog_browse_state').fetchone()
            self.assertEqual(row, (4, '{}'))
            self.assertIsNone(catalog_browsing.selection(db, 'store-a', 'customer-a', 'الثاني', self.catalog))

    def test_foreign_store_product_with_same_id_cannot_be_selected_or_displayed(self):
        self.page(start=True)
        foreign = dict(self.catalog[1], store_id='store-b', price='1000')
        self.assertIsNone(self.selection('الثاني', [foreign]))
        own = dict(self.catalog[1], store_id='store-a', price='19000')
        self.assertIs(self.selection('الثاني', [foreign, own]), own)
        result = self.page(start=True, catalog=[foreign, own])
        self.assert_page(result, ['P2'])
        self.assertIn('19,000', result['reply'])
        self.assertNotIn('1,000', result['reply'])


if __name__ == '__main__':
    unittest.main()
