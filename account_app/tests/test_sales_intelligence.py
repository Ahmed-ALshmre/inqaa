from datetime import datetime
import json
import sqlite3

import pytest

from account_app import ai_jobs, pricing, sales_intelligence as intelligence
from account_app.sales_runtime.cart import Catalog
from account_app.sales_runtime.store import Store


def product(pid, **overrides):
    return dict(dict(store_id='default', product_id=pid, status='active', stock='متوفر',
                     audience='women', product_type='dresses', reply_category='فستان',
                     price='15000', sizes='38 إلى 46', colors='اسود', image_url=f'https://example.com/{pid}'),
                **overrides)


@pytest.fixture
def db():
    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    Store(connection).migrate()
    ai_jobs.init_db(connection)
    connection.executescript('''
        CREATE TABLE messages (id INTEGER PRIMARY KEY, store_id TEXT, sender_id TEXT, direction TEXT,
            text TEXT, raw_payload TEXT, created_at TEXT, message_type TEXT);
        CREATE TABLE orders (store_id TEXT, sender_id TEXT, product_id TEXT, status TEXT, created_at TEXT);
    ''')
    yield connection
    connection.close()


def test_catalog_scope_and_tracked_size_survive_refresh(db):
    def load(quantity):
        return Catalog('default', [product('A'), product('B', clearance=True, discount_iqd=2000, variant_stock=[
            dict(color='اسود', size='44', quantity=quantity), dict(color='اسود', size='42', quantity=2)]),
            product('C'), product('other-store', store_id='another'), product('hidden', status='inactive')])
    cart = {'cart': [dict(product_id='A', size='44')], 'order_id': 1}
    eligible = intelligence.relevant(db, load(1), cart, 'A', 'اكو موديل ثاني قياس 44؟', order_confirmed=True)
    assert [item['product_id'] for item in eligible['matches']] == ['B']
    assert eligible['matches'][0]['price_iqd'] == 13000
    sold = intelligence.relevant(db, load(0), cart, 'A', 'اكو موديل ثاني؟', order_confirmed=True)
    assert [item['product_id'] for item in sold['matches']] == ['C']
    assert not intelligence.relevant(db, load(1), cart, 'A', 'شكرا', order_confirmed=True)['matches']
    assert not intelligence.relevant(db, load(1), cart, 'A', 'اكو موديل ثاني؟')['matches']
    assert not intelligence.relevant(db, load(1), cart, 'A', 'وين صار طلبي؟', order_confirmed=True)['matches']
    assert [item['product_id'] for item in intelligence.relevant(db, load(1), {}, None, 'اكو عروض؟')['matches']] == ['B']
    assert [item['product_id'] for item in intelligence.relevant(db, load(1), {}, None, 'عندكم فساتين قياس 44؟')['matches']] == ['B', 'A', 'C']
    assert set(intelligence.catalog_memory(db, load(1))) == {'A', 'B', 'C'}


def test_discount_uses_best_published_offer_and_correct_sale_unit():
    item = product('A', offer='قطعتين ب 25 ألف', discount_iqd='٢٠٠٠')
    assert pricing.terms(item)[1] == 13000
    order = pricing.quote([dict(product_id='A', quantity=3)], [item], 5000)
    assert order['product_total'] == 38000
    assert sum(line['quantity'] * line['unit_price'] for line in order['items']) == 38000
    item['discount_iqd'] = 4000
    assert pricing.quote([dict(product_id='A', quantity=2)], [item], 5000)['product_total'] == 22000
    bundle = product('A', price='بكج 3 قطع سعره 25 الف', discount_iqd=2000)
    assert pricing.quote([dict(product_id='A', quantity=3)], [bundle], 0)['product_total'] == 23000
    for invalid in (-1, True, False, '500.5', 15000):
        with pytest.raises(ValueError):
            pricing.terms(product('A', discount_iqd=invalid))


def test_proactive_offer_needs_interest_and_live_fit(db):
    catalog = Catalog('default', [product('A', occasions=['outing']), product('B', price='12000', clearance=True),
        product('C', price='11000', sizes='38 إلى 42')])
    state = {'cart': [dict(product_id='A', size='44')]}
    search = lambda text, **kwargs: intelligence.relevant(db, catalog, state, 'A', text, **kwargs)
    result = search('السعر غالي شوي')
    assert not result['requested'] and result['opportunity'] == 'price_hesitation'
    assert [row['product_id'] for row in result['matches']] == ['B']
    assert [row['product_id'] for row in search('لأعيني غالي')['matches']] == ['B']
    assert not search('التوصيل غالي')['matches']
    assert not search('السعر غالي شوي', already_suggested=True)['matches']
    assert not search('السعر غالي شوي أفكر بعدين')['matches']
    assert not search('شكد السعر؟')['matches']
    state['purchase_event'] = 1
    assert not search('السعر غالي شوي')['matches']
    state['order_id'] = 1
    assert not search('شكراً', order_confirmed=True)['matches']
    assert not search('وين طلبي؟', order_confirmed=True)['matches']
    assert not search('عجبني الموديل')['matches']
    catalog.products['B']['occasions'] = ['outing']
    assert search('عجبني الموديل', order_confirmed=True)['opportunity'] == 'after_confirmation'
    assert [row['product_id'] for row in search('عجبني الموديل', order_confirmed=True)['matches']] == ['B']


def test_alternative_keeps_infant_age_across_intervening_product_question(db):
    def dress(pid, sizes):
        return product(pid, size_system='age', audience='girls', sizes=sizes)
    catalog = Catalog('default', [dress('A','من سنة الى 3 سنوات'), dress('B','من 3 الى 7 سنوات'),
        dress('C','من 6 الى 9 اشهر'), dress('D','من 7 الى 12 شهر')])
    history = [dict(actor='customer', text=text) for text in ('بشكد هذا الفستان', 'واكو عمر 6الى9اشهر', 'عندج تراكات للبيت', 'وشكد التوصيل')]
    result = intelligence.relevant(db, catalog, {}, 'A', 'زين اريد فستان الخ غير موديل', history=history)
    assert result['requested_age_months'] == [6,9]
    assert [item['product_id'] for item in result['matches']] == ['C']


def test_nightly_business_window_scope_and_retry_reuse(db):
    now = datetime.fromisoformat('2026-11-14T23:30:00+03:00')
    intelligence.schedule(db, 'default', now.replace(minute=29))
    for _ in range(2):
        intelligence.schedule(db, 'default', now)
    keys = [json.loads(row[0])['sales_day'] for row in db.execute('SELECT payload FROM ai_jobs ORDER BY rowid')]
    assert keys == ['2026-11-13', '2026-11-14']
    rows = [
        ('default', 'buyer', 'السلام عليكم شكد السعر', '2026-11-14T10:00:00+03:00'),
        ('default', 'buyer', '٠٧٧٠١٢٣٤٥٦٧ بغداد شارع المدرسة قرب الجامع', '2026-11-14T10:02:00+03:00'),
        ('default', 'prospect', 'السعر غالي اكو خصم؟', '2026-11-14T23:29:59+03:00'),
        ('default', 'too-late', 'فقط بعد اغلاق اليوم', '2026-11-14T23:30:00+03:00'),
        ('another', 'private', 'خارج المتجر', '2026-11-14T10:00:00+03:00'),
    ]
    db.executemany("INSERT INTO messages(store_id,sender_id,text,created_at,direction,message_type) VALUES(?,?,?,?,'incoming','text')", rows)
    db.execute("INSERT INTO orders VALUES('default','buyer','A','new','2026-11-14T10:05:00+03:00')")
    db.commit()
    calls = []
    def review(model, evidence, rules, proposal=None):
        calls.append(model)
        assert 'products' not in evidence
        assert rules == ['حلل وقت طلب بيانات الحجز']
        assert evidence['metrics']['booked_in_window'] == 1
        assert evidence['metrics']['conversations'] == 2
        sent = json.dumps(evidence, ensure_ascii=False)
        assert all(item not in sent for item in ('7701234567', 'شارع المدرسة', 'buyer', 'خارج المتجر', 'فقط بعد اغلاق اليوم'))
        if model == 'first':
            return dict(tactics=['lead_with_offer','invent'], findings=[], clearance=[])
        if len(calls) == 2:
            raise ValueError('provider_http_402')
        assert proposal is not None
        return dict(tactics=['lead_with_offer', 'resolve_fit','invent'], findings=[], clearance=[])
    intelligence.save_rule(db, 'default', 'conversations', {'text': 'حلل وقت طلب بيانات الحجز'})
    intelligence.save_rule(db, 'default', 'products', {'text': 'راجع القياسات'})
    for expected in ('retry', 'done', 'done'):
        result = intelligence.run(db, 'default', '2026-11-14', 'conversations', 0,
                                  lambda: pytest.fail('Conversation analysis read the catalog'), ['first','second'], review)
        assert result['status'] == expected
    assert calls == ['first', 'second', 'second']
    assert intelligence.latest_report(db, 'default', 'conversations')['tactics'] == ['lead_with_offer']
    assert intelligence.latest_report(db, 'default', 'products') is None
    assert intelligence.learned_tactics(db, 'default', 'شكد السعر؟', False) == [intelligence.TACTICS['lead_with_offer']]
    assert not intelligence.learned_tactics(db, 'another', 'شكد السعر؟', False)
    intelligence.set_enabled(db, 'default', 'learned_rules', False)
    assert not intelligence.learned_tactics(db, 'default', 'شكد السعر؟', False)
    assert intelligence.settings(db, 'default', 'conversations') == (True, 0)


def test_products_only_run_on_request_and_analysis_ignores_conversation_data(db):
    now = datetime.now(intelligence.BAGHDAD)
    day = now.date().isoformat()
    intelligence.set_enabled(db, 'default', 'conversations', False)
    intelligence.schedule(db, 'default', now)
    assert db.execute('SELECT count(*) FROM ai_jobs').fetchone()[0] == 0
    intelligence.request_product_analysis(db, 'default')
    intelligence.request_product_analysis(db, 'default')
    assert db.execute('SELECT count(*) FROM ai_jobs').fetchone()[0] == 1
    intelligence.save_rule(db, 'default', 'products', {'text': 'أولوية التصفية'})
    calls = []
    def review(model, evidence, rules, proposal=None):
        calls.append(model)
        assert set(evidence) == {'metrics', 'products'}
        assert rules == ['أولوية التصفية']
        return {'clearance': [{'product_id': 'A', 'suggestion': 'اعرض القياسات المسجلة'}]}
    result = intelligence.run(db, 'default', day, 'products', None,
        lambda: Catalog('default', [product('A')]), ['first','second'], review)
    assert result['status'] == 'done'
    assert calls == ['first', 'second']


def test_pause_during_first_model_supersedes_followup_even_if_reenabled(db):
    now = datetime.now(intelligence.BAGHDAD).replace(hour=23, minute=0)
    db.execute("INSERT INTO messages VALUES (1,'default','buyer','incoming','شكد السعر',NULL,?,'text')", (now.isoformat(),))
    db.commit()
    calls = []
    def review(model, evidence, rules, proposal=None):
        calls.append(model)
        intelligence.set_enabled(db, 'default', 'conversations', False)
        intelligence.set_enabled(db, 'default', 'conversations', True)
        return {'tactics': ['lead_with_offer'], 'findings': []}
    args = (db, 'default', now.date().isoformat(), 'conversations', 0, None, ['first','second'], review)
    assert intelligence.run(*args)['status'] == 'disabled'
    assert calls == ['first']
    assert intelligence.run(*args)['status'] == 'disabled'
