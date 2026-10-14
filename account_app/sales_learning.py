"""Learn verified photo identities and bounded lessons, never customer instructions."""
import hashlib
import json

LESSONS = {
    'stock_contradiction': 'تحقق من توفر كل موديل من بياناته الحالية؛ لا تخلط غياب لون أو فيديو مع نفاد الموديل، ولا تعتبر الصورة غير المعروفة نافدة.',
    'partial_album': 'أجب عن صور الزبون بالترتيب، وواصل على المنتجات المعروفة المتوفرة ولو تعذر تحديد صورة أخرى. المراجعة للصورة المجهولة فقط.',
    'repeated_product': 'استعمل اختيار الزبون المحفوظ وترتيب الصور؛ لا تطلب اسم الموديل أو صورته من جديد بعد تحديده.',
    'repeated_contact': 'استعمل رقم وعنوان ومحافظة الزبون المعروفين، واطلب الناقص فقط بعد موافقته على الشراء.',
    'internal_language': 'تكلم عن حاجة الزبون وإجراء متاح بلهجة عراقية طبيعية؛ لا تقل البيانات قليلة أو غير موثقة أو حسب الكتالوج.',
    'unsupported_claim': 'أجب من الحقائق الحالية فقط؛ لا تخترع قياساً أو ضماناً أو ندرة أو تنفيذ إجراء لم يحصل.',
}


def init_db(db):
    db.execute('''CREATE TABLE IF NOT EXISTS verified_image_memory (
        store_id TEXT NOT NULL, fingerprint TEXT NOT NULL, product_id TEXT NOT NULL,
        product_signature TEXT NOT NULL, source TEXT NOT NULL, source_message_id INTEGER,
        confirmations INTEGER NOT NULL DEFAULT 1, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(store_id,fingerprint))''')
    db.execute('''CREATE TABLE IF NOT EXISTS sales_mistake_memory (
        store_id TEXT NOT NULL, code TEXT NOT NULL, occurrences INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(store_id,code))''')


def signature(product):
    identity = {k: product.get(k) for k in ('product_id', 'product_name', 'image_url', 'image_colors', 'visual_description')}
    return hashlib.sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def teach_image(db, store, fingerprint, product, message_id, source='manual_admin'):
    if source not in {'manual_admin', 'telegram_human'} or not fingerprint or not product.get('product_id'):
        return False
    db.execute('''INSERT INTO verified_image_memory
        (store_id,fingerprint,product_id,product_signature,source,source_message_id)
        VALUES(?,?,?,?,?,?) ON CONFLICT(store_id,fingerprint) DO UPDATE SET
        product_id=excluded.product_id, product_signature=excluded.product_signature,
        source=excluded.source,source_message_id=excluded.source_message_id,
        confirmations=CASE WHEN verified_image_memory.product_id=excluded.product_id
            THEN verified_image_memory.confirmations+1 ELSE 1 END,updated_at=CURRENT_TIMESTAMP''',
        (store, fingerprint, product['product_id'], signature(product), source, message_id))
    db.commit()
    return True


def recalled_image(db, store, fingerprint, products):
    row = db.execute('SELECT product_id,product_signature FROM verified_image_memory WHERE store_id=? AND fingerprint=?',
                     (store, fingerprint)).fetchone()
    if not row:
        return None
    product = next((p for p in products if p['product_id'] == row[0]), None)
    if not product or signature(product) != row[1]:
        return None
    return {'product_found': True, 'product_id': product['product_id'], 'confidence': 100,
            'reason': 'Staff verified image memory', 'reference_source': 'verified_image_memory'}


def record(db, store, code):
    if code not in LESSONS:
        return
    db.execute('''INSERT INTO sales_mistake_memory(store_id,code) VALUES(?,?)
        ON CONFLICT(store_id,code) DO UPDATE SET occurrences=occurrences+1,updated_at=CURRENT_TIMESTAMP''', (store, code))
    db.commit()


def guidance(db, store):
    rows = db.execute('SELECT code FROM sales_mistake_memory WHERE store_id=? ORDER BY occurrences DESC,code LIMIT 6', (store,)).fetchall()
    rules = [LESSONS[x[0]] for x in rows if x[0] in LESSONS]
    return '\n'.join(rules)
