"""Store/product-scoped answers supplied by staff, never inferred from customers."""
import hashlib
import json
import re

try:
    from . import customer_copy, sales_answers, sales_strategy
except ImportError:
    import customer_copy, sales_answers, sales_strategy

LABELS = {'fabric': 'نوع القماش', 'fabric_feel': 'سمك القماش وملمسه', 'season': 'الموسم',
          'stretch': 'الليكرا والمطاطية', 'cut': 'قصة القطعة', 'lining': 'البطانة',
          'closure': 'الإغلاق', 'care': 'الغسل والعناية', 'length': 'الطول', 'width': 'العرض',
          'photo_source': 'مصدر الصورة', 'price': 'السعر', 'location':'عنوان المحل', 'details': 'تفصيل المنتج', 'service':'متابعة خاصة بهذه المحادثة — لا تُعمّم'}
FIELDS = {'fabric': 'fabric', 'fabric_feel': 'fabric_feel', 'season': 'season', 'stretch': 'stretch',
          'cut': 'cut', 'lining': 'lining', 'closure': 'closure', 'care': 'care_instructions',
          'photo_source': 'real_photo_notes', 'price': 'price', 'location':'shop_address'}


def init(db):
    db.execute('''CREATE TABLE IF NOT EXISTS product_questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, store_id TEXT NOT NULL, product_id TEXT NOT NULL,
        product_name TEXT NOT NULL DEFAULT '', topic TEXT NOT NULL, question TEXT NOT NULL,
        question_key TEXT NOT NULL, answer TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending',
        occurrences INTEGER NOT NULL DEFAULT 1, sender_id TEXT, review_id INTEGER,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, answered_by TEXT,
        UNIQUE(store_id,product_id,topic,question_key))''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_product_questions_pending ON product_questions(store_id,status,id)')


def question_key(question):
    text = customer_copy.normalized(question)
    return hashlib.sha256(re.sub(r'[^\w]+', ' ', text).strip().encode()).hexdigest()


def missing(question, product):
    if not product:
        return []
    selected = set(sales_answers.topics(question))
    checks = {
        'fabric': lambda: sales_answers.field_fact(product, 'fabric'),
        'fabric_feel': lambda: sales_answers.fabric_feel_fact(product),
        'season': lambda: sales_answers.season_fact(product),
        'care': lambda: sales_answers.care_fact(product),
        'photo_source': lambda: sales_answers.field_fact(product, 'real_photo_notes'),
        'cut': lambda: sales_answers.field_fact(product, 'cut'),
        'stretch': lambda: sales_answers.field_fact(product, 'stretch'),
        'lining': lambda: sales_answers.field_fact(product, 'lining'),
        'closure': lambda: sales_answers.field_fact(product, 'closure'),
        'width': lambda: sales_strategy.measurement_fact(product, 'width'),
        'length': lambda: sales_strategy.measurement_fact(product, 'length'),
        'location': lambda: sales_answers.field_fact(product,'shop_address'),
    }
    missing = [topic for topic, known in checks.items() if topic in selected and not known()]
    _, gaps = sales_answers.product_fragments(question, product)
    negotiation = re.search(r'ارخص|ميزاني|بديل|عدكم|عندكم|غالي|خصم|تخفيض',customer_copy.normalized(question))
    if not missing and gaps and not negotiation and not exact_answer(product, question):
        missing.append('price' if 'price' in selected else 'details')
    return missing


def capture(db, store, sender, product, question, topics, review_id, now, *, increment=True):
    ids = []
    if not topics and re.search(r'باجر|باچر|المندوب|وين.{0,12}طلب|تاخر',customer_copy.normalized(question)):
        topics=['service']
    for topic in topics or ['details']:
        key = question_key(question)
        # One unresolved card per product/topic; retain the first original
        # question and count further occurrences without duplicating the queue.
        prior = db.execute("SELECT id FROM product_questions WHERE store_id=? AND product_id=? AND topic=? AND status='pending' ORDER BY id LIMIT 1",
                           (store, product['product_id'], topic)).fetchone()
        if prior:
            if increment:
                db.execute('UPDATE product_questions SET occurrences=occurrences+1,updated_at=? WHERE id=?', (now, prior['id']))
            ids.append(prior['id'])
        else:
            db.execute('''INSERT INTO product_questions(store_id,product_id,product_name,topic,question,question_key,
                          sender_id,review_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)
                          ON CONFLICT(store_id,product_id,topic,question_key) DO UPDATE SET
                          status='pending',occurrences=occurrences+1,updated_at=excluded.updated_at''',
                       (store, product['product_id'], product.get('product_name', ''), topic, question, key, sender, review_id, now, now))
            row = db.execute('SELECT id FROM product_questions WHERE store_id=? AND product_id=? AND topic=? AND question_key=?',
                             (store, product['product_id'], topic, key)).fetchone()
            ids.append(row['id'])
    db.commit()
    return ids


def apply(db, store, products):
    if db is None:
        return list(products)
    rows = db.execute("SELECT * FROM product_questions WHERE store_id=? AND status='answered' ORDER BY updated_at,id", (store,)).fetchall()
    grouped = {}
    for row in rows:
        if row['topic']=='service':
            continue
        grouped.setdefault(row['product_id'], []).append(dict(row))
    result = []
    for original in products:
        product = dict(original)
        approved = [dict(row) for row in rows if row['topic']=='location'] + [row for row in grouped.get(product.get('product_id'), []) if row['topic']!='location']
        for row in approved:
            field = FIELDS.get(row['topic'])
            if field:
                product[field] = row['answer']
            elif row['topic'] in {'length', 'width'}:
                label = 'الطول' if row['topic']=='length' else 'العرض'
                product['measurements'] = str(product.get('measurements') or '') + '\n' + label + ': ' + row['answer']
        if approved:
            product['_staff_answers'] = [{key:row[key] for key in ('topic','question_key','answer')} for row in approved]
            product['_catalog_faq'] = product.get('_catalog_faq',str(product.get('faq') or ''))
            product['faq'] = product['_catalog_faq'] + '\n' + '\n'.join(
                LABELS.get(row['topic'], 'معلومة') + ': ' + row['answer'] for row in approved)
        result.append(product)
    return result


def exact_answer(product, question):
    key = question_key(question)
    return next((row['answer'] for row in reversed(product.get('_staff_answers') or [])
                 if row['topic']=='details' and row['question_key']==key), '')


def save_answer(db, store, identity, answer, author, now):
    answer = str(answer or '').strip()
    instructions = re.search(r'تجاهل.{0,30}(?:تعليم|قاعد)|ignore.{0,25}instructions|system\s*prompt',customer_copy.normalized(answer))
    if not 2 <= len(answer) <= 1200 or customer_copy.has_instructions(answer) or instructions:
        raise ValueError('أدخل جواباً واضحاً عن المنتج، دون تعليمات لتغيير سلوك النظام.')
    row = db.execute('SELECT * FROM product_questions WHERE id=? AND store_id=?', (identity, store)).fetchone()
    if not row:
        return None
    if row['topic']=='price':
        try:
            from .pricing import terms
        except ImportError:
            from pricing import terms
        try:
            terms(dict(price=answer))
        except (ValueError,TypeError):
            raise ValueError('اكتب سعر القطعة والعرض بشكل واضح، مثلاً: القطعة ب12 ألف والقطعتين ب20 ألف.')
    db.execute("UPDATE product_questions SET answer=?,status='answered',answered_by=?,updated_at=? WHERE id=? AND store_id=?",
               (answer, author, now, identity, store))
    db.commit()
    # Saving knowledge does not send a customer message, close a review or
    # re-enable automation. Staff retains control of the paused conversation.
    return dict(db.execute('SELECT * FROM product_questions WHERE id=? AND store_id=?', (identity, store)).fetchone())


def seed_owner_facts(db, products, now):
    confirmed = [
        ('default', 'P009', 'دشداشة قديفة', 'fabric_feel', 'القماش قديفة ثقيل'),
        ('baraah-kids', 'P016', 'تراك ولادي تطعيم', 'fabric_feel', 'قماش ثخين شتوي'),
        ('baraah-kids', 'P016', 'تراك ولادي تطعيم', 'stretch', 'بيه ليكرا'),
    ]
    for store, pid, name, topic, answer in confirmed:
        if not any(p.get('store_id')==store and p.get('product_id')==pid and p.get('product_name')==name for p in products):
            continue
        question = 'معلومة أكدها المالك: ' + LABELS[topic]
        db.execute('''INSERT OR IGNORE INTO product_questions(store_id,product_id,product_name,topic,question,question_key,
                      answer,status,created_at,updated_at,answered_by) VALUES(?,?,?,?,?,?,?,'answered',?,?,?)''',
                   (store,pid,name,topic,question,question_key(question),answer,now,now,'المالك — توجيهات 1 نوفمبر'))
