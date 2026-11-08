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
    if 'guidance' not in {row[1] for row in db.execute('PRAGMA table_info(product_questions)')}:
        # Keep historical factual answers intact. Explanatory coaching must not
        # become a price, a specification, an FAQ or a verbatim customer reply.
        db.execute("ALTER TABLE product_questions ADD COLUMN guidance TEXT NOT NULL DEFAULT ''")
    db.execute('CREATE INDEX IF NOT EXISTS idx_product_questions_queue ON product_questions(status,id)')


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
    guided = {row['topic'] for row in guidance_for(product, question)} - {'price', 'service'}
    missing = [topic for topic, known in checks.items() if topic in selected and not known() and topic not in guided]
    _, gaps = sales_answers.product_fragments(question, product)
    # A mixed question still needs review if any OTHER requested fact is
    # unknown. Coaching about fabric must never hide an unknown price/lining.
    gap_topics = {'سعر الموديل': 'price',
                  'عدد قطع وحدة': 'wholesale', 'مصدر الصورة': 'photo_source',
                  'الموسم': 'season', 'العناية': 'care', 'توفر': 'availability',
                  'سمك القماش': 'fabric_feel', 'ألوان': 'colors', 'قياسات': 'sizes',
                  'القماش': 'fabric', 'البطانة': 'lining', 'المطاط': 'stretch',
                  'الإغلاق': 'closure', 'قصة القطعة': 'cut', 'عرض القطعة': 'width',
                  'طول القطعة': 'length', 'فيديو': 'video'}
    gaps = [gap for gap in gaps if next((topic for prefix, topic in gap_topics.items()
                                       if gap.startswith(prefix)), 'details') not in guided]
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


def apply(db, store, products, *, sender=None):
    if db is None:
        return list(products)
    rows = db.execute("SELECT * FROM product_questions WHERE store_id=? AND status='answered' ORDER BY updated_at,id", (store,)).fetchall()
    grouped = {}
    for row in rows:
        if row['topic']=='service' and (not sender or row['sender_id'] != sender):
            continue
        grouped.setdefault(row['product_id'], []).append(dict(row))
    result = []
    for original in products:
        product = dict(original)
        product.pop('_staff_guidance', None)
        product.pop('_active_staff_guidance', None)
        approved = [dict(row) for row in rows if row['topic']=='location'] + [row for row in grouped.get(product.get('product_id'), []) if row['topic']!='location']
        factual = [row for row in approved if row['answer'] and row['topic'] != 'service']
        for row in factual:
            field = FIELDS.get(row['topic'])
            if field:
                product[field] = row['answer']
            elif row['topic'] in {'length', 'width'}:
                label = 'الطول' if row['topic']=='length' else 'العرض'
                product['measurements'] = str(product.get('measurements') or '') + '\n' + label + ': ' + row['answer']
        if factual:
            product['_staff_answers'] = [{key:row[key] for key in ('topic','question_key','answer')} for row in factual]
            product['_catalog_faq'] = product.get('_catalog_faq',str(product.get('faq') or ''))
            product['faq'] = product['_catalog_faq'] + '\n' + '\n'.join(
                LABELS.get(row['topic'], 'معلومة') + ': ' + row['answer'] for row in factual)
        # Only the latest teaching for each topic is active. Details and
        # case-specific service notes require the same normalized question.
        notes = {}
        for row in approved:
            if row.get('guidance'):
                key = (row['topic'], row['question_key'] if row['topic'] in {'details', 'service'} else '')
                notes[key] = {key: row[key] for key in ('topic', 'question_key', 'guidance')}
        if notes:
            product['_staff_guidance'] = list(notes.values())
        result.append(product)
    return result


def guidance_for(product, question):
    selected = set(sales_answers.topics(question))
    key = question_key(question)
    return [row for row in (product or {}).get('_staff_guidance', [])
            if (row['question_key'] == key if row['topic'] in {'details', 'service'}
                else row['topic'] in selected)]


def guidance_prompt(product, question):
    notes = guidance_for(product, question)
    if not notes:
        return ''
    return ('[شرح الموظف لطريقة التعامل مع هذا السؤال عن المنتج الحالي]\n'
            'هذا شرح داخلي لفهم الجواب، وليس رسالة جاهزة للزبون. افهمي المقصود وصيغي جواباً جديداً '
            'قصيراً باللهجة العراقية وبصوت بنت تشتغل بالمتجر، يناسب سجل المحادثة وآخر سؤال. '
            'لا تنقلي الشرح حرفياً ولا تقولي إن الموظف وجّهك. لا تعيدي ترحيباً أو سؤالاً عن معلومة معروفة. '
            'تبقى أسعار وخصومات ومخزون ومقاسات المنتج وقواعد حفظ الطلب المعتمدة هي المرجع؛ '
            'الشرح لا يعدّلها ولا يثبت حجزاً. إذا لم يكفِ الشرح والحقائق لإجابة مؤكدة، '
            'حوّلي لمراجعة بشرية بصمت ولا تختلقي معلومة.\n'
            + json.dumps([{'الموضوع': LABELS.get(row['topic'], 'تفصيل المنتج'),
                           'الشرح': row['guidance']} for row in notes], ensure_ascii=False))


def copies_guidance(reply, product, question):
    text = customer_copy.normalized(reply)
    return any(len(note) >= 24 and note in text for note in
               (customer_copy.normalized(row['guidance']) for row in guidance_for(product, question)))


def revision(row):
    return hashlib.sha256(json.dumps([row.get(key) for key in
        ('id', 'status', 'answer', 'guidance', 'updated_at')], ensure_ascii=False).encode()).hexdigest()


class GuidanceConflict(ValueError):
    pass


def save_guidance(db, store, identity, guidance, author, now, *, expected_revision=None):
    guidance = str(guidance or '').strip()
    if not 5 <= len(guidance) <= 4000:
        raise ValueError('اكتب شرحاً لطريقة الإجابة من 5 إلى 4000 حرف.')
    if re.search(r'تجاهل.{0,30}(?:تعليم|قاعد)|ignore.{0,25}instructions|system\s*prompt',
                 customer_copy.normalized(guidance)):
        raise ValueError('اشرح التعامل مع السؤال، دون تعليمات لتجاوز قواعد النظام.')
    found = db.execute('SELECT * FROM product_questions WHERE id=? AND store_id=?', (identity, store)).fetchone()
    if not found:
        return None
    row = dict(found)
    if expected_revision is not None and expected_revision != revision(row):
        raise GuidanceConflict('حفظ موظف آخر تحديثاً لهذا السؤال. شرحك محفوظ في الحقل؛ حدّث الأسئلة قبل الحفظ مجدداً.')
    changed = db.execute("""UPDATE product_questions SET guidance=?,status='answered',answered_by=?,updated_at=?
        WHERE id=? AND store_id=? AND status=? AND answer=? AND guidance=? AND updated_at=?""",
        (guidance, author, now, identity, store, row['status'], row['answer'], row['guidance'], row['updated_at']))
    if changed.rowcount != 1:
        db.rollback()
        raise GuidanceConflict('تغيّر السؤال أثناء الحفظ. شرحك ما زال في الحقل؛ حدّث الأسئلة ثم راجعه.')
    db.commit()
    # Knowledge editing never sends, resumes AI or closes a human review.
    return dict(db.execute('SELECT * FROM product_questions WHERE id=? AND store_id=?', (identity, store)).fetchone())


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
