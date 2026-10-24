"""Small, persistent catalogue pages; browsing never creates a purchase choice.

Pass the application's public-image resolver as ``image_urls``. This module
does not download images or send messages. Its text and attachments always
describe the same page and can be delivered by the normal final sender.
"""
import json
import re
import time

try:
    from . import customer_copy, product_albums
    from .pricing import terms
except ImportError:
    import customer_copy, product_albums
    from pricing import terms

PAGE_SIZE = 4


def init_db(db):
    # execute(), rather than executescript(), preserves a caller's transaction.
    db.execute('''CREATE TABLE IF NOT EXISTS catalog_browse_state (
        store_id TEXT NOT NULL, sender_id TEXT NOT NULL,
        product_ids_json TEXT NOT NULL, cursor INTEGER NOT NULL,
        page_number INTEGER NOT NULL, updated_at REAL NOT NULL,
        last_page_json TEXT NOT NULL DEFAULT '{}',
        PRIMARY KEY (store_id, sender_id))''')
    columns = {row[1] for row in db.execute('PRAGMA table_info(catalog_browse_state)')}
    if 'last_page_json' not in columns:
        db.execute("ALTER TABLE catalog_browse_state ADD COLUMN last_page_json TEXT NOT NULL DEFAULT '{}'")
    db.execute('''CREATE TABLE IF NOT EXISTS catalog_browse_requests (
        store_id TEXT NOT NULL, sender_id TEXT NOT NULL, request_id TEXT NOT NULL,
        result_json TEXT NOT NULL, created_at REAL NOT NULL,
        PRIMARY KEY (store_id, sender_id, request_id))''')


def is_next_request(text):
    """Only explicit continuation phrases, never 'complete the booking'."""
    clean = customer_copy.normalized(text)
    clean = re.sub(r'[؟?!.،,:؛«»"\s]+', ' ', clean).strip()
    clean = re.sub(r'^(?:(?:اي|نعم|تمام|زين|اوكي|حلو)\s+)+', '', clean)
    clean = re.sub(r'\s+(?:عيني|حبيبتي|لو سمحتي|بلا زحمه|بلا زحمة)$', '', clean).strip()
    return bool(re.fullmatch(
        r'(?:البقي[ةه]|الباقي|بقي[ةه] الموديلات|باقي الموديلات|'
        r'كمل(?:ي)?|كملي الباقي|كمل الباقي|كمل(?:ي)? (?:الصور|الموديلات)|'
        r'(?:دزي|دزيلي|ارسلي|ارسل|وريني) (?:البقي[ةه]|الباقي|باقي الموديلات)|'
        r'بعد ?موديلات|موديلات بعد|موديلات ثاني[ةه]|اكو بعد موديلات|شنو بعد عدكم)', clean))


def has_session(db, store_id, sender_id):
    init_db(db)
    return db.execute('SELECT 1 FROM catalog_browse_state WHERE store_id=? AND sender_id=?',
                      (str(store_id), str(sender_id))).fetchone() is not None


def _raw_images(product):
    values = product.get('image_url') or []
    return [values] if isinstance(values, str) else values if isinstance(values, list) else []


def _available(product, store_id=None):
    return (isinstance(product, dict) and product_albums.stock_state(product) == 'available'
            and str(product.get('status') or '').strip().lower() not in {'archived', 'deleted', 'draft'}
            and (not store_id or not product.get('store_id')
                 or str(product['store_id']).strip() == str(store_id).strip()))


def selection(db, store_id, sender_id, text, products):
    """Resolve one explicit ordinal without binding a model or making a booking.

    'الثاني' and 'تفاصيل رقم 2' identify a model. 'ثنين' and 'أريد ثنين'
    describe quantity and never select the second model. Returning a current
    catalog record supplies product context only; it is not purchase consent.
    """
    clean = customer_copy.normalized(text)
    clean = re.sub(r'[؟?!.،,:؛«»"\s]+', ' ', clean).strip()
    clean = re.sub(r'\s+(?:عيني|حبيبتي|لو سمحتي|بلا زحمه|بلا زحمة)$', '', clean).strip()
    ordinals = {'الاول': 1, 'الاولي': 1, 'الاولى': 1, 'الاوله': 1,
                'الثاني': 2, 'الثانية': 2, 'الثانيه': 2,
                'الثالث': 3, 'الثالثة': 3, 'الثالثه': 3,
                'الرابع': 4, 'الرابعة': 4, 'الرابعه': 4}
    ordinal_pattern = '|'.join(sorted(ordinals, key=len, reverse=True))
    match = re.fullmatch(
        r'(?:(?:اريد(?: اشوف)?|اختار|عجبني|عجبتني|حبيت|اشوف|تفاصيل|شنو تفاصيل|'
        r'دزي تفاصيل|سعر|شكد سعر|قياسات|شنو قياسات)\s*)?'
        r'(?:(?:الموديل|الصورة|الصوره|القطعة|القطعه|الفستان)\s*)?'
        r'(?P<ordinal>' + ordinal_pattern + r'|رقم\s*[1-4])'
        r'(?:\s+(?:(?:شنو|شكد)\s+)?(?:سعره|سعرها|قياساته|قياساتها|قماشه|قماشها|الوانه|الوانها))?', clean)
    if not match:
        return None
    value = match['ordinal']
    number = int(re.search(r'[1-4]', value)[0]) if value.startswith('رقم') else ordinals[value]
    init_db(db)
    row = db.execute('''SELECT last_page_json FROM catalog_browse_state
        WHERE store_id=? AND sender_id=?''', (str(store_id), str(sender_id))).fetchone()
    if not row:
        return None
    try:
        meta = json.loads(row[0])
        item = next((item for item in meta.get('items', [])
                     if item.get('number') == number), None)
    except (ValueError, TypeError, AttributeError):
        return None
    if not item:
        return None
    identity = str(item.get('product_id') or '').strip()
    return next((product for product in products or []
                 if _available(product, store_id) and str(product.get('product_id') or '').strip() == identity), None)


def _eligible(products, image_urls, store_id):
    result = {}
    for product in products or []:
        if not _available(product, store_id):
            continue
        identity = str(product.get('product_id') or '').strip()
        if not identity or identity in result:
            continue
        values = image_urls(product)
        if isinstance(values, str):
            values = [values]
        first = next((value.strip() for value in values or []
                      if isinstance(value, str) and value.strip()), '')
        if first:
            result[identity] = (product, first)
    return result


def _item(product, image_url, number):
    name = customer_copy.fact(product.get('product_name')) or f'موديل {number}'
    # Product titles can contain paragraphs; one title must not swamp the page.
    display_name = re.sub(r'\s+', ' ', name).strip()
    if len(display_name) > 100:
        display_name = display_name[:100].rstrip() + '…'
    try:
        safe_product = dict(product, price=customer_copy.fact(product.get('price')))
        count, price, _ = terms(safe_product)
        price_text = f'{price:,} د.ع.' + (f' للبكج ({count} قطع)' if count > 1 else '')
    except (ValueError, TypeError):
        price_text = 'السعر يحتاج تأكيد'
    return {'number': number, 'product_id': str(product['product_id']),
            'product_name': name, 'image_url': image_url,
            'text': f'{number}. {display_name} — {price_text}'}


def page(db, store_id, sender_id, products, *, image_urls=None, start=False, request_id=None):
    """Prepare up to four available models and one image per model.

    ``start=True`` starts a fresh snapshot in the supplied catalog order. A
    continuation keeps that order, skips items no longer available and never
    reintroduces already displayed models. New inventory appears on a new
    catalogue request. Supply the incoming message ID for retry-safe pagination.
    A continuation without a prior session returns ``None`` so 'كمل' cannot
    silently start browsing or be confused with a booking action.
    """
    store_id, sender_id = str(store_id or '').strip(), str(sender_id or '').strip()
    if not store_id or not sender_id:
        raise ValueError('Catalogue browsing requires an explicit store and sender')
    request_id = str(request_id) if request_id is not None else None
    init_db(db)
    owned = not db.in_transaction
    if owned:
        db.execute('BEGIN IMMEDIATE')
    try:
        if request_id is not None:
            cached = db.execute('''SELECT result_json FROM catalog_browse_requests
                WHERE store_id=? AND sender_id=? AND request_id=?''',
                (store_id, sender_id, request_id)).fetchone()
            if cached:
                result = json.loads(cached[0])
                if owned:
                    db.commit()
                return result
        row = db.execute('''SELECT product_ids_json,cursor,page_number,last_page_json FROM catalog_browse_state
            WHERE store_id=? AND sender_id=?''', (store_id, sender_id)).fetchone()
        if not start and not row:
            if owned:
                db.commit()
            return None
        eligible = _eligible(products, image_urls or _raw_images, store_id)
        identities = list(eligible) if start else json.loads(row[0])
        cursor = 0 if start else int(row[1])
        page_number = 1 if start else int(row[2]) + 1
        selected = []
        while cursor < len(identities) and len(selected) < PAGE_SIZE:
            identity = identities[cursor]
            cursor += 1
            if identity in eligible:
                product, image_url = eligible[identity]
                item = _item(product, image_url, len(selected) + 1)
                item['snapshot_position'] = cursor
                selected.append(item)
        more = any(identity in eligible for identity in identities[cursor:])
        if not more:
            cursor = len(identities)
        if selected:
            prefix = (f'هاي أول {len(selected)} موديلات متوفرة، ويا كل واحد صورته:' if start
                      else f'هاي الدفعة التالية، بيها {len(selected)} موديلات متوفرة:')
            ending = ('أي واحد لفت نظرج؟ وإذا تحبين تشوفين البقية اكتبي «البقية».' if more
                      else 'هذي آخر دفعة بهالمجموعة. أي موديل عجبج حتى نشوف قياسه ولونه؟')
            reply = prefix + '\n' + '\n'.join(item['text'] for item in selected) + '\n\n' + ending
        elif start:
            reply = 'هسه ما عندي صور لموديلات متوفرة أعرضها إلك.'
        else:
            reply = 'خلصت الموديلات المصوّرة المتوفرة بهالمجموعة. أي واحد عجبج أكثر؟'
        urls = [item['image_url'] for item in selected]
        attachments = [{'type': 'image', 'url': url} for url in urls]
        meta = {'product_ids': [item['product_id'] for item in selected], 'items': selected,
                'page_number': page_number, 'page_size': PAGE_SIZE, 'has_more': more,
                'cursor': cursor, 'browsing_only': True}
        result = {'reply': reply, 'reply_parts': [reply], '_single_message': True,
                  'intent': 'catalog_request', 'create_order': False, 'order': {},
                  '_browse_only': True, 'requires_human': False,
                  'send_image': bool(urls), 'image_urls': urls, 'product_image_urls': urls,
                  'product_image_url': urls[0] if urls else '', 'attachments': attachments,
                  'meta': {'catalog': True, 'image_count': len(urls), 'catalog_page': meta}}
        now = time.time()
        last_page_json = json.dumps(meta, ensure_ascii=False) if selected or start else row[3]
        db.execute('''INSERT INTO catalog_browse_state
            (store_id,sender_id,product_ids_json,cursor,page_number,updated_at,last_page_json)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(store_id,sender_id) DO UPDATE SET
            product_ids_json=excluded.product_ids_json,cursor=excluded.cursor,
            page_number=excluded.page_number,updated_at=excluded.updated_at,
            last_page_json=excluded.last_page_json''',
            (store_id, sender_id, json.dumps(identities, ensure_ascii=False), cursor,
             page_number, now, last_page_json))
        if request_id is not None:
            db.execute('INSERT INTO catalog_browse_requests VALUES(?,?,?,?,?)',
                       (store_id, sender_id, request_id, json.dumps(result, ensure_ascii=False), now))
        if owned:
            db.commit()
        return result
    except Exception:
        if owned:
            db.rollback()
        raise
