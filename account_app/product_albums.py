"""Ordered, factual answers for customer photos, independent of model prose."""
import re
from collections import Counter

ORDINALS = ('الأولى', 'الثانية', 'الثالثة', 'الرابعة', 'الخامسة', 'السادسة', 'السابعة', 'الثامنة', 'التاسعة', 'العاشرة')


def label(index):
    return 'الصورة ' + (ORDINALS[index - 1] if 1 <= index <= len(ORDINALS) else str(index))


def stock_state(product):
    stock = str((product or {}).get('stock') or '').strip().lower()
    if str((product or {}).get('status') or '').lower() in ('inactive', 'disabled'):
        return 'out'
    if any(word in stock for word in ('نفذ', 'نافذ', 'نافد', 'خلص', 'غير متوفر', 'مو متوفر', 'not available', 'out')):
        return 'out'
    return 'available' if 'متوفر' in stock or 'available' in stock else 'unknown'


def current_positions(images, products):
    catalog = {p['product_id']: p for p in products}
    result = []
    for index, image in enumerate(images, 1):
        item = dict(image, image_index=index)
        product = catalog.get(item.get('product_id')) if item.get('product_found') else None
        item['product_found'] = bool(product)
        # Re-read the stock, name and price; recognition memory is identity only.
        for field in ('product_name', 'stock', 'price', 'sizes', 'colors', 'fabric'):
            item[field] = (product or {}).get(field)
        item['available'] = bool(product and stock_state(product) == 'available')
        result.append(item)
    return result


def summary(images, products, question=''):
    lines = []
    for item in current_positions(images, products):
        prefix = label(item['image_index']) + ': '
        if not item['product_found']:
            lines.append(prefix + ('أحتاج أتأكد من موديلها' if item.get('service_error') else 'بعدني ما حددت موديلها'))
            continue
        name = item.get('product_name') or 'هذا الموديل'
        state = stock_state(item)
        if state == 'out':
            lines.append(prefix + name + '، غير متوفر حالياً')
            continue
        line = prefix + name + ('، متوفر' if state == 'available' else '، توفره يحتاج تأكيد')
        price = str(item.get('price') or '').strip()
        if price:
            line += ' بـ' + price + ' د.ع'
        # Answer requested facts for each photo, never a single model's facts for all.
        for pattern, field, title in [(r'قماش|خام[ةه]', 'fabric', 'القماش'),
                                      (r'قياس|مقاس|عمر|وزن', 'sizes', 'قياساته'),
                                      (r'لون|الوان|ألوان', 'colors', 'ألوانه')]:
            if re.search(pattern, question) and item.get(field):
                line += '، ' + title + ' ' + str(item[field])
        lines.append(line)
    return '\n'.join(lines)


def next_question(images, products, customer, context=None, store='default'):
    available = [x for x in current_positions(images, products) if x['available']]
    if not available:
        return ''
    distinct = {x['product_id'] for x in available}
    if len(distinct) > 1:
        return 'تحبين أي موديل منهن، لو أكثر من قطعة؟'
    evidence = ' '.join(x.get('text', '') for x in (context or {}).get('evidence', []))
    if store == 'golden-threads':
        return 'شكد الكمية اللي تحتاجها؟'
    if not re.search(r'قياس|مقاس|سايز|عمر|سنوات|وزن|كيلو|\b(?:xs|s|m|l|xl|xxl)\b', evidence, re.I):
        return 'شكد عمر الطفل حتى نتأكد من القياس؟' if store == 'baraah-kids' else 'شكد قياسج حتى نتأكد من المتوفر؟'
    # A known size is useful memory, but is not permission to book.
    return 'تحبين نكمل على الموديل المتوفر؟'


def availability_error(reply, matched, products, images=None):
    """Check model-level claims while allowing an unavailable color/video/size."""
    text = str(reply or '').replace('أ', 'ا').replace('إ', 'ا').replace('ى', 'ي')
    known = {p['product_id']: p for p in products}
    products = list(known.values())
    positions = current_positions(images or [], products)
    normalize = lambda s: str(s or '').replace('أ', 'ا').replace('إ', 'ا').replace('ى', 'ي')
    names = Counter(normalize(p.get('product_name')) for p in products)
    markers = {normalize(label(x['image_index'])): known.get(x.get('product_id')) for x in positions}
    markers.update({normalize(p['product_name']): p for p in products
                    if len(normalize(p.get('product_name'))) >= 4 and names[normalize(p['product_name'])] == 1})
    targets = []
    if markers:
        matches = list(re.finditer('|'.join(re.escape(x) for x in sorted(markers, key=len, reverse=True)), text))
        for i, match in enumerate(matches):
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            targets.append((markers[match.group()], text[match.start():end]))
    if not positions and matched and not targets:
        targets.append((matched, text))
    negative = r'(?<!\w)(?:(?:غير|مو|ما)\s*(?:متوفر|موجود)|(?:مو|ما)\s*عدنا|مخلص|خلصان|ناف[ذد]|نفذ|خلص)(?!\w)'
    for product, segment in targets:
        # Qualified absence is a missing variant/fact, not whole-product stock.
        clauses = re.split(r'[،,؛]|\b(?:بس|اما)\b', segment)
        for clause in clauses:
            if re.search(r'فيديو|قياس\s*\d|مقاس\s*\d|لون|الابيض|الاسود|الاحمر|الرصاصي|البيج|الازرق|عمر\s*\d|طول|عرض|بطان', clause):
                continue
            absent = bool(re.search(negative, clause))
            if not product and absent:
                return 'الصورة غير المعروفة لا تثبت نفاد المنتج؛ وضح أن موديلها لم يُحدد بعد.'
            state = stock_state(product)
            if state == 'available' and absent:
                return 'الموديل متوفر حالياً؛ لا تنف توفره بسبب صورة مجهولة أو معلومة ناقصة.'
            if state == 'out' and not absent and re.search(r'متوفر|موجود|احجز', clause):
                return 'الموديل نافد حالياً؛ لا تعرض شراءه كأنه متوفر.'
    return ''
