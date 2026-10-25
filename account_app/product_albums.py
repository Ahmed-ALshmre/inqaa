"""Ordered, factual answers for customer photos, independent of model prose."""
import re
from collections import Counter
try:
    from . import customer_copy, product_variants
    from .pricing import terms
except ImportError:
    import customer_copy, product_variants
    from pricing import terms

ORDINALS = ('الأولى', 'الثانية', 'الثالثة', 'الرابعة', 'الخامسة', 'السادسة', 'السابعة', 'الثامنة', 'التاسعة', 'العاشرة')
REFERENCES = ('الأول', 'الثاني', 'الثالث', 'الرابع', 'الخامس', 'السادس', 'السابع', 'الثامن', 'التاسع', 'العاشر')


def label(index):
    return 'الصورة ' + (ORDINALS[index - 1] if 1 <= index <= len(ORDINALS) else str(index))


def reference(index):
    return REFERENCES[index-1] if 1<=index<=len(REFERENCES) else f'الصورة رقم {index}'


def image_question(question,index):
    """Do not apply the first photo's requested size/color to every photo."""
    normalized=customer_copy.normalized(question)
    refs={customer_copy.normalized(word):i for i,pair in enumerate(zip(ORDINALS,REFERENCES),1) for word in pair}
    matches=list(re.finditer(r'(?<!\w)(?:و)?('+ '|'.join(sorted(refs,key=len,reverse=True))+r')(?!\w)',normalized))
    if not matches:
        return question
    common=re.sub(r'(?:الصورة|الموديل)\s*$','',normalized[:matches[0].start()])
    pieces=[normalized[m.end():matches[i+1].start() if i+1<len(matches) else len(normalized)]
            for i,m in enumerate(matches) if refs[m[1]]==index]
    return common + ' ' + ' '.join(pieces)


def eligible_positions(images,products,question=''):
    positions=current_positions(images,products)
    return [p for p in positions if p['available']
            and all(v['supported'] is True for v in product_variants.assess(p,question if len(positions)==1 else image_question(question,p['image_index'])))]


def variant_sentences(product,question):
    checks=product_variants.assess(product,question)
    sentences=[]
    for check in checks:
        value=check['value']
        if check['field']=='size':
            if check['supported'] is False:
                known=customer_copy.fact(product.get('sizes'),'sizes')
                sentences.append(f'قياس {value} مو موجود بهذا الموديل، قياساته {known}')
            elif check['supported'] is True:
                sentences.append(f'قياس {value} موجود ضمن قياساته')
            else:
                sentences.append(f'قياس {value} يحتاج تأكيد حتى أنطيك جواب مضبوط')
        else:
            if check['supported'] is False:
                known=customer_copy.fact(product.get('colors'),'colors')
                sentences.append(f'اللون {value} مو موجود بهالموديل، المتوفر {known}')
            elif check['supported'] is True:
                sentences.append(f'اللون {value} موجود')
            else:
                sentences.append(f'اللون {value} يحتاج تأكيد من المتجر')
    return checks,sentences


def stock_state(product):
    stock = str((product or {}).get('stock') or '').strip().lower()
    if str((product or {}).get('status') or '').lower() in ('inactive', 'disabled'):
        return 'out'
    quantity = (product or {}).get('stock_quantity')
    if quantity is not None and str(quantity).strip() and str(quantity).strip().isdigit() and int(str(quantity).strip()) == 0:
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
        for field in ('product_name', 'stock', 'price', 'sizes', 'colors', 'fabric', 'status'):
            item[field] = (product or {}).get(field)
        item['available'] = bool(product and stock_state(product) == 'available')
        result.append(item)
    return result


UNKNOWN_REPLY = 'هذا الموديل مو متوفر عدنا حالياً.'


def parts(images, products, question=''):
    lines = []
    available_count = 0
    catalog={p['product_id']:p for p in products}
    positions=current_positions(images,products)
    for item in positions:
        # An ordinal is useful only when the customer is comparing photos.
        # A known single-model lookup should answer about that model directly.
        ref = reference(item['image_index']) if len(positions) > 1 else ''
        prefix = ref + '، ' if ref else ''
        specific=question if len(positions)==1 else image_question(question,item['image_index'])
        if not item['product_found']:
            # No match is the owner's customer-facing availability policy.
            # Keep identity unknown internally, including its pending review.
            lines.append(prefix + ('ما گدرت أتأكد من هذا الموديل هسه، خليني أراجعه.'
                                   if item.get('service_error') else UNKNOWN_REPLY))
            continue
        name = customer_copy.fact(item.get('product_name')) or 'هذا الموديل'
        state = stock_state(item)
        if state == 'out':
            lines.append(prefix + name + ' مو متوفر عدنا حالياً.')
            continue
        checks,details=variant_sentences(item,specific)
        blocked=any(c['supported'] is not True for c in checks)
        if state=='available' and not blocked:
            openings=('متوفر عيني','هم عدنا','موجود')
            availability=openings[min(available_count,2)]
            line=(ref+' ' if ref else '')+availability+'، '+name
            available_count += 1
        elif state=='unknown':
            line=prefix+name+' خليني أتأكد من توفره'
        else:
            line=prefix+name
        try:
            count,price,_=terms(catalog[item['product_id']])
        except ValueError:
            count,price=1,None
        if price and not blocked:
            line += f' بـ{price:,} د.ع' + (f' للبكج ({count} قطع)' if count>1 else '')
        if details:
            line += '، ' + '؛ '.join(details)
        # Answer requested facts for each photo, never a single model's facts for all.
        for pattern, field, title in [(r'قماش|خام[ةه]', 'fabric', 'القماش'),
                                      (r'قياس|مقاس|عمر|وزن', 'sizes', 'قياساته'),
                                      (r'لون|الوان|ألوان', 'colors', 'ألوانه')]:
            value = customer_copy.fact(item.get(field), field)
            if re.search(pattern, specific) and not (checks and field in {'sizes','colors'}):
                line += '، ' + title + (' ' + value if value else ' تحتاج تأكيد من المتجر')
        lines.append(line + '.')
    return lines


def summary(images, products, question=''):
    return '\n\n'.join(parts(images, products, question))


def response(images, products, question='', extra=''):
    positions = current_positions(images, products)
    if len(positions) > 1 and all((not p['product_found'] or stock_state(p) == 'out') and not p.get('service_error') for p in positions):
        message = 'للأسف هالموديلات ما متوفرة، تحبين أقترحلج غيرهن؟'
        return {'reply': message, 'reply_parts': [message], 'create_order': False, 'order': {},
                'intent': 'image_check', '_local_reply': True, '_ordered_album': True, 'requires_human': False}
    bubbles = parts(images, products, question)
    if extra and bubbles:
        eligible=eligible_positions(images,products,question)
        target = len(bubbles)-1  # One next-step question, at the end of the whole turn.
        if eligible and eligible[-1]['image_index'] - 1 != target:
            extra = 'وبالنسبة للموديل المتوفر، ' + extra
        bubbles[target] += '\n' + extra
    return {'reply': '\n\n'.join(bubbles), 'reply_parts': bubbles,
            'create_order': False, 'order': {}, 'intent': 'image_check',
            '_needs_fact_review': any(p['product_found'] and stock_state(p)!='out' and
                (any(v['supported'] is None for v in product_variants.assess(p,question if len(images)==1 else image_question(question,p['image_index'])))
                 or any(re.search(pattern,question if len(images)==1 else image_question(question,p['image_index']))
                        and not customer_copy.fact(p.get(field),field)
                        for pattern,field in [(r'قماش|خام[ةه]','fabric'),(r'قياس|مقاس','sizes'),(r'لون|الوان|ألوان','colors')]))
                for p in current_positions(images,products)),
            '_local_reply': True, '_ordered_album': True, 'requires_human': False}


def next_question(images, products, customer, context=None, store='default', question=''):
    available = eligible_positions(images,products,question)
    if not available:
        return ''
    distinct = {x['product_id'] for x in available}
    if product_variants.requested(question)['sizes'] or product_variants.requested(question)['colors']:
        return ''  # A specific availability question needs its answer, not a sales prompt.
    if len(distinct) > 1:
        return 'أي واحد عجبج أكثر؟'
    evidence = ' '.join(x.get('text', '') for x in (context or {}).get('evidence', []))
    if store == 'golden-threads':
        return 'شكد الكمية اللي تحتاجها؟'
    if not re.search(r'قياس|مقاس|سايز|عمر|سنوات|وزن|كيلو|\b(?:xs|s|m|l|xl|xxl)\b', evidence, re.I):
        return 'شكد عمر الطفل حتى نتأكد من القياس؟' if store == 'baraah-kids' else 'شنو قياسج عادةً؟'
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
    markers = {normalize(marker): known.get(x.get('product_id')) for x in positions
               for marker in (label(x['image_index']),reference(x['image_index']))}
    unknown_policy = {normalize(marker) for x in positions for marker in (label(x['image_index']),reference(x['image_index']))
                      if not x['product_found'] and not x.get('service_error')}
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
    negative = r'(?<!\w)(?:(?:غير|مو|ما)\s*(?:متوفر[ةه]?|موجود[ةه]?)|(?:مو|ما)\s*(?:عدنا|عندنا)|مخلص[ةه]?|خلصان[ةه]?|ناف[ذد][ةه]?|نف[ذد]ت?|خلصت?)(?!\w)'
    for product, segment in targets:
        # Qualified absence is a missing variant/fact, not whole-product stock.
        clauses = re.split(r'[،,؛]|\b(?:بس|اما)\b', segment)
        for clause in clauses:
            # Color words inside the catalog name do not qualify a stock claim.
            qualifier = clause.replace(normalize((product or {}).get('product_name')), '') if product else clause
            if re.search(r'فيديو|قياس\s*\d|مقاس\s*\d|لون|الابيض|الاسود|الاحمر|الرصاصي|البيج|الازرق|عمر\s*\d|طول|عرض|بطان', qualifier):
                continue
            absent = bool(re.search(negative, clause))
            if not product and absent:
                if (normalize(UNKNOWN_REPLY) in segment
                        and any(segment.startswith(marker) for marker in unknown_policy)):
                    continue
                return 'الصورة غير المعروفة لا تثبت نفاد المنتج؛ وضح أن موديلها لم يُحدد بعد.'
            state = stock_state(product)
            if state == 'available' and absent:
                return 'الموديل متوفر حالياً؛ لا تنف توفره بسبب صورة مجهولة أو معلومة ناقصة.'
            if state == 'out' and not absent and re.search(r'متوفر|موجود|احجز', clause):
                return 'الموديل نافد حالياً؛ لا تعرض شراءه كأنه متوفر.'
    return ''
