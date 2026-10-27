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


def question_for(item, question, count):
    if count == 1:
        return question
    caption = item.get('question')
    scoped = image_question(question, item['image_index'])
    # Explicit ordinals override attachment chronology. Otherwise a caption
    # after a photo belongs to that photo, not to every image in the burst.
    if scoped != question:
        return scoped
    if caption is not None and re.search(r'عمر|قياس|مقاس|لون|سن[ةه]|سنوات', question):
        return caption
    return question


def eligible_positions(images,products,question=''):
    positions=current_positions(images,products)
    return [p for p in positions if p['available']
            and all(v['supported'] is True for v in variant_sentences(p,position_question(p,question,len(positions)))[0])]


def position_question(item, question, count):
    specific = question_for(item, question, count)
    color = product_variants.single_color(item.get('observed_color'))
    if color and not product_variants.requested(specific)['colors']:
        specific += ' اللون ' + color
    return specific


def variant_sentences(product,question):
    checks=product_variants.assess(product,question)
    try:
        from .conversation_quality import child_age
    except ImportError:
        from conversation_quality import child_age
    years = product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))['years']
    age = child_age(question)
    if years and age is not None and not any(check.get('age_based') for check in checks):
        checks.append({'field': 'age', 'value': f'{age:g}',
                       'supported': any(low <= age <= high for low, high in years)})
    sentences=[]
    for check in checks:
        value=check['value']
        if check['field']=='size':
            if check.get('age_based'):
                age_label = product_variants.age_text(value)
                if check['supported']:
                    sentences.append(f'عمر {age_label} متوفر')
                else:
                    known = customer_copy.fact(product.get('sizes'), 'sizes')
                    sentence = f'عمر {age_label} مو ضمن قياسات هالموديل، المتوفر {known}'
                    fit = product_variants.boundary_fit(product, float(value)) if re.fullmatch(r'\d+(?:\.\d+)?', value) else None
                    build = product_variants.child_build(question)
                    if fit and (not build or build == fit['build']):
                        sentence += f"؛ ممكن يناسبه قياس {fit['size']} سنوات إذا {fit['condition']}"
                    sentences.append(sentence)
                continue
            if check['supported'] is False:
                known=customer_copy.fact(product.get('sizes'),'sizes')
                sentences.append(f'قياس {value} مو موجود بهذا الموديل، قياساته {known}')
            elif check['supported'] is True:
                sentences.append(f'قياس {value} موجود ضمن قياساته')
            else:
                sentences.append(f'قياس {value} يحتاج تأكيد حتى أنطيك جواب مضبوط')
        elif check['field'] == 'age':
            if check['supported']:
                sentences.append(f'عمر {value} موجود ضمن أعمار الموديل')
            else:
                known = customer_copy.fact(product.get('sizes'), 'sizes')
                sentences.append(f'عمر {value} مو موجود بهذا الموديل، أعمار الموديل {known}')
        else:
            if check['supported'] is False:
                known=customer_copy.fact(product.get('colors'),'colors')
                sentences.append(f'اللون {value} مو موجود بهالموديل، المتوفر {known}')
            elif check['supported'] is True:
                sentences.append(f'اللون {value} موجود')
            else:
                sentences.append(f'اللون {value} يحتاج تأكيد من المتجر')
    return checks,sentences


def remaining_colors(product):
    """Read explicit stock facts, never instructions embedded in catalog prose."""
    note = customer_copy.normalized(customer_copy.fact((product or {}).get('notes')))
    match = re.search(r'(?:باقي|متبقي|المتبقي)\s+(?:بس|فقط)\s+([^.!\n]+)', note)
    if not match:
        return []
    return product_variants.colors(match[1])


def sales_product(product):
    """Narrow stock from a precise remaining-color note without editing the catalog.

    A subset is useful stock information (cream sold out, black remains), not
    a reason to silence every question about the model. Disjoint declarations
    still require review; they cannot introduce a new color into inventory.
    """
    result = dict(product)
    remaining = remaining_colors(product)
    recorded = set(product_variants.colors(product.get('colors')))
    if remaining and set(remaining) <= recorded:
        result['colors'] = '، '.join(remaining)
    return result


def color_conflict(product):
    recorded = set(product_variants.colors((product or {}).get('colors')))
    remaining = set(remaining_colors(product))
    return bool(recorded and remaining and not remaining <= recorded)


def stock_state(product):
    stock = str((product or {}).get('stock') or '').strip().lower()
    if str((product or {}).get('status') or '').lower() in ('inactive', 'disabled'):
        return 'out'
    quantity = (product or {}).get('stock_quantity')
    if quantity is not None and str(quantity).strip() and str(quantity).strip().isdigit() and int(str(quantity).strip()) == 0:
        return 'out'
    if any(word in stock for word in ('نفذ', 'نافذ', 'نافد', 'خلص', 'غير متوفر', 'مو متوفر', 'not available', 'out')):
        return 'out'
    if color_conflict(product):
        return 'unknown'
    return 'available' if 'متوفر' in stock or 'available' in stock else 'unknown'


def current_positions(images, products):
    catalog = {p['product_id']: p for p in products}
    result = []
    for index, image in enumerate(images, 1):
        item = dict(image, image_index=index)
        product = catalog.get(item.get('product_id')) if item.get('product_found') else None
        product = sales_product(product) if product else None
        item['product_found'] = bool(product)
        # Re-read the stock, name and price; recognition memory is identity only.
        for field in ('product_name', 'stock', 'stock_quantity', 'price', 'sizes', 'colors', 'fabric', 'status', 'notes'):
            item[field] = (product or {}).get(field)
        item['observed_color'] = product_variants.single_color(item.get('observed_color'))
        item['available'] = bool(product and stock_state(product) == 'available')
        result.append(item)
    return result


UNKNOWN_REPLY = ''  # No visual match is uncertainty, not evidence of no stock.


def _individual_parts(images, products, question=''):
    lines = []
    available_count = 0
    catalog={p['product_id']:p for p in products}
    positions=current_positions(images,products)
    for item in positions:
        # An ordinal is useful only when the customer is comparing photos.
        # A known single-model lookup should answer about that model directly.
        ref = reference(item['image_index']) if len(positions) > 1 else ''
        prefix = ref + '، ' if ref else ''
        specific=position_question(item, question, len(positions))
        if not item['product_found']:
            # The caller creates a review for this image. Answer known photos
            # independently without inventing stock or sending a holding loop.
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
            continue
        else:
            line=prefix+name
        try:
            count,price,_=terms(catalog[item['product_id']])
        except ValueError:
            count,price=1,None
        if price and (not blocked or re.search(r'سعر|بشكد|بكم|بيش', specific)):
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


def _price(product, named=False):
    try:
        count, price, _ = terms(product)
    except (ValueError, TypeError, KeyError):
        return ''
    if not price:
        return ''
    label = ('سعر ' + customer_copy.fact(product.get('product_name'))) if named else 'سعره'
    return f'{label} {price:,} د.ع' + (f' للبكج ({count} قطع)' if count > 1 else '')


def parts(images, products, question=''):
    """Group repeated identities while keeping original positions in memory."""
    positions = current_positions(images, products)
    if len(positions) <= 1:
        return _individual_parts(images, products, question)
    if all(p['product_found'] and stock_state(p) == 'out' for p in positions):
        return ['هذي الموديلات مو متوفرة عدنا حالياً.']

    catalog = {p['product_id']: p for p in products}
    groups = {}
    for item in positions:
        specific = position_question(item, question, len(positions))
        checks, details = variant_sentences(item, specific) if item['product_found'] else ([], [])
        # Different explicit sizes or factual questions are separate choices.
        # A color change alone belongs to the same model description.
        key = (item.get('product_id'),
               tuple((c['field'], c['value'], c['supported']) for c in checks if c['field'] != 'color'),
               tuple(bool(re.search(pattern, specific)) for pattern in (r'قماش|خام[ةه]', r'قياس|مقاس|عمر|وزن', r'الوان|ألوان')))
        if not item['product_found']:
            key = ('unknown', item['image_index'])
        group = groups.setdefault(key, dict(item=item, positions=[], checks=[], details=[], questions=[]))
        group['positions'].append(item['image_index'])
        group['questions'].append(specific)
        for check, detail in zip(checks, details):
            if check not in group['checks']:
                group['checks'].append(check)
                group['details'].append(detail)

    groups = list(groups.values())
    lines = []
    collective = []
    for group in groups:
        item, checks = group['item'], group['checks']
        prefix = (' و'.join(reference(i) for i in group['positions']) + '، ') if len(groups) > 1 else ''
        if not item['product_found']:
            collective.append(False)
            continue
        name = customer_copy.fact(item.get('product_name')) or 'هذا الموديل'
        state = stock_state(item)
        if state == 'out':
            lines.append(prefix + name + ' مو متوفر عدنا حالياً.')
            collective.append(False)
            continue
        colors = [c for c in checks if c['field'] == 'color']
        available_colors = [c['value'] for c in colors if c['supported'] is True]
        sizes_ok = all(c['supported'] is True for c in checks if c['field'] == 'size')
        available = state == 'available' and sizes_ok and (not colors or bool(available_colors))
        blocked = not sizes_ok or (bool(colors) and not available_colors)
        line = prefix + name
        if available:
            color_text = (' بال' + ' وال'.join(available_colors)) if available_colors else ''
            line += ' متوفر' + color_text + ' عيني 🌷'
        elif state == 'unknown':
            collective.append(False)
            continue
        price = _price(catalog[item['product_id']]) if not blocked else ''
        if price:
            line += '، ' + price
        details = []
        for check, detail in zip(checks, group['details']):
            if check['field'] == 'color':
                if check['supported'] is True:
                    continue
                if check['supported'] is False:
                    detail = 'أما ال' + check['value'] + ' فمو متوفر حالياً'
            details.append(detail)
        specific = ' '.join(group['questions'])
        for pattern, field, title in [(r'قماش|خام[ةه]', 'fabric', 'القماش'),
                                      (r'قياس|مقاس|عمر|وزن', 'sizes', 'قياساته'),
                                      (r'لون|الوان|ألوان', 'colors', 'ألوانه')]:
            if re.search(pattern, specific) and not any(c['field'] == field.rstrip('s') for c in checks):
                value = customer_copy.fact(item.get(field), field)
                details.append(title + (' ' + value if value else ' تحتاج تأكيد من المتجر'))
        if details:
            line += '، ' + '؛ '.join(details)
        lines.append(line + '.')
        collective.append(available and all(c['supported'] is True for c in checks) and not details and not colors)

    return lines


def summary(images, products, question=''):
    return '\n\n'.join(parts(images, products, question))


def response(images, products, question='', extra=''):
    positions = current_positions(images, products)
    if len(positions) > 1 and all(p['product_found'] and stock_state(p) == 'out' for p in positions):
        message = 'للأسف هالموديلات ما متوفرة، تحبين أقترحلج غيرهن؟'
        return {'reply': message, 'reply_parts': [message], 'create_order': False, 'order': {},
                'intent': 'image_check', '_local_reply': True, '_ordered_album': True, 'requires_human': False}
    bubbles = parts(images, products, question)
    if extra and bubbles:
        eligible=eligible_positions(images,products,question)
        target = len(bubbles)-1  # One next-step question, at the end of the whole turn.
        described = [p for p in positions if p['product_found'] and stock_state(p) != 'unknown']
        if eligible and described and eligible[-1]['image_index'] != described[-1]['image_index']:
            extra = 'وبالنسبة للموديل المتوفر، ' + extra
        bubbles[target] += '\n' + extra
    return {'reply': '\n\n'.join(bubbles), 'reply_parts': bubbles,
            'create_order': False, 'order': {}, 'intent': 'image_check',
            '_needs_fact_review': any(p['product_found'] and stock_state(p)!='out' and
                (any(v['supported'] is None for v in product_variants.assess(p, position_question(p, question, len(images))))
                 or any(re.search(pattern, question_for(p, question, len(images)))
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
    if any(product_variants.requested_age(item, question) is not None for item in available):
        return ''
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
            if (product_variants.colors(qualifier) or
                    re.search(r'فيديو|قياس\s*\d|مقاس\s*\d|لون|الابيض|الاسود|الاحمر|الرصاصي|البيج|الازرق|عمر\s*\d|طول|عرض|بطان', qualifier)):
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
