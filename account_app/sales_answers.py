"""Small factual recovery for compound questions; never infers a basket or consent."""
import re

try:
    from . import customer_copy
    from .checkout import normalized
    from .pricing import terms
except ImportError:
    import customer_copy
    from checkout import normalized
    from pricing import terms


def topics(question):
    text = normalized(question)
    result = []
    patterns = {
        'price': r'سعر|بشكد|بكم|بيش|غالي|ميزاني|خصم|تخفيض',
        'colors': r'الوان|(?<!\w)(?:و?ال)?لون(?:ه|ها)?(?!\w)|الاسود|الابيض|الوردي|الجوزي',
        'sizes': r'قياس|مقاس|سايز',
        'fabric': r'قماش|خام[ةهت]',
        'delivery': r'توصيل|شحن',
        'payment': r'دفع|ادفع',
        'delivery_time': r'(?:شوكت|يمتى|متى|كم يوم|شكد وقت).{0,30}(?:يوصل|توصيل|الطلب)',
        'inspection': r'فحص|افحص|ارجع|يرجع',
        'video': r'فيديو',
        'weight_fit': r'وزن|كيلو|كغم',
        'width': r'عرض(?:ه|ها)|عرض.{0,25}(?:سم|سانتي|سنتيمتر)|تحت الابط|محيط الصدر',
        'length': r'طول(?:ه|ها)|(?<!\w)الطول(?!\w)|طول (?:القطعة|الفستان|القميص|الموديل|البنطرون|البنطلون|الكيلوت|اكيلوت|التنورة|العباءة)',
        'lining': r'بطان|مبطن',
        'stretch': r'مطاط|ليكرا|يتمدد',
        'closure': r'ينفتح|سحاب|سح?اب|اغلاق|ازرار',
        'location': r'وين\s+(?:المحل|محلكم|موقعكم)|عنوان\s+المحل|موقع\s+المحل',
        'checkout_help': r'شلون\s+(?:اطلب|احجز)|كيف\s+(?:اطلب|احجز)|طريقة\s+(?:الطلب|الحجز)',
    }
    for key, pattern in patterns.items():
        if re.search(pattern, text):
            result.append(key)
    if re.search(r'(?:سعر|بشكد|بكم|بيش)\s*(?:ال)?(?:توصيل|شحن)', text) and not re.search(
            r'(?:سعر(?:ه|ها)|سعر\s+(?:القطعة|الموديل|الفستان)|السعر\s*و)', text):
        result = [key for key in result if key != 'price']
    return result


def isolated_gap(question):
    """A fast missing-fact answer must never consume a buying or mixed turn."""
    return (len(topics(question)) == 1 and not re.search(
        r'ثبت|احجز|اطلب|اريد.{0,20}(?:اخذ|اشتري)|077|078|079|075', normalized(question)))


def product_fragments(question, product):
    """Return only sanitized requested facts and precise gaps for staff review."""
    if not product:
        return [], []
    selected = topics(question)
    facts, gaps = [], []
    if 'price' in selected:
        try:
            count, price, _ = terms(product)
            facts.append((f'سعر البكج من {count} قطع: {price:,} د.ع.' if count > 1
                          else f'سعر الموديل: {price:,} د.ع.'))
        except (ValueError, TypeError):
            gaps.append('سعر الموديل يحتاج تأكيد من المتجر.')
    fields = [('colors', 'ألوان الموديل'), ('sizes', 'قياسات الموديل'),
              ('fabric', 'القماش'), ('lining', 'البطانة'),
              ('stretch', 'المطاط'), ('closure', 'الإغلاق')]
    for key, label in fields:
        if key not in selected:
            continue
        value = customer_copy.fact(product.get(key), key).strip()
        # A field is evidence only if it can be rendered without staff directions.
        if value and not customer_copy.has_instructions(value) and len(value) <= 220:
            facts.append(f'{label}: {value}.')
        else:
            gaps.append(f'{label} يحتاج تأكيد من المتجر.')
    measurements = customer_copy.fact(product.get('measurements')).strip()
    for key, label, pattern in [('width', 'عرض القطعة بالسنتيمتر', r'عرض|ابط|محيط الصدر'),
                                ('length', 'طول القطعة', r'طول')]:
        if key in selected:
            if measurements and len(measurements) <= 220 and re.search(pattern, normalized(measurements)):
                if measurements not in facts:
                    facts.append(measurements)
            else:
                gaps.append(f'{label} يحتاج قياس من المتجر حتى نتأكد من الرقم الصحيح.')
    if 'video' in selected and not product.get('video_url'):
        facts.append('هسه ما عندي فيديو لهالموديل، المتوفر صوره.')
    if 'location' in selected:
        gaps.append('عنوان المحل يحتاج تأكيد من المتجر.')
    # Unrecognized clauses remain visible to staff instead of disappearing
    # behind the parts we can answer. Customer choices/statements are not gaps.
    clauses = re.split(r'\n+|[؟?]|\s+و(?=شنو|شكد|كم|منين|وين|متى|شلون|هل)', str(question or ''))
    for clause in clauses:
        if re.search(r'شنو|شكد|منين|وين|متى|شلون|كيف|هل\s', normalized(clause)) and not topics(clause):
            gaps.append('بقية التفاصيل المطلوبة تحتاج تأكيد من المتجر.')
            break
    return list(dict.fromkeys(facts)), list(dict.fromkeys(gaps))


def known_budget(question, context=None):
    """Read the latest explicit user budget, never an assistant's offered price."""
    sources = [item.get('text') or '' for item in (context or {}).get('evidence') or []]
    sources.append(question)
    for value in reversed(sources):
        text = normalized(value).replace(',', '').replace('٬', '')
        match = re.search(r'(?:ميزاني(?:تي|ه|ة)?|بحدود|ما اتجاوز|حدي|سقفي)\s*(\d{1,7})\s*(الف|الاف)?', text)
        if match:
            amount = int(match[1])
            return amount * 1000 if match[2] or amount < 1000 else amount
    return None


def missing_answers(reply, question, product):
    """Check a few unambiguous facts so a mixed question gets every answer.

    These checks request one repair; they never manufacture an order or pretend
    that a generic size range answers a personal fit question.
    """
    selected = topics(question)
    if len(selected) < 2 or not product:
        return []
    text, clean = normalized(question), normalized(reply)
    missing = []
    if 'price' in selected and re.search(r'سعر|بكم|بشكد|بيش', text):
        try:
            _, price, _ = terms(product)
            flat = clean.replace(',', '').replace('٬', '')
            values = [r'(?<!\d)' + str(price) + r'(?!\d)']
            if price % 1000 == 0:
                values.append(r'(?<!\d)' + str(price // 1000) + r'\s*(?:الف|الاف)(?!\w)')
            if not any(re.search(value, flat) for value in values):
                missing.append('سعر المنتج الصحيح')
        except (ValueError, TypeError):
            pass
    if 'fabric' in selected:
        value = normalized(customer_copy.fact(product.get('fabric'), 'fabric'))
        if value and len(value.split()) == 1 and value not in clean:
            missing.append('القماش')
    if 'video' in selected and not product.get('video_url') and 'فيديو' not in clean:
        missing.append('عدم توفر فيديو حالياً')
    if 'delivery' in selected and not re.search(r'توصيل|شحن|مجاني', clean):
        missing.append('التوصيل')
    return missing
