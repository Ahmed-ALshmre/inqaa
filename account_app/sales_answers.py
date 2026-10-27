"""Small factual recovery for compound questions; never infers a basket or consent."""
import re

try:
    from . import customer_copy, sales_strategy, product_variants
    from .checkout import normalized
    from .pricing import terms, offer_tiers, quote
    from .sales_playbook import requested_quantity
except ImportError:
    import customer_copy, sales_strategy, product_variants
    from checkout import normalized
    from pricing import terms, offer_tiers, quote
    from sales_playbook import requested_quantity


def topics(question):
    text = normalized(question)
    result = []
    patterns = {
        'price': r'سعر|بشكد|بكم|بيش|غالي|ميزاني|خصم|تخفيض|عرض\s*(?:ال)?(?:قطعتين|قطعتان|\d+\s*قطع)|(?:شكد|اشكد|كم)\s*(?:ثنين|قطعتين|\d+\s*قطع)',
        'colors': r'الوان|(?<!\w)(?:و?ال)?لون(?:ه|ها)?(?!\w)|الاسود|الابيض|الوردي|الجوزي',
        'sizes': r'قياس|مقاس|سايز',
        'fabric': r'قماش|خام[ةهت]',
        'delivery': r'توصيل|شحن',
        'payment': r'دفع|ادفع',
        'delivery_time': r'(?:شوكت|يمتى|متى|كم يوم|شكد وقت).{0,30}(?:يوصل|توصيل|الطلب)',
        'inspection': r'فحص|افحص|ارجع|يرجع',
        'video': r'فيديو',
        'weight_fit': r'وزن|كيلو|كغم',
        'width': r'عرض(?:ه|ها)|(?<!\w)و?العرض(?!\w)|عرض\s+(?:القطعة|الصدر)|عرض.{0,25}(?:سم|سانتي|سنتيمتر)|تحت الابط|محيط الصدر',
        'length': r'طول(?:ه|ها)|(?<!\w)الطول(?!\w)|طول (?:القطعة|الفستان|القميص|الموديل|البنطرون|البنطلون|الكيلوت|اكيلوت|التنورة|العباءة)',
        'lining': r'بطان|مبطن',
        'stretch': r'مطاط|ليكرا|يتمدد',
        'closure': r'ينفتح|سحاب|سح?اب|اغلاق|ازرار|زرار|كبس|لاستيك|استيك|(?<!\w)(?:دگم|دكم)(?!\w)',
        'location': r'وين\s+(?:المحل|محلكم|موقعكم)|عنوان\s+المحل|موقع\s+المحل',
        'checkout_help': r'شلون\s+(?:اطلب|احجز)|كيف\s+(?:اطلب|احجز)|طريقة\s+(?:الطلب|الحجز)',
        'specifications': r'مواصفات|تفاصيل\s+(?:الموديل|الفستان|القطعة)',
        'composition': r'هو\s+قطعتين|قطعتين\s+يعني|(?:هو|هي|الموديل|الفستان)\s+قطعة\s+وحد[ةه]|(?:فستان|تنور[ةه])\s+لو\s+(?:فستان|تنور[ةه]|قطعتين)',
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
    if 'specifications' in selected:
        selected = list(dict.fromkeys(selected + ['price', 'fabric', 'colors', 'sizes']))
    if 'composition' in selected or 'specifications' in selected:
        recorded = normalized(' '.join(customer_copy.fact(product.get(k)) for k in
                                       ('faq', 'bundle_contents', 'sale_unit', 'description')))
        if re.search(r'فستان', normalized(product.get('product_name'))) and re.search(
                r'قطعة\s+وحد[ةه]|قطعه\s+وحد[ةه]', recorded):
            facts.append('الفستان قطعة وحدة.')
        elif 'composition' in selected:
            gaps.append('عدد قطع الموديل يحتاج تأكيد من المتجر.')
    if 'price' in selected:
        try:
            count, price, _ = terms(product)
            facts.append((f'سعر البكج من {count} قطع: {price:,} د.ع.' if count > 1
                          else f'سعر الموديل: {price:,} د.ع.'))
            for quantity, amount in offer_tiers(product):
                facts.append(f'عرض {quantity} قطع: {amount:,} د.ع.')
            requested = requested_quantity(question)
            if requested:
                amount = quote([{'product_id': product['product_id'], 'quantity': requested}], [product], 0)['product_total']
                facts.append(f'سعر {requested} قطع: {amount:,} د.ع.')
        except (ValueError, TypeError, KeyError):
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
    for key, label in [('width', 'عرض القطعة بالسنتيمتر'), ('length', 'طول القطعة')]:
        if key in selected:
            measurement = sales_strategy.measurement_fact(product, key)
            if measurement:
                if measurement not in facts:
                    facts.append(measurement)
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


def evidence_text(item):
    """Interpret a short answer only when the saved question establishes its unit."""
    value = str(item.get('text') or '')
    question = normalized(item.get('in_reply_to'))
    clean = normalized(value).strip(' .!،؟?')
    if (re.search(r'ميزاني|حدود السعر|سقف السعر', question)
            and re.fullmatch(r'\d{1,7}\s*(?:الف|الاف)?(?:\s+(?:ويا|مع)\s+التوصيل)?', clean)):
        return 'ميزانيتي ' + clean
    if re.fullmatch(r'\d{1,3}|[2-6]?xl|xxl|xxxl|xs|[sml]', clean):
        # Several different numeric slots in one question are ambiguous.
        if re.search(r'قياس|مقاس|سايز', question) and not re.search(r'عمر|وزن|عدد|كمية|كم قط|شكد قط', question):
            return 'قياس ' + clean
        if (re.search(r'عمر', question) and re.search(r'بالسنوات|سنوات|سنين|سنة|سنه', question)
                and not re.search(r'شهر|اشهر|قياس|مقاس|وزن|عدد|كمية', question)):
            return 'عمر ' + clean + ' سنوات'
    return value


def known_budget(question, context=None):
    """Read the latest explicit user budget, never an assistant's offered price."""
    sources = [evidence_text(item) for item in (context or {}).get('evidence') or []]
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
    if 'price' in selected and re.search(r'سعر|بكم|بشكد|بيش|عرض\s*(?:ال)?(?:قطعتين|قطعتان|\d+\s*قطع)|(?:شكد|اشكد|كم)\s*(?:ثنين|قطعتين|\d+\s*قطع)', text):
        try:
            _, price, _ = terms(product)
            requested = requested_quantity(question)
            if requested:
                price = quote([{'product_id': product['product_id'], 'quantity': requested}], [product], 0)['product_total']
            flat = clean.replace(',', '').replace('٬', '')
            values = [r'(?<!\d)' + str(price) + r'(?!\d)']
            if price % 1000 == 0:
                values.append(r'(?<!\d)' + str(price // 1000) + r'\s*(?:الف|الاف)(?!\w)')
            if not any(re.search(value, flat) for value in values):
                missing.append('مجموع القطع الصحيح' if requested else 'سعر المنتج الصحيح')
        except (ValueError, TypeError, KeyError):
            pass
    if 'fabric' in selected:
        value = normalized(customer_copy.fact(product.get('fabric'), 'fabric'))
        if value and len(value.split()) == 1 and value not in clean:
            missing.append('القماش')
    if 'video' in selected and not product.get('video_url') and 'فيديو' not in clean:
        missing.append('عدم توفر فيديو حالياً')
    if 'delivery' in selected and not re.search(r'توصيل|شحن|مجاني', clean):
        missing.append('التوصيل')
    # Check coverage separately from factual accuracy. A fee alone does not
    # answer an arrival question, and a price cannot replace a lining answer.
    coverage = {
        'delivery_time': ('مدة الوصول', r'يوم|ايام|اسبوع|باجر|باكر|خلال|مد[ةه]|وقت.{0,16}(?:وصول|توصيل)|موعد.{0,16}(?:تاكيد|مضمون)'),
        'payment': ('طريقة الدفع', r'دفع|ادفع|استلام|كاش|نقد|بطاق|تحويل'),
        'inspection': ('الفحص والإرجاع', r'فحص|افحص|ارجاع|يرجع|ترجع|استبدال'),
        'lining': ('البطانة', r'بطان|مبطن'),
        'stretch': ('المطاط', r'مطاط|ليكرا|يتمدد|تمدد'),
        'closure': ('الإغلاق', r'سحاب|كبس|ازرار|زرار|اغلاق|ينفتح'),
        'width': ('عرض القطعة', r'عرض|ابط|محيط الصدر'),
        'length': ('طول القطعة', r'طول'),
        'location': ('عنوان المحل', r'محل|عنوان|موقع|شارع|منطق[ةه]'),
        'checkout_help': ('طريقة الحجز', r'حجز|طلب|نحدد|رقم|عنوان'),
        'weight_fit': ('ملاءمة القياس', r'وزن|كيلو|كغم|قياس|مقاس|سايز|جدول'),
        'composition': ('محتويات الموديل', r'قطعتين|قطع[ةه]|طقم|مكون|محتو'),
    }
    for key, (label, pattern) in coverage.items():
        if key == 'weight_fit' and sales_strategy._declared_size(question):
            continue
        if key in selected and not re.search(pattern, clean):
            missing.append(label)
    # A declared choice ("الأسود قياس 44، شنو قماشه؟") is not a second
    # question. Require an actual options/availability question here.
    if ('colors' in selected and re.search(r'الوان|(?:لون|الاسود|الابيض|الوردي|الجوزي).{0,16}(?:متوفر|موجود)|(?:متوفر|موجود).{0,16}(?:لون|الاسود|الابيض|الوردي|الجوزي)', text)
            and not product_variants.colors(clean) and not re.search(r'لون|الوان', clean)):
        missing.append('الألوان')
    if ('sizes' in selected and re.search(r'قياسات|مقاسات|(?:شنو|شكد|كم).{0,12}(?:قياس|مقاس)|(?:قياس|مقاس)(?:ه|ها)\s*(?:شكد|شنو)|(?:قياس|مقاس)\s*\d+\s*(?:متوفر|موجود)', text)
            and not re.search(r'قياس|مقاس|سايز', clean)
            and not product_variants.size_data(clean)['known']):
        missing.append('القياسات')
    return missing
