"""Current catalog facts and bounded checks for a single selected model."""
import re

try:
    from . import customer_copy
    from .pricing import terms, quote, money
except ImportError:
    import customer_copy
    from pricing import terms, quote, money


GUIDE = """[مرجع حقائق المنتج]
حقل price الحالي وحساب السعر المرفق يتقدمان على أي مبلغ قديم في الوصف أو الإعلان أو المحادثة. المخزون الحالي هو مرجع التوفر؛ عدم التعرف على صورة لا يعني نفاد الموديل.
اسم المنتج وفئته يحددان نوعه. الفستان يبقى فستاناً؛ صف اتساعه بعبارة «واسع من الأسفل» ولا تسمه تنورة أو طقماً. لا تستنتج أن له قطعة منفصلة من وصف القصّة. عدد قطع الطقم من محتوياته المسجلة فقط.
أجب عن السؤال مباشرة وبصياغة عراقية مترابطة. لا تسرد أسماء حقول مثل «قياسات الموديل المسجلة» ولا تلحق كل جواب بسؤال وزن أو حجز. إذا الموديل معروف لا تطلب صورته مجدداً.
"""


def is_dress(product):
    return bool(re.search(r'(?<!\w)(?:ال)?فستان(?!\w)',
                          customer_copy.normalized((product or {}).get('product_name'))))


def dress_wording(text, product):
    """Clarify the cut of a dress without inventing its construction."""
    text = str(text or '')
    if not is_dress(product):
        return text
    text = re.sub(r'تنورت[هة]\s+واسع[ةه]', 'قصته واسعة من الأسفل', text)
    text = re.sub(r'(?<!\w)و?تنور[ةه]\s+واسع[ةه]', 'وواسع من الأسفل', text)
    return text


def product_view(product):
    """Remove stale price prose from AI context, leaving the catalog untouched."""
    result = dict(product)
    for field in ('description', 'visual_description'):
        value = str(result.get(field) or '')
        # Structured price/offer/delivery remain available independently.
        clauses = re.split(r'(?<=[.؛\n])\s*|[،,](?!\d)', value)
        clauses = [part for part in clauses if not re.search(
            r'سعر|دينار|د\.ع|\d\s*(?:ألف|الف|آلاف|الاف)', part)]
        result[field] = dress_wording(' '.join(clauses).strip(), product)
    return result


def identity_error(reply, product):
    if not is_dress(product):
        return ''
    text = customer_copy.normalized(reply)
    construction = customer_copy.normalized(' '.join(str((product or {}).get(k) or '') for k in
                                                     ('product_name', 'bundle_contents', 'sale_unit', 'faq')))
    two_pieces = bool(re.search(r'(?<!مو )(?<!ليس )(?:قطعتين|قطعتان|2\s*قطع)', construction))
    for clause in re.split(r'[،؛.!\n]', text):
        # Explicitly correcting a customer's question is valid.
        if re.search(r'(?:مو|ليس|مش|بدون)\s+(?:تنور[ةه]|بلوز|قميص|طقم|قطعتين)', clause):
            continue
        if re.search(r'(?:هذا|هاي|هو|هي|الموديل)\s+(?:عبار[ةه]\s+عن\s+)?(?:تنور[ةه]|طقم)|تنور[ةه]\s+و(?:بلوز|قميص)', clause) or (
                not two_pieces and re.search(r'(?:يجي|تجي|هو|هي)\s+قطعتين', clause)):
            return 'هذا فستان؛ لا تحوله إلى تنورة أو طقم ولا تخترع قطعة منفصلة.'
    return ''


def price_error(reply, product, delivery_options, question=''):
    """Check labelled current monetary claims, never infer prices from prose.

    The caller limits this to a single model before checkout. Historical prices,
    comparisons, and unlabelled measurements are deliberately left alone.
    """
    if not product:
        return ''
    try:
        count, unit_price, _ = terms(product)
        text = customer_copy.normalized(reply)
        from_quantity = re.search(r'(?:قطعتين|ثنين|(?<!\d)(\d+)\s*قطع)',
                                  customer_copy.normalized(question))
        quantity = (int(from_quantity[1]) if from_quantity and from_quantity[1]
                    else 2 if from_quantity else count)
        if not 1 <= quantity <= 20:
            return ''
        quotes = [quote([dict(product_id=product['product_id'], quantity=quantity)],
                        [product], fee) for fee in delivery_options]
        totals = {q['product_total'] + q['delivery_fee'] for q in quotes}
        shipping = {q['delivery_fee'] for q in quotes}
    except (ValueError, KeyError, TypeError):
        return ''
    amount_re = r'(?<!\d)\d+(?:[,٬]\d{3})*(?:\.\d+)?\s*(?:الف|الاف|دينار|د\.ع)(?!\w)'
    labels = r'المجموع|الاجمالي|ويا التوصيل|مع التوصيل|التوصيل|الشحن|سعر(?:ه|ها)?|بسعر|(?<!\w)ب\s*$'
    previous_end = 0
    for match in re.finditer(amount_re, text):
        prefix = text[previous_end:match.start()]
        previous_end = match.end()
        markers = list(re.finditer(labels, prefix))
        if not markers:
            continue
        marker = markers[-1]
        if re.search(r'كان|سابق|بدل|قبل|بالاعلان', prefix):
            continue
        value = money(match[0])
        label = marker[0]
        suffix = text[match.end():match.end() + 24]
        inclusive = re.match(r'\s*(?:شامل\s+(?:ال)?توصيل|ويا التوصيل|مع التوصيل)', suffix)
        if label in {'المجموع', 'الاجمالي', 'ويا التوصيل', 'مع التوصيل'} or inclusive:
            expected = totals
        elif label in {'التوصيل', 'الشحن'}:
            expected = shipping
        elif re.search(r'قطعتين|ثنين|\d+\s*قطع', prefix):
            scoped_quantity = re.search(r'قطعتين|ثنين|(?<!\d)(\d+)\s*قطع', prefix)
            scoped_count = int(scoped_quantity[1]) if scoped_quantity[1] else 2
            if not 1 <= scoped_count <= 20:
                continue
            try:
                expected = {quote([dict(product_id=product['product_id'], quantity=scoped_count)], [product], 0)['product_total']}
            except ValueError:
                continue
        else:
            expected = {unit_price}
        if value not in expected:
            return 'السعر أو التوصيل أو المجموع يخالف حساب المنتج الحالي؛ استخدم الحساب المرفق.'
    return ''


def conversational(text):
    """Render deterministic facts as a chat reply while retaining every fact."""
    replacements = {
        'سعر الموديل:': 'سعره', 'ألوان الموديل:': 'ألوانه',
        'قياسات الموديل:': 'قياساته', 'القياسات المسجلة:': 'قياساته',
        'الألوان المتوفرة:': 'ألوانه', 'القماش:': 'قماشه',
        'التوصيل:': 'التوصيل', 'الإجمالي لقطعة واحدة:': 'المجموع ويا التوصيل',
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text
