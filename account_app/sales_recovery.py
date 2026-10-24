"""Narrow recovery for policy questions and explicit photo requests."""
import re

try:
    from .checkout import PROVINCES, normalized
except ImportError:
    from checkout import PROVINCES, normalized


def policy_question(text):
    clean = normalized(text).strip(' ؟?!.،\n')
    clean = re.sub(r'(شكد|اشكد|كم|بشكد)(?=التوصيل|توصيل|الشحن|شحن)', r'\1 ', clean)
    clean = re.sub(r'^(?:السلام عليكم|سلام عليكم|هلو|مرحبا)\s*', '', clean)
    clean = re.sub(r'^و(?=توصيل|التوصيل)', '', clean)
    names = {normalized(name) for aliases in PROVINCES.values() for name in aliases}
    names.update('لل' + name[2:] for name in list(names) if name.startswith('ال'))
    location = '|'.join(sorted({re.escape(name) for name in names}, key=len, reverse=True))
    clean = re.sub(r'(?:^|\s)(?:ل|الى |لـ)?(?:' + location + r')(?=\s|$)', ' ', clean)
    clean = ' '.join(clean.split())
    if re.fullmatch(r'(?:متى|يمتى|شوكت)\s+(?:(?:يتم|يصير)\s+)?(?:التوصيل|توصيل|التسليم|تسليم|يوصل الطلب)', clean):
        return 'delivery_time'
    if re.fullmatch(r'(?:ما هي طريقة|ماهي طريقة|كيفية|شلون|كيف|شنو طريقة)\s*(?:الدفع|ادفع)(?:\s*(?:و|او)\s*(?:الشحن|التوصيل))?(?: المتاحة)?', clean):
        return 'payment'
    if re.fullmatch(r'(?:اكو|عدكم|عندكم|هل يوجد)\s+(?:توصيل|شحن)|(?:ما هي طريقة|ماهي طريقة)\s+التوصيل', clean):
        return 'delivery'
    if re.fullmatch(r'(?:شكد|اشكد|كم|بشكد)(?: تبلغ)?\s+(?:(?:تكلفة|اجور|سعر)\s+)?(?:التوصيل|توصيل|الشحن|شحن)|(?:التوصيل|توصيل|الشحن|شحن)\s+(?:شكد|اشكد|كم|بشكد)', clean):
        return 'delivery'
    return ''


def independent_policy_questions(text):
    """Extract explicit store-policy questions from a turn with an unknown photo.

    Do not turn a delivery complaint, a conditional offer or a basket-total
    question into a generic shipping quote. Other clauses stay with the review.
    """
    clean = normalized(text)
    if re.search(r'مجاني|بلاش|اذا|لو |ليش|تاخر|تأخر|مندوب|استبدال|ارجاع|رفض|وصلني|وصلتني', clean):
        return []
    questions = []
    clauses = re.split(r'\n+|[؟?،,!؛]|\s+و(?=شكد|اشكد|كم|متى|يمتى|شوكت|شلون)', clean)
    for clause in clauses:
        clause = clause.strip()
        if policy_question(clause):
            questions.append(clause)
        elif re.fullmatch(r'(?:و)?(?:شكد|اشكد|كم|بشكد)\s*(?:سعر |اجور |تكلفة )?(?:ال)?(?:توصيل|شحن)(?:\s+[^\d]+)?', clause):
            # A district after the province does not change the province fee.
            if not re.search(r'سعره|سعرها|قطعه|قطعة|مجموع|ويا|مع |\bو\b', clause):
                questions.append(clause)
        elif re.fullmatch(r'(?:و)?(?:كم يوم|متى|يمتى|شوكت)\s+(?:يوصل الطلب|يوصل|التوصيل|توصيل)', clause):
            questions.append('متى التوصيل')
    return list(dict.fromkeys(questions))


def policy_province(text):
    """Read a quote destination without capturing a question as an address."""
    clean = normalized(text)
    found = []
    for province, aliases in PROVINCES.items():
        variants = {normalized(alias) for alias in aliases}
        variants.update('ل' + alias[2:] for alias in list(variants) if alias.startswith('ال'))
        if any(re.search(r'(?<!\w)(?:ل|ب|بال)?' + re.escape(alias).replace('ة', '[ةه]') + r'(?!\w)', clean)
               for alias in variants):
            found.append(province)
    return found[0] if len(found) == 1 else None


def explicit_photo_followup(text):
    clean = normalized(text).strip(' ؟?!.،\n')
    clean = ' '.join(clean.split())
    if re.fullmatch(r'(?:دزي|دزلي|دزيلي|دزو)(?:\s+بله)?\s+خل(?:يني)?\s+اشوف', clean):
        return True
    return bool(re.fullmatch(r'(?:دزو|دزوا|دزيهن|دزهن|دزهم|ارسلهم|ارسليهن|شوفينياهن|اشوفهن|اشوفها|اشوف صورته|اشوف صورتها)', clean))


def simple_photo_request(text):
    clean = normalized(text).strip(' ؟?!.،\n')
    clean = re.sub(r'\s+(?:بدون\s+(?:اي\s*)?حجز|بس\s+للمشاهد[هة]|(?:بس\s+)?ما\s*اريد\s*احجز)$', '', clean).strip(' ،')
    clean = re.sub(r'(?:\s+(?:حبيبتي|عيني|حبي|حياتي|بلا زحمة|لو سمحتي))+$', '', clean)
    if explicit_photo_followup(clean):
        return True
    return bool(re.fullmatch(r'(?:(?:ممكن|اريد)\s+)?(?:اشوف|دزيلي|دزي|دزلي|ارسلي|شوفيني|وريني)\s+(?:الصور|صور|صوره|صورة|صورته|صورتها|صورهم|صورهن)', clean))


def claims_sent_models(text):
    clean = normalized(text)
    if re.search(r'(?:ما|لم|مو)\s+(?:دزيت|ارسلت|رسلت|بعثت)', clean):
        return False
    return bool(re.search(r'(?:دزيت\w*|ارسلت\w*|رسلت\w*|بعثت\w*)\s+(?:الموديلات|البدائل|الخيارات|الصور)|شوفي\s+الخاص\s+(?:دزيت|ارسلت)', clean))


def checkout_reply(answer, notice, question):
    """Keep approved answers to open questions when checkout needs more data."""
    text = normalized(question)
    if not (re.search(r'[؟?]', text) or re.search(
            r'شكد|شلون|شوكت|يمته|متى|افحص|الفحص|نوع القماش|خامت|موضوع القياس', text)):
        return notice
    kept = []
    for part in re.split(r'\n+|(?<=[.؟?؛])\s+', str(answer or '')):
        part = part.strip()
        clean = normalized(part)
        if not part or re.search(r'تم\s+(?:تثبيت|حجز)|ثبت(?:ت|نا|نالج)|حجز(?:ت|نا|نالج)', clean):
            continue
        # A validated factual answer can precede a checkout request in the same
        # sentence. Retain that answer, then use the validator's missing fields.
        request = re.search(r'(?:[،,؛]\s*|\s+بس\s+|\s+حتى\s+)?(?:دز\w*|[أا]رسل\w*|[أا]حتاج|نحتاج|ناقص).{0,80}(?:رقم|موبايل|هاتف|محافظ|عنوان)', part)
        if request:
            part = part[:request.start()].strip(' ،,؛')
        if part and normalized(part) != normalized(notice) and part not in kept:
            kept.append(part)
    return '\n'.join(kept + [notice]) if kept else notice


def disputed_product_photo(text):
    clean = normalized(text)
    if re.search(r'(?:اذا|لو|في حال).{0,80}(?:ارجع|يرجع|نرجع|ترجع|مو نفس|ما طابق)', clean):
        return False
    return bool(re.search(r'(?:مو نفس|مش نفس).{0,30}(?:الصور|الصوره|الصورة|موديل)|'
                          r'(?:ادزلج|ادزلك|دزيت\w*).{0,55}(?:غير موديل|موديل ثاني)|'
                          r'(?:الصوره|الصورة).{0,30}(?:مو المطلوب|مو الي اريد|غير الي)', clean))
