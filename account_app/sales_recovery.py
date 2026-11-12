"""Narrow recovery for policy questions and explicit photo requests."""
import re

try:
    from .checkout import PROVINCES, normalized, delivery_fee_inquiry
except ImportError:
    from checkout import PROVINCES, normalized, delivery_fee_inquiry


def policy_question(text):
    clean = normalized(text).translate(str.maketrans('یک','يك')).strip(' ؟?!.،\n')
    # Normalize these delivery spellings here, never addresses or saved choices.
    clean = re.sub(r'(?<!\w)(ال)?توسيل(?!\w)',r'\1توصيل',clean)
    clean = re.sub(r'^متي(?=\s)','متى',clean)
    # Opening/checking the parcel before payment is a store policy, not a
    # missing garment detail or a request to identify an advertising photo.
    if (re.search(r'فحص|افحص|اتفحص', clean) and re.search(r'قبل.{0,15}(?:دفع|ادفع)|المندوب', clean)
            and not re.search(r'ثبت|احجز|اطلب|اشتري|سعر|قماش|بطان|قياس|عمر|لون|طول|استلمت|دفعت|خياط[ةه].{0,12}(?:مفتوح|مقطوع)', clean)):
        return 'inspection'
    if re.fullmatch(r'(?:هل (?:يمكنني|يمكن)|اكدر|اگدر|يصير)(?: ان)? (?:ادفع|الدفع) (?:عند الاستلام|للمندوب من استلم)', clean):
        return 'payment'
    if re.fullmatch(r'كم تبلغ تكلفة التوصيل (?:(?:الى\s+|ل)?(?:محافظتي|منزلي)|(?:الى\s+)?البيت|للبيت)', clean):
        return 'delivery'
    if delivery_fee_inquiry(text) and not re.search(
            r'قماش|خام|عمر|قياس|مكان|عنوانكم|وين|سعر(?:ه|ها)|سعر\s+(?!التوصيل|الشحن)|دفع|فحص', clean):
        return 'delivery'
    clean = re.sub(r'(شكد|اشكد|كم|بشكد)(?=التوصيل|توصيل|الشحن|شحن)', r'\1 ', clean)
    clean = re.sub(r'^(?:السلام عليكم|سلام عليكم|هلو|مرحبا)\s*', '', clean)
    clean = re.sub(r'^و(?=توصيل|التوصيل)', '', clean)
    clean = re.sub(r'^خدم[هة]\s+', '', clean)
    names = {normalized(name) for aliases in PROVINCES.values() for name in aliases}
    names.update('لل' + name[2:] for name in list(names) if name.startswith('ال'))
    location = '|'.join(sorted({re.escape(name) for name in names}, key=len, reverse=True))
    clean = re.sub(r'(?:^|\s)(?:ل|الى |لـ)?(?:' + location + r')(?=\s|$)', ' ', clean)
    clean = re.sub(r'(?<!\w)(?:الى\s+|ل)?(?:(?:كل|جميع|باقي)\s+)?(?:ال)?محافظات(?:ي)?(?!\w)', ' ', clean)
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
    clean = normalized(text).strip(' ؟?!.،,\n')
    clean = re.sub(r'^(?:(?:تمام|اوكي|اوك|ماشي|زين|طيب|اي|نعم|عيني|حبيبتي|حبي|عمري)[\s،,]+)*(?:بس[\s،,]+)?', '', clean)
    clean = re.sub(r'\s+(?:بدون\s+(?:اي\s*)?حجز|بس\s+للمشاهد[هة]|(?:بس\s+)?ما\s*اريد\s*احجز)$', '', clean).strip(' ،')
    clean = re.sub(r'(?:\s+(?:حبيبتي|عيني|حبي|حياتي|بلا زحمة|لو سمحتي))+$', '', clean)
    if explicit_photo_followup(clean):
        return True
    words = re.sub(r'[؟?!.،,\s]+', ' ', clean).strip()
    photo = r'(?:ال)?صور(?:[هة]|ها|هن|هم)?'
    send = r'ت(?:دز|رسل|نزل|رفق)\w*'
    if re.fullmatch(rf'(?:اريد|ممكن|ادزي|دزيلي|ارسلي|وين|ما\s*وصل\w*)\s+{photo}(?:\s+(?:الالوان|الموديل))?|وين\s+اصور|'
                    rf'(?:شوكت|اشوكت|يمتى|متى)\s+(?:راح\s+)?{send}\s+(?:(?:هي|هيه|هاي)\s+)?{photo}',
                    words):
        return True
    # Color alternatives describe which pictures to inspect, not purchase
    # quantities. Mixed factual/booking requests still need the full answer.
    try:
        from . import product_variants
    except ImportError:
        import product_variants
    if (product_variants.requested(clean)['colors']
            and re.search(r'اشوف|شوفيني|وريني|(?:دزي|دزلي|دزيلي|ارسلي|ممكن|اريد).{0,30}صور', clean)
            and not re.search(r'قماش|خام|بطان|سحاب|طول|عرض|قياس|عمر|سعر|شكد|بكم|توصيل|شحن|دفع|ثبت|احجز|اطلب|اذا|بشرط|حقيقي|المحل|تصوير|ما\s*اريد|لا\s*(?:اريد|تدز|ترسل)',clean)):
        return True
    return bool(re.fullmatch(r'(?:(?:ممكن|اريد)\s+)?(?:اشوف|دزيلي|دزي|دزلي|ارسلي|شوفيني|وريني)\s+(?:الصور|صور|صوره|صورة|صورته|صورتها|صورهم|صورهن)(?:\s+(?:كلهن|كلهم|ثنينهن|ثنينهم|الاثنين))?', clean))


def offers_alternative_photos(text):
    clean = normalized(text)
    return bool(re.search(r'(?:ادز|ارسل|اشوف|تشوف)\w*.{0,30}(?:بديل|بدائل|موديلات|خيارات|سوتات|اطقم)', clean))


def alternative_photo_request(text, offered=False):
    clean = normalized(text).strip(' .!؟?،')
    if re.search(r'(?:ما|لا)\s*(?:اريد|تدز|ترسل)', clean):
        return False
    if re.search(r'صور|وريني|اشوف|(?:دز|رسل)\w*', clean) and re.search(
            r'بدائل|بديل|موديلات\s+(?:ثاني[هة]|غير|اخر)|باقي\s+(?:ال)?موديلات', clean):
        return True
    return offered and bool(re.fullmatch(
        r'(?:(?:اي|نعم|تمام|زين|يلا)\s+)?(?:دزي(?:لي|هن|هم|ها)?|ارسلي(?:لي|هن|هم|ها)?|'
        r'(?:اريد\s+)?اشوف(?:هن|هم|ها)|اي|نعم)(?:\s+(?:الصور|الباقي|حبيبتي))?', clean))


def claims_sent_models(text):
    clean = normalized(text)
    if re.search(r'(?:ما|لم|مو)\s+(?:دزيت|ارسلت|رسلت|بعثت)', clean):
        return False
    if re.search(r'(?:دزيت\w*|ارسلت\w*|رسلت\w*|بعثت\w*)\s+(?:الموديلات|البدائل|الخيارات|الصور)|'
                 r'شوفي\s+الخاص\s+(?:دزيت|ارسلت)|(?:هاي|هذي|هذني|تلكين)\s+(?:ال)?(?:موديلات|بدائل)(?!\w)', clean):
        return True
    for clause in re.split(r'[.!؟?،\n]', clean):
        offer = re.search(r'(?:تحبين|تريدين|تحب|تريد).{0,25}(?:ادز|ارسل)\w*', clause)
        if not offer and re.search(r'(?:ادز\w*|ارسل\w*)\s+(?:الموديلات|البدائل|الخيارات)', clause):
            return True
    return False


def promises_photo_attachment(text):
    clean = normalized(text)
    photo = r'(?:ال)?صور(?:[هة]|ها|هن|هم)?(?!\w)'
    return bool(re.search(
        rf'(?:راح|رح|هس[هةا])\s+(?:ادز|ارسل|ارفق|انزل)\w*\s+{photo}|'
        rf'(?:ثواني|دقايق|دقائق|شوي[هة]?)\s+و?توصل\w*\s+{photo}|'
        r'(?:راح|رح|هس[هةا])\s+(?:انزل|ارفق)(?:ل[جچك])?(?:هن|هم)\s+(?:(?:ال[جچك]|هنا)\s+){0,2}(?:بالدردشة|بالخاص)',
        clean))


def checkout_reply(answer, notice, question):
    """Keep approved answers to open questions when checkout needs more data."""
    text = normalized(question)
    if not (re.search(r'[؟?]', text) or re.search(
            r'شكد|شلون|شوكت|يمته|متى|افحص|اقيس|اجرب|الفحص|نوع القماش|خامت|موضوع القياس', text)):
        return notice
    try:
        from .customer_copy import chat_money
    except ImportError:
        from customer_copy import chat_money
    answer = chat_money(answer)
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
        if normalized(part) in {'بس', 'حتى', 'و'}:
            continue
        if part and normalized(part) != normalized(notice) and part not in kept:
            kept.append(part)
    return '\n'.join(kept + [notice]) if kept else notice


def disputed_product_photo(text):
    clean = normalized(text)
    if re.search(r'(?:اذا|لو|في حال).{0,80}(?:ارجع|يرجع|نرجع|ترجع|مو نفس|ما طابق)|'
                 r'(?:خاف|اخاف).{0,40}(?:تطلع|يطلع|مو نفس|مش نفس)|'
                 r'(?:افحص|فحص).{0,35}(?:المندوب|استلم|الاستلام|ادفع|الدفع)', clean):
        return False
    return bool(re.search(r'(?:مو نفس|مش نفس).{0,30}(?:الصور|الصوره|الصورة|موديل)|'
                          r'(?:ادزلج|ادزلك|دزيت\w*).{0,55}(?:غير موديل|موديل ثاني)|'
                          r'(?:الصوره|الصورة).{0,30}(?:مو المطلوب|مو الي اريد|غير الي)', clean))
