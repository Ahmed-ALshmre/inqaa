"""Deterministic contact and checkout guards; no model or network calls."""
import re
import unicodedata
try:
    from . import product_variants
except ImportError:
    import product_variants


DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")


def normalized(value):
    text = unicodedata.normalize("NFKC", str(value or "")).translate(DIGITS)
    return re.sub(r"[ـ\u064b-\u065f\u0670]", "", text).replace("أ", "ا").replace("إ", "ا").replace("آ", "ا").lower()


PHONE = re.compile(r"(?<!\d)(?:(?:\+?964|00964)[\s()-]*7|07)(?:[\s()-]*\d){9}(?!\d)")
PROVINCES = {
    "بغداد": ("بغداد",), "البصرة": ("البصرة", "البصره", "بصره", "بصرة"),
    "نينوى": ("نينوى", "الموصل", "موصل"), "أربيل": ("اربيل", "هولير"),
    "دهوك": ("دهوك",), "السليمانية": ("السليمانية", "سليمانية", "سليمانيه"),
    "كركوك": ("كركوك",), "الأنبار": ("الانبار", "انبار"),
    "النجف": ("النجف", "نجف"), "كربلاء": ("كربلاء", "كربلا"),
    "بابل": ("بابل", "الحلة", "حلة", "حله"), "واسط": ("واسط", "الكوت", "كوت"),
    "ميسان": ("ميسان", "العمارة", "عمارة"), "ذي قار": ("ذي قار", "الناصرية", "ناصرية"),
    "المثنى": ("المثنى", "السماوة", "سماوة"), "الديوانية": ("الديوانية", "ديوانية", "القادسية"),
    "صلاح الدين": ("صلاح الدين", "تكريت"), "ديالى": ("ديالى",),
}


def phone_number(text):
    match = PHONE.search(str(text or "").translate(DIGITS))
    if not match:
        return ""
    digits = re.sub(r"\D", "", match.group())
    if digits.startswith("00964"):
        digits = "0" + digits[5:]
    elif digits.startswith("964"):
        digits = "0" + digits[3:]
    return digits


def invalid_shipping_address(text):
    """Reject known non-address declarations, including saved model guesses."""
    clean = re.sub(r"^محافظ[ةه]\s+", "", normalized(text).strip(" ,،.\n"))
    return (not clean or
            any(clean.replace('ة', 'ه') == normalized(alias).replace('ة', 'ه') for aliases in PROVINCES.values() for alias in aliases) or
            bool(re.search(r"(?:ليش\s+انتو|انتو\s+(?:في|ب)|وين\s+(?:مكانكم|محلكم)|لو\s+خارجها)", clean)))


def has_shipping_landmark(address):
    clean = normalized(address)
    return bool(re.search(r'(?<!\w)(?:قرب|مقابل|مقابيل|خلف|يم|مجاور|بجانب|بجوار)\s+\S{2,}', clean)
                or re.search(r'(?:اقرب\s+)?نقط[ةه](?:\s+دال[ةه])?\s*[:：-]?\s*\S{2,}', clean)
                or re.search(r'(?<!\w)(?:مدرس[ةه]|جامع[ةه]?|مستشفى|صيدلي[ةه]|مجمع)\s+\S{2,}', clean)
                or (re.search(r'محل[ةه]\s*\d+', clean) and re.search(r'زقاق\s*\d+', clean)
                    and re.search(r'دار\s*\d+', clean)))


def contact_fields(text, previous=None):
    """Extract declarations, never an address from a shipping question."""
    previous = previous or {}
    original = str(text or "").translate(DIGITS)
    clean = normalized(original)
    if "http://" in clean or "https://" in clean:
        return {}
    result = {}
    phone = phone_number(original)
    if phone:
        result["phone"] = phone
    is_question = bool(re.search(r"[؟?]|\b(?:شكد|كم|بشكد|شوكت|يوصل|توصيل|يصير|ليش|وين)\b", clean))
    declaration = bool(re.search(r"(?:عنواني|العنوان|اني من|انا من)", clean))
    if phone and is_question:
        # A question after declared contact details does not erase the address.
        prefix = re.split(r'\s+(?:و?شكد|و?كم|و?شوكت|و?يمتى|و?متى|و?شلون|و?اكدر)\b', original, maxsplit=1)[0]
        if prefix != original and phone_number(prefix):
            result.update(contact_fields(prefix, previous))
    province = ""
    # A mall/hospital name must not overrule the city at the start of an address.
    locality = re.split(r"(?:قرب|مقابل|مقابيل|خلف|يم|مجاور|شارع|مول|جامع|مستشفى)\s", clean, maxsplit=1)[0]
    if declaration or not is_question:
        for canonical, aliases in PROVINCES.items():
            if any(re.search(r"(?<!\w)(?:بال|ب)?" + re.escape(alias).replace('ة', '[ةه]') + r"(?!\w)", locality) for alias in aliases):
                province = canonical
                result["province"] = canonical
                break
    removal = re.fullmatch(r"(?:احذف|احذفي|شيل|شيلي)\s+(?:كلمة\s+)?(.+?)(?:\s+من العنوان)?[.!]?$", original.strip())
    if removal and previous.get("address"):
        token = removal.group(1).strip()
        address = re.sub(re.escape(token) + r"(?:\s+\d+)?", "", str(previous["address"])).strip()
        if address and address != previous["address"]:
            return {"address": address}
    without_phone = PHONE.sub("", original).strip(" ,،.\n")
    explicit = re.search(r"(?:عنواني|العنوان)\s*[:：-]?\s*(.+)", without_phone, re.S)
    location_marker = re.search(r"\b(?:حي|قرب|شارع|سريع|محلة|محله|زقاق|دار|قرية|قريه|قضاء|ناحية|ناحيه|مجمع|مستشفى|جامع|جامعة|جامعه|مدرسة|مدرسه|صيدلية|صيدليه|مول|محطة|محطه|مقابل|خلف|زون|منطقة|منطقه|مرتفعات)\b|(?:رقم\s*(?:ال)?دار|دار|زقاق)\s*\d+", normalized(without_phone))
    if explicit:
        address = explicit.group(1).strip(" ,،.\n")
    elif not is_question and not re.search(r"\b(?:اريد|احجز|فستان|سوت|قميص|وزن|قياس)\b", clean) and (location_marker or province):
        address = re.sub(r"^(?:اني من|انا من|من)\s+", "", without_phone).strip()
    else:
        address = ""
    if province and address and not explicit:
        # Mixed bargaining + contact messages must not print the proposed price
        # or purchase instruction on the courier's address label.
        for alias in PROVINCES[province]:
            match = re.search(r'(?<!\w)(?:بال|ب)?' + re.escape(alias).replace('ة', '[ةه]') + r'(?!\w)', address)
            if match and re.search(r'اذا|ثبتي|احجزي|تخفيض|خصم|سعر', normalized(address[:match.start()])):
                address = address[match.start():].strip()
                break
    # A province on its own is not a delivery address.
    address_words = re.findall(r"\w+", normalized(address))
    province_only = any(re.sub(r"^محافظ[ةه]\s+", "", normalized(address)).replace('ة', 'ه') == normalized(alias).replace('ة', 'ه') for aliases in PROVINCES.values() for alias in aliases)
    numbered_house = bool(re.search(r'(?:رقم\s*(?:ال)?دار|دار|زقاق)\s*\d+', normalized(address)))
    if address and not province_only and not invalid_shipping_address(address) and (len(address_words) >= 2 or numbered_house) and (location_marker or province or len(address_words) >= 3):
        # A landmark sent in the next bubble supplements the established area.
        previous_address = str(previous.get("address") or "").strip()
        if (previous_address and previous.get('province') and not province
                and re.match(r'^(?:جامع[ةه]?|مدرس[ةه]|صيدلي[ةه]|مستشفى|مول|محط[ةه])\s', normalized(address))
                and normalized(address) not in normalized(previous_address)):
            address = 'قرب ' + address
        if (previous_address and not invalid_shipping_address(previous_address) and
                not province and not explicit and re.match(r"^(?:(?:مقابل|قرب|خلف|يم|مجاور|بجانب|بجوار|اقرب\s+نقط[ةه]|نقط[ةه])\s|(?:رقم\s*(?:ال)?دار|دار|زقاق)\s*\d)", normalized(address)) and
                normalized(address) not in normalized(previous_address)):
            address = previous_address + "، " + address
        elif previous_address and normalized(address) in normalized(previous_address) and not explicit:
            address = previous_address  # Repeating an area must not erase its landmark.
        result["address"] = address[:500]
    return result


def is_confirmation(text):
    # Only affirmative words and courtesy fillers; corrections, questions and
    # negations must still invalidate a pending cart, including mixed bursts.
    affirmative = r"(?:اي+|يي+|نعم|تمام|موافق|موافقة|اوكي|اوك|ثبتي|ثبت|ثبتيه|ثبتوا)"
    filler = r"(?:عيني|حياتي|حبيبتي|حبي|عمري|يمعوده|يمعودة|الطلب|الحجز|هسه|رجاء|شكرا)"
    return bool(re.fullmatch(affirmative + r"(?:\s+(?:" + affirmative + "|" + filler + r"))*[.!،\s]*", normalized(text).strip()))


def delivery_fee_inquiry(text):
    """A question about a single piece's shipping is not a condition of purchase."""
    clean = normalized(text)
    if not (re.search(r'توصيل|شحن', clean) and re.search(r'مجاني|بلاش|اجور|رسوم|شكد|كم', clean)):
        return False
    if re.search(r'بشرط|(?<!\w)شرط(?!\w)|فقط|احجز|حجز|ثبت|اشتري|اطلب|اخذ|اريد|رفض|ارجع|رجع|ما\s*عجب|استلام', clean):
        return False
    return bool(re.search(r'حتى\s*اذا|(?<!\w)(?:هل|يعني)(?!\w)|'
                          r'(?<!\w)اذا\s*(?:بس\s*)?(?:قطع[هة]|وحد[هة]|1)(?!\w)', clean))


def unresolved_purchase_terms(text):
    """Do not treat price questions or conditional terms as purchase consent.

    An unconditional instruction can still ask about documented inspection at
    delivery. This guard targets unresolved payment/shipping conditions rather
    than blocking every question mark or every occurrence of "if".
    """
    clean = normalized(text)
    # Inspection/return at the courier is the published policy, not a demand
    # for a discount or free shipping on a kept purchase.
    if (re.search(r'ارجع|يرجع|ترجع|رجع|فحص|افحص', clean)
            and not re.search(r'خصم|تخفيض|تحسب|تخلي|بشرط.{0,20}(?:سعر|يوصل|يلحق)', clean)):
        return ''
    if delivery_fee_inquiry(text):
        return 'unanswered_price_or_shipping'
    if re.search(
            r'(?:شكد|كم|بشكد|بيش|شنو).{0,22}(?:سعر|المجموع|توصيل|تحسب|يطلع)|'
            r'(?:سعر|توصيل|المجموع).{0,16}(?:شكد|كم|بيش)|'
            r'(?:شكد|بشكد|بيش)(?:\s+(?:هذا|هاي|هذ|الموديل|الحجز|حساب))?\s*[؟?]', clean):
        return 'unanswered_price_or_shipping'
    if re.search(
            r'(?<!\w)(?:اذا|لو|بشرط|شرط)(?!\w)[^.!؟?\n]{0,65}'
            r'(?:توصيل|سعر|مجاني|خصم|تخفيض|تحسب|يوصل|يلحق|اليوم|باجر|غدا|ساعه|ساعة|يومين|اسبوع)', clean):
        return 'conditional_purchase_terms'
    return ''


def unresolved_condition(history, text=''):
    """Unanswered conditions persist; a quoted refusal ends price haggling.

    Ending a negotiation is not purchase consent. The checkout caller still
    requires the customer's next unconditional purchase/contact action.
    Arrival promises never establish that an arrival condition was fulfilled.
    """
    pending = None
    entries = list(history or [])
    incoming = [row.get('text') or row.get('content') or '' for row in entries
                if row.get('direction') == 'incoming' or row.get('role') == 'user']
    if text and (not incoming or incoming[-1] != text):
        entries.append({'direction': 'incoming', 'text': text})
    for row in entries:
        entry = row.get('text') or row.get('content') or ''
        clean = normalized(entry)
        if row.get('direction') == 'outgoing' or row.get('role') == 'assistant':
            if pending and pending['kind'] in {'price', 'free_delivery'}:
                priced = bool(re.search(r'سعر|توصيل', clean) and re.search(r'\d', clean))
                declined = re.search(r'ما\s*(?:عدنا|عندنا|نكدر|اكدر|اگدر).{0,25}(?:تخفيض|خصم|ننزل|انزل|مجاني)|'
                                     r'(?:هذا|السعر)\s+(?:السعر\s+)?النهائي|ما\s*نكدر\s*ننزل\s*اكثر', clean)
                confirmed_free_offer = (pending['kind'] == 'free_delivery'
                                        and 'التوصيل مجاني بهذا العرض' in clean)
                if priced and (declined or confirmed_free_offer):
                    pending = None
            continue
        if not (row.get('direction') == 'incoming' or row.get('role') == 'user'):
            continue
        if re.search(r'بدون\s*(?:اي\s*)?شرط|الغ[يى]\s*الشرط|مو\s*شرط|'
                     r'(?:عادي|موافق[هة]?|ما\s*عندي\s*مانع).{0,30}(?:المد[هة]|يومين|ثلاث|التوصيل|السعر|موعد)|'
                     r'(?:المد[هة]|السعر|الموعد).{0,20}(?:عادي|موافق|يناسبني)', clean):
            pending = None
        elif unresolved_purchase_terms(entry) == 'conditional_purchase_terms':
            kind = ('arrival' if re.search(r'يوصل|يلحق|(?<!\w)(?:باچر|باجر|غدا|اليوم|ساع[هة]|يومين|اسبوع)(?!\w)', clean)
                    else 'free_delivery' if re.search(r'توصيل.{0,18}مجاني|مجاني.{0,18}توصيل', clean)
                    else 'price')
            pending = {'kind': kind, 'text': str(entry)}
        elif re.fullmatch(r'(?:لا\s*شكرا\s*)?(?:ما\s*اريد|ماريد|بطلت)(?:\s*(?:احجز|اطلب|اشتري|هسه|شكرا))*[ .!،]*', clean):
            pending = None
    return pending


def is_existing_order_followup(text):
    clean = normalized(text)
    if re.search(r'(?:شوكت|يمتى|يمته|متى|وين).{0,35}(?:طلبي|طلبيه|طلبية|الطلب|حجزي)|(?:طلبي|الطلب|طلبية|طلبيه|حجزي).{0,35}(?:شوكت|يمتى|يمته|متى|يوصل)', clean) and not re.search(r'اذا|لو احجز|لو اطلب|اريد احجز|اريد اطلب', clean):
        return True
    return bool(re.search(r"(?:طلبي|الطلب|حاجزه|حاجزة|موصيه|موصية|وصيت|حجزت|فستان).{0,65}(?:ماوصل|ما وصل|موصل|مو وصل|وين|تاخر|سبوع|ايام)|(?:وين|شو ماوصل|شو ما وصل).{0,20}(?:طلبي|الطلب|فستان)|(?:صارلي|صار الي).{0,35}(?:حاجزه|حاجزة|موصيه|وصيت|حجزت)", normalized(text), re.S))


def unsupported_order_action(reply):
    text = normalized(reply)
    return bool(re.search(
        r"(?:لغيت|الغيت|الغينا|لغينا|عدلت|عدلنا|غيرت|غيرنا|حذفت|حذفنا|شلت|شلنا|ضفت|ضفنا|اضفت|اضفنا|ثبتنالج|ثبتنا|سجلنالج|سجلنا|بلغت|بلغنا|كتبت|كتبنا).{0,50}(?:قميص|فستان|سوت|قطعة|طلب|حجز|مندوب|ملاحظة|استعجال|قياس|عنوان|رقم|لون|كمية)|(?:تم).{0,12}(?:الغاء|تعديل|تغيير|تثبيت|اضافة|حذف|تسجيل)|(?:الطلب|الحجز)\s*(?:صار|اصبح)|(?:اثبتلج|اثبتلك|نثبت|احجزلج|احجزلك).{0,55}[؟?]",
        text, re.S,
    ))


def ambiguous_measurement(value):
    clean = normalized(value)
    # Owner accepts adjacent numeric sizes as a range, not two unresolved choices.
    if re.fullmatch(r'\d{2}\s*[-،,/]\s*\d{2}', clean):
        return False
    return bool(re.search(r"\b(?:او|لو)\b|واذا|[/–-]", clean))


def measurement_history_error(items, messages):
    """An unresolved customer size alternative cannot be settled by model output."""
    kinds = ("فستان", "سوت", "قميص", "عباء", "تنورة")
    for item in items:
        name = normalized(item.get("product_name"))
        kind = next((k for k in kinds if k in name), "")
        unresolved = False
        for message in messages:
            for line in normalized(message).splitlines():
                numbers = re.findall(r"(?<!\d)\d{2,3}(?!\d)", line)
                if not numbers or re.search(r"وزن|وزني|كيلو|كغم|طول|سعر|الف|[؟?]", line):
                    continue
                mentioned_kinds = [k for k in kinds if k in line]
                if mentioned_kinds and kind not in mentioned_kinds:
                    continue
                if not mentioned_kinds and len(items) > 1:
                    continue
                if "قياس" in line or mentioned_kinds:
                    unresolved = len(numbers) > 1 and ambiguous_measurement(line)
        if unresolved:
            return f"ذكرتِ أكثر من قياس لـ{item.get('product_name') or 'القطعة'}؛ أي قياس نعتمد قبل تثبيت الطلب؟"
    return ""


def child_cart_error(items, messages, catalog):
    """An explicit purchase for two ages cannot silently become one piece."""
    try:
        from .conversation_quality import child_ages, child_age
    except ImportError:
        from conversation_quality import child_ages, child_age
    ids = {str(item.get('product_id') or '') for item in items}
    if len(ids) != 1:
        return ''  # Different-model allocation needs its own source evidence.
    product = next((p for p in catalog if str(p.get('product_id')) in ids), {})
    if not product_variants.size_data(product.get('sizes'))['years']:
        return ''
    wanted = []
    for message in messages:
        clean = normalized(message)
        if re.search(r'(?:بس|فقط)\s*(?:قطعه|قطعة|وحده|واحد[ةه]?|عمر)', clean):
            wanted = []
        ages = child_ages(clean)
        if len(ages) > 1 and re.search(r'اريد|اخذ|احتاج|قطعتين|ثنين|توام', clean) and not re.search(r'[؟?]|\b(?:لو|او)\b', clean):
            wanted = ages
    if not wanted:
        return ''
    supplied = []
    for item in items:
        age = child_age('عمر ' + str(item.get('size') or ''))
        supplied.extend([age] * int(item.get('quantity') or 1))
    for age in wanted:
        if age not in supplied:
            return 'اختيارات الأعمار محفوظة؛ باقي نحدد قطعة مستقلة لكل عمر قبل تثبيت الطلب.'
        supplied.remove(age)
    return ''


def order_line_error(raw_items, catalog):
    """Reject an invalid line instead of silently dropping it from a purchase."""
    by_id = {str(p.get("product_id")): p for p in catalog}
    if not isinstance(raw_items, list) or not raw_items or len(raw_items) > 20:
        return "حددي القطع المطلوبة أولاً حتى أثبت الطلب."
    for item in raw_items:
        if not isinstance(item, dict):
            return "تفاصيل إحدى القطع غير واضحة؛ نحتاج نحددها قبل التثبيت."
        product = by_id.get(str(item.get("product_id") or ""))
        if not product:
            return "نحتاج نحدد الموديل من منتجات هذا المتجر قبل التثبيت."
        supplied_name = normalized(item.get("product_name"))
        if supplied_name and supplied_name != normalized(product.get("product_name")):
            return "اسم الموديل لا يطابق المنتج الحالي؛ نحتاج نتأكد من القطعة قبل التثبيت."
        label = product.get("product_name") or "القطعة"
        if str(product.get("status", "active")) != "active":
            return f"{label} غير متاح للحجز حالياً."
        try:
            quantity = int(str(item.get("quantity", 1)).translate(DIGITS))
        except (ValueError, TypeError):
            quantity = 0
        if not 1 <= quantity <= 20:
            return f"نحتاج كمية صحيحة من {label} قبل التثبيت."
        weight = normalized(item.get('weight') or (item.get('size') if item.get('size_type') == 'weight' else '')).strip()
        if weight and (not re.fullmatch(r'\d{1,3}(?:\.\d{1,2})?', weight) or float(weight) <= 0):
            return f'حتى نسجل {label} بصورة صحيحة، شكد الوزن بالكيلو برقم واحد؟ النطاق وحده ما يكفي لتسجيل الطلب.'
        if item.get('size_type') == 'weight' and not weight:
            return f'باقي الوزن بالكيلو برقم واحد لـ{label} حتى نكمل تسجيل الطلب.'
        for key, field, name in (("color", "colors", "لون"), ("size", "sizes", "قياس")):
            value = str(item.get(key) or "").strip()
            if product.get(field) and (not value or ambiguous_measurement(value)):
                return f"باقي نحدد {name} {label} بشكل واضح حتى أثبت الطلب."
            problem=product_variants.order_choice_error(product,key,value)
            if problem:
                return f'{label}: {problem}.'
    return ""
