"""Deterministic contact and checkout guards; no model or network calls."""
import re
import unicodedata


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
    "النجف": ("النجف", "نجف"), "كربلاء": ("كربلاء",),
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
    is_question = bool(re.search(r"[؟?]|\b(?:شكد|كم|بشكد|شوكت|يوصل|توصيل|يصير)\b", clean))
    declaration = bool(re.search(r"(?:عنواني|العنوان|اني من|انا من)", clean))
    province = ""
    if declaration or not is_question:
        for canonical, aliases in PROVINCES.items():
            if any(re.search(r"(?<!\w)(?:بال|ب)?" + re.escape(alias) + r"(?!\w)", clean) for alias in aliases):
                province = canonical
                result["province"] = canonical
                break
    without_phone = PHONE.sub("", original).strip(" ,،.\n")
    explicit = re.search(r"(?:عنواني|العنوان)\s*[:：-]?\s*(.+)", without_phone, re.S)
    location_marker = re.search(r"\b(?:حي|قرب|شارع|محلة|محله|زقاق|دار|قرية|قريه|قضاء|مستشفى|مقابل|خلف|زون|منطقة|منطقه|مرتفعات)\b", normalized(without_phone))
    if explicit:
        address = explicit.group(1).strip(" ,،.\n")
    elif not is_question and not re.search(r"\b(?:اريد|احجز|فستان|سوت|قميص|وزن|قياس)\b", clean) and (province or previous.get("province")) and (location_marker or province):
        address = re.sub(r"^(?:اني من|انا من|من)\s+", "", without_phone).strip()
    else:
        address = ""
    # A province on its own is not a delivery address.
    address_words = re.findall(r"\w+", normalized(address))
    province_only = any(normalized(address) == normalized(alias) for aliases in PROVINCES.values() for alias in aliases)
    if address and not province_only and len(address_words) >= 2 and (location_marker or province or len(address_words) >= 3):
        result["address"] = address[:500]
    return result


def is_confirmation(text):
    # Only affirmative words and courtesy fillers; corrections, questions and
    # negations must still invalidate a pending cart, including mixed bursts.
    affirmative = r"(?:اي+|يي+|نعم|تمام|موافق|موافقة|اوكي|اوك|ثبتي|ثبت|ثبتيه|ثبتوا)"
    filler = r"(?:عيني|حياتي|حبيبتي|حبي|عمري|يمعوده|يمعودة|الطلب|الحجز|هسه|رجاء|شكرا)"
    return bool(re.fullmatch(affirmative + r"(?:\s+(?:" + affirmative + "|" + filler + r"))*[.!،\s]*", normalized(text).strip()))


def is_existing_order_followup(text):
    return bool(re.search(r"(?:طلبي|الطلب|حاجزه|حاجزة|موصيه|موصية|وصيت|حجزت|فستان).{0,65}(?:ماوصل|ما وصل|موصل|مو وصل|وين|تاخر|سبوع|ايام)|(?:وين|شو ماوصل|شو ما وصل).{0,20}(?:طلبي|الطلب|فستان)|(?:صارلي|صار الي).{0,35}(?:حاجزه|حاجزة|موصيه|وصيت|حجزت)", normalized(text), re.S))


def unsupported_order_action(reply):
    text = normalized(reply)
    return bool(re.search(
        r"(?:لغيت|الغيت|الغينا|لغينا|عدلت|عدلنا|غيرت|غيرنا|حذفت|حذفنا|شلت|شلنا|ضفت|ضفنا|اضفت|اضفنا|ثبتنالج|ثبتنا|سجلنالج|سجلنا|بلغت|بلغنا|كتبت|كتبنا).{0,50}(?:قميص|فستان|سوت|قطعة|طلب|حجز|مندوب|ملاحظة|استعجال|قياس|عنوان|رقم|لون|كمية)|(?:تم).{0,12}(?:الغاء|تعديل|تغيير|تثبيت|اضافة|حذف|تسجيل)|(?:الطلب|الحجز)\s*(?:صار|اصبح)|(?:اثبتلج|اثبتلك|نثبت|احجزلج|احجزلك).{0,55}[؟?]",
        text, re.S,
    ))


def ambiguous_measurement(value):
    return bool(re.search(r"\b(?:او|لو)\b|واذا|[/–-]", normalized(value)))


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
        for key, field, name in (("color", "colors", "لون"), ("size", "sizes", "قياس")):
            value = str(item.get(key) or "").strip()
            if product.get(field) and (not value or ambiguous_measurement(value)):
                return f"باقي نحدد {name} {label} بشكل واضح حتى أثبت الطلب."
    return ""
