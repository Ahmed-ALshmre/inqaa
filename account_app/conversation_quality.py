"""Narrow factual safeguards; customer text is data, never executable policy."""
import re
try:
    from .checkout import normalized
    from .pricing import money, terms
except ImportError:
    from checkout import normalized
    from pricing import money, terms

GUIDE = """
قواعد معالجة المحادثة الحالية تتقدم على الأمثلة القديمة:
- عند رغبة الزبون بحجز منتجين أو أكثر واكتمال بيانات القطع والتوصيل، أنشئ الطلب مباشرة دون ملخص يحتاج موافقة ثانية. «ثنينهم/ثنينه/كلهن» تحدد المجموعة المعروفة ولا تعني الاختيار بين منتجين. الصور وحدها ليست موافقة شراء.
- quantity عدد القطع الفعلية. للبكج سجل كل لون وقياس في سطر مستقل. إذا استخدمت quantity_unit=bundle فهي عدد البكجات مع شرح ألوان وقياسات محتوياتها. لا تخلط بكجاً واحداً بقطعة واحدة ولا تفترض سعراً للمفرد أو للقطعة الرابعة.
- شرط الفحص والإرجاع والمساومة لا يلغيان الموديل. اعتمد القياس الذي طلبه الزبون صراحة؛ الوزن تقدير وليس ضماناً. لا تصغر القياس بسبب حمية مستقبلية. قياس جسم الزبون ليس طول المنتج.
- لا تعرض أي بديل لا يحمل معرفاً موجوداً في كتالوج هذا المتجر ومناسباً لشروط العمر واللون والسعر. عدم وجود بديل يقال بوضوح دون وعد بصور وهمية.
- عند طلب الصور لا تشترط معرفة الوزن؛ حدد المنتجات المرغوبة في image_product_ids من الكتالوج فقط، وimage_color للون المطلوب، ولا ترسل الموديل السابق بدلاً منها. إذا طلب كل الموديلات بسعر معين فصف الصور بهذا السعر فقط.
- لا تفترض المطاط أو البطانة أو الشال أو طولاً غير مدون في المنتج. لا تعد بموعد تسليم من عندك. حالة الطلب new ليست دليلاً على تجهيزه أو شحنه. لا تقل بلغت الشركة أو استعجلت المندوب أو سجلت إشعار توفر، ولا تؤكد الإلغاء أو التعديل دون نتيجة تنفيذ.
- العنوان: المدينة أولاً؛ اسم مول بغداد في الحلة لا يجعل المحافظة بغداد. حذف كلمة من العنوان يجب أن ينعكس في العنوان نفسه، وليس في الملاحظات فقط.
"""


def wants_photos(text):
    return bool(re.search(r"(?:اريد|أريد|دزي|دز|ارسلي|أرسلي|ممكن|اكو|عندج).{0,35}صور|اشوف|أشوف|شوفيني|كتالوج|موديلات", str(text or "")))


def budget(text):
    text = normalized(text)
    match = re.search(r"(?:ب(?:سعر)?\s*|سعر(?:ها|ه)?\s*)(\d+(?:[,٬]\d+)*)\s*(الف|الاف)?", text)
    if not match:
        return None
    value = money(match.group(1) + (" الف" if match.group(2) else ""))
    return value * 1000 if value < 1000 else value


def child_age(text):
    text = normalized(text)
    match = re.search(r"(?:عمر(?:ه|ها)?\s*)?(\d+)\s*(?:اشهر|شهر)", text)
    if match:
        return int(match.group(1)) / 12
    match = re.search(r"(?:عمر(?:ه|ها)?\s*)?(\d+)\s*(?:سنوات|سنه|سنة)", text)
    return int(match.group(1)) if match else None


def operational_request(text):
    text = normalized(text)
    if re.search(r"(?:اذا|يذا|لو|في حال).*(?:ارجع|يرجع|يرجه|ترجع)", text):
        return None
    if re.search(r"(?:الغ[يوا]*|الغاء|مااريد|ما اريد).{0,15}(?:طلب|حجز)|(?:طلب|حجز).{0,10}(?:الغ[يوا]*|مااريد|ما اريد)", text):
        return {"kind": "cancel", "value": "cancelled"}
    match = re.search(r"(?:بدل|بدلي|غير|غيري|قياس)\s*(?:القياس|قياس)?\s*(\d{2})", text)
    if match and not re.search(r"[؟?]|متوفر|اكو|يلبس|شكد|كم", text):
        return {"kind": "size", "value": match.group(1)}
    if re.search(r"غير|بدل|ضيف|اضيف|استعجل|تأخر|تاخر|ماوصل|ما وصل|شوكت|يمتى|متى|رقم المندوب", text):
        return {"kind": "service", "value": text[:300]}
    return None


def unsupported_claim(reply):
    text = normalized(reply)
    return bool(re.search(r"(?:بلغت|بلغنا|استعجلت|استعجلنا|اتصلت|اتصلنا|دا اتابع|هسه اتابع).{0,45}(?:شرك|مندوب)|(?:سجلت|ثبتت).{0,35}(?:نبلغ|نوفر|قياسات اكبر)|(?:تم الغاء|الغيت|الغينا|عدلت|غيرت).{0,25}(?:طلب|حجز)", text))


def grounded_error(reply, product, products):
    text = normalized(reply)
    if unsupported_claim(reply):
        return "لا تؤكد إجراء لم ينفذه النظام؛ وضح المعلومة المتاحة فقط."
    if product:
        try:
            count, price, free = terms(product)
        except ValueError:
            count, price, free = 1, None, False
        if count > 1 and re.search(r"(?:المفرد|مفرد|قطعه وحده|قطعة وحدة).{0,25}(?:سعر|الف|الاف|\d)", text):
            return "هذا المنتج يباع بكجاً فقط؛ لا يوجد سعر مفرد معتمد."
        recorded = normalized(" ".join(str(product.get(k) or "") for k in ("description", "notes", "fabric", "sizes")))
        if re.search(r"(?:طولها|طوله|طول الموديل)\s*\d", text) and not re.search(r"طول|سم", recorded):
            return "طول المنتج غير مسجل؛ لا تستنتجه من طول الزبون."
        if re.search(r"(?:بدون|بلا|مع|ويه) شال", text) and "شال" not in recorded:
            return "وجود الشال غير مسجل في المنتج؛ لا تخمن."
        if re.search(r"(?:مو|غير|ما|بدون)\s*(?:مبطن|مبطنة|مطاط|ليكرا)|بي(?:ها|ه)?\s+ليكرا", text) and not re.search(r"بطان|مبطن|مطاط|ليكرا", recorded):
            return "المطاط أو البطانة غير موثقين؛ لا تخمن صفات القماش."
    return ""
