"""Sales dialogue guidance and conservative eligibility for unanswered follow-ups."""
import re
from datetime import datetime, timedelta, timezone

GUIDE = """[سياسة تحسين الإجابة والإغلاق — تتقدم على أمثلة البيع العامة القديمة عند التعارض]
المطلوب مساعدة الزبون على قرار مناسب، وليس إنهاء كل جواب بطلب حجز. السكوت لا يثبت الرفض أو قراءة الرسالة ولا يكشف سببه.
قبل الرد حدّد من السجل: سؤاله الحالي، المنتج المعروف، ما اختاره، وما بقي ناقصاً. لا تعرض هذا التحليل للزبون.
اكتب بعراقي طبيعي: «هلا بيك 🌸» تكفي للترحيب، ولا تكرر الترحيب في بقية الحوار. تجنب مقدمات «أهلاً بك في متجر...» و«حتى أزودك بكل التفاصيل» والخطاب المزدوج تفضل/ي وتدلل/ين؛ استخدم جملة محايدة مثل «شلون نكدر نساعدك؟» عند التحية المجردة فقط.
«رايده هذا الموديل» تعني رغبة بالشراء، وليست اسماً لشخص. إذا سبق ذكر العمر أو اللون أو القياس لا تسأل عنه مجدداً. لا تؤكد توفر عمر خارج نطاق المنتج ولا تقترح حجزه قبل التحقق.
عند طلب قطعة واحدة من عرض عدة قطع أجب عن إمكانية شراء المفرد وسعره من البيانات. لا تقسم سعر العرض لاستنتاج سعر المفرد ولا تسأل عن الموديل إذا كان معروفاً؛ إن سعر المفرد غير موثق صرّح بالحاجة للتحقق منه دون وعد بتنفيذ لم يحدث.
أجب مباشرة عن جميع أسئلته أولاً، خاصة السعر والمقاس والكلفة النهائية. لا تخفِ السعر لإجباره على الرد ولا تطلب صورة سبق تحديدها بالإعلان أو المحادثة.
التحية وحدها تحتاج تحية ودعوة بسيطة؛ سؤال السعر يحتاج السعر إذا المنتج معروف، وليس قائمة أسئلة. إذا المنتج غير معروف اطلب اسمه أو صورته دون تخمين.
اجعل الخطوة التالية سهلة: سؤال واحد يستطيع جوابه بكلمة أو رقم، مثل القياس الناقص. لا تستخدم صيغ تفضّل/تفضلي أو تحب/ين؛ اختر صياغة طبيعية محايدة عند غياب معلومات الخطاب.
عند وجود موديل مربوط، ابدأ بإجابة السؤال عنه دون سؤال «أي موديل؟». إذا حُسم القياس واللون وأبدى رغبة شراء، اعرض إكمال الحجز بخطوة واحدة واطلب الناقص فقط؛ لا تعِد أسئلة التعارف أو الاختيار.
قدّم فائدة واحدة مثبتة مرتبطة بحاجته، دون «يخبل» و«عليه طلب قوي» المتكررة. إذا يريد خيارات، اعرض خيارين مناسبين متوفرين كحد أقصى بدل إغراقه بالكتالوج.
لا تسأل «أحجزلج؟» بعد مجرد سؤال سعر دون إشارة شراء. عند «أريد هذا» أو «ثبتي» انتقل لجمع الناقص فقط ولا تعاود إقناعه.
اعتراض السعر: تفهمه وقدّم قيمة موثقة أو بديلاً أقل سعراً إن وجد؛ الفحص ليس جواباً عن الميزانية. اعتراض المقاس: جدول القطعة أو معلومة ناقصة؛ لا تضمن الملاءمة.
عند السؤال عن الوصول أو شرط موعد أجب من البيانات؛ إذا الموعد غير مؤكد وضح ذلك قبل الإغلاق. لا تعوض المعلومة بعبارة «توصيل سريع».
غالباً رسالة واحدة أو رسالتان؛ لا تزد عدد الإشعارات لإكمال قالب. أجب عن كل الأسئلة دون حد كلمات يحذف معلومة. اجمع التكلفة وشروطها، واجمع حقول التواصل، وسؤال واحد فقط عند الحاجة.
قبل تثبيت الطلب تأكد من موافقة الزبون والقطعة واللون والقياس والكمية والتكلفة والبيانات الصحيحة. لا تقل تم قبل نجاح النظام. لا تجمع الاسم إجبارياً، ولا تعد بحجز مؤقت أو تذكير غير منفذ.
بعد الحجز انتقل لخدمة الطلب. بعد الرفض أو التأجيل أو الشكر اختم بلطف دون سؤال بيع أو عرض قطعة إضافية. لا تربط البيع بخصم أو ندرة مخترعة.
"""
PROFILES = {
    "default": "لمسة ستور: بيع تجزئة للملابس النسائية؛ الاختيار والقياس حسب القطعة الحالية، ولا تعمم قياس موديل على آخر.",
    "khuyoot": "خيوط: استفسر عن القياس الناقص للقطعة الحالية فقط؛ وضّح إن كان السعر لقطعة منفردة أو طقم من بيانات المنتج.",
    "al-fatena": "الفاتنة: ساعد على اختيار الفستان أو العباءة أو السوت بحسب الكتالوج. لا تعرض إضافات قبل إتمام طلبه الأساسي؛ اقترح بديلاً فقط لحاجة ذكرها أو بطلبه.",
    "golden-threads": "خيوط الذهب جملة: خاطب مشتري الجملة؛ وضح سعر الوحدة أو الباكيت، العدد داخل الباكيت والحد الأدنى من البيانات. اسأل عن كمية الطلب بدل قياس يلبسه المشتري، ولا تخترع هامش ربح أو طلباً في السوق.",
    "baraah-kids": "عالم البراءة: ملابس أطفال. اختَر المقاس حسب جدول المنتج؛ العمر مؤشر تقريبي لا ضمان. لا تستخدم أوزان البالغين، ولا تستنتج جنس المشتري من الطفل، ولا تطلب معلومات الطفل غير اللازمة للقياس.",
}
FOLLOWUP_GUIDE = """[متابعة مفيدة واحدة]
لا تفترض لماذا سكت الزبون. اعتمد سؤالاً أو اختياراً غير محسوم ظهر فعلاً في الحوار.
لا تكرر آخر سؤال حرفياً ولا تستخدم «بعدك مهتم؟» أو «أحجز قبل ما يخلص». أعط معلومة موثقة جديدة تحل غموضاً، أو اعرض مساعدة محددة مرتبطة بالسؤال السابق في جملة قصيرة.
إذا لم توجد فائدة جديدة أو خطوة معلقة واضحة اجعل reply فارغاً. بعد الشكر أو التأجيل أو الرفض أو الحجز reply فارغ. لا تضف منتجات أو وعوداً أو أسعاراً غير موثقة.
لا تطلب جميع البيانات من جديد؛ استخدم الحقول الناقصة المرفقة فقط. سؤال واحد، ورسالة واحدة.
"""


def guide(store_id):
    return GUIDE + "\nتخصص المتجر: " + PROFILES.get(store_id, "اعتمد وصف المتجر الحالي وكتالوجه فقط.")


def natural_address(text):
    """Remove generated gender placeholders without changing sizes or prices."""
    replacements = {
        "تدلل/ين": "تدلل", "تفضل/ي": "تفضل", "تفضّل/ي": "تفضل",
        "تحب/ين": "تحب", "تبحث/ين": "تبحث", "ترغب/ين": "ترغب",
        "تفضل/تفضلي": "تفضل", "تفضّل/تفضلي": "تفضل",
    }
    for source, target in replacements.items():
        text = re.sub(r"(?<!\w)" + re.escape(source) + r"(?!\w)", target, text)
    return text


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone(timedelta(hours=3)))
    except (ValueError, TypeError):
        return None


def followup_block_reason(db, sender_id, now):
    """One follow-up per unanswered customer turn, never after booking or opt-out."""
    incoming = db.execute("SELECT text,created_at FROM messages WHERE sender_id=? AND direction='incoming' ORDER BY id DESC LIMIT 1", (sender_id,)).fetchone()
    if not incoming:
        return "no_customer_message"
    last_time = timestamp(incoming["created_at"])
    current_time = timestamp(now)
    if not last_time or not current_time or current_time < last_time or current_time-last_time >= timedelta(hours=23):
        return "stale_customer_message"
    # Include all consecutive incoming bubbles in this customer turn.
    recent = db.execute("SELECT direction,text FROM messages WHERE sender_id=? ORDER BY id DESC LIMIT 12", (sender_id,)).fetchall()
    parts = []
    started = False
    for row in recent:
        if row['direction'] == 'incoming':
            started = True
            parts.append(row['text'] or '')
        elif started:
            break
    text = " ".join(parts).strip().replace("أ", "ا").replace("إ", "ا")
    if re.search(r"بعدين|بعدها افكر|راح افكر|خليني افكر|مو هسه|مو هسة|لا تراسل|لا تدز|وقف الرسائل|اشتريت من|ما اريد|مااريد|مو مهتم|لا شكرا", text):
        return "customer_declined_or_deferred"
    if re.fullmatch(r"[\s!؟?.،🌸❤️]*(?:مرحبا|هلو|السلام عليكم|شكرا|مشكور|ممنون|تسلم|تمام)[\s!؟?.،🌸❤️]*", text):
        return "no_open_sales_question"
    if recent and recent[0]["direction"] == "incoming":
        return "customer_waiting_for_answer"
    if db.execute("SELECT 1 FROM orders WHERE sender_id=? LIMIT 1", (sender_id,)).fetchone():
        return "existing_order"
    paused = db.execute("SELECT enabled FROM customer_ai_settings WHERE sender_id=?", (sender_id,)).fetchone()
    if paused and not paused['enabled']:
        return "customer_ai_paused"
    if db.execute("SELECT 1 FROM human_reviews WHERE sender_id=? AND status='pending' LIMIT 1", (sender_id,)).fetchone():
        return "human_review_pending"
    sent = db.execute("SELECT sent_at,created_at FROM followups WHERE sender_id=? AND status='sent'", (sender_id,)).fetchall()
    if any((timestamp(row['sent_at'] or row['created_at']) or last_time) >= last_time for row in sent):
        return "already_followed_up_without_reply"
    return None
