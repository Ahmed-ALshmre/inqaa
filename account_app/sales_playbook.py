"""Reviewed reply patterns from booking journeys, without retaining private chats.

These are curated methods, not automatically trusted historical customer text or
historical product facts. Current catalog, policy and customer choices win.
"""
import re

try:
    from . import sales_strategy
    from .pricing import terms, quote, offer_tiers
except ImportError:
    import sales_strategy
    from pricing import terms, quote, offer_tiers

REVIEWED_LESSONS = {
    'cost': (
        'وضح تكلفة قرار الشراء قبل طلب البيانات: سعر وحدة البيع، التوصيل، والإجمالي إذا كان العدد واضحاً. '
        'إذا السعر لبكج أو درزن فاذكر ذلك، ولا تعرضه كسعر القطعة. '
        'نمط الجواب: «سعر الموديل …، التوصيل …، يصير الإجمالي …». الأرقام من التسعير الحالي فقط.'),
    'objection': (
        'احسم سبب التردد الذي ذكره الزبون قبل الانتقال لسؤال آخر. '
        'من سأل عن السحاب أو البطانة يحتاج جواب هذا التفصيل؛ سؤال الوزن أو طلب الرقم لا يحله. '
        'نمط الجواب عند غياب التفصيل: «الخامة …؛ نوع الإغلاق يحتاج تأكيد من المتجر». لا تضف سؤال حجز.'),
    'fit': (
        'في الحجوزات المكتملة حُفظ القياس أو العمر واللون خلال الأسئلة الجانبية. '
        'استعمل القياس الذي اختاره الزبون وجدول هذا المنتج إن وجد؛ لا تعاود سؤاله ولا تضمن الملاءمة من الوزن. '
        'نمط الجواب: «القياس اللي اخترته …؛ وبالنسبة للخامة …». لا تسجل قياساً لم يختره.'),
    'budget': (
        'إذا كان سبب التردد السعر فناقش التكلفة التي يقصدها: للقطعة أم مع التوصيل. '
        'قارن ببديل متوفر يلائم القياس والميزانية إن وجد فعلاً، وإلا اسأل سقف الميزانية مرة واحدة. '
        'نمط الجواب: «تقصد ميزانيتك للقطعة لو ويا التوصيل؟». لا تسأل إذا وضحها سابقاً، ولا تخترع خصماً.'),
    'bundle': (
        'الخلط بين سعر القطعة وسعر المجموعة أوقف محادثات؛ توضيح محتويات المجموعة ساعد على إكمال الاختيار. '
        'اذكر عدد القطع الحقيقي وسعر المجموعة، واحفظ لون وقياس كل قطعة اختارها. '
        'البديل المعروض أو العمر غير المتوفر لا يدخل السلة تلقائياً.'),
    'trust': (
        'عالج سبب القلق بالمعلومة المتاحة: صورة أو فيديو موجود فعلاً، وتفاصيل الخامة وسياسة الفحص الحالية. '
        'لا تعالج القلق بوعود «نفس الصورة بالضبط» أو ضمان قياس، ولا تدع أنك إنسان. '
        'طلب موظف يعني مراجعة بشرية فعلية، لا تكرار عرض الحجز.'),
    'checkout': (
        'بعد موافقة شراء واضحة، أكد الاختيار المعروف باختصار واطلب بيانات الحجز الناقصة فقط. '
        'نمط الجواب: «اختيارك للون والقياس محفوظ؛ باقي … حتى أكمل الحجز». '
        'لا تقل تم الحجز قبل نجاح الحفظ، ولا تعتبر «إي» بعد عرض الصور موافقة شراء.'),
    'answer': (
        'ابدأ بجواب السؤال عن المنتج المحدد، واجمع الأجوبة القصيرة المترابطة قبل أي سؤال جديد. '
        'لا تطلب صورة الموديل المرتبط مرة ثانية. بعد جواب مكتمل اسأل خطوة اختيار واحدة مفيدة فقط إن لزمت؛ '
        'ليس مطلوباً إنهاء كل رد بطلب حجز.'),
}


def lesson_ids(ev, product=None):
    if ev.get('_post_order'):
        return []
    text = sales_strategy.normalized(ev.get('text'))
    ids = []
    for pattern, key in (
        (r'غالي|ارخص|ميزاني|خصم|تخفيض|عبالي|عباليه', 'budget'),
        (r'اخاف|حقيقي|ذكاء|موظف|فحص|نفس الصور|فيديو', 'trust'),
        (r'سحاب|كبس|ازرار|زرار|بطان|مطاط|تمدد|عرض|ابط|طول', 'objection'),
        (r'بكج|باكج|درزن|دزين|ثلاث قطع|ثلاثه قطع', 'bundle'),
        (r'وزن|قياس|مقاس|عمر|سنين|سنتين|كيلو', 'fit'),
        (r'سعر|شكد|بشكد|بكم|توصيل|اجمالي|المجموع', 'cost'),
        (r'ثبتي|ثبتلي|احجزي|احجزلي|اريد اطلب|اريد احجز', 'checkout'),
    ):
        if re.search(pattern, text):
            ids.append(key)
    if not ids:
        ids.append('answer')
    if product and re.search(r'بكج|باكج|درزن|دزين', str(product.get('price', '')) + ' ' + str(product.get('offer', ''))):
        ids.insert(0, 'bundle')
    return list(dict.fromkeys(ids))[:3]


def guidance(ev, product=None):
    ids = lesson_ids(ev, product)
    if not ids:
        return ''
    return ('[أنماط رد راجعتها من محادثات انتهت بحجز وأخرى توقفت — تستخدم حسب الحاجة]\n'
            'تعلم طريقة معالجة السؤال، ولا تنسخ ضماناً أو سعراً أو بيانات زبون من الماضي. '
            'الأمثلة أنماط صياغة؛ لا تكتب علامة … للزبون، ولا تملأ معلومة مجهولة بالتخمين. '
            'هذه الأنماط لا تثبت سبباً إحصائياً للحجز ولا تتجاوز رفض الزبون أو شرطه غير المحسوم.\n'
            + '\n'.join(REVIEWED_LESSONS[key] for key in ids))


def price_preview(product, delivery_options, quantity=None):
    """One currently priced retail unit/package; ambiguous wholesale stays AI-led."""
    try:
        size, price, free = terms(product)
        unit_text = ' '.join(str(product.get(k) or '') for k in ('price', 'offer', 'notes', 'sale_unit'))
        if size == 1 and re.search(r'درزن|دزين|جملة|جمله|بالجمل', unit_text):
            return None
        label = f'البكج ({size} قطع)' if size > 1 else 'الموديل'
        scope = 'لبكج واحد' if size > 1 else 'لقطعة واحدة'
        lines = [f'سعر {label}: {price:,} د.ع.']
        for offer_quantity, amount in offer_tiers(product):
            lines.append(f'عرض {offer_quantity} قطع: {amount:,} د.ع.')
        requested = quantity if isinstance(quantity, int) else None
        if requested is not None and requested > 0:
            requested_quote = quote([{'product_id': product['product_id'], 'quantity': requested}], [product], 0)
            lines.append(f'سعر {requested} قطع: {requested_quote["product_total"]:,} د.ع.')
        totals = []
        for region, fee in delivery_options:
            priced = quote([{'product_id': product['product_id'], 'quantity': requested or size}], [product], fee)
            shipping = priced['delivery_fee']
            total = priced['product_total'] + shipping
            total_scope = f'لـ{requested} قطع' if requested else scope
            if free:
                return lines + [f'التوصيل مجاني؛ الإجمالي {total_scope}: {total:,} د.ع.']
            delivery_text = f'{shipping:,} د.ع' if shipping else 'مجاني'
            totals.append(f'التوصيل{region}: {delivery_text}؛ الإجمالي {total_scope}: {total:,} د.ع.')
        return lines + totals if totals else None
    except (ValueError, KeyError, TypeError):
        return None


def price_question(text):
    text = sales_strategy.normalized(text).strip(' ؟?!.،🌸')
    return bool(re.fullmatch(
        r'(?:ممكن\s+)?(?:السعر|سعر|سعره|سعرها|بشكد|بيش|بكم)|'
        r'(?:شكد|اشكد|بشكد|بيش|كم)\s*(?:سعر|سعره|سعرها|السعر|اسعر|سعر هذا|سعر الموديل)|'
        r'(?:شكد|اشكد|بشكد|بيش|بكم|سعر|عرض)\s*(?:(?:ال)?قطعتين|ثنين|\d+\s*قطع)', text))


def requested_quantity(text):
    text = sales_strategy.normalized(text)
    if re.search(r'قطعتين|قطعتان|ثنين', text):
        return 2
    match = re.search(r'(?<!\d)(\d+)\s*قطع', text)
    return int(match[1]) if match and 2 <= int(match[1]) <= 20 else None


def price_next_step(product, context, customer=None):
    """Invite a useful selection, without repeating fit questions or closing."""
    context, customer = context or {}, customer or {}
    if any(customer.get(key) for key in ('size', 'age', 'weight')):
        return ''
    evidence = ' '.join(str(item.get(key) or '') for item in context.get('evidence', [])
                        for key in ('text', 'in_reply_to'))
    previous_turn = ' '.join(str(item.get('text') or '') for item in context.get('last_staff_turn', []))
    if re.search(r'قياس|مقاس|سايز|عمر|سنوات|سنين|سنتين|وزن|كيلو',
                 sales_strategy.normalized(evidence + ' ' + previous_turn)):
        return ''
    sizes = sales_strategy.normalized(product.get('sizes'))
    if re.search(r'سن[ةه]|سنتين|سنوات|سنين|اشهر|شهر', sizes):
        return 'شنو العمر المطلوب حتى نتأكد من القياس؟'
    if (not re.search(r'وزن|كيلو|كغم', sizes)
            and re.fullmatch(r'[\d\s،,./–—\-]+|\d+\s*(?:الى|لحد)\s*\d+|[smlx]+(?:[\s،,/\-]+[smlx]+)*', sizes)):
        return 'شنو القياس المطلوب حتى نتأكد من المتوفر؟'
    return ''


def human_requested(text):
    text = sales_strategy.normalized(text)
    return bool(re.search(
        r'(?:ما\s*اريد|ماريد|لا اريد).{0,10}(?:ذكاء|بوت|روبوت|الي)\s*(?:يرد|يحجي|يجاوب)|'
        r'(?:اريد|ممكن|خليني|خلوني|خلي).{0,22}(?:احجي|احچي|اتكلم|اتواصل|اتحدث|يوصلني).{0,15}(?:موظف|بشر|انسان|ادار)|'
        r'(?:اريد|اربطني|وصلني|حولني|تحويل).{0,10}(?:موظف|انسان|بشر)', text))
