"""Reviewed reply patterns from booking journeys, without retaining private chats.

These are curated methods, not automatically trusted historical customer text or
historical product facts. Current catalog, policy and customer choices win.
"""
import re

try:
    from . import sales_strategy, customer_copy, product_variants, conversation_quality
    from .pricing import terms, quote, offer_tiers
except ImportError:
    import sales_strategy, customer_copy, product_variants, conversation_quality
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
        r'(?:و?شنو)\s*(?:سعره|سعرها|سعر الموديل|السعر)|'
        r'(?:شكد|اشكد|بشكد|بيش|كم)\s*(?:سعر|سعره|سعرها|السعر|اسعر|سعر هذا|سعر الموديل)|'
        r'(?:شكد|اشكد|بشكد|بيش|بكم|سعر|عرض)\s*(?:(?:ال)?قطعتين|ثنين|\d+\s*قطع)', text))


def requested_quantity(text):
    text = sales_strategy.normalized(text)
    if re.search(r'قطعتين|قطعتان|ثنين', text):
        return 2
    match = re.search(r'(?<!\d)(\d+)\s*قطع', text)
    return int(match[1]) if match and 2 <= int(match[1]) <= 20 else None


def customer_selection(context, customer=None):
    """Read customer answers; a staff question never supplies a missing size."""
    context, customer = context or {}, customer or {}
    selected = {key: customer.get(key) for key in ('size', 'age', 'color')}
    stored_colors = product_variants.colors(str(selected['color'] or ''))
    if len(stored_colors) == 1:
        selected['color'] = stored_colors[0]
    for item in context.get('evidence', []):
        text = sales_strategy.normalized(item.get('text'))
        question = sales_strategy.normalized(item.get('in_reply_to'))
        # A bare number belongs to the one dimension asked in the previous turn.
        if re.fullmatch(r'\d{1,3}', text):
            dimensions = [name for name, pattern in (
                ('size', r'قياس|مقاس|سايز'), ('age', r'عمر'), ('weight', r'وزن|كيلو'))
                if re.search(pattern, question)]
            if dimensions == ['size']:
                selected['size'] = text
            elif dimensions == ['age']:
                selected['age'] = int(text)
            continue
        if re.fullmatch(r'[2-6]?xl|xxl|xxxl|xs|[sml]', text):
            selected['size'] = text.upper()
            continue
        if re.search(r'[؟?]|شنو|شكد|كم قياس|متوفر|موجود|اكو', text):
            continue  # An availability question is not a confirmed choice.
        size_pattern = r'(?:قياسي|مقاسي|البس|قياس|مقاس|سايز)\s*(\d{1,2}|[2-6]?xl|xxl|xxxl|xs|[sml])(?!\w)'
        sizes = [hit[1].upper() for hit in re.finditer(size_pattern, text)
                 if not re.search(r'(?:مو|لا|ما اريد)\s*$', text[max(0, hit.start()-15):hit.start()])]
        if re.search(r'\d{1,2}\s*(?:لو|او|/)\s*(?:(?:قياس|مقاس)\s*)?\d{1,2}', text) or len(set(sizes)) > 1:
            selected['size'] = None
        elif sizes:
            selected['size'] = sizes[-1]
        elif re.search(r'(?:مو|لا|ما اريد)\s*(?:قياس|مقاس|سايز)', text):
            selected['size'] = None
        age = conversation_quality.child_age(text)
        if age is not None:
            selected['age'] = age
        colors = product_variants.requested(text)['colors']
        if len(colors) == 1:
            selected['color'] = colors[0]
        elif product_variants.colors(text):
            selected['color'] = None
    return selected


def selection_conflicts(product, context, customer=None):
    selected = customer_selection(context, customer)
    for field in ('size', 'color'):
        if selected[field] and product_variants.order_choice_error(product, field, str(selected[field])):
            return True
    years = product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))['years']
    if years and selected['age'] is not None:
        try:
            return not any(low <= float(selected['age']) <= high for low, high in years)
        except (TypeError, ValueError):
            return False
    return False


def price_next_step(product, context, customer=None, history=()):
    """Move to the next unanswered selection without inferring purchase consent."""
    selected = customer_selection(context, customer)
    sizes = product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))
    age_based = bool(sizes['years']) or bool(re.search(r'سن[ةه]|سنتين|سنوات|سنين|اشهر|شهر',
        sales_strategy.normalized(customer_copy.fact(product.get('sizes')))))
    if age_based and selected['age'] is None:
        return 'شكد العمر المطلوب؟'
    if not age_based and not selected['size']:
        recent = ' '.join(str(item.get('text') or '') for item in (history or [])[-8:]
                          if item.get('direction') == 'outgoing')
        if re.search(r'شنو\s+قياس|شكد\s+قياس|أي\s+قياس|اي\s+قياس', recent):
            colors = product_variants.colors(customer_copy.fact(product.get('colors'), 'colors'))
            if colors and not selected['color'] and not re.search(r'شنو\s+لون|يا\s+لون|اللون\s+\S+\s+مناسب', recent):
                return f'اللون ال{colors[0]} مناسبج؟' if len(colors) == 1 else 'يا لون يعجبج؟'
            return 'إذا تحبين، أساعدج نتأكد من القياس المناسب؟'
        return 'شنو قياس الملابس اللي ترتاحين بيه عادة حتى أشوفلج المتوفر؟'
    if age_based:
        try:
            supported = any(low <= float(selected['age']) <= high for low, high in sizes['years'])
        except (TypeError, ValueError):
            supported = False
    else:
        checks = product_variants.assess(product, 'قياس ' + str(selected['size']))
        supported = bool(checks and checks[0]['supported'] is True)
    if not supported or (context or {}).get('open_details'):
        return ''
    colors = product_variants.colors(customer_copy.fact(product.get('colors'), 'colors'))
    if not selected['color']:
        if len(colors) == 1:
            return f'اللون ال{colors[0]} مناسبج؟'
        return 'شنو اللون اللي يعجبج؟' if colors else ''
    if selected['color'] in colors:
        return 'تحبين نكمل حجزه؟'
    return ''


def selection_guidance(product, context, customer=None):
    if not product:
        return ''
    step = price_next_step(product, context, customer)
    return ('[توجيه الاختيار الحالي]\n'
            'سؤال الموظفة السابق عن القياس لا يعني أن الزبونة أجابت. '
            'لا تسرد نطاق القياسات أو الأوزان عند سؤال المواصفات أو القماش أو التوفر العام؛ '
            'اعرض النطاق فقط إذا طلبته صراحة. أجب عن حاجتها واسأل عن قياسها المعتاد إذا ما ذكرته. '
            'إذا القياس أو العمر معروف لا تعد السؤال عنه. '
            + ('الخطوة المناسبة الآن، بصياغة لطيفة واحدة بعد الجواب: ' + step if step else
               'أكمل على القياس أو العمر المعروف؛ إن بقي تفصيل غير محسوم فاسأل عنه فقط.'))


def cue_line(cue):
    """Render the approved cue conversationally without adding an expiry claim."""
    text = str(cue.get('text') or '').replace(' عيني', '').strip(' .')
    few_left = cue.get('kind') == 'limited_stock' or bool(re.search(r'(?:آخر|اخر)\s+الكمي[هة]', text))
    if few_left:
        text = re.sub(r'\s+على\s+(?:آخر|اخر)\s+الكمي[هة]', '', text).strip()
        offer = ('عليه ' + text + '، ' if re.match(r'^عرض\b', text) else '')
        return offer + 'تره الكمية الباقية قليلة كلش، كم قطعة بس.'
    if re.fullmatch(r'عرض خاص\s+الكمية محدودة', text):
        return 'تره عليه عرض خاص، والكمية محدودة هسه.'
    if re.match(r'^عرض\b', text):
        text = 'عليه ' + text
    return re.sub(r'(عرض خاص)\s+(الكمية)', r'\1، وتره \2', text) + '.'


def naturalize_last_quantity_reply(reply, product):
    """Use the store's Iraqi scarcity wording only for a documented last batch."""
    if not isinstance(reply, str):
        return reply
    offer = customer_copy.fact((product or {}).get('offer'))
    if not re.search(r'(?:آخر|اخر)\s+الكمي[هة]', offer):
        return reply
    return re.sub(r'\s+على\s+(?:آخر|اخر)\s+الكمي[هة]',
                  '، تره الكمية الباقية قليلة كلش، كم قطعة بس', reply)


def personalize_specifications(reply, question):
    """Remove only an unsolicited catalogue size range from a general description.

    Other facts, explicit size questions and fit answers stay intact. The next
    selection question is handled separately, using actual customer evidence.
    """
    text = sales_strategy.normalized(question)
    if (not re.search(r'مواصفات|تفاصيل', text)
            or re.search(r'قياس|مقاس|سايز|وزن|كيلو|عمر|طول|عرض', text)):
        return reply
    cleaned = re.sub(
        r'(?<!\w)و?(?:القياسات|قياسات(?:ه|ها)?|المقاسات|مقاسات(?:ه|ها)?)\s*'
        r'(?:المتوفرة\s*|متوفر[هة]?\s*)?(?:من\s*)?\d{1,3}\s*'
        r'(?:إلى|الى|لحد|حتى|[-–—])\s*\d{1,3}(?:\s*(?:كيلو|سنوات|سنة|سنه))?'
        r'\s*[.،؛]?', '', str(reply or ''))
    cleaned = re.sub(r'[،؛]\s*(?=[.!]|$)', '', cleaned)
    return re.sub(r'\.{2,}', '.', cleaned).strip()


def human_requested(text):
    text = sales_strategy.normalized(text)
    return bool(re.search(
        r'(?:ما\s*اريد|ماريد|لا اريد).{0,10}(?:ذكاء|بوت|روبوت|الي)\s*(?:يرد|يحجي|يجاوب)|'
        r'(?:اريد|ممكن|خليني|خلوني|خلي).{0,22}(?:احجي|احچي|اتكلم|اتواصل|اتحدث|يوصلني).{0,15}(?:موظف|بشر|انسان|ادار)|'
        r'(?:اريد|اربطني|وصلني|حولني|تحويل).{0,10}(?:موظف|انسان|بشر)', text))
