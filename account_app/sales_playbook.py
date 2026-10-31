"""Reviewed reply patterns from booking journeys, without retaining private chats.

These are curated methods, not automatically trusted historical customer text or
historical product facts. Current catalog, policy and customer choices win.
"""
import re

try:
    from . import sales_strategy, customer_copy, product_variants, conversation_quality, sales_recovery
    from .pricing import terms, quote, offer_tiers
    from .checkout import is_confirmation, delivered_total_inquiry
except ImportError:
    import sales_strategy, customer_copy, product_variants, conversation_quality, sales_recovery
    from pricing import terms, quote, offer_tiers
    from checkout import is_confirmation, delivered_total_inquiry

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
    lessons = [REVIEWED_LESSONS[key] for key in ids]
    if product and last_pieces_price_line(product):
        lessons = [lesson for key, lesson in zip(ids, lessons) if key != 'cost']
        lessons.append(
            'أسلوب عرض آخر القطع: افتتح بالعرض والسعر الحالي بجملة عراقية قصيرة، مثل «عليه عرض على آخر قطع، وصار سعره … 🌷»، '
            'وبعدها خطوة مناسبة تكمل الحوار. الصياغة مرنة بنفس الأسلوب، وليست نصاً إلزامياً. '
            'السعر من المنتج الحالي ولا تخترع سعراً قبل العرض أو نسبة خصم أو موعد نفاد. '
            'لا تضف القماش والتوصيل والمجموع لسؤال السعر وحده؛ إذا سأل عنها أو جمع عدة أسئلة أجب عن المطلوب كله. '
            'اسأل عن العمر أو القياس أو اللون المجهول فقط، ولا تكرر العرض في كل دور ولا تضغط بعد رفض أو تردد.')
    return ('[أنماط رد راجعتها من محادثات انتهت بحجز وأخرى توقفت — تستخدم حسب الحاجة]\n'
            'تعلم طريقة معالجة السؤال، ولا تنسخ ضماناً أو سعراً أو بيانات زبون من الماضي. '
            'الأمثلة أنماط صياغة؛ لا تكتب علامة … للزبون، ولا تملأ معلومة مجهولة بالتخمين. '
            'هذه الأنماط لا تثبت سبباً إحصائياً للحجز ولا تتجاوز رفض الزبون أو شرطه غير المحسوم.\n'
            + '\n'.join(lessons))


def last_pieces_price_line(product, history=(), quantity=None, *, allow_offer=True):
    """The owner's short style, only for a documented plain last-batch offer."""
    if not sales_strategy._sales_available(product) or quantity not in (None, 1):
        return ''
    offer = sales_strategy.normalized(sales_strategy._offer_fact(product)).strip(' .،')
    if not re.fullmatch(r'(?:عليه\s+)?عرض(?:\s+خاص)?(?:\s+لفتر[هة]\s+محدود[هة])?\s+عل[ىي]\s+'
                        r'(?:اخر\s+الكمي[هة]|اخر\s+(?:ال)?قطع)', offer):
        return ''
    try:
        count, price, _ = terms(product)
        if count != 1 or offer_tiers(product):
            return ''
    except (ValueError, TypeError, KeyError):
        return ''
    amount = customer_copy.chat_amount(price)
    if not allow_offer or sales_strategy.persuasion_used(history):
        return f'سعره {amount} 🌷'
    return f'عليه عرض على آخر قطع، وصار سعره {amount} 🌷'


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
        r'(?:بيش|بكم|بشكد)\s*(?:هذا|هاذا|هاي|هالقطعة|هالسوت|هالفستان)|'
        r'(?:شكد|اشكد|بشكد|بيش|بكم|سعر|عرض)\s*(?:(?:ال)?قطعتين|ثنين|\d+\s*قطع)', text))


def delivery_price_question(text):
    if delivered_total_inquiry(text):
        return True
    text = sales_strategy.normalized(text).strip(' ؟?!.،')
    text = re.sub(r'^(?:السلام عليكم|سلام عليكم)\s*[،,]?\s*', '', text)
    return bool(re.fullmatch(
        r'(?:شكد|اشكد|كم)\s+(?:تصير|يصير|المبلغ|السعر|سعره|سعرها|'
        r'سعر\s+(?:السوت|الفستان|الموديل|القطعة))\s*(?:ويا|مع)\s*التوصيل'
        r'(?:\s*(?:ل|الى)\s*بغداد)?(?:\s*و?شكد\s*(?:بي|بيه)\s*قياسات)?', text))


def delivered_price_line(product, delivery_options, quantity=None):
    """One confirmed retail total; variable fees and package pricing stay explicit."""
    if len(delivery_options) != 1 or quantity not in (None, 1):
        return ''
    try:
        count, price, _ = terms(product)
        if count != 1:
            return ''
        priced = quote([{'product_id': product['product_id'], 'quantity': 1}],
                       [product], delivery_options[0][1])
        amount, shipping = priced['product_total'], priced['delivery_fee']
        total = customer_copy.chat_amount(amount + shipping)
        name = customer_copy.fact(product.get('product_name'), 'product_name')
        label = 'السوت' if 'سوت' in name else ('الفستان' if 'فستان' in name else 'القطعة')
        unit = str(amount // 1000) if amount >= 1000 and amount % 1000 == 0 else customer_copy.chat_amount(amount)
        fee = customer_copy.chat_amount(shipping) if shipping else 'مجاني'
        return f'سعره مع التوصيل {total}؛ {label} {unit} والتوصيل {fee} 🌷'
    except (ValueError, TypeError, KeyError):
        return ''


def size_range_question(product):
    data = product_variants.size_data(customer_copy.fact(product.get('sizes')))
    if len(data['weights']) == 1 and not any(data[key] for key in ('ranges', 'years', 'numbers', 'labels')):
        low, high = data['weights'][0]
        return f'الأوزان المذكورة من {low} الى {high} كيلو، شنو القياس المطلوب؟'
    return ''


def requested_quantity(text):
    text = sales_strategy.normalized(text)
    if re.search(r'قطعتين|قطعتان|ثنين', text):
        return 2
    match = re.search(r'(?<!\d)(\d+)\s*قطع', text)
    return int(match[1]) if match and 2 <= int(match[1]) <= 20 else None


def selection_evidence(context):
    for item in (context or {}).get('evidence',[]):
        for line in str(item.get('text') or '').splitlines():
            if line.strip():yield dict(item,text=line.strip())


def customer_selection(context, customer=None):
    """Read customer answers; a staff question never supplies a missing size."""
    context, customer = context or {}, customer or {}
    selected = {key: customer.get(key) for key in ('size', 'age', 'color', 'weight')}
    selected['colors'] = []
    selected['color_options'] = []
    selected['ages'] = []
    stored_colors = product_variants.colors(str(selected['color'] or ''))
    if len(stored_colors) == 1:
        selected['color'] = stored_colors[0]
    selected['colors'] = stored_colors
    for item in selection_evidence(context):
        text = sales_strategy.normalized(product_variants.normalize_size_words(item.get('text')))
        question = sales_strategy.normalized(item.get('in_reply_to'))
        weight=product_variants.weight_answer(text,question)
        if weight is not None:selected['weight']=weight['value']
        replacement = product_variants.color_replacement(text)
        proposed = product_variants.color_replacement(question)
        if proposed and (is_confirmation(text) or product_variants.chosen_colors(text) == [proposed[0]]):
            replacement = proposed
        if replacement and replacement[1] in selected['colors']:
            selected['colors'] = list(dict.fromkeys(replacement[0] if color == replacement[1] else color
                                                    for color in selected['colors']))
            selected['color'] = selected['colors'][0] if len(selected['colors']) == 1 else None
            selected['color_options'] = []
            continue
        # A bare number belongs to the one dimension asked in the previous turn.
        if re.fullmatch(r'\d{1,3}', text):
            dimensions = [name for name, pattern in (
                ('size', r'قياس|مقاس|سايز'), ('age', r'عمر'), ('weight', r'وزن|كيلو'))
                if re.search(pattern, question)]
            if dimensions == ['size']:
                selected['size'] = text
            elif dimensions == ['age']:
                selected['age'] = int(text)
                selected['ages'] = [int(text)]
            continue
        if re.fullmatch(r'[2-6]?xl|xxl|xxxl|xs|[sml]', text):
            selected['size'] = text.upper()
            continue
        if re.search(r'[؟?]|شنو|شكد|كم قياس|متوفر|موجود|اكو', text):
            # Asking for a specific size retains that preference, never consent
            # to buy. General ranges and explicit alternatives remain unresolved.
            requested_sizes = product_variants.requested(text)['sizes']
            requested_colors = product_variants.requested(text)['colors']
            if len(requested_colors) == 1 and not re.search(r'ما اريد|ماريد|مو\s+' + re.escape(requested_colors[0]), text):
                selected['color'] = requested_colors[0]
            age = conversation_quality.child_age(text)
            if age is not None:
                selected['age'] = age
            numeric_range = re.search(r'(?:قياس|قياسي|البس)\s*(\d{2})\s*[،,/]\s*(\d{2})(?!\d)', text)
            if numeric_range:
                selected['size'] = numeric_range[1] + '-' + numeric_range[2]
            elif len(requested_sizes) == 1 and not re.search(r'(?<!\w)(?:لو|او|مو)(?!\w)|من\s*\d+|ما اريد', text):
                numbers = re.findall(r'(?<!\d)\d{2}(?!\d)', text)
                if not requested_sizes[0].isdigit() or len(numbers) == 1:
                    selected['size'] = requested_sizes[0]
            continue
        size_pattern = r'(?:قياسي|مقاسي|البس|قياس|مقاس|سايز)\s*(\d{1,2}|[2-6]?xl|xxl|xxxl|xs|[sml])(?!\w)'
        sizes = [hit[1].upper() for hit in re.finditer(size_pattern, text)
                 if not re.search(r'(?:مو|لا|ما اريد)\s*$', text[max(0, hit.start()-15):hit.start()])]
        size_range = re.search(r'(?:قياس|قياسي|البس)\s*(\d{2})\s*[،,/]\s*(\d{2})(?!\d)', text)
        if size_range:
            selected['size'] = size_range[1] + '-' + size_range[2]
            chosen_colors = product_variants.requested(text)['colors']
            if len(chosen_colors) == 1:
                selected['color'] = chosen_colors[0]
            continue
        labels = product_variants.requested(text)['sizes']
        letter_labels = [s for s in labels if not s.isdigit()]
        digit_labels = [s for s in labels if s.isdigit()]
        numbers = re.findall(r'(?<!\d)\d{2}(?!\d)', text)
        if letter_labels and ((not digit_labels and not re.search(r'(?<!\w)(?:لو|او|مو)(?!\w)|ما اريد', text))
                              or (len(letter_labels) == 1 and len(numbers) == 1 and not re.search(r'(?<!\w)مو(?!\w)|ما اريد', text))):
            selected['size'] = letter_labels[-1]
            chosen_colors = product_variants.chosen_colors(text)
            if chosen_colors:
                selected['colors'] = chosen_colors
                selected['color'] = chosen_colors[0] if len(chosen_colors)==1 else None
                selected['color_options'] = []
            continue
        if re.search(r'\d{1,2}\s*(?:لو|او|/)\s*(?:(?:قياس|مقاس)\s*)?\d{1,2}', text) or len(set(sizes)) > 1:
            selected['size'] = None
        elif sizes:
            selected['size'] = sizes[-1]
        elif re.search(r'(?:مو|لا|ما اريد)\s*(?:قياس|مقاس|سايز)', text):
            selected['size'] = None
        age = conversation_quality.child_age(text)
        if age is not None and (not sizes or re.search(r'عمر', text)):
            selected['age'] = age
            selected['ages'] = product_variants.child_ages(text) or [age]
        colors = product_variants.requested(text)['colors'] or list(product_variants.color_quantities(text))
        if len(colors) == 1:
            if (len(selected['colors'])>1 and product_variants.color_quantities(text)
                    and colors[0] in selected['colors'] and not re.search(r'(?<!\w)(?:بس|فقط|عوفي|بدل)(?!\w)',text)):
                continue  # A count for one line does not remove the other selected color.
            selected['color'] = colors[0]
            selected['colors'] = colors
            selected['color_options'] = []
        elif colors or product_variants.colors(text):
            selected['color'] = None
            selected['colors'] = product_variants.chosen_colors(text) or list(product_variants.color_quantities(text))
            selected['color_options'] = colors if colors and not selected['colors'] else []
    return selected


def quantities_for_colors(context, customer=None, current_text=''):
    try:
        from . import color_choices, sales_context
    except ImportError:
        import color_choices, sales_context
    prefix=[];counts={}
    for item in selection_evidence(context):
        prefix.append(item)
        chosen=customer_selection({'evidence':prefix},customer)['colors']
        text=item.get('text') or ''
        question=item.get('in_reply_to') or ''
        if len(chosen)>1:
            for color in chosen:counts.setdefault(color,None)
        replacement=product_variants.color_replacement(text)
        proposed=product_variants.color_replacement(question)
        if proposed and is_confirmation(text):replacement=proposed
        if replacement and replacement[1] in counts:
            new,old=replacement
            count=counts.pop(old)
            if new in counts:count=(counts[new]+count) if counts[new] and count else None
            counts[new]=count
        counts.update(product_variants.color_quantities(text))
        if color_choices.one_each(text,{'last_staff_turn':[{'text':question}]}):
            counts.update({color:1 for color in chosen})
        number=product_variants.quantity_number(text)
        asked=product_variants.requested(question)['colors']
        if number and len(asked)==1 and re.search(r'عدد|كمية|قطعة|قطعه|قطع',question):
            counts[asked[0]]=number
    chosen=customer_selection(context,customer)['colors']
    counts.update(product_variants.color_quantities(current_text))
    if color_choices.one_each(current_text,context):counts.update({color:1 for color in chosen})
    question=sales_context.question_from_turn((context or {}).get('last_staff_turn') or [])
    asked=product_variants.requested(question)['colors']
    number=product_variants.quantity_number(current_text)
    if number and len(asked)==1 and re.search(r'عدد|كمية|قطعة|قطعه|قطع',question):counts[asked[0]]=number
    return {color:count for color,count in counts.items() if color in chosen}


def weight_cart_choices(product, context, customer=None, current_text=''):
    ranges=product_variants.weight_only_ranges(product)
    if not ranges:return None
    selected=customer_selection(context,customer)
    size=selected.get('size');weight=selected.get('weight');size_type='weight'
    if not size and not weight and product_variants.is_free_size_product(product):
        size='فري سايز'
    if size and not str(size).isdigit():
        if product_variants.order_choice_error(product,'size',str(size)):
            if not weight:return None
        elif weight and (not re.fullmatch(r'\d{1,3}(?:\.\d{1,2})?',str(weight)) or not any(lo<=float(weight)<=hi for lo,hi in ranges)):
            return {'question':'الوزن اللي ذكرتيه مو ضمن مجال هالموديل، ما أگدر أضمن ملاءمته.'}
        else:
            size_type='size';weight=None  # A chosen garment label never requires a body weight.
    if size_type == 'weight':
        if size and not weight and (not re.fullmatch(r'\d{1,3}(?:\.\d{1,2})?',str(size))
                                    or not any(lo<=float(size)<=hi for lo,hi in ranges)):return None
        weight=weight or (str(size) if size else None)
        if not weight:return {'question':'شنو القياس المطلوب؟'}
        if not re.fullmatch(r'\d{1,3}(?:\.\d{1,2})?',str(weight)) or not any(lo<=float(weight)<=hi for lo,hi in ranges):
            return {'question':'الوزن اللي ذكرتيه مو ضمن مجال هالموديل، ما أگدر أضمن ملاءمته.'}
        size=str(weight)
    offered=product_variants.colors(customer_copy.fact(product.get('colors'),'colors'))
    chosen=selected['colors'] or ([selected['color']] if selected['color'] else [])
    if not chosen and len(offered)==1:chosen=offered
    if not chosen and not offered:chosen=['']
    if not chosen:
        return {'question':('تفضلين '+' لو '.join('ال'+c for c in selected['color_options'])+'؟')
                if selected['color_options'] else 'شنو اللون اللي تختارينه؟'}
    if offered and any(color not in offered for color in chosen):
        missing=[color for color in chosen if color not in offered]
        return {'question':'ال'+' وال'.join(missing)+' مو متوفر بهالموديل؛ المتوفر ال'+' وال'.join(offered)+'.'}
    counts=quantities_for_colors(context,customer,current_text)
    if len(chosen)>1:
        missing=[color for color in chosen if not counts.get(color)]
        if len(missing)==len(chosen):return {'question':'قطعة من كل لون؟'}
        if missing:return {'question':'شكد قطعة من ال'+missing[0]+'؟'}
    elif chosen[0] in counts and not counts[chosen[0]]:
        return {'question':'شكد قطعة من ال'+chosen[0]+'؟'}
    items=[dict(product_id=product['product_id'],product_name=product['product_name'],
                color=color,size=str(size),size_type=size_type,quantity=counts.get(color,1),
                **({'weight':str(weight)} if size_type=='weight' else {}))
           for color in chosen]
    return {'items':items,'weight':weight}


def selection_conflicts(product, context, customer=None):
    selected = customer_selection(context, customer)
    if any(product_variants.order_choice_error(product, 'color', color) for color in selected['colors']):
        return True
    for field in ('size', 'color'):
        if selected[field] and product_variants.order_choice_error(product, field, str(selected[field])):
            if field == 'size' and product_variants.weight_only_ranges(product) and selected.get('weight') and any(
                    lo <= float(selected['weight']) <= hi for lo, hi in product_variants.weight_only_ranges(product)):
                continue
            return True
    years = product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))['years']
    if years and selected['age'] is not None and not selected['size']:
        try:
            return not any(low <= float(selected['age']) <= high for low, high in years)
        except (TypeError, ValueError):
            return False
    return False


def price_next_step(product, context, customer=None, history=(), *, delivery_requires_province=False):
    """Move to the next unanswered selection without inferring purchase consent."""
    selected = customer_selection(context, customer)
    context, customer = context or {}, customer or {}
    if context.get('open_details'):
        return ''
    recent = ' '.join(str(item.get('text') or '') for item in (history or [])[-8:]
                      if item.get('direction') == 'outgoing')
    province = customer.get('province') or next((sales_recovery.policy_province(item.get('text'))
        for item in reversed(context.get('evidence', [])) if sales_recovery.policy_province(item.get('text'))), None)
    sizes = product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))
    age_based = bool(sizes['years']) or bool(re.search(r'سن[ةه]|سنتين|سنوات|سنين|اشهر|شهر',
        sales_strategy.normalized(customer_copy.fact(product.get('sizes')))))
    chosen_year_size = product_variants.year_size(selected['size']) if sizes['years'] else None
    if sizes['years'] and chosen_year_size is None and selected['size']:
        chosen_year_size = product_variants.child_age(selected['size'])
    if age_based and selected['age'] is None and chosen_year_size is None:
        if 'شكد العمر المطلوب' in recent:
            return 'يا عمر مطلوب للطفل؟'
        return 'شكد العمر المطلوب؟'
    weight_ranges=product_variants.weight_only_ranges(product)
    if not age_based and weight_ranges and (not selected['size'] or (selected.get('weight') and product_variants.order_choice_error(product,'size',str(selected['size'])))):
        plan=weight_cart_choices(product,context,customer)
        q = (plan.get('question') or booking_invitation(context,history)) if plan else ''
        if q == 'شنو القياس المطلوب؟' and 'شنو القياس المطلوب' in recent:
            if 'يا قياس تحبين نعتمدلج' in recent:
                return ''
            if 'شنو القياس اللي يناسبج' in recent:
                return 'يا قياس تحبين نعتمدلج؟'
            return 'شنو القياس اللي يناسبج؟'
        return q
    if not age_based and not selected['size']:
        if re.search(r'شنو\s+(?:ال)?قياس|شكد\s+قياس|أي\s+قياس|اي\s+قياس', recent):
            if not province and delivery_requires_province and 'محافظ' not in recent:
                return 'من يا محافظة حتى أحسبلج أجور التوصيل؟'
            if 'يا قياس تحبين نعتمدلج' in recent:
                return ''
            if 'شنو القياس اللي يناسبج' in recent and 'مضبوط لو واسع' not in recent:
                return 'يا قياس تحبين نعتمدلج؟'
            return 'شنو القياس اللي يناسبج؟'
        return 'شكد قياس تلبسين؟'
    if age_based:
        try:
            chosen_age = chosen_year_size if chosen_year_size is not None else float(selected['age'])
            supported = any(low <= chosen_age <= high for low, high in sizes['years'])
        except (TypeError, ValueError):
            supported = False
    else:
        checks = product_variants.assess(product, 'قياس ' + str(selected['size']))
        supported = bool(checks and checks[0]['supported'] is True)
    if not supported or context.get('open_details'):
        return ''
    colors = product_variants.colors(customer_copy.fact(product.get('colors'), 'colors'))
    if len(selected['colors']) > 1:
        if any(color not in colors for color in selected['colors']):
            return ''
        return 'قطعة من كل لون؟'
    if len(selected['color_options']) > 1:
        if any(color not in colors for color in selected['color_options']):
            return ''
        return 'تفضلين ' + ' لو '.join('ال' + color for color in selected['color_options']) + '؟'
    if not selected['color'] and len(colors) > 1:
        if 'شنو اللون اللي يعجبج' in recent:
            return 'أي لون تحبين نعتمدلج؟'
        return 'شنو اللون اللي يعجبج؟' if colors else ''
    if len(colors) == 1 or selected['color'] in colors:
        if not province and delivery_requires_province:
            return 'من يا محافظة حتى أحسبلج أجور التوصيل؟'
        return booking_invitation(context, history)
    return ''


def booking_invitation(context, history=()):
    """A second invitation needs a new buying-related turn, never a refusal."""
    evidence = (context or {}).get('evidence', [])
    latest = sales_strategy.normalized(evidence[-1].get('text')) if evidence else ''
    if (latest and (sales_strategy.explicit_pause(latest) or sales_strategy.stop_kind(latest)
                   or sales_strategy.soft_indecision(latest) or sales_strategy._courtesy_only(latest))):
        return ''
    offers = [item for item in history or [] if item.get('direction') == 'outgoing'
              and re.search(r'(?:أثبتلج|اثبتلج|أحجزلج|احجزلج|نكمل).{0,18}حجز', item.get('text') or '')]
    if not offers:
        return 'أثبتلج حجز؟'
    if len(offers) >= 2 or not latest:
        return ''
    previous = sales_strategy.normalized(evidence[-2].get('text')) if len(evidence) > 1 else ''
    if latest == previous or not re.search(r'سعر|قماش|متوفر|موجود|قياس|لون|توصيل|فحص|يناسب|حلو|عجب|تمام', latest):
        return ''
    return 'تحبين أثبتلج الحجز هسه؟'


def next_sales_message(product, history=()):
    """Mention the recorded offer now and save its scarcity for another turn."""
    cue = sales_strategy.persuasion_cue(product, history)
    if not cue:
        return ''
    opening, ending = price_cue_parts(cue)
    if opening:
        return opening
    offer = customer_copy.fact(product.get('offer'))
    if (cue.get('kind') == 'limited_stock' and re.search(r'الكمية\s+محدودة', offer)
            and not re.search(r'(?:آخر|اخر)\s+الكمية', offer)
            and not product.get('stock_quantity')):
        return 'تره الكمية محدودة.'
    return ending


def price_cue_parts(cue):
    """Keep the recorded offer before the price and scarcity after it."""
    if not cue:
        return '', ''
    text = str(cue.get('text') or '').strip(' .')
    if cue.get('kind') != 'offer':
        return '', cue_line(cue)
    last = bool(re.search(r'(?:آخر|اخر)\s+الكمي[هة]', text))
    limited = bool(re.search(r'الكمية\s+محدودة', text))
    offer = re.sub(r'\s+على\s+(?:آخر|اخر)\s+الكمي[هة]|\s+الكمية\s+محدودة', '', text).strip(' ،')
    opening = ('وعليه عرض هالفترة.' if re.fullmatch(r'عرض خاص(?: لفترة محدودة)?', offer) else
               ('وعليه ' if offer.startswith('عرض') else '') + offer + '.')
    ending = ('تره الكمية الباقية قليلة كلش، كم قطعة بس.' if last else
              'تره الكمية محدودة.' if limited else '')
    return opening, ending


def selection_guidance(product, context, customer=None, history=()):
    if not product:
        return ''
    step = price_next_step(product, context, customer, history)
    selected = customer_selection(context, customer)
    plural = ('الزبون اختار الألوان معاً: ' + ' و'.join(selected['colors'])
              + '. لا تسأله أي لون يريد مرة ثانية؛ تحقق من توفر كل لون واسأل عن عدد كل لون إذا لم يحدد. '
              if len(selected['colors']) > 1 else '')
    return ('[توجيه الاختيار الحالي]\n'
            + plural
            + 'سؤال الموظفة السابق عن القياس لا يعني أن الزبونة أجابت. '
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
    return re.sub(r'على\s+(?:آخر|اخر)\s+الكمي[هة]', 'على آخر قطع', reply)


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


def assistant_identity_question(text):
    """Questioning who is replying routes to staff; photo provenance does not."""
    clean = ' '.join(sales_strategy.normalized(text).split())
    identity = (r'ذكاء|روبوت|بوت|بشر|انسان[ةه]?|موظف[ةه]?|حقيقي[ةه]|برنامج|تلقائي[ةه]?|'
                r'(?:شات|تشات)\s*(?:جي\s*بي\s*تي|gpt)|chatgpt|'
                r'(?:ال)?مساعد[ةه]?\s*(?:المبيعات\s*)?(?:ال)?(?:الي[ةه]?|افتراضي[ةه]?|رقمي[ةه]?)|(?:ال)?الي[ةه]|'
                r'الي(?=\s*(?:لو|او|[؟?]|$))')
    subject = (r'انت[يىه]?|انتم|حضرت[كجچ]|رد(?:ود)?(?:[كجچ]م?|\s*مال[كجچ])?|'
               r'(?:هذا|هاي|هذي)\s*(?:ال)?(?:رد(?:ود)?|نظام|برنامج)|الرد(?:ود)?|'
               r'(?:اللي|منو)\s*(?:يرد|يحجي|يجاوب)(?:ني)?')
    for match in re.finditer(r'(?<!\w)(?:' + subject + r')(?!\w)(?P<gap>[^؟?\n.!]{0,40}?)(?:' + identity + r')(?!\w)', clean):
        if not re.search(r'صور|فيديو|تصوير|اعلان', match['gap']):
            return True
    return bool(re.search(r'\bare you\s+(?:(?:a|an)\s+)?(?:bot|robot|human|ai|real person|chatgpt)|'
                          r'\bis this\s+(?:(?:a|an)\s+)?(?:ai|bot|automated reply|real person)|'
                          r'\bwho is replying.{0,20}(?:person|human|ai|bot)', clean))


def assistant_identity_statement(text):
    clean = ' '.join(sales_strategy.normalized(text).split())
    return assistant_identity_question(re.sub(r'(?<!\w)(?:انا|اني|نحن)(?!\w)', 'انتي', clean)) or bool(
        re.search(r"\bi(?: am|'m)\s+(?:(?:a|an)\s+)?(?:ai|bot|robot|automated assistant|virtual assistant|language model|artificial intelligence|chatgpt|human|real person|not\s+(?:a\s+)?human)|"
                  r'\bas\s+(?:a|an)\s+(?:ai|language model|virtual assistant)|'
                  r'(?<!\w)(?:بصفتي|كوني|باعتباري)\s*(?:ال)?(?:مساعد|نموذج|ذكاء|برنامج|روبوت)|'
                  r'(?:تم\s*تطويري|تم\s*تدريبي).{0,30}(?:ذكاء|نموذج\s*لغ[ةه])', clean))


def human_requested(text):
    text = sales_strategy.normalized(text)
    return assistant_identity_question(text) or bool(re.search(
        r'(?:ما\s*اريد|ماريد|لا اريد).{0,10}(?:ذكاء|بوت|روبوت|الي)\s*(?:يرد|يحجي|يجاوب)|'
        r'(?:اريد|ممكن|خليني|خلوني|خلي).{0,22}(?:احجي|احچي|اتكلم|اتواصل|اتحدث|يوصلني).{0,15}(?:موظف|بشر|انسان|ادار)|'
        r'(?:اريد|اربطني|وصلني|حولني|تحويل).{0,10}(?:موظف|انسان|بشر)', text))
