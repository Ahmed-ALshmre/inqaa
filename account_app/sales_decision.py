"""Bounded decision support learned from reviewed Iraqi store conversations.

Stores topic codes only. A stalled conversation is not a known motive, and a
previous purchase is not proof that the preceding sales claim was true.
"""
import re
try:
    from .checkout import normalized
    from . import sales_answers, sales_strategy, product_variants, product_albums, customer_copy
    from .pricing import terms, quote
except ImportError:
    from checkout import normalized
    import sales_answers, sales_strategy, product_variants, product_albums, customer_copy
    from pricing import terms, quote

DETAILS = {
    'length': ('طول القطعة', r'طول(?:ه|ها)|الطول|طول\s+(?:القطعة|الفستان|القميص|البنطرون|البنطلون|الكيلوت|اكيلوت|التنورة|العباءة)'),
    'width': ('عرض القطعة', r'عرض(?:ه|ها)|عرض\s+(?:القطعة|الصدر)|تحت الابط|محيط الصدر'),
    'lining': ('البطانة', r'بطان|مبطن'),
    'closure': ('الإغلاق', r'سحاب|كبس|ازرار|زرار|ينفتح'),
}


def advance_details(pending, direction, text):
    pending = [key for key in pending or [] if key in DETAILS]
    clean = normalized(text)
    if direction == 'incoming':
        if re.search(r'غير\s+(?:الموديل|الفستان|القطعة)|ما اريد هذا|مو هذا الموديل|بدلي الموديل', clean):
            pending = []
        if re.search(r'ما\s*(?:احتاج|اريد)\s*(?:اعرف|تتاكد)|مو مهم|ما يهمني', clean):
            return [k for k in pending if not re.search(DETAILS[k][1], clean)]
        for key, (_, pattern) in DETAILS.items():
            if re.search(pattern, clean) and not re.search(r'طولي|طولج|طولك|طول البنت', clean):
                if key not in pending:
                    pending.append(key)
    elif direction == 'outgoing':
        # A later, separate question does not undo an explicit factual answer.
        clauses = re.split(r'[.!\n]+', clean)
        for key in list(pending):
            answer_text = ' '.join(part for part in clauses if not re.search(
                r'يحتاج|نتاكد|اتاكد|مو متاكد|ما عندي|ما اعرف|تاكيد|[؟?]', part))
            if not answer_text:
                continue
            if key in {'length', 'width'}:
                answered = re.search('(?:' + DETAILS[key][1] + r').{0,22}\d{2,3}\s*(?:سم|سانتي|سنتيمتر)', answer_text)
                if not answered and len(pending) == 1:
                    answered = re.fullmatch(r'\s*\d{2,3}\s*(?:سم|سانتي|سنتيمتر)[ .!]*', answer_text)
            elif key == 'lining':
                answered = re.search(r'(?:بدون|بيه|بيها|توجد|تجي|مع|ما بيه|ما بيها)\s+بطان|(?:غير\s+)?مبطن', answer_text)
            else:
                answered = re.search(r'(?:بيه|بيها|بسحاب|بكبس|بازرار|يجي|تجي|من الوسط|من المنتصف).{0,15}(?:سحاب|كبس|ازرار)|(?:سحاب|كبس|ازرار).{0,15}(?:من الوسط|من المنتصف|من الخلف)', answer_text)
            if answered:
                pending.remove(key)
    return pending


def price_mismatch(text):
    clean = normalized(text)
    if not re.search(r'كاتبين|مكتوب|اعلان|الصوره|الصورة|عبالي', clean):
        return False
    return bool(re.search(r'(?:ليش|مو|عبالي|كاتبين|مكتوب).{0,35}(?:اعلان|سعر|ب\s*\d|\d)|(?:الاعلان|الصوره).{0,25}(?:سعر|غير|ارخص)', clean)
                and re.search(r'سعر|اعلان|كاتبين|مكتوب', clean))


def brief(ev, product=None):
    if ev.get('_post_order'):
        return ''
    text = normalized(ev.get('text'))
    if sales_strategy.stop_kind(text):
        return ''
    state = ev.get('_sales_context') or {}
    pending = state.get('open_details') or []
    parts = []
    if price_mismatch(text):
        parts.append('الزبون يعترض على اختلاف السعر الذي شاهده. أقر بالاختلاف واذكر سعر المنتج الحالي ووحدة البيع والمجموع؛ راجع إحالة الإعلان إن وجدت. إذا لا يمكن تفسير الفرق بمصدر حقيقي اطلب مراجعة الموظف. لا تكرر سؤال الوزن ولا تطلب الحجز قبل حسم الفرق.')
    if pending:
        parts.append('أسئلة قرار ما زالت بلا جواب واضح: ' + '، '.join(DETAILS[k][0] for k in pending if k in DETAILS)
                     + '. سؤال لاحق عن اللون أو إرسال قياس لا يلغيها. أجبها من حقائق القطعة أو أبقها لتأكيد الموظف؛ لا تعوضها بضمان قياس أو بدعوة الحجز. إذا تغير الموديل تحقق من صلة السؤال بالاختيار الحالي.')
    if re.search(r'غالي|ارخص|ميزاني|خصم|تخفيض', text):
        parts.append('اعترف بالتكلفة دون مجادلة. اشرح ميزة واحدة موثقة تخدم حاجة ذكرها الزبون، ثم بديل واحد يطابق القياس والميزانية إن توفر. الفرق في السعر لا يبرر تغيير العمر أو نوع القطعة. إذا لا يوجد بديل مناسب صرّح بذلك.')
    if re.search(r'اخاف|حقيقي|نفس الصور|ضمان|فحص', text):
        parts.append('الثقة تُبنى بدليل: أرسل الصورة الموجودة فعلاً أو اشرح سياسة الفحص الحالية وحدودها. لا تصف الصورة بأنها تصوير حقيقي إن لم تثبت البيانات ذلك، ولا تستخدم مديحاً عاماً أو قصص زبائن وتقييمات غير موجودة.')
    if re.search(r'محتار|الفرق|فرق|افضل|احسن|42.*44|44.*42', text):
        parts.append('سهّل المقارنة بخيارين على معيار ذكره العميل. عند الحيرة بين قياسين استخدم قياسات القطعة بالسنتيمتر أو جدولها؛ الوزن وحده لا يحسم قياساً دقيقاً. لا تقل كلاهما مضمون.')
    if not parts:
        return ''
    return '[قرار البيع الحالي — داخلي]\n' + '\n'.join(parts[:3])


def unresolved_purchase_questions(state, product):
    """Only missing, product-specific facts can hold a checkout for confirmation."""
    if not product:
        return []
    result = []
    for key in (state or {}).get('open_details') or []:
        if key not in DETAILS:
            continue
        question = {'length': 'طول القطعة؟', 'width': 'عرضها؟',
                    'lining': 'بيها بطانة؟', 'closure': 'نوع الإغلاق سحاب؟'}[key]
        _, gaps = sales_answers.product_fragments(question, product)
        if gaps:
            result.append(question)
    return result


def budget_alternatives(question, state, products, matched, delivery_fee):
    """At most two verified cheaper retail options, preserving explicit filters.

    Unknown age/weight charts, mixed baskets and ambiguous bundle units must go
    through normal clarification, never a confident recommendation.
    """
    if not matched or not re.search(r'ارخص|ميزاني|باقل', normalized(question)):
        return []
    evidence = [str(item.get('text') or '') for item in (state or {}).get('evidence', [])]
    sources = evidence + [question]
    if any(re.search(r'طفلين|طفلتين|اثنين اطفال|واحد.*والثاني|قياس.*وقياس', normalized(t)) for t in sources):
        return []
    filters = {}
    for text in sources:
        request = product_variants.requested(text)
        for key, values in request.items():
            if values:
                filters[key] = values
    filter_text = ' '.join('قياس ' + size for size in filters.get('sizes', [])) + ' ' + ' '.join(filters.get('colors', []))
    age_text = next((normalized(t) for t in reversed(sources) if re.search(r'عمر|سنوات|سنه|سنة|شهر|سنتين', normalized(t))), '')
    age = None
    if age_text:
        match = re.search(r'(\d+)\s*(?:سنوات|سن[ةه])', age_text)
        if not match or re.search(r'نص|نصف|شهر|سنتين', age_text):
            return []
        age = int(match[1])
    # A customer's weight alone does not establish garment size equivalence.
    if not filters.get('sizes') and any(re.search(r'وزن|كيلو|كغم', normalized(t)) for t in sources):
        return []
    budget = sales_answers.known_budget(question, state)
    budget_text = next((normalized(t) for t in reversed(sources) if sales_answers.known_budget(t) is not None), '')
    includes_delivery = bool(re.search(r'ويا التوصيل|مع التوصيل|شامل|كله', budget_text))
    try:
        count, current_price, _ = terms(matched)
    except (ValueError, TypeError):
        return []
    if count != 1:
        return []
    category = normalized(matched.get('category'))
    if not category:
        return []
    result = []
    for product in products or []:
        if product.get('product_id') == matched.get('product_id') or product_albums.stock_state(product) != 'available':
            continue
        if normalized(product.get('category')) != category:
            continue
        if any(check['supported'] is not True for check in product_variants.assess(product, filter_text)):
            continue
        sizes = product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))
        if age is not None and not any(lo <= age <= hi for lo, hi in sizes['years']):
            continue
        try:
            count, price, _ = terms(product)
            if count != 1 or price >= current_price:
                continue
            priced = quote([dict(product_id=product['product_id'], quantity=1)], [product], delivery_fee)
            total = priced['product_total'] + priced['delivery_fee']
            if budget is not None and (total if includes_delivery else price) > budget:
                continue
        except (ValueError, TypeError, KeyError):
            continue
        result.append(dict(product_id=product['product_id'], product_name=customer_copy.fact(product.get('product_name')),
                           price=price, delivery_fee=priced['delivery_fee'], total=total,
                           budget_scope='with_delivery' if includes_delivery else 'product_only_or_unspecified'))
    return sorted(result, key=lambda item: (item['total'], item['product_id']))[:2]
