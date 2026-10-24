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
    'length': ('طول القطعة', r'طول(?:ه|ها)|الطول|طول\s+(?:القطعة|الموديل|الفستان|القميص|البنطرون|البنطلون|الكيلوت|اكيلوت|التنورة|العباءة)'),
    'width': ('عرض القطعة', r'عرض(?:ه|ها)|(?<!\w)و?العرض(?!\w)|عرض\s+(?:القطعة|الصدر)|تحت الابط|محيط الصدر'),
    'lining': ('البطانة', r'بطان|مبطن'),
    'closure': ('الإغلاق', r'سحاب|كبس|ازرار|زرار|ينفتح'),
}


def advance_details(pending, direction, text):
    pending = [key for key in pending or [] if key in DETAILS]
    clean = normalized(text)
    if direction == 'incoming':
        if re.search(r'غير\s+(?:الموديل|الفستان|القطعة)|ما اريد هذا|مو هذا الموديل|بدلي الموديل', clean):
            pending = []
        waived = set()
        for clause in re.split(r'[،,.؟?\n]+|\s+(?:بس|لكن)\s+', clean):
            if re.search(r'ما\s*(?:احتاج|اريد)\s*(?:اعرف|تتاكد)|مو مهم|ما يهمني', clause):
                waived.update(k for k in DETAILS if re.search(DETAILS[k][1], clause))
        pending = [key for key in pending if key not in waived]
        for key, (_, pattern) in DETAILS.items():
            if key in waived:
                continue
            # A body-height declaration must not erase a different question
            # in the same turn ("طولي 160، بيها بطانة؟").
            if key == 'length' and re.search(r'طولي|طولج|طولك|طول البنت', clean) and not re.search(
                    r'طول(?:ه|ها)|الطول|طول\s+(?:القطعة|الموديل|الفستان|القميص|البنطرون|البنطلون|التنورة|العباءة)', clean):
                continue
            if re.search(pattern, clean):
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
                answered = re.search(r'(?:بدون|بيه|بيها|توجد|تجي|مع|ما بيه|ما بيها)\s+بطان|(?:غير\s+)?مبطن|بطان[ةه].{0,10}(?:موجود|كامل|نصف|نص)', answer_text)
            else:
                answered = re.search(r'(?:بيه|بيها|يجي|تجي|الاغلاق|اغلاقه|اغلاقها|ينفتح).{0,16}(?:سحاب|كبس|ازرار)|(?<!\w)(?:بسحاب|بكبس|بازرار)(?!\w)|(?:سحاب|كبس|ازرار).{0,15}(?:من الوسط|من المنتصف|من الخلف|خلفي|امامي|جانبي)', answer_text)
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
    if sales_strategy.stop_kind(text) and not sales_strategy.soft_indecision(text):
        return ''
    state = ev.get('_sales_context') or {}
    pending = state.get('open_details') or []
    parts = []
    if sales_strategy.soft_indecision(text):
        parts.append('الزبون ما زال يقارن أو متردد، ولم يطلب مهلة صريحة. عالج السبب الذي ذكره؛ إن لم يذكر سبباً اسأل مرة واحدة بلطف عن القياس أو السعر. لا تعتبر الحيرة موافقة للحجز ولا تكرر الندرة.')
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
    evidence = [sales_answers.evidence_text(item) for item in (state or {}).get('evidence', [])]
    sources = evidence + [question]
    if any(re.search(r'طفلين|طفلتين|اثنين اطفال|واحد.*والثاني|قياس.*وقياس', normalized(t)) for t in sources):
        return []
    filters = {}
    rejected_colors = set()
    unresolved_size = False
    for text in sources:
        request = product_variants.requested(text)
        clean = normalized(text)
        negative_sizes = []
        for match in re.finditer(r'(?:قياس|مقاس|سايز)\s*(\d{1,3}|[2-6]?xl|xxl|xxxl|xs|[sml])(?!\w)', clean):
            before, after = clean[max(0, match.start() - 20):match.start()], clean[match.end():match.end() + 20]
            if (re.search(r'(?:مو|لا|ما)\s*(?:اريد(?:ه|ها)?\s*)?$', before)
                    or re.match(r'\s+ما\s*اريد(?:ه|ها)?(?:\s|$)', after)):
                negative_sizes.append(match[1].upper())
        request['sizes'] = [size for size in request['sizes'] if size not in negative_sizes]
        if negative_sizes and not request['sizes']:
            filters.pop('sizes', None)
            unresolved_size = True
        elif request['sizes']:
            unresolved_size = False
        declined_colors = set(product_variants.colors(text)) - set(request['colors'])
        rejected_colors.update(declined_colors)
        rejected_colors.difference_update(request['colors'])
        if declined_colors and filters.get('colors'):
            filters['colors'] = [color for color in filters['colors'] if color not in declined_colors]
        for key, values in request.items():
            if values:
                filters[key] = values
    if unresolved_size:
        return []
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
        available_colors = product_variants.colors(customer_copy.fact(product.get('colors'), 'colors'))
        if rejected_colors and (not available_colors or not set(available_colors) - rejected_colors):
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
        if rejected_colors:
            result[-1]['available_colors'] = [color for color in available_colors if color not in rejected_colors]
    linked = matched.get('related_product_ids')
    linked = [value for value in linked if isinstance(value, str)] if isinstance(linked, list) else []
    priority = {value: index for index, value in reversed(list(enumerate(linked)))}
    # A manual relation ranks only candidates that already passed every fit,
    # category, availability, price and delivery-budget check above.
    return sorted(result, key=lambda item: (item['product_id'] not in priority,
                  priority.get(item['product_id'], len(priority)), item['total'], item['product_id']))[:2]


def verified_budget_reply(question, state, products, matched, delivery_fee, *, fee_estimated=False):
    """Offer one verified alternative without changing the customer's selection.

    Run before single-model price guards: its price belongs to the named
    alternative, not to the currently bound product. Photos await consent.
    Documented compound details are answered before the alternative. Buying
    instructions and missing details keep the normal answer pipeline.
    """
    text = normalized(question)
    price_objection = bool(re.search(r'غالي|غاليه|غالية', text))
    if (not matched or not re.search(r'ارخص|باقل|ميزاني|غالي', text)
            or sales_strategy.stop_kind(question)
            or re.search(r'ثبت|احجز|اطلب|اشتري|فيديو|سحاب|اغلاق|كبس|ازرار|طول|عرض|متى|شوكت|فحص|دفع', text)
            or any((state or {}).get(key) for key in ('open_details', 'purchase_conditions'))):
        return None
    facts, gaps = sales_answers.product_fragments(question, matched)
    if gaps:
        return None  # Do not bury an unanswered detail under an alternative.
    alternatives = budget_alternatives(question + ('\nاريد ارخص' if price_objection else ''), state, products, matched, delivery_fee)
    if not alternatives:
        return None
    alternative = alternatives[0]
    product = next((item for item in products if item.get('product_id') == alternative['product_id']), None)
    if not product:
        return None
    name = customer_copy.fact(product.get('product_name')).strip()
    if not name or customer_copy.has_instructions(name):
        return None
    budget = sales_answers.known_budget(question, state)
    intro = ('أفهمج عيني، يهمج المجموع يكون مناسب؛ عدنا خيار أوفر ضمن ميزانيتج'
             if budget is not None and alternative['budget_scope'] == 'with_delivery'
             else 'أفهمج عيني، عدنا خيار أوفر')
    reply = (f"{intro}: {name} بسعر {alternative['price']:,} د.ع، "
             f"التوصيل {alternative['delivery_fee']:,} د.ع؛ المجموع {alternative['total']:,} د.ع.")
    if facts and (price_objection or set(sales_answers.topics(question)) - {'price'}):
        reply = '\n'.join(facts) + '\n' + reply
    if fee_estimated:
        reply += ' حسب أعلى أجرة توصيل؛ نحدد الأجرة لمحافظتج قبل الحجز.'
    if product.get('image_url') or product.get('image_urls'):
        reply += ' تحبين تشوفين صورته؟'
    return dict(reply=reply, reply_parts=[reply], create_order=False, order={},
                requires_human=False, intent='browse', _browse_only=True,
                _local_reply=True, _suppress_product_images=True,
                _verified_alternative_ids=[alternative['product_id']])
