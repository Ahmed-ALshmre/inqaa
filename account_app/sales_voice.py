"""Short Iraqi sales answers from verified facts, with one relevant next step."""
import re
try:
    from . import customer_copy, product_variants, product_albums, sales_answers, sales_playbook, sales_strategy
    from .checkout import explicit_purchase
    from .pricing import terms, offer_tiers, quote
except ImportError:
    import customer_copy, product_variants, product_albums, sales_answers, sales_playbook, sales_strategy
    from checkout import explicit_purchase
    from pricing import terms, offer_tiers, quote


def conversation_wording(reply, history):
    """A greeting belongs to the first seller turn, never each factual answer."""
    if not any(item.get('direction') == 'outgoing' and item.get('text') for item in history):
        return reply
    return re.sub(r'(?m)^\s*(?:(?:أهلا|أهلاً|اهلا|اهلاً)\s+وسهلا[ً]?|أهلاً\s+وسهلاً|اهلا\s+وسهلاً|يا\s+هلا|مرحبا|مرحباً)'
                  r'(?:\s+(?:بيج|بيك|حبيبتي|عمري|عيوني|عيني|روحي))*[،,.!🌷🌸\s]*', '', reply).strip()


def phone_first_step(reply, question):
    """On a phone-only turn, settle the garment choice before a second address question."""
    if not re.fullmatch(r'[+\d() \-]{10,20}',str(question or '').strip()):
        return reply
    first = re.fullmatch(r'(.{0,250}?(?:قياس|عمر|لون)[^؟?]{0,80}[؟?])\s*'
                         r'و?(?:بأي|بأيّ|بأي[ةه]|من (?:يا|أي)|شنو|وين|يا|دزيلي|دزي|ارسلي|أرسلي)\s+'
                         r'(?:ال)?(?:محافظ[ةه]|عنوان)[^؟?.!]{0,80}[؟?.!]?$', str(reply or '').strip(), re.S)
    return first[1] if first else reply


def buying_choice(question, product):
    """A definite available variant can invite contacts, never create an order."""
    text=customer_copy.normalized(question)
    if (not re.search(r'(?<!\w)(?:اريد|اخذ)(?!\w)',text)
            or re.search(r'[؟?]|شكد|بكم|بيش|بعدني|بس\s+اسال|ما\s+اريد|ماريد|صحح|بدل|(?<!\w)(?:لو|اذا)(?!\w)',text)
            or product_albums.stock_state(product)!='available'):
        return None
    requested=product_variants.requested(question)
    ages=product_variants.child_ages(question)
    sizes=requested['sizes'] or ([product_variants.age_text(ages[0])] if len(ages)==1 else [])
    if product.get('size_system')=='none':sizes=['']
    colors=requested['colors'] or product_variants.colors(product.get('colors'))
    if len(sizes)!=1 or len(colors)!=1:
        return None
    if product_variants.order_choice_error(product,'size',sizes[0]) or product_variants.order_choice_error(product,'color',colors[0]):
        return None
    return dict(items=[dict(product_id=product['product_id'],product_name=product.get('product_name'),
                            color=colors[0],size=sizes[0],size_type='none' if product.get('size_system')=='none' else 'size',
                            quantity=sales_playbook.requested_quantity(question) or 1)])


def child_detail_reply(question, product, inspection_policy):
    """Two owner-approved answers for child fabrics and measuring trousers at delivery."""
    text = customer_copy.normalized(question)
    if not product_variants.size_data(product.get('sizes'))['years']:
        return None
    topics = set(sales_answers.topics(question))
    if (topics == {'length'} and re.search(r'طول (?:البنطرون|البنطلون)',text)
            and not sales_strategy.measurement_fact(product,'length')
            and re.search(r'حق الفحص|الفحص عند الاستلام',inspection_policy or '')
            and not re.search(r'ممنوع|لا يسمح|ما يصير',inspection_policy or '')):
        return 'حبيبتي ما عندي طوله، تكدرين تتأكدين من الطول بوجود المندوب قبل الدفع.'
    if (topics <= {'fabric','stretch'} and 'stretch' in topics and re.search(r'قطن.{0,6}(?:100|مية بالمية)',text)
            and not re.search(r'امن|آمن|يناسب الحساسية|تنصح|يضر|اكزيما', text)):
        stretch = sales_answers.field_fact(product,'stretch')
        fabric = sales_answers.field_fact(product,'fabric')
        if stretch and not re.search(r'100|مية بالمية|خالص',fabric or ''):
            return stretch.rstrip(' .') + '، بس قطن مية بالمية ما أگدر أضمنلج.'
    return None


def mixed_child_offer(question, product):
    """Answer the offer for two selected colors and ages without restarting selection."""
    text = customer_copy.normalized(question)
    if not product or not re.search(r'(?:يصير|اكدر|اگدر) (?:اخلطهن|اجمعهن) بالعرض[؟?!. ]*$', text):
        return None
    colors = product_variants.requested(question)['colors']
    ages = product_variants.child_ages(question)
    if len(colors) != 2 or not ages or product_albums.stock_state(product) != 'available':
        return None
    if any(product_variants.order_choice_error(product,'color',c) for c in colors):
        return None
    if any(product_variants.order_choice_error(product,'size',product_variants.age_text(age)) for age in ages):
        return None
    terms_text = ' '.join(str(product.get(key) or '') for key in ('price','offer','notes','bundle_contents'))
    if re.search(r'نفس اللون|لون واحد|بشرط|حصرا|فقط للون', customer_copy.normalized(terms_text)):
        return None
    try:
        total = dict(offer_tiers(product)).get(2)
    except (ValueError, TypeError):
        return None
    if total:
        reply = 'إي حبيبتي، يصير تاخذين اللونين بالعرض، القطعتين بـ' + customer_copy.chat_amount(total) + '.'
        return dict(reply=reply,reply_parts=[reply],create_order=False,order={},_local_reply=True,
                    _mixed_child_offer=True,_single_message=True,_suppress_product_images=True)
    return None


def remembered_age_availability(question, product, context, customer=None):
    if (not product or set(sales_answers.topics(question)) != {'availability'}
            or product_variants.child_ages(question) or explicit_purchase(question)):
        return None
    bounds = product_variants.size_data(product.get('sizes'))['years']
    selected = sales_playbook.customer_selection(context, customer)
    age = product_variants.child_age(selected.get('size') or '')
    age = age if age is not None else selected.get('age')
    if not bounds or age is None or any(lo <= age <= hi for lo, hi in bounds):
        return None
    reply = 'لعمر ' + product_variants.age_text(age) + ' مو موجود بهالموديل، المتوفر ' + customer_copy.fact(product.get('sizes'), 'sizes') + '.'
    return dict(reply=reply, reply_parts=[reply], create_order=False, order={},
                requires_human=False, _local_reply=True, _browse_only=True, _contextual_stock_reply=True)


def answer(question, product, context, customer, history, delivery):
    comparison = bool(re.search(r'(?:بس|فقط)\s+(?:اقارن|أقارن|استفسر|أستفسر|اسأل|أسأل)', question or ''))
    if not product or (explicit_purchase(question) and not comparison):
        return None
    paused=comparison or sales_strategy.explicit_pause(question)
    text = customer_copy.normalized(question)
    selected = set(sales_answers.topics(question))
    supported = {'fabric','fabric_feel','season','stretch','cut','lining','closure','care',
                 'colors','sizes','availability','price','delivery','payment','inspection','delivery_time'}
    if not selected or selected - supported or product_albums.stock_state(product) != 'available':
        return None
    if (not comparison and 'اذا' in text) or re.search(r'ارخص|بديل|غير موديل|باجر|باچر|بشرط|صور[ةه]?.{0,15}(?:ثاني|اكثر)', text):
        return None
    if re.search(r'ميزاني|غالي|خصم|تخفيض|غير\s+(?:اللون\s*|لون\s*)?(?:ال)?اسود|عدا|باستثناء',text):
        return None
    # Quantity cart instructions and comparisons belong to the full context flow.
    if re.search(r'الاول|الثاني|بدل|صحح|قصدي|اقصد',text) and not re.search(r'[؟?]|شكد|كم|متوفر',text):
        return None
    try:
        count, price, _ = terms(product)
    except (ValueError,TypeError,KeyError):
        if 'price' in selected:
            return None
        count,price=1,None
    if count != 1 or sales_answers.wholesale_unit(product):
        return None
    parts = []
    available_colors = product_variants.colors(product.get('colors'))
    if any(color not in available_colors for color in product_variants.requested(question).get('colors') or []):
        return None
    facts = []
    if selected & {'fabric','fabric_feel','season'}:
        fabric = sales_answers.fabric_feel_fact(product) or sales_answers.field_fact(product,'fabric')
        if fabric:
            fabric = re.sub(r'^(?:القماش|قماش(?:ه|ها)?)\s*[:：]?\s*','',fabric)
            facts.append('القماش ' + fabric)
        if 'season' in selected:
            season = sales_answers.season_fact(product)
            if season and not any(word in customer_copy.normalized(' '.join(facts)) for word in ('شتوي','صيفي','بهاري','خريفي')):
                facts.append(season)
    for key in ('stretch','cut','lining','closure'):
        if key in selected:
            value = sales_answers.field_fact(product,key)
            if value and value not in ' '.join(facts):
                facts.append(value)
    if 'care' in selected:
        facts.append(sales_answers.care_fact(product))
    if facts:
        greeting = 'وعليكم السلام، ' if re.search(r'(?:السلام|سلام)\s+عليكم',text) else ''
        opening = '' if any(item.get('direction') == 'outgoing' for item in history) else greeting + 'أهلا وسهلا عيوني، '
        parts.append(opening + '، '.join(dict.fromkeys(filter(None,facts))) + '.')
    ages = product_variants.child_ages(question)
    if not ages:
        age=product_variants.requested_age(product,question)
        if age is not None:
            ages=[age]
    requested = product_variants.requested(question)
    sizes = requested.get('sizes') or []
    if 'sizes' in selected or ('availability' in selected and 'colors' not in selected):
        if ages:
            if any(product_variants.order_choice_error(product,'size', product_variants.age_text(age)) for age in ages):
                return None
            parts.append('إي حبيبتي، الأعمار اللي طلبتيها متوفرة.' if len(ages)>1 else 'إي حبيبتي، العمر المطلوب متوفر.')
        elif sizes:
            if any(product_variants.order_choice_error(product,'size',str(size)) for size in sizes):
                return None
            parts.append('إي حبيبتي، قياسج موجود.')
        elif 'sizes' in selected:
            parts.append('القياسات المتوفرة ' + customer_copy.fact(product.get('sizes'),'sizes') + '.')
        else:
            parts.append('إي حبيبتي متوفر.')
    if 'price' in selected:
        offers = offer_tiers(product)
        line = ('حالياً عليه عرض، ' if offers or product.get('offer') else '') + 'سعر القطعة بـ ' + customer_copy.chat_amount(price)
        for qty, total in offers:
            label = 'القطعتين' if qty==2 else f'{qty} قطع'
            line += ' و' + label + ' بـ ' + customer_copy.chat_amount(total)
        parts.append(line + ' بس.')
    if 'delivery' in selected:
        province=delivery.get('requested_province') or customer.get('province')
        fee = delivery.get('requested_fee') if delivery.get('requested_fee') is not None else (
            delivery.get('baghdad_fee') if province=='بغداد' or 'بغداد' in text else delivery.get('other_fee'))
        quantity = sales_playbook.quote_quantity(question,context)
        priced = (quote([dict(product_id=product['product_id'],quantity=quantity)], [product],fee or 0)
                  if price is not None else dict(delivery_fee=fee or 0,product_total=0))
        if delivery.get('baghdad_fee')==delivery.get('other_fee') or province or 'بغداد' in text or priced['delivery_fee']==0:
            shipping = 'التوصيل'+(' إلى '+province if province else '')+' ' + customer_copy.chat_amount(priced['delivery_fee']) + '.'
        else:
            shipping = 'التوصيل لبغداد ' + customer_copy.chat_amount(delivery.get('baghdad_fee',0)) + ' وللمحافظات ' + customer_copy.chat_amount(delivery.get('other_fee',0)) + '.'
        if 'price' in selected:
            shipping += ' المجموع ' + customer_copy.chat_amount(priced['product_total']+priced['delivery_fee']) + '.'
        if 'price' in selected:
            parts[-1] += ' ' + shipping
        else:
            parts.append(shipping)
    if 'delivery_time' in selected:
        parts.append(str(delivery.get('delivery_time') or delivery.get('time') or 'التوصيل عادةً من يوم إلى يومين.') + '، والوقت تقديري.')
    if 'payment' in selected:
        parts.append('الدفع عند الاستلام.')
    if 'inspection' in selected:
        parts.append('وأكيد إلج حق الفحص بوجود المندوب قبل الدفع، وإذا بيها إشكال ترجع مجاناً بدون دفع حتى أجور التوصيل.')
    colors = product_variants.colors(product.get('colors'))
    step = '' if paused else sales_playbook.price_next_step(product, context, customer, history)
    known_colors = product_variants.chosen_colors(question)
    photo_request=bool(re.search(r'صور|شوفيلي\s+صور',text))
    if (len(ages)>1 and len(requested.get('colors') or [])>1
            and re.search(r'للاول|للثاني|للصغير|للكبير',text)
            and not re.search(r'(?<!\w)(?:لو|او)(?!\w)',text)):
        known_colors=requested['colors']
    if len(requested.get('colors') or [])==1 and not re.search(r'(?<!\w)(?:لو|او)(?!\w)',text):
        known_colors=requested['colors']
    if ages and re.search(r'عمر',step):
        step = 'يا لون عجبج؟' if len(colors)>1 and not known_colors else 'تحبين أحجزلج؟'
    if 'colors' in selected:
        parts.append('إي حبيبتي تكدرين تختارين الألوان اللي تعجبج.' if re.search(r'اختار|مختلف|الوان مختلف',text)
                     else 'إي، ال'+known_colors[0]+' موجود.' if len(known_colors)==1
                     else 'متوفر بال' + ' وال'.join(colors) + '.')
        step = 'يا لون عجبج؟' if len(colors) > 1 and not known_colors else step
    if known_colors and re.search(r'(?:يا|شنو|اي|أي).*لون',step):
        step = 'تحبين أحجزلج؟'
    if selected=={'price'} and not parts:
        return None
    if not parts:
        return None
    cue = sales_strategy.persuasion_cue(product,history)
    if not paused and selected & {'price', 'availability'} and cue and (cue.get('kind')=='limited_stock' or re.search(r'الكمية\s+محدودة',customer_copy.normalized(cue.get('text')))) and not any(re.search(r'كمية|قطع.{0,10}قليل',str(t.get('text') or '')) for t in history if t.get('direction')=='outgoing'):
        parts.append('عليه عرض هالفترة، والكمية الباقية قليلة.' if product.get('offer') and not any('عرض' in p for p in parts) else 'والكمية الباقية قليلة.')
    if step:
        parts.append(step.replace('شكد العمر المطلوب؟','شكد العمر حتى أتأكدلج من المتوفر؟'))
    # Related answers and their next step are one short message. Longer
    # answers still pass through the semantic splitter without losing facts.
    reply = ' '.join(parts)
    show_colors = bool(len(colors)>1 and (photo_request or not known_colors and ('colors' in selected or re.search(r'(?:يا|شنو|اي|أي).*لون',step))))
    return dict(reply=reply,reply_parts=[reply],create_order=False,order={},requires_human=False,
                _local_reply=True,_human_sales_voice=True,_show_color_options=show_colors,
                _image_option_colors=known_colors if photo_request else [],
                image_product_ids=[product['product_id']] if show_colors else [],
                _suppress_product_images=not show_colors)
