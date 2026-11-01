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


def answer(question, product, context, customer, history, delivery):
    if not product or explicit_purchase(question) or sales_strategy.explicit_pause(question):
        return None
    text = customer_copy.normalized(question)
    selected = set(sales_answers.topics(question))
    supported = {'fabric','fabric_feel','season','stretch','cut','lining','closure','care',
                 'colors','sizes','availability','price','delivery','payment','inspection','delivery_time'}
    if not selected or selected - supported or product_albums.stock_state(product) != 'available':
        return None
    if re.search(r'ارخص|بديل|غير موديل|باجر|باچر|اذا|بشرط|صور[ةه]?.{0,15}(?:ثاني|اكثر)', text):
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
        parts.append(greeting + 'أهلا وسهلا عيوني، ' + '، '.join(dict.fromkeys(filter(None,facts))) + '.')
    ages = product_variants.child_ages(question)
    if not ages:
        age=product_variants.requested_age(product,question)
        if age is not None:
            ages=[age]
    requested = product_variants.requested(question)
    sizes = requested.get('sizes') or []
    if selected & {'sizes','availability'} and not ('colors' in selected and not ages and not sizes):
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
    if selected & {'price','delivery'}:
        fee = delivery.get('baghdad_fee') if customer.get('province')=='بغداد' or 'بغداد' in text else delivery.get('other_fee')
        quantity = sales_playbook.requested_quantity(question) or 1
        priced = (quote([dict(product_id=product['product_id'],quantity=quantity)], [product],fee or 0)
                  if price is not None else dict(delivery_fee=fee or 0,product_total=0))
        if delivery.get('baghdad_fee')==delivery.get('other_fee') or customer.get('province') or 'بغداد' in text:
            shipping = 'التوصيل ' + customer_copy.chat_amount(priced['delivery_fee']) + '.'
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
    step = sales_playbook.price_next_step(product, context, customer, history)
    known_colors = product_variants.chosen_colors(question)
    if len(requested.get('colors') or [])==1 and not re.search(r'(?<!\w)(?:لو|او)(?!\w)',text):
        known_colors=requested['colors']
    if ages and re.search(r'عمر',step):
        step = 'يا لون عجبج؟' if len(colors)>1 and not known_colors else 'تحبين أحجزلج؟'
    if 'colors' in selected:
        parts.append('إي حبيبتي تكدرين تختارين الألوان اللي تعجبج.' if re.search(r'اختار|مختلف|الوان مختلف',text)
                     else 'إي، ال'+known_colors[0]+' موجود.' if len(known_colors)==1
                     else 'متوفر بال' + ' وال'.join(colors) + '.')
        step = 'يا لون عجبج؟' if not known_colors else step
    if known_colors and re.search(r'(?:يا|شنو|اي|أي).*لون',step):
        step = 'تحبين أحجزلج؟'
    if selected=={'price'} and not parts:
        return None
    if not parts:
        return None
    cue = sales_strategy.persuasion_cue(product,history)
    if cue and (cue.get('kind')=='limited_stock' or re.search(r'الكمية\s+محدودة',customer_copy.normalized(cue.get('text')))) and not any(re.search(r'كمية|قطع.{0,10}قليل',str(t.get('text') or '')) for t in history if t.get('direction')=='outgoing'):
        parts.append('عليه عرض هالفترة، والكمية الباقية قليلة.' if product.get('offer') else 'والكمية الباقية قليلة.')
    if step:
        parts.append(step.replace('شكد العمر المطلوب؟','شكد العمر حتى أتأكدلج من المتوفر؟'))
    reply = '\n\n'.join(parts)
    show_colors = bool(len(colors)>1 and not known_colors and ('colors' in selected or re.search(r'(?:يا|شنو|اي|أي).*لون',step)))
    return dict(reply=reply,reply_parts=parts,create_order=False,order={},requires_human=False,
                _local_reply=True,_human_sales_voice=True,_show_color_options=show_colors,
                image_product_ids=[product['product_id']] if show_colors else [],
                _suppress_product_images=not show_colors)
