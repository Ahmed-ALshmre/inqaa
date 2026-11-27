"""Verified inputs and output checks for generated sales wording."""
import re

from .. import checkout, customer_copy, product_variants


SLOT = re.compile(r'\{\{([a-z_]+)\}\}')
RAW_MONEY = re.compile(
    r'\d[\d,.٬٫]*\s*(?:الف|الاف|دينار)|(?<!\w)(?:الفين|الفان)(?!\w)|'
    r'(?<!\w)(?:عشر(?:ين|ون|[هة])|[ثت]لاثين|اربعين|خمسين|ستين|سبعين|ثمانين|تسعين|'
    r'واحد|ثلاث|اربع|خمس|ست|سبع|ثمان|تسع|مي[هة]|مائ[هة])\s+(?:الف|الاف|دينار)(?!\w)'
)
FREE_SIZE_WORDS = re.compile(r'فري\s*سايز|free\s*size|(?:ال)?(?:قياس|مقاس)\s+(?:ال)?موحد')
WEIGHT_UNIT = re.compile(r'(?<![^\W\d])(?:كيلو\s*[غج]رام|كغم|كجم|kg)(?!\w)', re.IGNORECASE)
WEIGHT_RANGE = re.compile(r'(\d+(?:[.٫]\d+)?)\s*كيلو\s*(إلى|الى|لحد|لغاية|[-–—])\s*(\d+(?:[.٫]\d+)?)\s*كيلو(?!\w)')


def natural_weight_wording(text):
    """Use the spoken unit once per range, without changing the numbers."""
    text = WEIGHT_UNIT.sub(lambda match: (' ' if match.start() and text[match.start() - 1].isdigit() else '') + 'كيلو', text)
    text = WEIGHT_RANGE.sub(r'\1 \2 \3 كيلو', text)
    text = re.sub(r'((?:قماش(?:ه|ها)?|القماش)(?:\s+(?:ال)?(?:تنور[ةه]|فستان|قميص|بنطرون|بنطلون))?)'
                  r'\s+(?:ال)?خام(?:ة)?\s+', r'\1 ', text)
    text = re.sub(r'(?<!\w)(النمر|نمر|نمرة|نمره)(?=\s*(?:\d|من|متوفر))',
                  lambda match: 'القياسات' if match[1] == 'النمر' else 'قياسات' if match[1] == 'نمر' else 'قياس', text)
    return re.sub(r'(?<!\w)كيلو(?:[ \t]+كيلو)+(?!\w)', 'كيلو', text)


def mentions_free_size(text):
    return bool(FREE_SIZE_WORDS.search(checkout.normalized(text)))


def quote_amounts(quote, compact=False):
    discount = quote.get('discount', 0)
    total = quote.get('total')
    fee = quote.get('delivery_fee')
    if 'total' not in quote and fee is not None:
        total = quote['product_total'] + fee
    if compact and total is not None:
        return {'total': total, **({'discount': discount} if discount else {})}
    amounts = {'subtotal': quote.get('base_product_total', quote['product_total'])}
    if fee is not None:
        amounts['delivery_fee'] = fee
    if discount:
        amounts['discount'] = discount
    if total is not None:
        amounts['total'] = total
    elif discount:
        amounts['discounted_subtotal'] = quote['product_total']
    return amounts


def render_amounts(parts, amounts):
    """Substitute the server's figures into model-authored prose."""
    seen = []
    rendered = []
    for part in parts:
        if part.get('type') != 'text':
            rendered.append(part)
            continue
        text = part['text']
        if amounts and RAW_MONEY.search(checkout.normalized(text)):
            raise ValueError('reply.money: use the approved amount tokens instead of writing numbers')

        def replace(match):
            name = match[1]
            if name not in amounts or name in seen:
                raise ValueError('reply.money: unknown or repeated amount token')
            seen.append(name)
            amount = amounts[name]
            if name == 'delivery_fee' and amount == 0:
                return 'مجاناً'
            return 'ألفين' if amount == 2000 else customer_copy.chat_amount(amount)

        has_free_fee = amounts.get('delivery_fee') == 0 and '{{delivery_fee}}' in text
        text = SLOT.sub(replace, text)
        if has_free_fee:
            free = r'(?:مجان(?:اً|ًا|ا)|مجاني[هة]?)'
            text = re.sub(rf'(?<!\w)({free})\s+{free}(?!\w)', r'\1', text)
        if '{{' in text or '}}' in text:
            raise ValueError('reply.money: malformed amount token')
        rendered.append(dict(part, text=text))
    if set(seen) != set(amounts):
        raise ValueError('reply.money: include every required amount token exactly once')
    return rendered


def next_options(product, question):
    if not product or not question:
        return []
    if question.endswith('.color'):
        return product_variants.colors(product.get('colors'))[:2]
    if question.endswith('.size') and product.get('selection_mode') not in {'age', 'weight'}:
        sizes = product_variants.size_data(product.get('sizes'))
        options = list(dict.fromkeys(sizes['labels'] + [str(value) for value in sizes['numbers']]))
        # A forced choice is only accurate when it covers the catalog.
        if not sizes['ranges'] and not sizes['weights'] and not sizes['years'] and len(options) <= 2:
            return options
    return []


def validate_next_step(parts, step):
    """Check the planned request in a question or a natural Iraqi imperative."""
    question = step['field']
    if not question:
        return
    texts = [part['text'] for part in parts if part.get('type') == 'text']
    marks = sum(len(re.findall(r'[؟?]+', text)) for text in texts)
    if not texts or marks > 1 or marks and not re.search(r'[؟?]+[^\w؟?]*$', texts[-1]):
        raise ValueError('reply.next_step: end with one request about the planned field')
    last = checkout.normalized(re.split(r'[؟?]', texts[-1])[0]).strip().rstrip('.!')
    last = re.split(r'[.!\n]', last)[-1]
    if not marks and not re.search(
            r'دز|ارسل|نط|عط|قولي|[گك]ولي|خبر|باقي|اختار|تختار|حدد|تحدد|شنو|شكد|وين|'
            r'يا\s*لون|اي\s*(?:لون|قياس|محافظ)|تحب|تريد|يناسب|نمشي|نكمل|موافق', last):
        raise ValueError('reply.next_step: make the next request clear')
    required = {
        'contact.phone': (r'رقم|موبايل|تلفون|هاتف',),
        'contact.address': (r'عنوان|منطق|وين.*(?:اوصل|نوصل|توصيل)',),
        'contact.details': (r'رقم|موبايل|تلفون|هاتف', r'عنوان|منطق'),
        'contact.province': (r'محافظ|مدين',),
        'quote.province': (r'محافظ|مدين',),
        'price.accept': (r'سعر|حساب|مجموع|عرض|يناسب|موافق|نكم[لّ]|نمشي|اوك[ي]?',),
        'offer.quantity': (r'عرض|بكج|باكج|قطعت|ثنين|اثنين|تنوع|قطع',),
        'purchase_intent': (r'ثبت|حجز|نسجل|اكمل|نكمل|طلب|رتب',),
        'alternatives.photos': (r'صور|اشوف|تشوف|ادز|ارسل', r'بديل|بدائل|موديل|خيارات|سوتات|اطقم'),
        'catalog.show_option': (r'صور|اشوف|تشوف|ادز|ارسل',),
        'product': (r'صور|سكرين|لقط',),
        'photo.reference': (r'صور|سكرين|لقط|قطع[هة]|موديل|لون|تقصد',),
        'inventory.alternative': (r'بديل|(?:لون|قياس|موديل)\s+(?:ثاني|اخر|غير)|تحب|تريد|يناسب',),
    }
    field = question.rsplit('.', 1)[-1]
    if field == 'color' and question not in required:
        if not re.search(r'لون', last) and not set(step['options']).intersection(product_variants.colors(last)):
            raise ValueError('reply.next_step: ask about the color or its verified options')
    if field == 'size' and not re.search(r'قياس|مقاس|سايز', last):
        if not any(re.search(r'(?<!\w)' + re.escape(checkout.normalized(option)) + r'(?!\w)', last)
                   for option in step['options']):
            raise ValueError('reply.next_step: ask about the size or its verified options')
    if field == 'weight_kg' and not re.search(r'وزن|كيلو|كغم', last):
        raise ValueError('reply.next_step: ask about the weight for this model')
    if not all(re.search(pattern, last) for pattern in required.get(question, ())):
        raise ValueError('reply.next_step: ask for ' + question + ' in the final question')


def checkout_question(field, gender, *, final_detail):
    welcome = 'حبيبتي' if gender == 'female' else 'عيني'
    labels = {'contact.phone': 'رقم الموبايل', 'contact.address': 'العنوان بالتفصيل',
              'contact.province': 'المحافظة', 'contact.details': 'رقم الموبايل والعنوان بالتفصيل'}
    request = 'باقي بس' if final_detail else 'دزّيلي' if gender == 'female' else 'دزلي'
    return f'{welcome}، {request} {labels[field]} حتى أثبتلج الحجز مباشرة 🌷' if gender == 'female' else f'{welcome}، {request} {labels[field]} حتى أثبتلك الحجز مباشرة 🌷'


def fact_recovery_parts(context):
    """Render independently verifiable answers when both writing attempts fail."""
    from .. import sales_answers, variant_inventory

    if context['mode'] != 'continue' or context.get('provisional_photo'):
        return None
    question = context['question_field']
    photo = context.get('photo_clarification')
    if photo:
        matched_products = photo.get('matched_products') or []
        if len(matched_products) >= 1:
            items = matched_products[:4]
            avail_items = [item for item in items if item.get('stock') == 'available']
            out_lines = []
            for item in items:
                status = 'متوفر' if item.get('stock') == 'available' else 'للأسف مخلص حالياً'
                price_part = f" ({item['price_summary']})" if item.get('price_summary') and item.get('stock') == 'available' else ''
                out_lines.append(f"{item['name']}: {status}{price_part}.")
            if not avail_items:
                closing = 'للأسف هالقطع مخلصة حالياً، تحبين أقترحلج موديلات ثانية متوفرة بنفس العمر أو القياس؟'
            elif len(avail_items) == 1:
                closing = f"تحبين نكمّل على {avail_items[0]['name']} المتوفر؟"
            else:
                closing = 'أي واحد منهم عجبج أكثر أو تحبين تاخذيهم سوية بنفس التوصيل؟'
            if photo.get('per_model_photos') and len(items) >= 2:
                parts = [dict(type='text', text=line) for line in out_lines]
                if len(parts) < 5:
                    parts.append(dict(type='text', text=closing))
                return parts
            out_lines.append(closing)
            return [dict(type='text', text='\n\n'.join(out_lines))]
        if photo.get('possible_product'):
            text = f"تقصدين {photo['possible_product']}؟"
        elif photo.get('multiple'):
            text = 'وصلتني الصور عيوني، يا قطعة منهم تقصدين بالضبط؟'
        elif photo.get('unreadable'):
            text = 'ممكن اسم القطعة أو رابطها حتى ألكالج الموديل؟'
        else:
            text = 'وصلتني الصورة عيوني، بس گليلي اسم الموديل أو وصفه حتى أتأكدلج منه؟'
        parts = [dict(type='text', text=text)]
        if photo.get('quantity_quote') and 'delivery_fee' in context['amounts']:
            parts.insert(0, dict(type='text', text='التوصيل {{delivery_fee}}.'))
        return parts

    requested = set(context.get('requested_fields', ()))
    facts = {fact['field']: fact['value'] for fact in context.get('facts', ())}
    progress = context.get('progress', {})
    amounts = context['amounts']
    lines, covered = [], set()
    labels = {
        'fabric': 'قماشه', 'length': 'طوله', 'width': 'عرضه',
        'sizes': 'القياسات', 'colors': 'الألوان', 'delivery_time': 'التوصيل عادةً',
        'shop_address': 'عنوان المحل',
    }
    plain_facts = {'payment', 'inspection_policy', 'stretch', 'cut', 'lining', 'closure', 'care',
                   'season', 'fabric_feel', 'composition', 'origin'}
    fit = progress.get('fit')
    fit_stock = context.get('value_facts', {}).get('fit_availability', {}).get('stock')
    message = context['customer_message']
    if fit and ('sizes' in requested or sales_answers.fit_question(message)):
        if not fit['within_catalog_range']:
            lines.append('حبيبتي هالقياس للأسف ما يرهمج بهالموديل، بس تدللين نكدر نشوفلج غيره يناسبج.')
        elif fit_stock == 'available' and not context.get('variant_stock_feedback'):
            lines.append('إي عيني، القياس اللي يرهمج متوفر ويطلع كلش مرتب عليج.')
        else:
            lines.append('وزنج ضمن القياس اللي يرهم بهالموديل عيوني.')
        covered.add('sizes')
    feedback = context.get('business_feedback') or {}
    inventory = context.get('variant_stock')
    selected = product_variants.requested(message)
    selected_color = selected['colors'][0] if len(selected['colors']) == 1 else ''
    selected_size = selected['sizes'][0] if len(selected['sizes']) == 1 else ''
    if progress.get('selection_recorded') and selected_color and not feedback and not context.get('stock_conflict'):
        if not requested:
            lines.append(f'عاش ذوقج عيوني، اختيار اللون {selected_color} كلش حلو ومرتب.')
    if feedback and feedback.get('field') in {'color', 'size'} and feedback.get('requested'):
        selected_value = customer_copy.fact(feedback['requested'])
        if selected_value:
            label = 'اللون' if feedback['field'] == 'color' else 'القياس'
            lines.append(f'حبيبتي {label} {selected_value} للأسف مخلص بهالموديل حالياً.')
            covered.add('colors' if feedback['field'] == 'color' else 'sizes')
    if ('availability' in sales_answers.topics(message) and selected_color and selected_size
            and not context.get('stock_conflict') and {'colors', 'sizes'}.issubset(requested)):
        known = inventory is not None or context.get('catalog_stock') == 'out'
        matching = False
        if inventory:
            matching = any(variant_inventory.color_key(row['color']) == variant_inventory.color_key(selected_color)
                           and row['size'].casefold() == selected_size.casefold() for row in inventory['available'])
        elif context.get('catalog_stock') == 'available':
            colors = product_variants.colors(facts.get('colors'))
            sizes = product_variants.size_data(facts.get('sizes'))
            known = bool(colors and ((sizes['numbers'] or sizes['ranges']) if selected_size.isdigit() else sizes['labels']))
            matching = known and selected_color in colors and not product_variants.order_choice_error(
                {'sizes': facts['sizes']}, 'size', selected_size)
        if known:
            status = 'موجود' if matching and not feedback else 'ما متوفر'
            lines.append(f'{selected_color} بقياس {selected_size} {status}.')
            covered.update({'colors', 'sizes'})
    for field in context.get('requested_fields', ()):
        value = facts.get(field)
        if field in covered or not isinstance(value, str) or len(value) > 215 or field == 'sizes' and value == 'none':
            continue
        if field in labels:
            lines.append(f'{labels[field]} {value}.')
        elif field in plain_facts:
            lines.append(value.rstrip('.') + '.')
        else:
            continue
        covered.add(field)

    if amounts:
        price_unit = progress.get('price_unit_quantity') or 1
        piece_label = 'القطعة' if price_unit == 1 else f'البكج ({price_unit} قطع)'
        offer_qty = context.get('promotion_quantity')
        price_lines = {
            'unit_price': f'سعر {piece_label} {{{{unit_price}}}} عيوني، والدفع عند الاستلام بعد الفحص كدام المندوب.',
            'offer_total': (f'وعليه عرض خاص {offer_qty} قطع بـ{{{{offer_total}}}} بنفس التوصيل.'
                            if offer_qty != 2 else 'وعليه عرض خاص القطعتين بـ{{offer_total}} بنفس التوصيل، يطلع عليج أوفر هواية.'),
            'subtotal': 'سعر القطع {{subtotal}} عيوني.',
            'discount': 'والخصم {{discount}}.',
            'delivery_fee': 'التوصيل {{delivery_fee}} لكل المحافظات مع حق الفحص كدام المندوب قبل الدفع.',
            'total': 'المجموع الكلي ويا التوصيل {{total}} والدفع عند الاستلام.',
            'discounted_subtotal': 'سعر القطع بعد الخصم {{discounted_subtotal}}.',
            'alternative_price': 'وسعر البديل {{alternative_price}}.',
        }
        if amounts.keys() - price_lines.keys():
            return None
        lines.extend(price_lines[field] for field in price_lines if field in amounts)
        if amounts.keys() - {'delivery_fee'}:
            covered.add('price')
        if 'delivery_fee' in amounts:
            covered.add('delivery_fee')

    asks = {
        'product': 'حبيبتي دزّيلي صورة الموديل اللي عجبج حتى أشوفلج تفاصيله وتوفره فوراً؟',
        'quote.province': 'لأي محافظة التوصيل عيوني؟',
        'price.accept': 'ها عيوني، يناسبج هالحساب حتى نكمّل الحجز؟',
        'offer.quantity': 'تحبين تاخذين قطعة وحدة لو تستفادين من عرض القطعتين بنفس التوصيل؟',
        'purchase_intent': 'تحبين نثبتلج الحجز هسه قبل ما يخلص القياس؟',
        'alternatives.photos': 'تحبين أدزلج صور موديلات بديلة متوفرة تعجبج؟',
        'catalog.show_option': 'تحبين أدزلج صورته تشوفين شكله وتفاصيله؟',
        'inventory.alternative': 'تحبين أشوفلج لون أو قياس ثاني متوفر يناسبج؟',
        'alternative.color': 'شنو اللون اللي ببالج حتى أشوفلج خيار قريب؟',
    }
    by_field = {'color': 'يا لون عجبج أكثر حتى نعتمدلج إياه؟', 'size': 'شنو القياس اللي يناسبج عيوني؟',
                'weight_kg': 'شكد الوزن تقريباً حتى أختارلج القياس المضبوط؟',
                'age_months': 'فدوة شكد عمر الطفل حتى أشوفلج القياس المضبوط إله؟'}
    if question and question.startswith('contact.'):
        ask = checkout_question(question, 'female', final_detail=question != 'contact.details')
    else:
        ask = asks.get(question) or by_field.get((question or '').rsplit('.', 1)[-1])
        if not ask and (question or '').startswith('alternative.') and feedback:
            ask = 'تحبين أراويج موديل ثاني متوفر يناسبج؟'
    unanswered = requested - covered
    if question == 'quote.province':
        unanswered.discard('delivery_fee')
    if question == 'product' and progress.get('product_unresolved'):
        unanswered.intersection_update({'inspection_policy', 'delivery_time', 'delivery_fee', 'payment', 'shop_address'})
    if unanswered or question and not ask or not (lines or ask and (
            requested or context.get('verified_photo_product_id') or progress.get('contact_turn'))):
        return None
    if ask:
        lines.append(ask)
    # Keep the same layout and amount checks as normal output.
    return [dict(type='text', text='\n\n'.join(lines))]
