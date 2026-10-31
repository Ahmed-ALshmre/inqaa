"""Keep a customer's multiple color choices instead of asking for the same choice again."""
import re
try:
    from . import customer_copy, product_albums, product_variants, sales_playbook, sales_context
    from .checkout import is_confirmation, contact_fields
    from .pricing import terms
except ImportError:
    import customer_copy, product_albums, product_variants, sales_playbook, sales_context
    from checkout import is_confirmation, contact_fields
    from pricing import terms


def one_each(text, context):
    clean = customer_copy.normalized(text)
    if not re.search(r'[؟?]|(?<!\w)(?:لا|مو|ما|لو|او|بس)(?!\w)', clean) and re.search(
            r'(?:قطع[هة]|وحد[هة]|واحد[هة]|1)\s+من\s+كل\s+لون|كل\s+لون\s+(?:قطع[هة]|وحد[هة]|واحد[هة]|1)', clean):
        return True
    question = sales_context.question_from_turn((context or {}).get('last_staff_turn') or [])
    question = re.split(r'[.\n]', question)[-1].strip()
    return bool(is_confirmation(text) and re.fullmatch(r'قطع[هة]\s+من\s+كل\s+لون[؟?\s]*', customer_copy.normalized(question)))


def reply(product, text, context, customer=None):
    try:
        if terms(product)[0] != 1:
            return None
    except (ValueError, TypeError, KeyError):
        return None
    selected = sales_playbook.customer_selection(context, customer)
    current = product_variants.requested(text)['colors']
    chosen = selected['colors']
    options = (len(current) > 1 and not product_variants.chosen_colors(text)) or bool(selected['color_options'])
    colors = current if len(current) > 1 and options else chosen or selected['color_options']
    if len(colors) < 2:
        return None
    if (len(current) < 2 and product_variants.child_age(text) is None
            and not one_each(text, context) and not is_confirmation(text) and not contact_fields(text)):
        return None
    clean = customer_copy.normalized(text)
    if re.search(r'قماش|خام|فيديو|بطان|سحاب|طول|عرض|توصيل|شحن|دفع|سعر|خصم|غالي|اذا|بشرط|شرط|مكان|عنوانكم|وينكم', clean):
        return None
    state = product_albums.stock_state(product)
    offered = product_variants.colors(customer_copy.fact(product.get('colors'), 'colors'))
    if state == 'unknown' or not offered:
        return dict(reply='', requires_human=True, create_order=False, order={}, _plural_color_reply=True,
                    handoff_reason='تأكيد توفر الألوان التي طلبها الزبون')
    available = [color for color in colors if color in offered and state == 'available']
    missing = [color for color in colors if color not in available]
    # A question can supply an age for availability without becoming a purchase choice.
    age = product_variants.child_age(text)
    ages = product_variants.child_ages(text)
    if len(selected['ages']) > 1 and (product_variants.child_age_correction(text)
            or re.search(r'اصغر|صغير[هة]?|كبير[هة]?|اول[ىي]|ثاني[هة]?', clean)):
        return None  # Child references and corrections need the full conversation, not one shared age.
    if age is None:
        age, ages = selected['age'], selected['ages']
    bounds = product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))['years']
    age_label = product_variants.age_text(age) if age is not None else ''
    if age is not None and not bounds:
        return None
    age_ok = age is not None and any(lo <= float(age) <= hi for lo, hi in bounds)
    parts = []
    order = {}
    confirmed = False
    if state == 'out':
        parts = ['هالموديل مو متوفر عدنا حالياً.']
    elif missing:
        if available:
            parts.append('ال' + ' وال'.join(available) + ' متوفر' + (' لعمر ' + age_label if age_ok else '') + '.')
        parts.append('ال' + ' وال'.join(missing) + ' مو ضمن الألوان المتوفرة بهالموديل.')
        replacements = [color for color in offered if color not in colors]
        if len(missing) == 1 and replacements:
            parts.append('تحبين ال' + replacements[0] + ' بدل ال' + missing[0] + '؟')
        elif offered:
            parts.append('المتوفر ' + ' و'.join(offered) + '.')
    elif age is None:
        parts = ['ال' + ' وال'.join(colors) + ' متوفرات. شكد العمر المطلوب؟']
    elif len(ages) > 1:
        allocation_text = product_variants.age_choice_text(text)
        hits = list(product_variants.COLOR_RE.finditer(allocation_text))
        # Rich explicit allocations belong to the semantic cart parser. This
        # shortcut cannot turn two already-labelled ages into a missing-age question.
        if (len(hits) == len(colors)
                and all(len(product_variants.child_ages(allocation_text[hit.end():
                    hits[index + 1].start() if index + 1 < len(hits) else len(allocation_text)])) == 1
                    for index, hit in enumerate(hits))):
            return None
        parts = ['اختيارج ' + ' و'.join(colors) + ' واضح. يا عمر مطلوب لكل لون؟']
    elif not age_ok:
        parts = ['الألوان المطلوبة متوفرة، بس عمر ' + age_label + ' مو ضمن قياسات هالموديل.']
    elif options:
        parts = ['ال' + ' وال'.join(colors) + ' متوفرات لعمر ' + age_label + '.',
                 'تفضلين ' + ' لو '.join('ال' + color for color in colors) + '؟']
    else:
        parts = ['ال' + ' وال'.join(colors) + ' متوفرات لعمر ' + age_label + '.']
        confirmed = one_each(text, context)
        if confirmed:
            order = {'items': [dict(product_id=product['product_id'], product_name=product['product_name'],
                                   color=color, size=age_label, size_type='size', quantity=1) for color in colors]}
            parts.append('تمام، قطعة من كل لون؛ نكمل بيانات الحجز الناقصة.')
        else:
            parts.append('قطعة من كل لون؟')
    result = ' '.join(parts)
    return dict(reply=result, reply_parts=[result], _single_message=True, _local_reply=True,
                _plural_color_reply=True, _suppress_product_images=True, create_order=False,
                requires_human=False, order=order, _browse_only=not confirmed,
                _confirmed_color_quantities=confirmed)
