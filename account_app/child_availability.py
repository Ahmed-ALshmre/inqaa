"""Answer all requested child ages from the identified model and verified alternatives."""
import re
try:
    from . import customer_copy, product_albums, product_links, product_variants, sales_playbook
    from .pricing import terms
except ImportError:
    import customer_copy, product_albums, product_links, product_variants, sales_playbook
    from pricing import terms


def reply(product, products, question, *, known_color='', history=()):
    ages = product_variants.child_ages(question)
    if not product or not ages:
        return None
    clean = customer_copy.normalized(question)
    if re.search(r'قماش|خام|فيديو|بطان|سحاب|طول|عرض|توصيل|شحن|دفع|ثبت|احجز|اطلب|خصم|تخفيض|غالي|اذا|بشرط|شرط|مكان|عنوانكم|وينكم', clean):
        return None  # Mixed facts and buying instructions keep the complete answer/checkout flow.
    if product_variants.requested(question)['colors']:
        return None  # The color-specific answer must be checked along with the ages.
    state = product_albums.stock_state(product)
    data = product_variants.size_data(customer_copy.fact(product.get('sizes'), 'sizes'))
    if state == 'unknown' or (state == 'available' and not data['years']):
        return dict(reply='', create_order=False, order={}, requires_human=True, _child_availability=True,
                    handoff_reason='تأكيد توفر الموديل أو جدول أعمار الأطفال المطلوب قبل الجواب')
    # Preserve the previously approved single-child conditional fit conversation.
    if len(ages) == 1 and state == 'available' and product_variants.boundary_fit(product, ages[0]):
        return None
    try:
        count, price, _ = terms(product)
    except (ValueError, TypeError, KeyError):
        if state != 'out':
            return None
        count, price = 1, None
    offer = customer_copy.normalized(' '.join(str(product.get(k) or '') for k in ('offer', 'bundle_contents', 'notes')))
    if count != 1 or re.search(r'قطعتين|ثلاث قطع|3 قطع|بكج|بشرط', offer):
        return None  # Never replace package/conditional prices with a unit quote.
    checks = [dict(age=age, label=product_variants.age_text(age),
                   available=state == 'available' and any(lo <= age <= hi for lo, hi in data['years'])) for age in ages]
    available, missing = [c for c in checks if c['available']], [c for c in checks if not c['available']]
    name = customer_copy.fact(product.get('product_name')) or 'هالموديل'
    parts = []
    if state == 'out':
        parts.append(f'{name} مو متوفر عدنا حالياً.')
    elif available and not missing:
        parts.append(name + ' متوفر ' + ' و'.join('لعمر ' + c['label'] for c in available) + '.')
    else:
        parts.append(name + '، ' + '، '.join('لعمر ' + c['label'] + (' متوفر' if c['available'] else ' مو ضمن قياساته')
                                            for c in checks) + '.')
    if available and re.search(r'سعر|شكد|بكم|بشكد|بيش', clean):
        offer_line = sales_playbook.last_pieces_price_line(product, history) if not missing else ''
        if offer_line:
            parts.insert(0, offer_line)
        else:
            parts.append(customer_copy.chat_money(f'سعر القطعة {price:,} د.ع').rstrip(' .') + '.')
    suggestions = {}
    for check in missing:
        for candidate in product_links.eligible_age_alternatives(product, products, check['age']):
            if known_color and known_color not in product_variants.colors(customer_copy.fact(candidate.get('colors'), 'colors')):
                continue
            candidate_offer = customer_copy.normalized(' '.join(str(candidate.get(k) or '')
                for k in ('offer', 'bundle_contents', 'notes')))
            if re.search(r'قطعتين|ثلاث قطع|3 قطع|بكج|بشرط', candidate_offer):
                continue
            try:
                if terms(candidate)[0] != 1:
                    continue
            except (ValueError, TypeError, KeyError):
                continue
            suggestions[candidate['product_id']] = candidate
            break
    suggestions = list(suggestions.values())[:2]
    for candidate in suggestions:
        bounds = product_variants.size_data(customer_copy.fact(candidate.get('sizes'), 'sizes'))['years']
        covered = [c['label'] for c in missing if any(lo <= c['age'] <= hi for lo, hi in bounds)]
        candidate_name = customer_copy.fact(candidate.get('product_name')) or 'موديل بديل'
        parts.append(customer_copy.chat_money(f"عدنا بديل {candidate_name} متوفر "
            + ' و'.join('لعمر ' + label for label in covered)
            + f"، سعر القطعة {terms(candidate)[1]:,} د.ع").rstrip(' .') + '.')
    covered_ages = {c['age'] for c in missing for candidate in suggestions
                   if any(lo <= c['age'] <= hi for lo, hi in product_variants.size_data(
                       customer_copy.fact(candidate.get('sizes'), 'sizes'))['years'])}
    uncovered = [c['label'] for c in missing if c['age'] not in covered_ages]
    if uncovered:
        parts.append('ما عندنا بديل مؤكد حالياً ' + ' و'.join('لعمر ' + label for label in uncovered) + '.')
    if suggestions:
        parts.append('أي بديل عجبج؟' if len(suggestions) > 1 else 'يعجبج هالبديل؟')
    elif available:
        colors = product_variants.colors(customer_copy.fact(product.get('colors'), 'colors'))
        if len(colors) > 1 and not known_color:
            parts.append('شنو اللون اللي يعجبج؟')
    text = ' '.join(parts)
    return dict(reply=text, reply_parts=[text], _single_message=True, _local_reply=True,
                _child_availability=True, _child_age_results=checks,
                _verified_age_alternative_ids=[p['product_id'] for p in suggestions],
                image_product_ids=[p['product_id'] for p in suggestions],
                _suppress_product_images=not bool(suggestions),
                create_order=False, order={}, requires_human=False, intent='product_question')
