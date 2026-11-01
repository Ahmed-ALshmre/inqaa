"""Synthetic multi-turn reproductions of Nov 1 booking audit findings.

Uses the existing isolated webhook/sender harness. Only OpenRouter is reachable;
customer delivery and order exports are intercepted. Snapshot must contain only
public catalog facts and whitelisted settings, never customer tables.
"""
import re

import evaluate_recent_sales as recent

h = recent.h


def scenarios(catalog):
    cases = []

    def add(identity, store, product, texts, orders=0, **options):
        cases.append(dict(id=identity, store=store, product=product, purpose=identity,
                          expected_orders=orders, turns=[h.step(t, '') for t in texts], **options))

    add('nov01-old-price-two-pieces', 'baraah-kids', 'P016', [
        'اذا اخذ قطعتين من هذا شكد يصير ويا توصيل بغداد؟',
        'اريد الاخضر والبرتقالي لعمر 4 سنوات قطعة من كل لون ثبتي',
        h.PHONE, recent.ADDRESS, 'صار الحجز؟'], 1,
        catalog_patch={'P016': {'price': '12000 او قطعتين ب 20000'}},
        wanted_quantity=2, wanted_ages=[4, 4], wanted_colors=['أخضر', 'برتقالي'], wanted_total=25000)
    add('nov01-unit-first-two-ages', 'baraah-kids', 'P016', [
        'سنه 3\nسنه 5', 'ثبتي الاخضر لعمر 3 سنوات والبرتقالي لعمر 5 سنوات قطعة من كل لون',
        recent.ADDRESS, h.PHONE, 'تسلمين'], 1,
        wanted_quantity=2, wanted_ages=[3, 5], wanted_colors=['أخضر', 'برتقالي'])
    add('nov01-ordinal-two-children', 'baraah-kids', 'P016', [
        'عندي طفلين الاول 3 ونص والثاني 5، يناسبهن هذا؟',
        'اي ثبتي اخضر للاول عمر 3 ونص وبرتقالي للثاني عمر 5 سنوات قطعتين',
        h.PHONE, recent.ADDRESS, 'تمام'], 1,
        wanted_quantity=2, wanted_ages=[3.5, 5], wanted_colors=['أخضر', 'برتقالي'])
    add('nov01-soft-consent-clarification', 'default', 'P011', [
        'اريد اسأل عن الاخضر بالاكسين لارج قطعة وحدة', 'ان شاء الله',
        'شنو قصدج؟ الا تحتاجون للعنوان؟ كيف توصلون الطلب؟',
        'هسه بعدني افكر لا تحجزين'])
    add('nov01-delivery-payment-discount', 'default', 'P011', [
        'كم يوم لتوصيل الطلبية؟ هل يوجد خصم إضافي؟ ما هي طريقة الدفع؟',
        'يعني شلون ادفع للمندوب؟ هسه بس اسأل'])
    add('nov01-age-and-arrival', 'baraah-kids', 'P016', [
        'سنتين و9 شهور، شوكت يوصل؟', 'هسه بس اسأل ما اريد احجز'])
    add('nov01-single-card-followup', 'baraah-kids', 'P016', [
        'شنو قياساته وقماشه؟', 'هسه بس استفسار لا تحجزين'],
        history=[dict(direction='outgoing', text='طقم ولادي',
                      metadata={'catalog_page': {'product_ids': ['P014'], 'browsing_only': True}})])
    add('nov01-unknown-specific-stock', 'baraah-kids', 'P016', [
        'بيها عمر 4 سنوات؟'], unlinked=True)
    add('nov01-invalid-price-silent-review', 'baraah-kids', 'P016', [
        'ثبتي أخضر لعمر 4 سنوات قطعة وحدة', h.PHONE, recent.ADDRESS],
        catalog_patch={'P016': {'price': 'قطعتين ب20000', 'offer': '', 'sale_unit': '', 'bundle_contents': ''}},
        allow_review=True)
    for case in cases:
        if case.get('allow_review'):
            for turn in case['turns']:
                turn['allow_handoff'] = True
    return cases


def evaluate_turn(case, turn, index, prior, texts, orders, response, m, db, sender):
    checks = recent_evaluate_turn(case, turn, index, prior, texts, orders, response, m, db, sender)
    reply = '\n'.join(texts)
    checks['no_customer_catalog_maintenance'] = not re.search(r'حد.?ث السعر|سعر القطعة غير واضح', reply)
    identity = case['id']
    if identity == 'nov01-unit-first-two-ages' and index == 0:
        checks['both_requested_ages_answered'] = '3' in reply and '5' in reply
        checks['no_invented_one_year_child'] = 'لعمر سنة ' not in reply
    if identity == 'nov01-delivery-payment-discount' and index == 0:
        checks['delivery_time_answered'] = bool(re.search(r'24|٢٤|يوم|يومين|ساعة|ساعه', reply))
        checks['payment_answered'] = bool(re.search(r'استلام|مندوب|دفع', reply))
    if identity == 'nov01-age-and-arrival' and index == 0:
        checks['arrival_not_swallowed_by_age'] = bool(re.search(r'24|٢٤|يوم|يومين|ساعة|ساعه', reply))
    if identity == 'nov01-soft-consent-clarification':
        checks['no_order_from_soft_consent'] = not orders
        if index == 1:
            checks['no_premature_contacts'] = not re.search(r'رقم|عنوان|محافظ', reply)
            checks['no_dangling_checkout_clause'] = not re.search(r'حتى\s+(?:نثبت|نكمل)\w*[^؟?]*$', reply)
        if index == 2:
            checks['not_generic_catalog_gap'] = 'بقية التفاصيل المطلوبة' not in reply
    if identity == 'nov01-single-card-followup' and index == 0:
        binding = m.get_active_product_binding(db, sender)
        checks['single_card_is_discussion_focus'] = bool(binding and binding['product_id'] == 'P014')
        checks['new_card_age_range_used'] = '4' in reply and '7' not in reply
    if identity == 'nov01-unknown-specific-stock':
        checks['no_unverified_stock_claim'] = not re.search(r'(?:اي|نعم|إي).{0,12}متوفر', reply)
    if identity == 'nov01-invalid-price-silent-review' and index == 2:
        checks.update(silent_catalog_review=not texts,
                      catalog_review_created=bool(m.has_pending_human_review(db, sender)))
    return checks


def evaluate_case(case, turns, orders, m, db, sender):
    checks = recent_evaluate_case(case, turns, orders, m, db, sender)
    if case.get('wanted_total'):
        checks['correct_total_with_offer_and_delivery'] = bool(orders and orders[0]['total_amount'] == case['wanted_total'])
    return checks


recent_evaluate_turn = recent.evaluate_turn
recent_evaluate_case = recent.evaluate_case

if __name__ == '__main__':
    recent.scenarios = scenarios
    recent.evaluate_turn = evaluate_turn
    recent.evaluate_case = evaluate_case
    recent.main()
