"""Exercise audited sales journeys against a real model in disposable storage.

Only OpenRouter is reachable during simulation; delivery/export adapters are
intercepted by evaluate_sales_dialogues. Never pass a customer database here.
"""
import argparse
import getpass
import json
import os
import re
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import evaluate_sales_dialogues as h
import evaluate_natural_conversations as natural


ROOT = Path(__file__).resolve().parents[1]
PHONE = h.PHONE
ADDRESS = 'بغداد حي الاختبار قرب مدرسة التجربة'


def scenarios(catalog):
    cases = []

    def add(identity, store, product, texts, orders=0, **options):
        cases.append(dict(id=identity, store=store, product=product, purpose=identity,
                          expected_orders=orders, turns=[h.step(text, '') for text in texts], **options))

    add('single-delivered-total', 'default', 'P011', [
        'شكد سعر القطعة الوحدة؟', 'يعني اذا اطلب قطعة ويه التوصيل يصير 20؟',
        'تمام ثبتي ماروني XXL قطعة وحدة', PHONE, ADDRESS, 'تسلمين'], 1, wanted_size='XXL')
    add('two-color-offer', 'default', 'P011', [
        'اذا اخذ قطعتين شكد يصير حسابهن؟', 'اريد الماروني والاخضر قياس ٢xl',
        'اي ثبتي قطعة من كل لون نفس القياس', PHONE, ADDRESS, 'صار الحجز؟'], 1, wanted_size='XXL', wanted_quantity=2)
    add('corrected-numbered-size', 'khuyoot', 'P002', [
        'اريد قياس 44 اسود', 'شنو نوع القماش؟', 'لا اعتمدي 46 بدل 44 نفس الاسود ثبتي قطعة',
        PHONE, ADDRESS, 'تمام شكرا'], 1, wanted_size='46')
    add('corrected-child-age', 'baraah-kids', 'P010', [
        'بنتي عمرها 3 سنوات اريد اسأل عن هذا الطقم',
        'اقصد عمرها 4 سنوات مو 3 سنوات، الوردي قطعة وحدة ثبتي', PHONE, ADDRESS, 'يعني صار الحجز؟'],
        1, wanted_age=4)
    add('two-children-with-correction', 'baraah-kids', 'P010', [
        'اريد الوردي عمر 3 سنوات والكريمي عمر 6 سنوات قطعة من كل لون',
        'لا الوردي عمر 4 سنوات مو 3 سنوات والكريمي يبقى عمر 6 سنوات، ثبتي القطعتين',
        PHONE, ADDRESS, 'صار الحجز؟'], 1, wanted_ages=[4, 6], wanted_quantity=2)
    add('combined-years-and-months', 'baraah-kids', 'P010', [
        'اريد قطعة وردي لعمر سنتين و3 اشهر وقطعة كريمي لعمر 4 سنوات',
        'اي ثبتي القطعتين بنفس الاعمار والالوان', PHONE, ADDRESS, 'تسلمين'],
        1, wanted_ages=[2.25, 4], wanted_quantity=2)
    add('landmark-before-province', 'khuyoot', 'P002', [
        'ثبتي اسود قياس 44 قطعة وحدة', 'حي الاختبار قرب مدرسة التجربة', PHONE,
        'بغداد', 'شكرا'], 1, wanted_size='44')
    add('compound-questions', 'default', 'P011', [
        'شنو القماش والالوان والقياسات والسعر والتوصيل لبغداد والدفع والفحص؟',
        'هسه بس اسأل لا تحجزين، القماش شتوي لو بهاري؟', 'خلي افكر وارجعلكم'])
    add('negotiation-then-booking', 'al-fatena', 'P003', [
        'شكد سعره ويا التوصيل؟', 'ما تنزلون شوية من السعر؟',
        'ماشي موافقة على السعر ثبتي اسود قياس 44 قطعة', PHONE, ADDRESS, 'تسلمين'], 1, wanted_size='44')
    add('inspection-fear-and-total', 'al-fatena', 'P003', [
        'شكد سعره ويا التوصيل؟',
        'يعني المجموع واصل للنجف هم يصير 21 ألف لو يختلف التوصيل؟',
        'خامته شنو، القماش مالته ثقيل لو خفيف وبارد باللبس؟',
        'زين عيني اكدر افتح الطلب واشوفه قبل ما انطي الفلوس للمندوب؟ خاف يطلع مو نفس الصورة',
        'تمام موافقة ثبتي اسود قياس 44 قطعة وحدة', PHONE, ADDRESS, 'تسلمين'],
        1, wanted_size='44')
    add('respect-refusal', 'al-fatena', 'P003', [
        'هذا حلو بس غالي علي', 'لا شكرا هسه ما اريد احجز', 'شكرا'])
    add('silent-human-request', 'al-fatena', 'P003', [
        'اريد موظفة تجاوبني', 'زين اكو احد يتابع؟'], handoff_from=0)
    add('assistant-identity', 'al-fatena', 'P003', [
        'انتي روبوت لو انسان؟', 'اريد احجي ويا موظفة'], handoff_from=0)
    add('photo-ai-question', 'al-fatena', 'P003', [
        'هاي الصورة بالذكاء الاصطناعي لو تصوير؟', 'اكو فيديو للموديل؟'], allow_review=True)
    add('known-ordered-album', 'al-fatena', 'P003', [
        'شكد سعر الاول والثاني وشنو قماش كل واحد؟', 'دزيلي صورة الثاني بس بدون حجز',
        'تمام خليني افكر'], album=['P003', 'P017'], allow_review=True)
    add('wholesale-account', 'golden-threads', 'P001', [
        'اريد اسأل عن هذا للمحل، شلون البيع بالجملة؟',
        'شكد عدد القطع بالدرزن وشكد يصير الحساب ويا التوصيل؟', 'هسه بس استفسار'], allow_review=True)
    # The buyer reacts to the actual previous answer, while contact details stay synthetic.
    adaptive = dict(id='adaptive-attentive-buyer', store='al-fatena', product='P003',
                    purpose='Interactive buyer responding to actual sales answers', expected_orders=1,
                    wanted_size='44', wanted_color='أسود', adaptive=True, turns=[])
    for goal in ('اسألي عن ملاءمة الفستان لزيارة وعن خامته دون الحجز',
                 'تفاعلي مع الجواب واسألي عن السعر والتوصيل والفحص',
                 'اقبلي السعر الحقيقي واختاري الأسود قياس 44 واطلبي حجز قطعة واحدة صراحة'):
        adaptive['turns'].append(dict(h.step('', ''), milestone=goal, exact=None, source_message_ids=[]))
    for text in (PHONE, ADDRESS, 'تسلمين شكرا'):
        adaptive['turns'].append(dict(h.step(text, ''), exact=text))
    cases.append(adaptive)
    for case in cases:
        if case.get('allow_review') or 'handoff_from' in case:
            for turn in case['turns']:
                turn['allow_handoff'] = True
    return cases


def evaluate_turn(case, turn, index, prior, texts, orders, response, m, db, sender):
    reply = '\n'.join(texts)
    checks = dict(no_false_identity=not m.sales_playbook.assistant_identity_statement(reply),
                  at_most_one_order=len(orders) <= 1,
                  customer_simulator_succeeded=turn.get('customer_simulation', {}).get('mode') != 'failed',
                  no_false_zipper='closure' not in m.sales_answers.topics(turn['text'])
                  if 'حساب' in turn['text'] else True)
    if 'handoff_from' in case and index >= case['handoff_from']:
        checks.update(silent_handoff=not texts, review_created=bool(m.has_pending_human_review(db, sender)),
                      automation_paused=not m.is_customer_ai_enabled(db, sender))
    if case['id'] == 'single-delivered-total' and index == 1:
        checks.update(total_answered=h.has_amount(reply, 20000), not_discount_refusal='السعر الأقل' not in reply)
    if case['id'] == 'two-color-offer' and index == 0:
        checks.update(account_not_zipper='الإغلاق' not in reply and 'السحاب' not in reply,
                      offer_answered=h.has_amount(reply, 25000),
                      no_premature_contact=not any(term in reply for term in ('رقم الموبايل', 'رقم التلفون', 'رقمج', 'عنوانج')))
    if case['id'] in {'two-children-with-correction', 'combined-years-and-months'} and index == 0:
        checks['explicit_ages_not_requested_again'] = not re.search(r'يا عمر|شكد العمر|عمر مطلوب لكل', reply)
    if case['id'] == 'respect-refusal' and index >= 1:
        checks['no_sales_pressure'] = not m.sales_strategy.reply_error(reply, turn['text'])
    catalog_product = next((p for p in m.load_active_products(db) if p['product_id'] == case['product']), None)
    topics = m.sales_answers.topics(turn['text'])
    if catalog_product and reply:
        uncertain = bool(re.search(r'مو موضح|مو محدد|مو مذكور|ما موضح|ما محدد|ما مذكور|يحتاج.{0,20}ت[اأ]كيد|ما أگدر أأكد', reply))
        if 'season' in topics and not m.sales_answers.season_fact(catalog_product):
            checks['undocumented_season_not_invented'] = uncertain
        if 'care' in topics and not m.sales_answers.care_fact(catalog_product):
            checks['undocumented_care_not_guaranteed'] = uncertain
        if 'fabric_feel' in topics and not m.sales_answers.fabric_feel_fact(catalog_product):
            checks['undocumented_fabric_feel_not_invented'] = uncertain
        if 'photo_source' in topics and not catalog_product.get('real_photo_notes'):
            checks['undocumented_photo_source_not_invented'] = uncertain
    if case['id'] == 'long-adaptive-visit-purchase' and index == 0:
        checks['no_contacts_before_choice'] = not re.search(r'رقم الموبايل|رقمج|عنوانج|العنوان الكامل', reply)
    return checks


def evaluate_case(case, turns, orders, m, db, sender):
    checks = {}
    items = json.loads(orders[0].get('order_items') or '[]') if orders else []
    if case.get('wanted_size'):
        checks['chosen_size_saved'] = bool(items) and all(
            m.product_variants.normalize_size_words(item.get('size')).upper() == case['wanted_size'] for item in items)
    if case.get('wanted_age'):
        checks['corrected_age_saved'] = bool(items) and all(
            m.product_variants.child_age('عمر ' + item.get('size', '')) == case['wanted_age'] for item in items)
    if case.get('wanted_quantity'):
        checks['all_selected_pieces_saved'] = sum(int(item.get('quantity') or 1) for item in items) == case['wanted_quantity']
    if case.get('wanted_ages'):
        ages = [m.product_variants.child_age('عمر ' + item.get('size', ''))
                for item in items for _ in range(int(item.get('quantity') or 1))]
        checks['all_child_ages_preserved'] = None not in ages and sorted(ages) == sorted(case['wanted_ages'])
    if case.get('wanted_colors'):
        checks['all_chosen_colors_saved'] = sorted(m.product_variants.colors(' '.join(str(i.get('color') or '') for i in items))) == sorted(case['wanted_colors'])
    if case.get('wanted_product'):
        checks['only_final_product_saved'] = bool(items) and all(i.get('product_id') == case['wanted_product'] for i in items)
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--model', default='google/gemini-3.8-flash')
    parser.add_argument('--case', action='append')
    parser.add_argument('--long', action='store_true', help='Run extended journeys, including two adaptive buyers.')
    options = parser.parse_args()
    os.environ['QA_OPENROUTER_API_KEY'] = os.environ.get('QA_OPENROUTER_API_KEY') or getpass.getpass('OpenRouter test key: ')

    class ModelGate(h.HTTPGate):
        def request(self, session, method, url, **kwargs):
            payload = dict(kwargs.get('json') or {})
            payload.update(model=options.model)
            payload.pop('models', None)
            if payload.get('messages') and '[QA_CUSTOMER]' in str(payload['messages'][0].get('content')):
                payload['max_tokens'] = 1000
            payload['reasoning'] = {'exclude': True, 'effort': 'low'}
            kwargs['json'] = payload
            result = super().request(session, method, url, **kwargs)
            try:
                self.calls[-1]['returned_model'] = result.json().get('model')
                self.calls[-1]['synthetic_response'] = result.json().get('choices', [{}])[0].get('message', {}).get('content')
            except (ValueError, IndexError):
                pass
            return result

    def turn_factory(case, turn, index, transcript, gate, m, db, sender):
        if case.get('adaptive'):
            return natural.customer_turn(case, turn, index, transcript, gate, m, db, sender, corpus={}, model=options.model)
        return turn

    if options.long:
        from sales_long_journeys import scenarios as long_scenarios
        h.scenarios = long_scenarios
    else:
        h.scenarios = scenarios
    h.HTTPGate = ModelGate
    args = argparse.Namespace(mode='live', source_root=ROOT, catalog=options.snapshot / 'products.json',
        settings_db=options.snapshot / 'settings.db', env_file=ROOT / 'outputs' / 'absent.env', api_key_file=None,
        output=options.output, max_live_requests=250 if options.long else 120, max_live_cost_usd=3.0, scenario=options.case,
        repeat=1, copy_saved_prompts=False, save_partial_transcripts=True, require_complete_selection=True,
        turn_factory=turn_factory, turn_evaluator=evaluate_turn, case_evaluator=evaluate_case)
    try:
        report = h.run(args)
    finally:
        os.environ.pop('QA_OPENROUTER_API_KEY', None)
    report['requested_model'] = options.model
    report['model_ids_verified'] = all(call.get('returned_model') == options.model
                                      for call in report['http_requests'] if call['status'] == 200)
    (options.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    (options.output / 'المحادثات.md').write_text(h.report_markdown(report), encoding='utf-8')
    print(json.dumps({key: report[key] for key in ('checks_passed', 'checks_total', 'ai_http_requests',
                      'reported_cost_usd', 'model_ids_verified', 'source_changed_during_run')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
