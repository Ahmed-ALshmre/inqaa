"""Reproducible webhook-to-sender acceptance conversations using synthetic people.

Offline mode tests orchestration with explicit HTTP fixtures, not AI intelligence.
Live mode requires --allow-live after owner approval and permits only OpenRouter
POSTs. Facebook, Telegram and Menger remain intercepted in both modes. Nothing
from the backup's customer, message or order tables is loaded.

Examples (run a separate process for each source tree):
  python tools/evaluate_sales_dialogues.py --mode offline --output outputs/dialogues-after
  python tools/evaluate_sales_dialogues.py --mode offline --source-root analysis_20261024/deep-baseline --output outputs/dialogues-before
  python tools/evaluate_sales_dialogues.py --mode live --allow-live --output outputs/dialogues-live
"""
from __future__ import annotations

import argparse
import copy
import contextlib
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import shutil
import sys
import tempfile
import time
from unittest.mock import patch
from urllib.parse import urlsplit

WORKSPACE = Path(__file__).resolve().parents[1]
OPENROUTER_URL = 'https://openrouter.ai/api/v1/chat/completions'
PHONE = '07700000000'


def clean(value):
    text = str(value or '')
    text = re.sub(r'(?<!\d)(?:\+?964|0)?7\d{9}(?!\d)', '[SYNTHETIC_PHONE]', text)
    text = re.sub(r'sk-or-[A-Za-z0-9_-]+|Bearer\s+\S+', '[REDACTED]', text)
    return text


def step(text, reply, *, order=False, checks=None, **extra):
    return dict(text=text, fixture=dict(reply=reply, create_order=order, intent='question',
                confidence=100, requires_human=False, order={}), checks=checks or {}, **extra)


def scenarios(catalog):
    """The inputs/gates are shared unchanged by baseline and candidate runs."""
    lookup = {(p.get('store_id', 'default'), p['product_id']): p for p in catalog}
    dress = lookup[('al-fatena', 'P003')]
    cheap = lookup[('al-fatena', 'P017')]
    pants = lookup[('khuyoot', 'P002')]
    child = lookup[('baraah-kids', 'P010')]
    dp, cp, pp = int(dress['price']), int(cheap['price']), int(pants['price'])
    dname, cname, pname = [p['product_name'] for p in (dress, cheap, pants)]
    common = dict(store='al-fatena', product='P003', expected_orders=0)
    items = [dict(product_id='P002', color='اسود', size='44', quantity=1)]
    result = [
        dict(common, id='price_total', purpose='Answer price, final cost, fabric and delivery before any sales step.', turns=[
            step('شكد سعره؟', f'سعره {dp} والتوصيل 5000، المجموع {dp + 5000}.', checks=dict(numbers=[dp, dp + 5000])),
            step('شنو قماشه؟', f'قماش {dname}: ' + str(dress.get('fabric') or 'يحتاج تأكيد من المتجر') + '.'),
            step('شكد التوصيل لبغداد؟', 'التوصيل لبغداد خمسة آلاف.'),
            step('متى يوصل؟', 'المدة المعتادة من يوم إلى يومين، والموعد الدقيق يحتاج تأكيد.')]),
        dict(common, id='variant_context', store='khuyoot', product='P002', purpose='Remember an explicit size/color across side questions.', turns=[
            step('قياس 44 بالاسود متوفر؟', 'إي، قياس 44 بالأسود متوفر.', checks=dict(contains=['44'])),
            step('وشنو قماشه؟', 'قماشه ' + str(pants.get('fabric') or 'يحتاج تأكيد من المتجر') + '.'),
            step('شكد سعره ويا التوصيل؟', f'سعره {pp} والتوصيل 5000، المجموع {pp + 5000}.', checks=dict(numbers=[pp])),
            step('هسه بس دا اتأكد لا تحجزين', 'أكيد، ما راح أثبت طلب، نكمل أي تفصيل تحتاجه.')]),
        dict(common, id='budget_alternative', purpose='Keep requested size/color and a budget that includes shipping.', turns=[
            step('أريده قياس 44 اسود بس بعدني ما قررت', 'قياس 44 والأسود واضحين عندي، نكمل التفاصيل اللي تحتاجها.'),
            step(f'ميزانيتي {cp + 5000} ويا التوصيل، اكو ارخص؟', f'عدنا {cname} بسعر {cp}، ويا التوصيل يصير {cp + 5000}، بالأسود وقياس 44.', checks=dict(numbers=[cp, cp + 5000])),
            step('ممكن أشوف صوره بدون حجز؟', f'هاي صور {cname}، مجرد مشاهدة وما ثبتنا طلب.', fixture_images=['P017']),
            step('خليني افكر', 'خذ راحتك، إذا احتجت تفصيل إحنا بالخدمة.', checks=dict(no_booking_question=True))]),
        dict(common, id='photo_yes_is_not_booking', purpose='Short agreement after a photo offer never authorizes an order.', turns=[
            step('عندج موديل ارخص بالاسود؟', f'عدنا {cname} بسعر {cp}، تحب تشوف صوره؟'),
            step('اي', f'هاي صور {cname}.', fixture_images=['P017']),
            step('وشنو قياساته؟', 'قياساته من 38 إلى 52.'),
            step('تمام شكرا بس ما اريد احجز', 'تدلل، إحنا بالخدمة.', checks=dict(no_booking_question=True))]),
        dict(common, id='explicit_checkout_split_contact', store='khuyoot', product='P002', expected_orders=1,
             purpose='Create exactly one local order after explicit intent and split contact details.', turns=[
            step(f'اريد احجز {pname} اسود قياس 44 قطعة وحدة', 'اختيارك الأسود قياس 44، باقي رقم الموبايل والمحافظة والعنوان حتى نكمل الطلب.', order=True),
            step(PHONE, 'وصل الرقم، باقي المحافظة والعنوان بالتفصيل.', order=True, checks=dict(no_phone_reask=True)),
            step('بغداد', 'باقي العنوان بالتفصيل ببغداد حتى نكمل الطلب.', order=True, checks=dict(no_phone_reask=True)),
            step('بغداد حي اختبار قرب مدرسة الاختبار', 'نكمل على الأسود قياس 44 وقطعة وحدة.', order=True, checks=dict(no_phone_reask=True)),
            step('تمام ثبتي', 'نكمل طلبك على الاختيارات والبيانات المحفوظة.', order=True, checks=dict(allow_silent_duplicate_ack=True))]),
        dict(common, id='conditional_deadline', purpose='A phone number cannot override an unresolved arrival condition.', turns=[
            step('ثبتيه إذا يوصل باجر فقط', 'موعد باجر مو مضمون، يحتاج تأكيد من المتجر قبل تثبيت الطلب.'),
            step('هذا رقمي ' + PHONE, 'الرقم وصل، بس شرط الوصول باجر بعده يحتاج تأكيد وما ثبتنا طلب.'),
            step('إذا مو مضمون لا تحجزين', 'أكيد، ما راح أثبت الطلب بدون تحقق شرط الوصول.'),
            step('شكرا', 'تدلل، إحنا بالخدمة.', checks=dict(no_booking_question=True))]),
        dict(common, id='refusal_and_optout', purpose='Answer the last question, then stop sales pressure after refusal.', turns=[
            step('غالي بس شنو قماشه؟', 'أفهم عليك، القماش ' + str(dress.get('fabric') or 'يحتاج تأكيد من المتجر') + '.'),
            step('لا شكرا ما اريد', 'تدلل، ما راح أثبت أي طلب.', checks=dict(no_booking_question=True)),
            step('ما اريد رسائل', 'حاضر، ما راح نراسلك بعد.', checks=dict(no_booking_question=True))]),
        dict(id='child_age_correction', store='baraah-kids', product='P010', expected_orders=0,
             purpose='Respect the child age range, then remember the corrected age without adult weight rules.', turns=[
            step('هذا الطقم البناتي يناسب عمر سنة؟', 'هذا الموديل من عمر سنتين إلى 6 سنوات، عمر سنة خارج القياسات المتوفرة.', checks=dict(no_adult_weight=True)),
            step('اقصد عمر سنتين', 'تمام، عمر سنتين ضمن نطاق هالموديل، شنو اللون المطلوب؟', checks=dict(no_adult_weight=True)),
            step('الوردي متوفر؟', 'الوردي ضمن ألوان هالموديل.', checks=dict(no_adult_weight=True)),
            step('وشكد سعره ويا التوصيل؟', 'سعره 15000 والتوصيل 5000، المجموع 20000.', checks=dict(numbers=[15000, 20000], no_adult_weight=True))]),
        dict(common, id='saved_album_reference', album=['P003', 'P017'],
             purpose='Use the recorded photo order without inferring a purchase; vision matching itself is outside this case.', turns=[
            step('شكد سعر الاول والثاني؟', f'الأول {dname} بسعر {dp}، والثاني {cname} بسعر {cp}.', checks=dict(numbers=[dp, cp])),
            step('الثاني قماشه شنو؟', 'قماش ' + cname + ': ' + str(cheap.get('fabric') or 'يحتاج تأكيد من المتجر') + '.'),
            step('تمام بس دزيلي صورته بدون حجز', f'هاي صورة {cname}.', fixture_images=['P017'])]),
    ]
    for scenario in result:
        for turn in scenario['turns']:
            if scenario['id'] == 'explicit_checkout_split_contact':
                turn['fixture']['order'] = dict(items=items)
            if turn.get('fixture_images'):
                turn['fixture']['image_product_ids'] = turn['fixture_images']
    result.extend([
        dict(common, id='plan_hesitation', purpose='Warmly explore a soft hesitation once, then respect an explicit pause.', turns=[
            step('الفستان حلو بس أريد أفكر', 'أكيد عيني، خذي راحتج. التردد من القياس لو من السعر؟'),
            step('خليني أفكر وأرجعلج', 'من عيوني عيني، خذي راحتج وإذا احتجتي أي تفصيل أني بالخدمة.',
                 checks=dict(no_booking_question=True))]),
        dict(common, id='plan_price', purpose='Acknowledge price and offer the owner-linked lower-price dress without changing the chosen product.',
             catalog_patch={'P003': {'related_product_ids': ['P017']}, 'P017': {'related_product_ids': ['P003']}}, turns=[
            step('غالي شوية، اكو موديل قريب منه أرخص؟', f'أفهمج عيني. {cname} سعره {cp} ومتوفر، تحبين أوضحلج الفرق بينه وبين {dname}؟',
                 checks=dict(numbers=[cp])),
            step('خلي أشوف الثاني بس بعدني ما قررت', f'هاي صورة {cname}، شوفيه براحتج وما ثبتنا طلب.', fixture_images=['P017'])]),
        dict(common, id='plan_fit_fear', purpose='Reassure a worried shopper using only the actual size table and inspection policy.', turns=[
            step('أخاف القياس ما يضبط واخسر فلوسي', 'حقج تخافين عيني. نراجع جدول قياسات القطعة ونوضح الفحص عند الاستلام حسب سياسة المتجر. شنو قياس ملابسج المعتاد؟')]),
        dict(common, id='plan_scarcity_verified', purpose='Use gentle scarcity only when the synthetic catalog explicitly records low remaining stock.',
             catalog_patch={'P003': {'sales_cues': ['limited_stock'], 'stock_quantity': 3}}, turns=[
            step('هذا الفستان عجبني، شنو تفاصيله وهل متوفر؟', f'{dname} متوفر عيني، قماشه لينن وسعره {dp}. باقي منه قطع قليلة، شنو قياسج حتى نكمل اختيارج؟',
                 checks=dict(numbers=[dp]))]),
        dict(common, id='plan_scarcity_unknown', purpose='Keep the same warmth without claiming scarcity when current quantity is unknown.',
             catalog_patch={'P003': {'sales_cues': [], 'stock_quantity': ''}}, turns=[
            step('هذا الفستان عجبني، شنو تفاصيله وهل متوفر؟', f'{dname} متوفر عيني، قماشه لينن وسعره {dp}. شنو قياسج حتى نكمل اختيارج؟',
                 checks=dict(numbers=[dp]))]),
        dict(common, id='plan_linked_addon', purpose='Suggest an owner-linked second dress before booking and stop after refusal.',
             catalog_patch={'P003': {'related_product_ids': ['P017']}, 'P017': {'related_product_ids': ['P003']}}, turns=[
            step('عجبني هذا وأريد أشوف فستان ثاني قريب منه، شنو الخيارات؟',
                 f'اختيارج حلو عيني. عندنا {cname} بسعر {cp} ومتوفر، تحبين تشوفين صورته؟', checks=dict(numbers=[cp])),
            step('لا بس الأول، ما أريد الثاني', 'تدللين عيني، نخلي اختيارج على الفستان الأول. شنو القياس اللي تلبسينه عادة؟')]),
    ])
    return result


class HTTPGate:
    """Intercept at requests' HTTP boundary; leave ai_transport/parser intact."""
    def __init__(self, mode, original, max_requests, max_cost):
        self.mode, self.original, self.max_requests = mode, original, max_requests
        self.max_cost, self.spent, self.budget_stopped = max_cost, 0.0, False
        self.turn, self.calls, self.blocked = None, [], []
        self.unavailable = None

    def request(self, session, method, url, **kwargs):
        import requests
        if method.upper() != 'POST' or url != OPENROUTER_URL:
            self.blocked.append(dict(method=method.upper(), host=urlsplit(url).hostname))
            raise AssertionError('External endpoint blocked by acceptance harness')
        if self.mode == 'live' and (self.unavailable or len(self.calls) >= self.max_requests or self.spent >= self.max_cost):
            self.budget_stopped = not bool(self.unavailable)
            raise requests.RequestException('Live provider stopped by acceptance budget')
        payload = kwargs.get('json') or {}
        if self.mode == 'live':
            payload = dict(payload, max_tokens=min(int(payload.get('max_tokens') or 1500), 1500))
            kwargs['json'] = payload
        messages = payload.get('messages') or []
        system = str(messages[0].get('content') or '') if messages else ''
        kind = ('customer_simulation' if '[QA_CUSTOMER]' in system else
                'vision' if any(isinstance(message.get('content'), list) and any(part.get('type') == 'image_url' for part in message['content'] if isinstance(part, dict)) for message in messages) else
                'selection' if 'حلل اختيار المنتجات' in system else
                'checker' if 'approved' in system and 'fix_instruction' in system and 'create_order' not in system else 'reply')
        started = time.monotonic()
        record = dict(model=payload.get('model'), kind=kind, status=None, seconds=0,
                      prompt_tokens=None, completion_tokens=None, reported_cost_usd=None,
                      max_tokens=payload.get('max_tokens'), fixture=self.mode == 'offline')
        self.calls.append(record)  # Timeouts still count toward the request cap.
        try:
            if self.mode == 'offline':
                fixture = dict((self.turn or {}).get('fixture') or {})
                if kind == 'selection':
                    fixture = dict(action='none', reject_ids=[], select_ids=[], focus_id='')
                elif kind == 'checker':
                    fixture = dict(approved=True, problem='', fix_instruction='')
                response = requests.Response(); response.status_code = 200
                response._content = json.dumps(dict(choices=[dict(message=dict(content=json.dumps(fixture, ensure_ascii=False)), finish_reason='stop')],
                    usage=dict(prompt_tokens=0, completion_tokens=0, total_tokens=0, cost=0)), ensure_ascii=False).encode('utf-8')
                response.headers['Content-Type'] = 'application/json'
            else:
                response = self.original(session, method, url, **kwargs)
            try:
                body = response.json()
            except ValueError:
                body = {}
            usage = body.get('usage') or {}
            record.update(status=response.status_code, prompt_tokens=usage.get('prompt_tokens'),
                          completion_tokens=usage.get('completion_tokens'), reported_cost_usd=usage.get('cost'))
            record['finish_reasons'] = [choice.get('finish_reason') for choice in body.get('choices', [])]
            record['response_text_chars'] = sum(len(str((choice.get('message') or {}).get('content') or '')) for choice in body.get('choices', []))
            if kind == 'selection':
                try:
                    record['complete_selection_json'] = isinstance(json.loads(body['choices'][0]['message']['content']), dict) and body['choices'][0].get('finish_reason') != 'length'
                except (ValueError, KeyError, IndexError, TypeError):
                    record['complete_selection_json'] = False
            if isinstance(usage.get('cost'), (int, float)):
                self.spent += usage['cost']
            if response.status_code in (401, 402, 403):
                self.unavailable = response.status_code
            return response
        except requests.RequestException as exc:
            record['error_type'] = type(exc).__name__
            record['error_detail'] = clean(str(exc))[:350]
            raise
        finally:
            record['seconds'] = round(time.monotonic() - started, 3)
            if getattr(self, 'audit_path', None):
                with self.audit_path.open('a', encoding='utf-8') as audit:
                    audit.write(json.dumps(record, ensure_ascii=True) + '\n')


def has_amount(text, amount):
    text = text.translate(str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')).replace(',', '').replace('٬', '')
    return bool(re.search(r'(?<!\d)' + str(amount) + r'(?!\d)', text)
                or (amount % 1000 == 0 and re.search(r'(?<!\d)' + str(amount // 1000) + r'\s*(?:ألف|الف|آلاف|الاف)', text)))


def evaluate_turn(m, scenario, turn, texts, orders, sent):
    reply = '\n'.join(texts)
    positive_confirmation = any(re.search(r'تم تثبيت|تم حجز|ثبتنالج|ثبتنا طلب|حجزنالج', clause)
        and not re.search(r'ما\s*(?:ثبت|حجز)|مو\s*(?:مثبت|محجوز)|لم\s*(?:يثبت|يحجز)', clause)
        for clause in re.split(r'[.\n،]+', m.sales_strategy.normalized(reply)))
    checks = dict(response_sent=bool(sent and texts), no_empty_bubbles=all(bool(p.strip()) for p in texts),
                  bubble_limit=len(texts) <= 5,
                  one_next_question=len(re.findall(r'[؟?]', reply)) <= 1,
                  clean_punctuation=not re.search(r'[.!؟?؛]\s*[،؛]|،\s*،', reply),
                  no_internal_instructions=not m.customer_copy.has_instructions(reply),
                  no_known_product_reask=not m.reply_reasks_known_product(reply),
                  no_fake_confirmation=not (positive_confirmation and not orders))
    if not scenario.get('album') and not turn.get('multiple_images_in_context'):
        checks['no_single_product_ordinal'] = not re.match(r'\s*(?:الاول|الأول|الثاني)[،\s]', reply)
    if scenario['id'] == 'conditional_deadline':
        checks['no_advance_before_condition_resolved'] = not re.search(
            r'(?:ناقص|باقي|دزي|ارسلي|احتاج|شنو|شكد|اي قياس).{0,28}(?:محافظ|عنوان|قياس)|'
            r'(?:نثبت|نحجز|اثبت|احجز)[^؟?\n]{0,35}[؟?]', m.sales_strategy.normalized(reply))
    if scenario['id'] == 'photo_yes_is_not_booking' and 'ما اريد احجز' in m.sales_strategy.normalized(turn['text']):
        checks['decline_not_false_cancellation'] = not re.search(r'الغاء|الحجز السابق|موظف|مراجعه', m.sales_strategy.normalized(reply))
    if scenario['id'] == 'explicit_checkout_split_contact' and orders:
        checks['no_unsupported_shipping_readiness'] = not re.search(r'جاهز.{0,20}(?:يوصل|شحن)|انشحن|طلع.{0,15}مندوب', m.sales_strategy.normalized(reply))
    for amount in turn['checks'].get('numbers', []):
        checks['amount_' + str(amount)] = has_amount(reply, amount)
    for value in turn['checks'].get('contains', []):
        checks['contains_' + value] = value in reply
    if turn['checks'].get('no_booking_question'):
        checks['no_booking_question'] = not re.search(r'احجز.{0,12}[؟?]|اثبت.{0,12}[؟?]|دزي.{0,15}(?:رقم|عنوان)', m.sales_strategy.normalized(reply))
    if turn['checks'].get('no_adult_weight'):
        checks['no_adult_weight'] = not re.search(r'وزن.{0,10}(?:[5-9]\d|1[0-3]\d)|وزنج|وزنك', m.sales_strategy.normalized(reply))
    if turn['checks'].get('no_phone_reask'):
        checks['no_phone_reask'] = not m.sales_context.reasks_contact(reply, dict(phone=PHONE))
    if not scenario.get('expected_orders'):
        checks['no_unauthorized_order'] = not orders
    if turn['checks'].get('allow_silent_duplicate_ack') and orders:
        checks.pop('response_sent')
        checks['safe_duplicate_acknowledgement'] = bool((sent and texts) or not texts)
    return checks


def report_markdown(report):
    lines = ['# Sales dialogue acceptance', '',
             'This uses synthetic customers and local orders. It does not measure sales conversion.', '',
             f"Mode: **{report['mode']}**. Deterministic gates: **{report['checks_passed']}/{report['checks_total']}**. "
             f"Manual quality review: **{report['manual_quality']['status']}**.", '',
             'Offline HTTP replies are authored fixtures; passing them proves orchestration, not model persuasion or warmth.', '',
             '| Scenario | Turns | Gates | Local orders | AI HTTP requests |', '|---|---:|---:|---:|---:|']
    for scenario in report['scenarios']:
        all_checks = [value for turn in scenario['turns'] for value in turn['checks'].values()] + list(scenario['checks'].values())
        lines.append(f"| {scenario['id']} | {len(scenario['turns'])} | {sum(all_checks)}/{len(all_checks)} | {scenario['local_orders']} | {sum(len(t['ai_requests']) for t in scenario['turns'])} |")
    for scenario in report['scenarios']:
        lines += ['', '## ' + scenario['id'], '', scenario['purpose'], '']
        for number, turn in enumerate(scenario['turns'], 1):
            lines += [f"{number}. Customer: {turn['customer']}"]
            lines += [f"   - Bubble {i}: {text}" for i, text in enumerate(turn['outgoing'], 1)]
            failed = [name for name, passed in turn['checks'].items() if not passed]
            lines += [f"   - {turn['elapsed_seconds']} seconds; {len(turn['outgoing'])} text bubbles; failed gates: {', '.join(failed) or 'none' }."]
    lines += ['', '## Manual quality acceptance', '',
              'A human reviewer must read each transcript and record a decision independently of keyword gates:', '',
              '- Does the reply answer the actual question and preserve choices across turns?',
              '- Does it feel attentive and natural in Iraqi Arabic, with a useful next step only when needed?',
              '- Does it resolve the stated hesitation with a truthful relevant fact or an appropriate alternative?',
              '- Are notification count and separation helpful, with cost and its conditions together?',
              '- Does checkout occur after informed consent, and does refusal end sales pressure?', '',
              'Acceptance requires all deterministic gates plus documented manual approval. No run is automatically approved.']
    return '\n'.join(lines) + '\n'


def source_file_hashes(source):
    """Fingerprint executable application modules without opening configuration."""
    return {path.relative_to(source).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted((source / 'account_app').glob('*.py'))}


def run(args):
    import requests
    source = args.source_root.resolve()
    initial_source_hashes = source_file_hashes(source)
    progress_stream = sys.stdout
    args.output.mkdir(parents=True, exist_ok=True)
    catalog_file = args.catalog.resolve()
    catalog = json.loads(catalog_file.read_text(encoding='utf-8-sig'))
    if isinstance(catalog, dict):
        catalog = catalog['products']
    cases = scenarios(catalog)
    if args.scenario:
        cases = [case for case in cases if case['id'] in args.scenario]
        if not cases:
            raise ValueError('No matching scenario')
    if args.repeat > 1:
        cases = [dict(copy.deepcopy(case), id=f"{case['id']}-run{number}")
                 for number in range(1, args.repeat + 1) for case in cases]
    original_request = requests.sessions.Session.request
    gate = HTTPGate(args.mode, original_request, args.max_live_requests, args.max_live_cost_usd)
    gate.audit_path = args.output / 'http-requests.jsonl'
    source_db = sqlite3.connect(args.settings_db.resolve().as_uri() + '?mode=ro', uri=True)
    allowed = {'ai_main_model', 'ai_main_temperature', 'ai_main_max_tokens', 'delivery_baghdad_fee',
               'delivery_other_fee', 'delivery_time', 'delivery_inspection_message', 'checker_enabled'}
    if getattr(args, 'copy_saved_prompts', False):
        allowed |= {'prompt_main_system', 'prompt_main_rules', 'prompt_main_output',
                    'ai_vision_model', 'ai_catalog_match_model', 'vision_enabled', 'catalog_match_enabled'}
    conditions, parameters = [], []
    for name in sorted(allowed):
        conditions += ['key=?', 'key LIKE ?']
        parameters += [name, 'store:%:' + name]
    settings = list(source_db.execute('SELECT key,value FROM app_settings WHERE ' + ' OR '.join(conditions), parameters))
    source_db.close()
    key = 'offline-fixture-placeholder'
    if args.mode == 'live':
        if args.api_key_file:
            key = args.api_key_file.read_text(encoding='utf-8-sig').strip()
        else:
            from dotenv import dotenv_values
            key = os.environ.get('QA_OPENROUTER_API_KEY') or dotenv_values(args.env_file).get('OPENROUTER_API_KEY') or ''
        if not key:
            raise ValueError('Live mode: configured API key is absent')
    captured = []
    result_cases = []
    completed_turns = 0
    run_started = time.monotonic()
    def progress(case_id=None, case_turns=None, *, case_complete=False, finished=False):
        request_costs = [call['reported_cost_usd'] for call in gate.calls]
        costs_known = all(isinstance(cost, (float, int)) for cost in request_costs)
        latest_turns = case_turns or []
        current_checks = [value for turn in latest_turns for value in turn['checks'].values()]
        summary = dict(mode=args.mode, status='finished' if finished else 'running',
            completed_cases=len(result_cases), total_cases=len(cases),
            completed_turns=completed_turns, total_turns=sum(len(case['turns']) for case in cases),
            current_case=case_id, current_case_completed=case_complete,
            ai_http_requests=len(gate.calls), elapsed_seconds=round(time.monotonic() - run_started, 3),
            provider_status=gate.unavailable, live_budget_stopped=gate.budget_stopped,
            provider_reported_cost_usd=sum(request_costs) if costs_known else None,
            known_cost_subtotal_usd=sum(cost for cost in request_costs if isinstance(cost, (float, int))),
            requests_without_cost=sum(cost is None for cost in request_costs),
            current_case_checks_passed=sum(current_checks), current_case_checks_total=len(current_checks),
            current_case_error_types=[turn['error_type'] for turn in latest_turns if turn['error_type']],
            cost_scope='fixture_zero_not_provider_billing' if args.mode == 'offline' else 'provider_reported_only')
        temporary_progress = args.output / 'progress.json.tmp'
        # Windows readers/OneDrive can briefly deny an atomic replacement.
        # Progress is observational; a locked progress file must never discard
        # an otherwise completed paid provider run or its final transcript.
        try:
            temporary_progress.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding='utf-8')
            temporary_progress.replace(args.output / 'progress.json')
        except OSError:
            pass
        if case_complete:
            print(json.dumps(summary, ensure_ascii=True), file=progress_stream, flush=True)
    progress()
    with tempfile.TemporaryDirectory(prefix='lamsa-dialogue-qa-') as temporary:
        temp = Path(temporary)
        (temp / 'products.json').write_text(json.dumps(catalog, ensure_ascii=False), encoding='utf-8')
        # Catalog photos are public product assets. Copy existing local assets
        # so the real image eligibility checks run without downloading media.
        uploads = temp / 'uploads'; uploads.mkdir()
        snapshot_uploads = catalog_file.parent / 'images' / 'uploads'
        for product in catalog:
            values = product.get('image_url') or []
            for value in ([values] if isinstance(values, str) else values):
                name = Path(urlsplit(str(value)).path).name
                asset = snapshot_uploads / name
                if asset.is_file():
                    shutil.copy2(asset, uploads / name)
        environment = dict(ENABLE_BACKGROUND_JOBS='0', DISABLE_CLIP='1', DATA_DIR=str(temp), DB_PATH=str(temp / 'sales.db'),
                           PRODUCTS_FILE=str(temp / 'products.json'), OPENROUTER_API_KEY=key,
                           TELEGRAM_BOT_TOKEN='', MANYCHAT_API_KEY='', STORAGE_RESTORE_REQUIRED='0', AI_MAX_CONCURRENT='1')
        original_exists = os.path.exists
        def safe_exists(path):
            return False if Path(path).name == '.env' else original_exists(path)
        sys.path.insert(0, str(source))
        with contextlib.ExitStack() as stack, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            stack.enter_context(patch.dict(os.environ, environment))
            stack.enter_context(patch('dotenv.load_dotenv', return_value=False))
            stack.enter_context(patch('os.path.exists', side_effect=safe_exists))
            stack.enter_context(patch('requests.sessions.Session.request', new=lambda session, method, url, **kwargs: gate.request(session, method, url, **kwargs)))
            m = importlib.import_module('account_app.app')
            stack.enter_context(m.app.app_context())
            db = m.get_db()
            if getattr(args, 'incoming_assets', None):
                import base64
                assets = dict(args.incoming_assets)
                def local_image(reference):
                    if str(reference).startswith('data:'):
                        return reference
                    path = assets.get(str(reference))
                    if not path:
                        path = snapshot_uploads / Path(urlsplit(str(reference)).path).name
                    path = Path(path)
                    if not path.is_file():
                        raise FileNotFoundError('QA media is not available locally')
                    mime = 'image/png' if path.suffix.lower() == '.png' else 'image/jpeg'
                    return 'data:' + mime + ';base64,' + base64.b64encode(path.read_bytes()).decode('ascii')
                stack.enter_context(patch.object(m, '_download_image_to_data_url', side_effect=local_image))
                stack.enter_context(patch.object(m, '_image_ref_for_openrouter', side_effect=local_image))
            for setting, value in settings:
                db.execute('INSERT INTO app_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (setting, value))
            db.commit()
            # Only final delivery adapters are intercepted. Webhook routing,
            # provider parsing, validation, persistence and finalsender all run.
            def text_send(sender, text, *rest, **kwargs):
                captured.append(dict(kind='text', text=clean(text))); return True
            def image_send(sender, url, *rest, **kwargs):
                captured.append(dict(kind='image', url=str(url))); return True
            stack.enter_context(patch.object(m, 'send_text_to_facebook', side_effect=text_send))
            stack.enter_context(patch.object(m, 'send_image_to_facebook', side_effect=image_send))
            for name in ('send_order_to_telegram', 'send_telegram_message', 'send_problem_to_telegram', 'save_booking_to_file'):
                if hasattr(m, name):
                    stack.enter_context(patch.object(m, name, return_value=False))
            if hasattr(m.menger, 'deliver_due'):
                stack.enter_context(patch.object(m.menger, 'deliver_due', return_value=dict(sent=0, blocked=0)))
            for case_index, case in enumerate(cases):
                m._current_store_id.set(case['store'])
                for setting, value in [('ai_enabled', '1'), ('followup_enabled', '0'), ('smart_reviewer_enabled', '0'), ('auto_product_enabled', '0')]:
                    m.set_store_setting(db, setting, value)
                sender = case['store'] + '::synthetic-qa-' + case['id']
                m.get_or_create_customer(db, sender, 'synthetic-page', 'facebook')
                if case.get('gender'):
                    db.execute('UPDATE customers SET gender=? WHERE sender_id=?', (case['gender'], sender))
                    db.commit()
                if case.get('catalog_patch'):
                    scenario_catalog = copy.deepcopy(catalog)
                    for item in scenario_catalog:
                        if item.get('store_id') == case['store']:
                            item.update(case['catalog_patch'].get(item.get('product_id'), {}))
                    (temp / 'products.json').write_text(json.dumps(scenario_catalog, ensure_ascii=False), encoding='utf-8')
                else:
                    (temp / 'products.json').write_text(json.dumps(catalog, ensure_ascii=False), encoding='utf-8')
                products = m.load_active_products(db)
                product = next(p for p in products if p['product_id'] == case['product'])
                if not case.get('unlinked'):
                    m.complete_customer_product_link(db, sender, product, 'manual', source='manual_admin')
                for prior in case.get('history', []):
                    m.save_message(db, sender, prior['direction'], 'text', prior['text'], None, None, None,
                                   prior.get('metadata') or {})
                    if prior['direction'] == 'incoming':
                        m.capture_customer_contact(db, sender, prior['text'])
                if case.get('album') and not case.get('unlinked'):
                    selected = [next(p for p in products if p['product_id'] == pid) for pid in case['album']]
                    for item in selected:
                        m.complete_customer_product_link(db, sender, item, 'manual', source='manual_admin', preserve_existing=True)
                    m.customer_image_positions(db, sender, [dict(image_index=i + 1, product_id=p['product_id'], product_found=True,
                        product_name=p['product_name'], image_url=p.get('image_url')) for i, p in enumerate(selected)])
                turns = []
                for index, turn in enumerate(case['turns']):
                    if args.mode == 'live' and (gate.unavailable or gate.budget_stopped):
                        break
                    if getattr(args, 'turn_factory', None):
                        turn = args.turn_factory(case, turn, index, turns, gate, m, db, sender)
                    turn['multiple_images_in_context'] = sum(len(t.get('incoming_images') or []) for t in turns) + len(turn.get('incoming_images') or []) > 1
                    gate.turn = turn
                    start_capture, start_calls = len(captured), len(gate.calls)
                    started = time.monotonic()
                    event = dict(entry=[dict(id='synthetic-page', _store_id=case['store'], messaging=[dict(sender=dict(id=sender),
                        timestamp=1792800000000 + case_index * 100000 + index * 10000,
                        message=dict(mid=f'qa-{case_index}-{index}', text=turn['text']))])])
                    if turn.get('incoming_images'):
                        event['entry'][0]['messaging'][0]['message']['attachments'] = [
                            dict(type='image', payload=dict(url=url)) for url in turn['incoming_images']]
                    error = None
                    try:
                        response = m.process_webhook(db, event, use_debounce=False)
                        sent = m.send_webhook_result_to_facebook(response, sender)
                    except Exception as exc:
                        error = type(exc).__name__; sent = False
                    orders = [dict(row) for row in db.execute('SELECT * FROM orders WHERE sender_id=?', (sender,))]
                    emitted = captured[start_capture:]
                    texts = [part['text'] for part in emitted if part['kind'] == 'text']
                    sent = bool(sent or emitted)
                    checks = evaluate_turn(m, case, turn, texts, orders, sent)
                    if getattr(args, 'turn_evaluator', None):
                        checks.update(args.turn_evaluator(case, turn, index, turns, texts, orders, response if error is None else {}, m, db, sender))
                        if turn.get('allow_handoff'):
                            checks.pop('response_sent', None)
                    if case['id'].startswith('plan_'):
                        inspection_policy = (m.customer_copy.fact(product.get('inspection_policy')).strip()
                                             or m.get_delivery_settings(db).get('inspection_message') or '')
                        checks['inspection_claim_supported'] = not m.sales_strategy.inspection_claim_error(
                            '\n'.join(texts), inspection_policy)
                        minimum = 1 if (case['id'].startswith('plan_hesitation') and index == 1) else 2
                        checks['visible_message_count'] = minimum <= len(emitted) <= 5
                        if case['id'].startswith('plan_scarcity_unknown'):
                            checks['no_unverified_scarcity'] = not m.sales_strategy.persuasion_error(
                                '\n'.join(texts), product, question=turn['text'])
                        if case['id'].startswith('plan_scarcity_verified'):
                            checks['scarcity_style_present'] = bool(re.search(
                                r'قطع قليلة|قطع قليله|آخر قطع|اخر قطع|الكمية الباقية(?: منه)? قليلة', '\n'.join(texts)))
                    if turn.get('fixture_images'):
                        checks['requested_image_emitted'] = any(part['kind'] == 'image' for part in emitted)
                        expected_urls = {url for item in products if item['product_id'] in turn['fixture_images']
                                         for url in m.product_image_urls(item)}
                        actual_urls = [part['url'] for part in emitted if part['kind'] == 'image']
                        checks['correct_requested_product_images'] = bool(actual_urls) and all(url in expected_urls for url in actual_urls)
                        checks['no_duplicate_images'] = len(actual_urls) == len(set(actual_urls))
                    checks['no_pipeline_exception'] = error is None
                    if getattr(args, 'require_complete_selection', False):
                        checks['complete_internal_product_selection'] = all(call.get('complete_selection_json') for call in gate.calls[start_calls:] if call['kind'] == 'selection')
                    turns.append(dict(customer=clean(turn['text']), outgoing=texts, image_bubbles=sum(p['kind'] == 'image' for p in emitted),
                        incoming_images=turn.get('incoming_images') or [], source_message_ids=turn.get('source_message_ids') or [],
                        milestone=turn.get('milestone'), customer_simulation=turn.get('customer_simulation'),
                        local_orders=len(orders), pending_human_review=bool(m.has_pending_human_review(db, sender)),
                        elapsed_seconds=round(time.monotonic() - started, 3), ai_requests=gate.calls[start_calls:], checks=checks, error_type=error))
                    completed_turns += 1
                    progress(case['id'], turns)
                    if getattr(args, 'save_partial_transcripts', False):
                        (args.output / 'partial-results.json').write_text(json.dumps(
                            result_cases + [dict(id=case['id'], turns=turns, local_orders=len(orders))], ensure_ascii=False, indent=2), encoding='utf-8')
                orders = [dict(row) for row in db.execute('SELECT * FROM orders WHERE sender_id=?', (sender,))]
                checks = dict(expected_local_orders=len(orders) == case['expected_orders'], all_turns_completed=len(turns) == len(case['turns']))
                if getattr(args, 'case_evaluator', None):
                    checks.update(args.case_evaluator(case, turns, orders, m, db, sender))
                if orders and case['id'] == 'explicit_checkout_split_contact':
                    parsed = json.loads(orders[0].get('order_items') or '[]')
                    checks['preserved_product_size_color'] = bool(parsed and parsed[0]['product_id'] == 'P002' and str(parsed[0]['size']) == '44' and 'اسود' in m.sales_strategy.normalized(parsed[0]['color']))
                order_details = [{key: row.get(key) for key in ('product_id', 'order_items', 'province', 'product_total', 'delivery_fee', 'total_amount')} for row in orders]
                result_cases.append(dict(id=case['id'], purpose=case['purpose'], turns=turns, local_orders=len(orders), local_order_details=order_details, checks=checks))
                progress(case['id'], turns, case_complete=True)
    all_checks = [v for case in result_cases for turn in case['turns'] for v in turn['checks'].values()]
    all_checks += [v for case in result_cases for v in case['checks'].values()]
    costs = [call['reported_cost_usd'] for call in gate.calls]
    final_source_hashes = source_file_hashes(source)
    report = dict(mode=args.mode, source_root=str(source), source_fingerprint=initial_source_hashes.get('account_app/app.py'),
        source_file_hashes=initial_source_hashes, source_file_hashes_after=final_source_hashes,
        source_changed_during_run=initial_source_hashes != final_source_hashes,
        catalog_fingerprint=hashlib.sha256(catalog_file.read_bytes()).hexdigest(),
        pipeline='process_webhook -> send_webhook_result_to_facebook -> intercepted delivery adapters',
        data_scope='Synthetic people/phones/addresses only. Catalog and whitelisted settings read from snapshot. No customer tables copied.',
        no_conversion_measurement=True, checks_passed=sum(all_checks), checks_total=len(all_checks),
        deterministic_pass=bool(all_checks and all(all_checks)), scenarios=result_cases,
        ai_http_requests=len(gate.calls), http_requests=gate.calls, provider_available=gate.unavailable is None, provider_error_status=gate.unavailable,
        live_budget_stopped=gate.budget_stopped, max_live_requests=args.max_live_requests, max_live_cost_usd=args.max_live_cost_usd,
        reported_cost_usd=sum(costs) if costs and all(isinstance(cost, (float, int)) for cost in costs) else None,
        cost_scope='fixture_zero_not_provider_billing' if args.mode == 'offline' else 'provider_reported_only_null_if_any_request_omits_cost',
        blocked_external_requests=gate.blocked,
        manual_quality=dict(status='pending', required=True, rubric=['context and answer completeness', 'natural Iraqi warmth', 'truthful objection handling', 'useful next step', 'message grouping', 'consent and graceful refusal']),
        fixture_scope='Authored HTTP replies; model quality is not measured' if args.mode == 'offline' else 'Actual configured model HTTP replies; human review remains mandatory')
    progress(finished=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--mode', choices=['offline', 'live'], default='offline')
    parser.add_argument('--allow-live', action='store_true', help='Required explicit launch flag after owner approval; incurs provider usage.')
    parser.add_argument('--source-root', type=Path, default=WORKSPACE)
    parser.add_argument('--catalog', type=Path, default=WORKSPACE / 'analysis_20261024' / 'products.json')
    parser.add_argument('--settings-db', type=Path, default=WORKSPACE / 'analysis_20261024' / 'sales.db')
    parser.add_argument('--env-file', type=Path, default=WORKSPACE / 'account_app' / '.env')
    parser.add_argument('--api-key-file', type=Path, help='Read this secret file only in live mode; never included in reports or logs.')
    parser.add_argument('--output', type=Path, default=WORKSPACE / 'outputs' / 'sales-dialogues')
    parser.add_argument('--max-live-requests', type=int, default=60)
    parser.add_argument('--max-live-cost-usd', type=float, default=1.0,
                        help='Stop before the next request when cumulative provider-reported cost reaches this value. A final in-flight request can cross it; missing cost is never treated as zero billing.')
    parser.add_argument('--scenario', action='append', help='Run only a named scenario; may be repeated.')
    parser.add_argument('--repeat', type=int, default=1, help='Repeat selected scenarios with separate synthetic customers.')
    args = parser.parse_args()
    if args.max_live_requests < 1 or args.max_live_cost_usd <= 0:
        parser.error('Request and cost limits must be positive.')
    if args.repeat < 1 or args.repeat > 10:
        parser.error('Repeat count must be between 1 and 10.')
    if args.mode == 'live' and not args.allow_live:
        parser.error('Live mode requires owner approval and --allow-live. No request was sent.')
    try:
        report = run(args)
    except Exception as exc:
        print(json.dumps(dict(harness_error=type(exc).__name__, detail=clean(str(exc))), ensure_ascii=True))
        return 2
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    (args.output / 'report.md').write_text(report_markdown(report), encoding='utf-8')
    print(json.dumps(dict(mode=report['mode'], deterministic_pass=report['deterministic_pass'], passed=report['checks_passed'],
        total=report['checks_total'], scenarios=len(report['scenarios']), turns=sum(len(c['turns']) for c in report['scenarios']),
        ai_http_requests=report['ai_http_requests'], manual_quality='pending', output=str(args.output.resolve())), ensure_ascii=True))
    return 0 if report['deterministic_pass'] else 1


if __name__ == '__main__':
    sys.exit(main())
