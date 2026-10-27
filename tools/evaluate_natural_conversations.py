"""Live multi-turn acceptance: real backup phrasing + adaptive OpenRouter buyers.

Only OpenRouter requests are allowed. The app, parser, image matching, context,
checkout, database writes and final delivery planning run normally in disposable
storage. Customer delivery adapters and external order exports are intercepted.
"""
import argparse
import json
import os
from pathlib import Path
import re
import sqlite3
import time

import requests
import evaluate_sales_dialogues as harness


PHONE = harness.PHONE
ADDRESS = 'مجمع الاختبار ع سريع التجربة'
SNAPSHOT_DEFAULT = Path('C:/Users/Ghost الشبح/OneDrive/المستندات/ChatGPT/لمسة ستور/analysis_20261027')


def card(milestone, ids=(), *, exact=None, images=(), handoff=False, checks=None):
    return dict(milestone=milestone, source_message_ids=list(ids), exact=exact,
                incoming_images=list(images), allow_handoff=handoff, checks=checks or {})


def plans(snapshot):
    db = sqlite3.connect((snapshot / 'sales.db').as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    source_ids = [150505,150457,150523,150424,149861,150085,140768,146671,150493,
                  150495,150497,150492,150528,150314,149789,150346,149342,147047,
                  150058,150469,150476,149229,149236,149277,149292,149293]
    corpus = {}
    for identity in source_ids:
        row = db.execute('SELECT text FROM messages WHERE id=?', (identity,)).fetchone()
        if row:
            text = re.sub(r'(?<!\d)(?:\+?964|0)?7[\d\s-]{9,}(?!\d)', '[PHONE]', row[0])
            if identity in {150495,150497}:
                text = PHONE if identity == 150495 else 'مجمع الاختبار حولي'
            corpus[identity] = text
    db.close()
    image_assets = {}
    def archive(name):
        url = 'https://qa-images.test/' + name
        image_assets[url] = snapshot / 'media' / 'incoming' / name
        return url
    anu = archive('0d0826eb315a7c63857bae31e4c33574.jpg')
    child = archive('56b345ac875bcb8e1acdc0a1b245c4d4.jpg')
    catalog = json.loads((snapshot / 'products.json').read_text(encoding='utf-8-sig'))
    def product_image(store, pid):
        p = next(p for p in catalog if p['store_id']==store and p['product_id']==pid)
        url = p['image_url'][0] if isinstance(p['image_url'], list) else p['image_url']
        return url
    negative = Path(__file__).resolve().parents[1] / 'outputs' / 'natural-acceptance' / 'blank-negative.jpg'
    negative.parent.mkdir(parents=True, exist_ok=True)
    if not negative.exists():
        from PIL import Image
        Image.new('RGB',(600,600),'#e5e5e5').save(negative)
    unknown = 'https://qa-images.test/blank-negative.jpg'
    image_assets[unknown] = negative
    def case(identity, group, store, product, wanted_size='44', wanted_color='أسود', *, turns,
             expected_orders=1, unlinked=False, **extra):
        return dict(id=identity, group=group, store=store, product=product, expected_orders=expected_orders,
                    purpose=identity, wanted_size=wanted_size, wanted_color=wanted_color, gender='female',
                    unlinked=unlinked, turns=turns, **extra)
    def buy_finish(address_first=False):
        return [card('وافقي بوضوح على حجز قطعة واحدة بالاختيارات النهائية', [150493,147047]),
                card('أرسلي رقم الموبايل فقط', [150495], exact=PHONE, checks={'no_phone_reask':True}),
                *([card('أرسلي العنوان فقط قبل المحافظة', [150497], exact=ADDRESS),
                   card('أرسلي المحافظة فقط', [150492], exact='بغداد')]
                  if address_first else
                  [card('أرسلي المحافظة فقط', [150492], exact='بغداد'),
                   card('أرسلي العنوان فقط، لا تعيدي الموبايل أو المحافظة', [150497], exact=ADDRESS)])]
    cases = [
        case('natural-text-context','core','al-fatena','P003', turns=[
            card('اسألي عن السعر', [150505]), card('اسألي عن القماش', [150457]),
            card('حددي قياس 44', [150424]), card('اسألي عن الألوان ثم اختاري الأسود فقط', [149861]),
            card('اسألي عن الفحص والإرجاع', [150085]), *buy_finish()]),
        case('natural-hiba-landmark-before-province','core','default','P002','3XL','سماوي', turns=[
            card('اسألي عن القماش والقياسات', [150457,150424]),
            card('أجيبي عن القياس بهذه الصيغة الأصلية', [150424], exact='3اكس'),
            card('اختاري اللون السماوي بهذه الصيغة', [149861], exact='سمائي'),
            *buy_finish(address_first=True)], hiba_case=True),
        case('natural-haggling-then-booking','core','al-fatena','P003', turns=[
            card('اسألي عن السعر والقماش', [150505,150457]),
            card('حددي الأسود وقياس 44', [150424,149861]),
            card('اطلبي تخفيضاً إضافياً بصورة عفوية', [140768,150058]),
            card('اقبلي السعر الفعلي بعد الرفض واسألي عن أجور التوصيل', [150528]),
            *buy_finish()]),
        case('natural-correct-size-before-checkout','core','khuyoot','P002','46','أسود', turns=[
            card('اسألي عن السعر', [150505]), card('قولي تريدين قياس 44 أسود', [150424],exact='اريد قياس 44 اسود'),
            card('اسألي عن القماش', [150457]),
            card('صححي القياس إلى 46 بوضوح، نفس الموديل والأسود وقطعة واحدة', [150424],exact='اقصد قياس 46 بدل القياس السابق، نفس الموديل اسود قطعة وحده'),
            card('اسألي عن التوصيل والفحص دون تغيير القياس', [150528,150085]), *buy_finish()]),
        case('natural-anu-photo-followup','images','al-fatena','P017','42','أسود', unlinked=True, turns=[
            card('اسألي عن الصورة', [150176], exact='', images=[anu]),
            card('أجيبي بالقياس الأصلي', [150523], exact='قياس٤٢'),
            card('اسألي عن القماش ورباط الفستان دون طلب موديل جديد', [150457]),
            card('اختاري الأسود واسألي عن مجموع السعر مع التوصيل', [149861,150314]), *buy_finish()]),
        case('natural-child-photo-and-booking','images','baraah-kids','P009','سنة','جوزي', unlinked=True, turns=[
            card('أرسلي الصورة واسألي هل متوفر لعمر سنة', [150440,150476], exact='عمر سنة متوفر؟', images=[child]),
            card('اختاري الجوزي وقطعة واحدة لعمر سنة', [150471,150476]),
            card('اسألي عن إرجاعه مع المندوب إذا لم يناسبها', [150085]), *buy_finish()]),
        case('natural-unknown-photo-then-clear-photo','images','al-fatena','P003','44','أسود', unlinked=True, turns=[
            card('أرسلي صورة غير معروفة', exact='', images=[unknown], handoff=True),
            card('اسألي عن التوصيل بصورة مستقلة', [150528], exact='شكد التوصيل لبغداد؟', handoff=True),
            card('أرسلي صورة واضحة من الكتالوج لتحديد الموديل', exact='هذا قصدي', images=[product_image('al-fatena','P003')]),
            card('حددي الأسود وقياس 44', [150424,149861]), *buy_finish()], unknown_case=True),
        case('natural-two-photo-selection','images','al-fatena','P017','42','أسود', unlinked=True, turns=[
            card('أرسلي صورتين واسألي عن السعر والقماش لكل وحدة', [150505,150457],
                 exact='شكد سعر كل واحد وشنو قماشهن؟', images=[anu,product_image('al-fatena','P003')]),
            card('اختاري فستان الرباط فقط، قياس 42 أسود، ولا تريدين الفستان الثاني', [150523,149861],
                 exact='اريد فستان الرباط بس، اسود قياس 42 قطعة وحده'),
            card('اسألي عن الفحص لهذا الفستان', [150085]), *buy_finish()], album=['P017','P003']),
        case('natural-free-delivery-negotiation','commerce','baraah-kids','P009','سنة','جوزي', turns=[
            card('اسألي عن العمر والسعر', [150476,150505]),
            card('اختاري الجوزي لعمر سنة', [150471,150476]),
            card('اطلبي أن يجعلوا التوصيل مجانياً', [150528], exact='خلي التوصيل مجاني'),
            card('اقبلي أجور التوصيل الفعلية دون طلب التثبيت بعد', [150528], exact='تمام ماكو مشكلة بأجور التوصيل'), *buy_finish()]),
        case('natural-no-offer-no-invented-discount','commerce','khuyoot','P002','44','أسود', turns=[
            card('اسألي عن السعر', [150505]),
            card('اطلبي تخفيضاً إضافياً', [140768]),
            card('اسألي هل هذا آخر سعر، ثم اقبليه', [150058]),
            card('اختاري الأسود قياس 44', [150424,149861]), *buy_finish()], no_offer_case=True),
        case('natural-conditional-price-with-contact','commerce','al-fatena','P003','44','أسود', turns=[
            card('اسألي عن السعر والقماش', [150505,150457]),
            card('حددي الأسود قياس 44 وقطعة واحدة', [150424,149861]),
            card('اطلبي سعراً أقل وأرسلي بياناتك لكن علقي الشراء على التخفيض', [140768],
                 exact=f'اذا السعر 10 الف ثبتي {PHONE} بغداد {ADDRESS}', checks={'must_not_book':True}),
            card('وافقي الآن على السعر الحقيقي بوضوح دون أي شرط', [147047], exact='تمام ثبتي بالسعر اللي كلتيه'),
            card('اسألي متى يوصل الطلب دون إضافة قطعة', [150528]),
            card('اشكريها بعد تأكيد الحجز', [149293], exact='شكرا')], quoted_price_case=True),
        case('natural-long-compound-replies','commerce','default','P002','3XL','وردي', turns=[
            card('اسألي كل الأسئلة الموجودة بهذه الرسالة', [150505,150457,149789,150528,150085],
                 exact='شنو قماشها وشكد السعر والالوان والقياسات والتوصيل لبغداد وشلون الدفع واكدر افحص قبل ما ادفع؟',
                 checks={'long_reply_split':True}),
            card('اختاري الوردي و3XL ثم اسألي عن القماش فقط', [150424,149861,150457]),
            card('اسألي عن الإرجاع مرة واحدة دون تغيير الموديل', [150085]), *buy_finish()], long_case=True),
        case('natural-post-order-service','service','al-fatena','P003','44','أسود', turns=[
            card('حددي الأسود قياس 44 واطلبي الحجز', [150424,150493]), *buy_finish(),
            card('بعد التثبيت اطلبي تغيير القياس إلى 46، دون طلب جديد', [150424], exact='بدلي القياس إلى 46', handoff=True),
            card('اشكريها دون أي طلب إضافي', [149293], exact='شكرا', handoff=True)], post_order_case=True),
        case('natural-refusal-without-pressure','service','al-fatena','P003','44','أسود', expected_orders=0, turns=[
            card('اسألي عن القماش والسعر', [150457,150505]),
            card('اطلبي تخفيضاً إضافياً', [140768]),
            card('ارفُضي الشراء الآن بصورة واضحة', [149292], exact='لا شكرا ما اريد احجز'),
            card('اشكريها فقط ولا توافقي على شراء', [149293], exact='شكرا')], refusal_case=True),
        case('natural-sold-out-color-before-consent','boundaries','al-fatena','P017','42','أسود', turns=[
            card('اسألي عن السعر والألوان المتوفرة', [150505,149789]),
            card('اسألي عن قماش الفستان والرباط', [150457]),
            card('اطلبي الكريمي وأرسلي بياناتك، لا تقبلي الأسود بعد', exact=f'ثبتي الكريمي قياس 42 {PHONE} بغداد {ADDRESS}',
                 checks={'must_not_book':True,'sold_out_disclosed':True}),
            card('وافقي صراحة على بديل أسود واحد قياس 42', exact='تمام اريد الاسود بدل الكريمي قطعة وحده قياس 42 ثبتي'),
            card('اسألي متى يصل طلبك', [150528]), card('اشكريها', [149293], exact='شكرا')]),
        case('natural-address-is-not-courtesy','boundaries','al-fatena','P003', turns=[
            card('اسألي عن السعر والقماش', [150505,150457]),
            card('اختاري الأسود قياس 44 واطلبي حجزه', [150424,150493]),
            card('أرسلي بيانات ناقصة لا تحتوي عنواناً', exact=f'{PHONE} بغداد', checks={'must_not_book':True}),
            card('أرسلي مجاملة فقط، ليست عنواناً', exact='تمام حبيبتي', checks={'must_not_book':True}),
            card('أرسلي اللون فقط، ليس عنواناً', exact='اسود', checks={'must_not_book':True}),
            card('أرسلي العنوان الحقيقي', exact=ADDRESS), card('اشكريها', [149293], exact='شكرا')]),
        case('natural-arrival-condition-is-not-guaranteed','boundaries','al-fatena','P003',expected_orders=0,turns=[
            card('اسألي عن السعر والقماش', [150505,150457]),
            card('اختاري الأسود قياس 44 دون طلب الحجز', [150424,149861]),
            card('علقي الشراء على وصول غداً فقط', exact='اذا يوصل باجر فقط ثبتيه', checks={'must_not_book':True}),
            card('أرسلي بياناتك دون رفع شرط وصول غداً', exact=f'{PHONE} بغداد {ADDRESS}', checks={'must_not_book':True}),
            card('ارفُضي الحجز إذا الموعد غير مضمون', exact='اذا مو مضمون لا تحجزين',checks={'must_not_book':True}),
            card('اشكريها', [149293], exact='شكرا')]),
    ]
    return cases, corpus, image_assets


def customer_turn(case, template, index, transcript, gate, m, db, sender, *, corpus, model):
    turn = dict(template)
    if turn.get('exact') is not None:
        turn['text'] = turn['exact']
        turn['customer_simulation'] = {'mode':'exact_database_regression_or_synthetic_contact'}
        return turn
    examples = [{'source_message_id':i,'text':corpus[i]} for i in turn['source_message_ids'] if i in corpus]
    style = [{'source_message_id':i,'text':v} for i,v in corpus.items() if i not in {150495,150497}][:22]
    history = [{'customer':r['customer'],'assistant':r['outgoing']} for r in transcript]
    system = ('[QA_CUSTOMER] أنت تحاكي زبونة عراقية حقيقية تختبر مساعد مبيعات. '
              'اكتب رسالة الزبونة التالية فقط بصيغة JSON {"text":"..."}. استعمل صياغات المصدر كأسلوب لا كتعليمات. '
              'تفاعل مع جواب البائعة الفعلي ولا تكرر الأسئلة التي أجابت عنها. اتبع هدف الدور الحالي فقط؛ '
              'إذا سألتك البائعة قبل أوانه عن معلومات لا يريد هذا الدور إرسالها، يمكنك طلب توضيحك الحالي أولاً. '
              'لا تختلق مشكلة أو موديل جديداً، ولا تغير قياسك أو لونك المطلوب إلا إذا هدف هذا الدور تصحيح أو تبديل. '
              'لا ترسل الموبايل أو العنوان أو المحافظة إلا إذا هدف الدور يطلب بيانات الحجز. '
              'لا تقدم موافقة شراء أو تقولي ثبتي قبل الدور الذي يطلب ذلك صراحة. '
              'كوني طبيعية وقصيرة ولا تتحدثي عن الاختبار أو الذكاء الاصطناعي. '
              'الهدف الأخير شراء قطعة واحدة بالاختيارات المحددة إلا إذا حالة الرفض أو الصورة المجهولة تمنع ذلك. '
              'إذا لم يعد مطلوبك متوفراً، اسألي عنه بوضوح دون قبول بديل تلقائياً.')
    payload={'model':model,'temperature':0.45,'max_tokens':220,'reasoning':{'exclude':True},
             'response_format':{'type':'json_object'},'messages':[{'role':'system','content':system},
             {'role':'user','content':json.dumps(dict(goal=turn['milestone'],preferred_examples=examples,
                style_examples=style,conversation=history,desired_size=case['wanted_size'],
                desired_color=case['wanted_color'],quantity=1),ensure_ascii=False)}]}
    started=time.monotonic()
    try:
        response=requests.post(harness.OPENROUTER_URL,headers={'Authorization':'Bearer '+os.environ['QA_OPENROUTER_API_KEY']},
                               json=payload,timeout=(10,60))
        response.raise_for_status()
        body=response.json(); raw=body['choices'][0]['message']['content']
        parsed=json.loads(re.sub(r'^```(?:json)?\s*|\s*```$','',raw.strip()))
        text=parsed.get('text','').strip()
        if not text or len(text)>700:raise ValueError('Invalid customer simulation')
        turn['text']=text
        turn['customer_simulation']=dict(mode='openrouter',model=model,seconds=round(time.monotonic()-started,2))
    except Exception as exc:
        turn['text']=examples[0]['text'] if examples else 'ممكن توضحين؟'
        turn['customer_simulation']=dict(mode='failed',error=type(exc).__name__)
    return turn


def evaluate_turn(case, turn, index, prior, texts, orders, response, m, db, sender):
    reply='\n'.join(texts)
    result={'customer_simulation_succeeded':turn.get('customer_simulation',{}).get('mode')!='failed',
            'no_unexecuted_reservation_claim':bool(orders) or not m.claims_order_created(reply),
            'no_empty_photo_placeholder':not m.owner_sales.FORBIDDEN_REPLY.search(m.customer_copy.normalized(reply)),
            'no_policy_filler':not re.search(r'حسم هالشرط|حسب سياسة المتجر|قبل حسم',reply)}
    if turn.get('checks',{}).get('must_not_book'):
        result['no_order_during_conditional_haggling']=not orders
    if turn.get('checks',{}).get('sold_out_disclosed'):
        result['sold_out_color_disclosed']=bool(re.search(r'خلص|نافد|مو متوفر|غير متوفر|باقي بس',reply))
    if PHONE in turn['text'] or any('[SYNTHETIC_PHONE]' in t['customer'] for t in prior):
        result['saved_phone_never_requested_again']=not m.sales_context.reasks_contact(reply,dict(phone=PHONE))
    if turn.get('incoming_images') and not (case.get('unknown_case') and index<2):
        linked=m.load_customer_products(db,sender)
        linked_ids={p.get('product_id') for p in linked}
        result['actual_photo_linked_expected_product']=case['product'] in linked_ids
    if turn.get('checks',{}).get('long_reply_split'):
        result['long_response_has_multiple_bubbles']=len(texts)>=2
    if turn.get('allow_handoff') and not texts:
        result['silent_turn_has_real_review']=bool(m.has_pending_human_review(db,sender))
    if case.get('refusal_case') and index>=2:
        result['no_pressure_after_refusal']=not re.search(r'(?:ثبت|حجز|رقم|عنوان).{0,25}[؟?]',reply)
    if case.get('no_offer_case'):
        result['no_invented_offer']=not re.search(r'عليه عرض|صار سعره|خصم خاص|عرض هالفترة',reply)
    result['at_most_one_order']=len(orders)<=1
    return result


def evaluate_case(case, turns, orders, m, db, sender):
    result={'multi_turn_conversation':len(turns)>=4,
            'multiple_real_customer_generations':sum(t.get('customer_simulation',{}).get('mode')=='openrouter' for t in turns)>=2}
    if orders:
        items=json.loads(orders[0].get('order_items') or '[]')
        result['single_chosen_item_only']=len(items)==1 and items[0]['product_id']==case['product'] and int(items[0]['quantity'])==1
        result['chosen_color_preserved']=bool(items and m.product_variants.single_color(items[0].get('color'))==m.product_variants.single_color(case['wanted_color']))
        actual=items[0].get('size','') if items else ''
        wanted=case['wanted_size']
        result['chosen_size_preserved']=(m.product_variants.child_age(actual)==m.product_variants.child_age(wanted)
            if wanted=='سنة' else m.product_variants.normalize_size_words(str(actual)).upper()==wanted.upper())
        result['phone_preserved']=orders[0]['phone']==PHONE
        result['address_not_replaced_by_negotiation']=not re.search(r'اذا|تخفيض|السعر|ثبتي',orders[0]['address'])
        catalog=m.load_active_products(db)
        q=m.price_order(items,catalog,m.delivery_fee_for_province(orders[0]['province'],db))
        result['exact_catalog_total']=orders[0]['total_amount']==q['product_total']+q['delivery_fee']
    return result


def write_report(report, output):
    lines=['# اختبار محادثات طبيعية عبر OpenRouter','',
           f"المحادثات: {len(report['scenarios'])} — الأدوار: {sum(len(c['turns']) for c in report['scenarios'])}",
           f"الفحوص الناجحة: {report['checks_passed']} / {report['checks_total']}",
           f"طلبات OpenRouter الفعلية: {report['ai_http_requests']} — التكلفة المبلغة: {report['reported_cost_usd']}",
           '', 'الموبايل والهوية والعنوان تجريبية. الإرسال للزبائن وتصدير الطلبات محجوبان؛ قاعدة المصدر للقراءة فقط.',
           'ردود الزبون غير الثابتة مولدة فعلياً من OpenRouter بناءً على جواب النظام السابق، بأسلوب رسائل المصدر.', '']
    for case in report['scenarios']:
        lines += ['## '+case['id'],f"الطلبات المحلية: {case['local_orders']}"]
        failures=[k for k,v in case['checks'].items() if not v]
        if failures:lines+=['فشل التحقق: '+', '.join(failures)]
        for index,turn in enumerate(case['turns'],1):
            lines+=['',f"**الدور {index} — الزبونة:** {turn['customer']}"]
            if turn['incoming_images']:lines+=['صورة واردة: '+', '.join(Path(url).name for url in turn['incoming_images'])]
            lines += ['**النظام:** '+(' | '.join(turn['outgoing']) or '[مراجعة بشرية / لا رد]')]
            failed=[k for k,v in turn['checks'].items() if not v]
            if failed:lines+=['فشل: '+', '.join(failed)]
    (output/'report.md').write_text('\n'.join(lines),encoding='utf-8')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot',type=Path,default=SNAPSHOT_DEFAULT)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--group',choices=['core','images','commerce','service','boundaries','all'],default='all')
    parser.add_argument('--case',action='append')
    parser.add_argument('--customer-model',default='google/gemini-2.5-flash')
    parser.add_argument('--max-cost',type=float,default=0.75)
    parser.add_argument('--allow-live',action='store_true')
    args=parser.parse_args()
    if not args.allow_live:parser.error('--allow-live is required')
    cases,corpus,assets=plans(args.snapshot.resolve())
    cases=[c for c in cases if args.group=='all' or c['group']==args.group]
    if args.case:cases=[c for c in cases if c['id'] in args.case]
    if not cases:parser.error('No cases selected')
    harness.scenarios=lambda catalog:cases
    config=argparse.Namespace(mode='live',source_root=Path(__file__).resolve().parents[1],
        catalog=args.snapshot/'products.json',settings_db=args.snapshot/'sales.db',env_file=Path('absent.env'),
        api_key_file=None,output=args.output.resolve(),max_live_requests=180,max_live_cost_usd=args.max_cost,
        scenario=None,repeat=1,copy_saved_prompts=True,incoming_assets=assets,
        save_partial_transcripts=True,require_complete_selection=True,case_evaluator=evaluate_case,turn_evaluator=evaluate_turn,
        turn_factory=lambda *params:customer_turn(*params,corpus=corpus,model=args.customer_model))
    report=harness.run(config)
    report['data_scope']='Actual backup question styles and source IDs, adaptive OpenRouter customer turns, synthetic contacts. Real image recognition on archived product screenshots and catalog references. All customer sends intercepted.'
    report['customer_model']=args.customer_model
    report['source_phrases']=corpus
    report['coverage_group']=args.group
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    write_report(report,args.output)
    print(json.dumps(dict(group=args.group,passed=report['checks_passed'],total=report['checks_total'],
        cases=len(report['scenarios']),turns=sum(len(c['turns']) for c in report['scenarios']),
        requests=report['ai_http_requests'],cost=report['reported_cost_usd'],passed_all=report['deterministic_pass']),ensure_ascii=True))
    return 0 if report['deterministic_pass'] else 1


if __name__=='__main__':
    raise SystemExit(main())
