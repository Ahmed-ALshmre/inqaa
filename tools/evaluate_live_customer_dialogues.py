"""Read-only live selection and isolated model replay. Private exports stay in outputs.

collect screens bounded recent inbox pages, follows message pagination and ranks
by explicit complexity indicators. prepare/run never post to the live application.
No image bytes or original customer identities are sent to the model.
"""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys
import time
from datetime import datetime
import os
import sqlite3
import tempfile
import threading
import hashlib
from concurrent.futures import ThreadPoolExecutor,as_completed

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tools.live_system import Client
from account_app import checkout

ROOT=Path('outputs/hard50-live-audit')
RESULT_LABEL=os.environ.get('QA_RESULT_LABEL','')
if RESULT_LABEL and not re.fullmatch(r'[A-Za-z0-9_-]{1,40}',RESULT_LABEL):raise ValueError('Invalid QA_RESULT_LABEL')
RESULTS=ROOT/('results-'+RESULT_LABEL if RESULT_LABEL else 'results')
RUNTIME_HASHES={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path('account_app/sales_runtime').glob('*.py'))}


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf8')
    temporary.replace(path)


def load(path):return json.loads(path.read_text(encoding='utf8'))


def records(client,resource,**params):
    import requests
    for attempt in range(3):
        try:return [row for page in client.pages(resource,params) for row in page['items']]
        except requests.RequestException:
            if attempt==2:raise
            time.sleep(2)


def inbox_score(row):
    return (min(int(row.get('pending_reviews_count') or 0),3)*4+
            min(int(row.get('system_issue_count') or 0),3)*5+
            int(bool(row.get('ai_job_error')))*6+int(bool(row.get('phone') and row.get('address')))*7+
            int(bool(row.get('unanswered')))*3+int(bool(row.get('product_ids') and ',' in row['product_ids']))*5)


def complexity(messages,row):
    texts=[checkout.normalized(m.get('text','')) for m in messages if m['direction']=='incoming' and m.get('text')]
    text='\n'.join(texts)
    checks={
        'تصحيح الاختيار':bool(re.search(r'بدل|غيرت|غيري|مو هذا|لا اريد|ماروني.{0,80}ازرق',text)),
        'عدة قطع':bool(re.search(r'قطعتين|قطعتان|اثنين|ثلاث|قطعه.{0,40}قطعه|اللونين|وحده.{0,40}وحده',text)),
        'مقاس أو وزن أو عمر':bool(re.search(r'قياس|وزن|كيلو|عمر|سنوات|سنه|اشهر',text)),
        'مساومة أو شرط':bool(re.search(r'خصم|تخفيض|تنزل|اذا|بشرط|غالي|اخر سعر',text)),
        'بيانات حجز':bool(checkout.phone_number(text) or row.get('phone') or row.get('address')),
        'عنوان أو توصيل':bool(re.search(r'عنوان|محافظ|توصيل|يوصل|قرب|بغداد|بصره',text)),
        'صورة أو صوت':any(m.get('message_type') not in {'text','sticker'} for m in messages if m['direction']=='incoming'),
        'مراجعة أو توقف':bool(row.get('pending_reviews_count') or row.get('system_issue_count') or row.get('ai_job_error')),
        'خدمة طلب سابق':bool(re.search(r'الغ[يى]|وين طلبي|وين صار|تبديل|وصلني|تالف|راجع',text)),
    }
    score=sum(5 for v in checks.values() if v)+min(len(texts),30)+min(inbox_score(row),12)
    return score,[k for k,v in checks.items() if v],len(texts)


def collect():
    client=Client()
    manifest=client.get('manifest');save(ROOT/'manifest.json',manifest)
    stores=client.get('stores')['items'];save(ROOT/'stores.json',stores)
    pool=[]
    for store in stores:
        sid=store['store_id']
        save(ROOT/'raw'/f'products-{sid}.json',client.get('products',dict(store_id=sid)))
        save(ROOT/'raw'/f'settings-{sid}.json',client.get('settings',dict(store_id=sid)))
        cursor={}
        for page_index in range(2):
            page=client.get('inbox',dict(store_id=sid,limit=100,**cursor))
            rows=page['conversations'];pool.extend(rows)
            if not page['has_more'] or not rows:break
            cursor=dict(cursor_time=rows[-1]['last_time'],cursor_sender=rows[-1]['sender_id'])
        print(f'Screened store {sid}',flush=True)
    save(ROOT/'raw'/'inbox.json',pool)
    shortlist=[]
    for store in stores:
        rows=sorted([r for r in pool if r['store_id']==store['store_id']],key=inbox_score,reverse=True)
        # Include high-complexity flags and recent unflagged cases, not reviews alone.
        chosen=rows[:15]
        chosen += [r for r in rows if r not in chosen][:5]
        shortlist.extend(chosen)
    ranked=[]
    for index,row in enumerate(shortlist):
        messages=records(client,'messages',store_id=row['store_id'],sender_id=row['sender_id'],order='asc')
        score,tags,count=complexity(messages,row)
        record=dict(inbox=row,messages=messages,score=score,tags=tags,incoming_text_count=count)
        save(ROOT/'raw'/'screened'/f'{index:03}.json',record)
        ranked.append(dict(file=f'{index:03}.json',score=score,tags=tags,
                           store_id=row['store_id'],incoming_text_count=count))
        if (index+1)%10==0:print(f'Read {index+1}/{len(shortlist)} complete message histories',flush=True)
    ranked.sort(key=lambda r:r['score'],reverse=True)
    selected=ranked[:50]
    save(ROOT/'screening.json',dict(inbox_count=len(pool),details_count=len(ranked),
        method='Latest up to 200 active inboxes per store; 20 highest flagged candidates per store, then ranked by dialogue complexity. Not an exhaustive hardest-of-all-database claim.',
        selected=selected,ranked=ranked))
    enrich(client)


def enrich(client=None):
    client=client or Client()
    for index,item in enumerate(load(ROOT/'screening.json')['selected']):
        if (ROOT/'raw'/'hard50'/f'{index+1:02}.json').exists():continue
        record=load(ROOT/'raw'/'screened'/item['file'])
        row=record['inbox']
        for resource in ('product_interests','ad_context','orders'):
            record[resource]=records(client,resource,store_id=row['store_id'],sender_id=row['sender_id'])
        save(ROOT/'raw'/'hard50'/f'{index+1:02}.json',record)
        if (index+1)%5==0:print(f'Enriched {index+1}/50 hard cases',flush=True)
    print('Live collection complete; no operational writes performed.',flush=True)


def scrub(text,row,number):
    text=str(text or '')
    phone='0771000'+f'{number:04}'
    province=row.get('province') or checkout.contact_fields(text).get('province') or 'بغداد'
    address=province+' حي الاختبار قرب مدرسة التجربة'
    for field in ('phone','address','name'):
        value=str(row.get(field) or '').strip()
        if len(value)>=3:
            text=text.replace(value,phone if field=='phone' else address if field=='address' else '[اسم محذوف]')
    text=re.sub(r'https?://\S+','[رابط محذوف]',text)
    text=re.sub(r'(?:اسمي|الاسم)\s*[:：]?\s*[^\n،,؟?]{2,60}','[اسم محذوف]',text)
    # Never send a customer's street/landmark, even when live contact extraction missed it.
    normalized=checkout.normalized(text)
    location=r'(?<!\w)(?:حي|منطقة|منطقه|شارع|محلة|محله|زقاق|قرب|مقابل|مجمع|قرية|قريه)\s+[^\n،,؟?]{3,}'
    text=re.sub(location,'حي الاختبار قرب مدرسة التجربة',text)
    province_in_text=checkout.contact_fields(text).get('province')
    if province_in_text and len(text.split())>=3 and not re.search(r'شكد|شنو|توصيل|يوصل|سعر|عندكم|قياس|وزن|اريد',normalized):
        intent=' ثبتيلي الطلب' if checkout.explicit_purchase(text) else ''
        text=province_in_text+' حي الاختبار قرب مدرسة التجربة'+intent
    digits=checkout.normalized(text)
    # Replace valid phone numbers first, including Arabic digit spelling.
    text=checkout.PHONE.sub(phone,digits)
    text=re.sub(r'(?<!\d)\+?\d[\d ()-]{5,22}\d(?!\d)',
                lambda m: phone if checkout.phone_number(m.group()) else '0'*len(re.sub(r'\D','',m.group())),text)
    return text.strip()


def safe_product(product):
    # No image URLs, video URLs, attachment bytes or customer identifiers leave here.
    return {k:(re.sub(r'https?://\S+','[رابط محذوف]',v) if isinstance(v,str) else v)
            for k,v in product.items() if 'url' not in k and k not in {'image_colors','faq'}}


def episodes(messages):
    groups=[];current=[];last=None
    for message in sorted(messages,key=lambda m:m['id']):
        try:stamp=datetime.fromisoformat(message['created_at']).timestamp()
        except (ValueError,TypeError):stamp=last
        if last is not None and stamp is not None and stamp-last>12*3600:
            if current:groups.append(current)
            current=[]
        current.append(message)
        if stamp is not None:last=stamp
    if current:groups.append(current)
    return groups


def prepare():
    screening=load(ROOT/'screening.json')
    catalogue={sid['store_id']:[safe_product(p) for p in load(ROOT/'raw'/f'products-{sid["store_id"]}.json')['items']]
               for sid in load(ROOT/'stores.json')}
    candidates=[];index_lines=['# فهرس 50 محادثة صعبة من السيرفر','',screening['method'],'']
    for number,path in enumerate(sorted((ROOT/'raw'/'hard50').glob('*.json')),1):
        record=load(path);row=record['inbox'];sid=row['store_id']
        index_lines.append(f'{number}. {row["store_name"]} — {row["name"]} — درجة {record["score"]} — '+ '، '.join(record['tags']))
        possible=[]
        for group in episodes(record['messages']):
            incoming=[m for m in group if m['direction']=='incoming' and m.get('message_type')=='text' and str(m.get('text') or '').strip()]
            if 4<=len(incoming)<=18:
                score,tags,count=complexity(group,row)
                possible.append((score,incoming,group,tags))
        if not possible:continue
        score,incoming,group,tags=max(possible,key=lambda x:x[0])
        product_ids=[]
        for interest in record.get('product_interests',[]):
            pid=interest.get('product_id')
            if pid not in product_ids and any(p['product_id']==pid for p in catalogue[sid]):product_ids.append(pid)
        for pid in str(row.get('product_ids') or '').split(','):
            if pid.strip() and pid.strip() not in product_ids and any(p['product_id']==pid.strip() for p in catalogue[sid]):
                product_ids.append(pid.strip())
        if not product_ids:continue
        first_ad=next((dict(ad_id=m['ad_id']) for m in group if m.get('ad_id')),None)
        candidates.append(dict(source_number=number,store_id=sid,store_name=row['store_name'],
            score=score,tags=tags,product_ids=product_ids,initial_ad=first_ad,
            binding='saved_product_link_assumed' if not first_ad else 'recorded_ad',
            omitted_media=sum(m['direction']=='incoming' and m.get('message_type')!='text' for m in group),
            turns=[dict(text=scrub(m['text'],row,number),source_message_id=m['id']) for m in incoming],
            source_start=group[0]['created_at'],source_end=group[-1]['created_at']))
    candidates.sort(key=lambda x:x['score'],reverse=True)
    selected=[];counts=Counter()
    # Balance the available stores; then fill by difficulty when a store has fewer cases.
    for cap in (4,20):
        for case in candidates:
            if case not in selected and counts[case['store_id']]<cap and len(selected)<20:
                selected.append(case);counts[case['store_id']]+=1
    save(ROOT/'dataset.json',dict(cases=selected,catalogue=catalogue,selection=dict(
        source_cases=50,eligible=len(candidates),tested_target=20,stores=dict(counts)),
        limitations=['Historical customer turns are replayed unchanged in meaning except deidentification; replies can diverge from the historical conversation.',
                      'Current catalogue snapshot, not point-in-time historical stock or prices.',
                      'Saved product links are supplied as a known-context assumption when ad metadata is absent; final historical orders are never supplied to the model.',
                      'Text-only replay; no photos, audio or video uploaded. Omitted media is counted.']))
    (ROOT/'فهرس-50-محادثة.md').write_text('\n'.join(index_lines),encoding='utf8')
    print(json.dumps(dict(prepared=len(selected),eligible=len(candidates),stores=dict(counts),
                          turns=sum(len(c['turns']) for c in selected)),ensure_ascii=False),flush=True)


def initialize_test_db(path):
    from account_app import product_questions
    db=sqlite3.connect(path)
    source=Path('account_app/app.py').read_text(encoding='utf8')
    for table in ('orders','human_reviews'):
        db.executescript(re.search(r'CREATE TABLE IF NOT EXISTS '+table+r' \(.*?\);',source,re.S)[0])
        db.execute(f'ALTER TABLE {table} ADD COLUMN store_id TEXT')
    for field in ('product_total','delivery_fee','total_amount'):
        db.execute(f'ALTER TABLE orders ADD COLUMN {field} INTEGER')
    product_questions.init(db);db.commit()
    return db


def run_case(number,case,dataset,stop):
    from account_app.sales_runtime.entrypoint import Entrypoint
    from account_app.sales_runtime.gateway import DecisionGateway,WriterGateway
    from account_app.sales_runtime.store import Scope
    from account_app.sales_runtime.orders import CONFIRMATION
    import copy
    result=dict(case=number,source_number=case['source_number'],store_id=case['store_id'],
                store_name=case['store_name'],tags=case['tags'],binding=case['binding'],
                omitted_media=case['omitted_media'],source_start=case['source_start'],
                source_end=case['source_end'],turns=[])
    result['runtime_hashes']=RUNTIME_HASHES
    products=copy.deepcopy(dataset['catalogue'][case['store_id']])
    ad=case.get('initial_ad')
    if not ad and len(case['product_ids'])==1:
        ad=dict(ad_id=f'qa-saved-reference-{number}')
        for product in products:
            if product['product_id']==case['product_ids'][0]:
                product['ad_ids']=list(product.get('ad_ids') or [])+[ad['ad_id']]
    settings=load(ROOT/'raw'/f'settings-{case["store_id"]}.json')['items'][0]['values']
    def fee(sid,province):
        return int(settings.get('delivery_baghdad_fee' if province=='بغداد' else 'delivery_other_fee') or 0)
    with tempfile.TemporaryDirectory() as temporary:
        db=initialize_test_db(str(Path(temporary)/'qa.db'))
        model=os.environ.get('QA_DECISION_MODEL','google/gemini-3.8-flash')
        key=os.environ['QA_OPENROUTER_API_KEY']
        entry=Entrypoint(db,lambda sid:products,fee,DecisionGateway(model,key),
                         writer=WriterGateway(os.environ.get('QA_WRITER_MODEL',model),key),
                         confirmation=settings.get('inspection_message') or CONFIRMATION)
        entry.migrate()
        scope=Scope(case['store_id'],'chatwoot','isolated-qa-account',f'qa-live-{number:02}')
        for index,source in enumerate(case['turns']):
            turn=dict(customer=source['text'],source_message_id=source['source_message_id'],bot=[])
            result['turns'].append(turn)
            if stop.is_set():turn['action']='provider_budget_stopped';continue
            start=time.monotonic()
            try:
                outcome=entry.receive(store_id=scope.store,channel=scope.channel,account_id=scope.account,
                                      sender_id=scope.sender,message_id=f'm{index}',text=source['text'],ad=ad)
                if outcome['action']=='service_failure' and any(code in outcome.get('error_code','') for code in ('503','429','transport')):
                    turn['provider_retry']=outcome['error_code']
                    time.sleep(2)
                    outcome=entry.receive(store_id=scope.store,channel=scope.channel,account_id=scope.account,
                                          sender_id=scope.sender,message_id=f'm{index}',text=source['text'],ad=ad)
                turn['action']=outcome['action'];turn['outcome']=outcome
                while True:
                    part=entry.store.claim_reply(scope)
                    if not part:break
                    turn['bot'].append(part['payload'].get('text','[صورة كتالوج مفترضة]'))
                    entry.store.settle_reply(scope,part['id'],'accepted',f'qa-simulated-{part["id"]}')
                    if part['payload'].get('purpose')=='checkout_contacts':
                        entry.runtime.accept_checkout_prompt(scope,f'prompt:{part["id"]}',
                            entry.store.snapshot(scope)['version'],part['id'])
                # Simulate viewing the saved confirmation once. Never call any sender.
                for task_id,payload in db.execute("SELECT id,payload FROM sales_v2_order_tasks WHERE kind='confirmation' AND status='queued'").fetchall():
                    turn['bot'].append(json.loads(payload)['text'])
                    db.execute("UPDATE sales_v2_order_tasks SET status='accepted',provider_id='qa-simulated' WHERE id=?",(task_id,))
                db.commit()
                turn['state']=entry.store.snapshot(scope)['data']
                if outcome['action']=='service_failure' and any(code in outcome.get('error_code','') for code in ('401','402')):
                    stop.set()
            except Exception as exc:
                turn['action']='exception';turn['error']=type(exc).__name__+': '+str(exc)
            turn['seconds']=round(time.monotonic()-start,2)
            save(RESULTS/f'{number:02}.json',result)
            print(f'Case {number:02}, turn {index+1}/{len(case["turns"])}: {turn["action"]}',flush=True)
        cursor=db.execute('SELECT * FROM orders')
        result['orders']=[dict(zip([d[0] for d in cursor.description],row)) for row in cursor.fetchall()]
        result['reviews']=[dict(reason=r[0],status=r[1]) for r in db.execute('SELECT reason,status FROM human_reviews')]
        result['model_attempts']=[dict(stage=r[0],status=r[1],error_code=r[2]) for r in db.execute('SELECT stage,status,error_code FROM sales_v2_model_attempts')]
        result['contract_diagnostics']=entry.runtime.decide.failures
        result['final_state']=entry.store.snapshot(scope)['data']
        db.close()
    save(RESULTS/f'{number:02}.json',result)
    return result


def report():
    results=[load(p) for p in sorted(RESULTS.glob('*.json'))]
    counts=Counter(t['action'] for r in results for t in r['turns'])
    claims=[]
    for case in results:
        for index,turn in enumerate(case['turns'],1):
            text='\n'.join(turn.get('bot',[]))
            if ((not turn.get('state',{}).get('order_id') or turn.get('state',{}).get('order_status')=='cancelled') and
                    re.search(r'ثبتت(?:لج|لك|لچ)|طلب(?:ج|ك)\s+مثبت|حجزت(?:لج|لك)|تم\s+تثبيت|تثبت\s+(?:طلبج|طلبك|الطلب)',checkout.normalized(text))):
                claims.append(dict(case=case['case'],turn=index,text=text))
    summary=dict(cases=len(results),turns=sum(len(r['turns']) for r in results),actions=dict(counts),
                 cases_without_budget_stop=sum(not any(t['action']=='provider_budget_stopped' for t in r['turns']) for r in results),
                 turns_not_executed=counts.get('provider_budget_stopped',0),
                 test_orders=sum(len(r.get('orders',[])) for r in results),
                 cases_with_orders=sum(bool(r.get('orders')) for r in results),
                 cases_with_reviews=sum(bool(r.get('reviews')) for r in results),
                 decision_calls=sum(sum(a['stage']=='decision' for a in r.get('model_attempts',[])) for r in results),
                 repair_calls=sum(sum(a['stage']=='repair' for a in r.get('model_attempts',[])) for r in results),
                 false_booking_claims=claims)
    save(ROOT/(f'summary-{RESULT_LABEL}.json' if RESULT_LABEL else 'summary.json'),summary)
    labels={'human':'متوقفة بانتظار الموظف','needs_review':'مراجعة تقنية بعد استنفاد التصحيح',
            'service_failure':'تعذر المزود','exception':'خطأ داخلي','provider_budget_stopped':'لم ينفذ بسبب توقف رصيد/تفويض المزود',
            'continue':'استمرار الحوار','booked':'طلب محفوظ','cancelled':'إلغاء محفوظ','superseded':'رد قديم ألغته رسالة أحدث'}
    lines=['# اختبار أسئلة الزبائن من السيرفر — محاكاة معزولة','',
           'المصدر: 50 محادثة صعبة اختيرت من فحص 100 سجل بالتفصيل و1000 محادثة نشطة. أُعيدت أسئلة 20 محادثة بعد إخفاء الأسماء والأرقام والعناوين.',
           'الموديل: google/gemini-3.8-flash للقرار والصياغة. لا إرسال للزبائن ولا تجهيز ولا إنشاء طلبات إنتاج.',
           'هذه إعادة تسلسل لأسئلة تاريخية؛ بعض «نعم/تمام» تعتمد على رد قديم مختلف. ربط منتج محفوظ يُفترض حين لا توجد إحالة إعلان. الصور والصوت غير مختبرين؛ الكتالوج الحالي قد يختلف عن كتالوج وقت المحادثة.','',
           '## ملخص التنفيذ','',
           f'المحادثات المدرجة: {summary["cases"]}. رسائل المجموعة: {summary["turns"]}. غير المنفذ بسبب توقف المزود: {summary["turns_not_executed"]}. طلبات الاختبار الفعلية: {summary["test_orders"]}.',
           f'محاولات القرار: {summary["decision_calls"]}. تصحيحات الأدوات: {summary["repair_calls"]}.',
           f'الحالات التي انتهت بمراجعة: {summary["cases_with_reviews"]}. ادعاءات تثبيت لم يقابلها طلب في الفحص النصي: {len(claims)}.',
           'تفاصيل الأعداد وأدلة الادعاءات في summary.json. المرور على رسالة بعد التوقف البشري لا يعني أن الموديل ولّد جوابًا لها.','']
    for result in results:
        lines.extend([f'## المحادثة {result["case"]:02} — {result["store_name"]}',
            f'رقمها بفهرس الـ50: {result["source_number"]}. المحاور: '+ '، '.join(result['tags']),
            f'سياق المنتج: {result["binding"]}. رسائل ميديا لم تُرسل للموديل: {result["omitted_media"]}.',''])
        for index,turn in enumerate(result['turns'],1):
            lines.extend([f'**الزبون {index}:** '+turn['customer'],''])
            for text in turn.get('bot',[]):lines.extend(['**البوت:** '+text,''])
            if not turn.get('bot'):lines.extend(['**حالة الاختبار، وليست رسالة للزبون:** '+labels.get(turn['action'],turn['action']),''])
            if turn.get('error'):lines.extend(['**خطأ الاختبار:** '+turn['error'],''])
        lines.extend([f'طلبات الاختبار المحفوظة: {len(result.get("orders",[]))}. مراجعات بشرية: {len(result.get("reviews",[]))}.',''])
        for review in result.get('reviews',[]):lines.extend(['سبب المراجعة: '+review['reason'],''])
    (ROOT/(f'المحادثات-20-{RESULT_LABEL}.md' if RESULT_LABEL else 'المحادثات-20-كاملة.md')).write_text('\n'.join(lines),encoding='utf8')
    print(json.dumps(summary,ensure_ascii=False),flush=True)


def run():
    if not os.environ.get('QA_OPENROUTER_API_KEY'):raise SystemExit('QA_OPENROUTER_API_KEY required')
    if not RESULT_LABEL and any(RESULTS.glob('*.json')):
        raise SystemExit('Use a new QA_RESULT_LABEL to preserve the original baseline')
    dataset=load(ROOT/'dataset.json');stop=threading.Event()
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs=[]
        for number,case in enumerate(dataset['cases'],1):
            existing=RESULTS/f'{number:02}.json'
            if existing.exists():
                prior=load(existing)
                if prior.get('runtime_hashes')!=RUNTIME_HASHES:
                    raise ValueError('Runtime changed: select a new QA_RESULT_LABEL; do not mix revisions')
                if 'final_state' in prior and not any(t['action'] in {'provider_budget_stopped','service_failure','exception'} for t in prior['turns']):continue
                save(ROOT/'previous-runs'/RESULT_LABEL/f'{number:02}-{time.time_ns()}.json',prior)
            jobs.append(pool.submit(run_case,number,case,dataset,stop))
        for job in as_completed(jobs):job.result()
    report()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['collect','enrich','prepare','run','report'])
    args=parser.parse_args()
    if args.mode=='collect':collect()
    elif args.mode=='enrich':enrich()
    elif args.mode=='prepare':prepare()
    elif args.mode=='run':run()
    elif args.mode=='report':report()


if __name__=='__main__':main()
