"""Replay opening-question routing against an isolated copy of the supplied backup.

No customer sends or external model calls. Model wording is a fixture; catalog
loading, store selection, message storage, routing and product bindings are real.
"""
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import uuid
import zipfile
from collections import Counter
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
source = Path(os.environ['USERPROFILE'])/'Downloads'/'lamsa-store-backup-20260909-105954.zip'
destination = Path(__file__).parent
before = hashlib.sha256(source.read_bytes()).hexdigest()
records = []
def record(group, case, expected, actual):
    records.append(dict(group=group, case=case, expected=expected, actual=actual, passed=expected == actual))

with tempfile.TemporaryDirectory(prefix='lamsa-ad-audit-') as directory, ExitStack() as stack:
    root = Path(directory)
    with zipfile.ZipFile(source) as archive:
        for member, local in [('sales.db','sales.db'), ('products.json','products.json'),
                              ('ai/instructions.txt','instructions.txt'), ('ai/gemini_sales_playbook.md','gemini_sales_playbook.md')]:
            (root/local).write_bytes(archive.read(member))
    os.environ.update(DATA_DIR=directory, DB_PATH=str(root/'sales.db'), PRODUCTS_FILE=str(root/'products.json'),
                      BOOKINGS_FILE=str(root/'bookings.jsonl'), ENABLE_BACKGROUND_JOBS='0', DISABLE_CLIP='1')
    network = stack.enter_context(patch('requests.sessions.Session.request', side_effect=RuntimeError('External requests blocked by audit')))
    import account_app.app as m
    stack.enter_context(m.app.app_context())
    db=m.get_db()
    catalog=m.load_products_from_file(all_stores=True)
    archived_orders=db.execute('SELECT count(*) FROM orders').fetchone()[0]
    history=db.execute("SELECT m.store_id,m.text,m.message_type FROM messages m WHERE direction='incoming' AND id=(SELECT MIN(id) FROM messages WHERE sender_id=m.sender_id AND direction='incoming')").fetchall()
    for name in ['send_telegram_message','send_webhook_result_to_facebook','send_product_image_if_available']:
        stack.enter_context(patch.object(m,name,return_value=False))
    stack.enter_context(patch.object(m,'is_ai_enabled',return_value=True))
    stack.enter_context(patch.object(m,'is_customer_ai_enabled',return_value=True))
    stack.enter_context(patch.object(m,'is_store_feature_enabled',return_value=False))
    stack.enter_context(patch.object(m,'generate_first_message_reply',return_value=('حددي الموديل المطلوب','')))
    captures=[]
    def model(*args, **kwargs):
        product=args[5]
        captures.append((product or {}).get('product_id'))
        return {'reply': f"نتابع {product['product_name']}، شنو القياس المطلوب؟" if product else 'أي موديل تقصدين؟', 'create_order':False,'order':{}}
    stack.enter_context(patch.object(m,'_call_main_ai_once',side_effect=model))
    templates=['شنو قياسات {name} المتوفرة؟','شكد سعر {name}؟','شلون أطلب {name} وكم التوصيل؟',
               'ما هي مواصفات {name}؟','كم سعر {name} بدون توصيل؟','{name} متوفر لو لا؟']
    negatives=['ما اريد {name}','{name} ماريده','{name} ماعجبني','مو {name}','اريد غير {name}']
    started=time.perf_counter()
    for p in catalog:
        sid=p['store_id'];m._current_store_id.set(sid)
        name=p['product_name']
        expected=sorted(x['product_id'] for x in catalog if x['store_id']==sid and x['product_name']==name)
        variants=[name, 'ال'+name, name.replace('ة','ه'), name.replace('ا','أ'), 'ـ'.join(name), '  '.join(name.split())]
        for variant in variants:
            for template in templates:
                text=template.format(name=variant)
                actual=sorted(x['product_id'] for x in m.first_message_named_products(text,catalog))
                record('catalog_matrix', f'{sid}/{name}: {text}',expected,actual)
        for template in negatives:
            text=template.format(name=name)
            record('negative_names',f'{sid}/{name}: {text}',[],[x['product_id'] for x in m.first_message_named_products(text,catalog)])
        for other_sid in sorted({x['store_id'] for x in catalog}-{sid}):
            m._current_store_id.set(other_sid)
            allowed={x['product_id'] for x in catalog if x['store_id']==other_sid}
            matches=m.first_message_named_products('شكد سعر '+name,catalog)
            record('store_isolation',f'{sid} to {other_sid}/{name}',True,all(x['store_id']==other_sid and x['product_id'] in allowed for x in matches))
    print('Catalog matrix complete',len(records),flush=True)

    def run_message(sid,text,expected,platform='facebook',burst=False,ad_id=None,ref=None):
        sender=sid+'::audit-'+uuid.uuid4().hex
        event=dict(sender_id=sender,store_id=sid,page_id='audit-page',platform=platform,text=text,image_url=None,
                   attachments=[],ref=ref,ad_id=ad_id,postback=None,quick_reply=None,timestamp=0,referral_source=None,referral_type=None)
        m._current_store_id.set(sid)
        if burst:
            m.get_or_create_customer(db,sender,'audit-page',platform)
            m.save_message(db,sender,'incoming','text','السلام عليكم',None,None,None,{})
        captures.clear()
        with patch.object(m,'extract_facebook_event',return_value=event):
            result=m.process_webhook(db,{},use_debounce=False)
        bindings=sorted(x['product_id'] for x in m.load_customer_products(db,sender,limit=50))
        label=f'{sid}/{platform}/burst={burst}: {text}'
        record('pipeline_bindings',label,expected,bindings)
        if len(expected)==1:
            record('pipeline_model_context',label,expected[0],captures[-1] if captures else None)
        else:
            record('pipeline_ambiguity',label,True,bool(result.get('meta',{}).get('first_message_name_ambiguous')))
        return sender,result

    for p in catalog:
        sid=p['store_id']; name=p['product_name']
        same=[x for x in catalog if x['store_id']==sid and x['product_name']==name]
        expected=[p['product_id']] if len(same)==1 else []
        for template in templates[:3]:
            for platform in ['facebook','instagram']:
                run_message(sid,template.format(name='ال'+name),expected,platform)
    print('Actual catalog pipeline complete',len(records),flush=True)

    # Synthetic names test the general rule, not a special-case skirt feature.
    extras=[dict(catalog[0],store_id='al-fatena',product_id='AUDIT_SK',product_name='تنورة'),
            dict(catalog[0],store_id='al-fatena',product_id='AUDIT_BG',product_name='حقيبة جلد')]
    m.save_products_to_file(catalog+extras)
    for product in extras:
        for template in templates:
            for burst in [False,True]:
                run_message('al-fatena',template.format(name='ال'+product['product_name']),[product['product_id']],burst=burst)
    m.save_products_to_file(catalog)

    # Diagnostic coverage only: historical text is not labelled ground truth.
    replay=Counter()
    for message in history:
        if message['message_type'] != 'text':
            replay['non_text']+=1;continue
        m._current_store_id.set(message['store_id'] or 'default')
        matches=m.first_message_named_products(message['text'],catalog)
        replay['unique_name' if len(matches)==1 else 'ambiguous_name' if matches else 'no_full_name']+=1
    record('safety','No external HTTP requests',0,network.call_count)
    record('safety','Archived orders unchanged',archived_orders,db.execute('SELECT count(*) FROM orders').fetchone()[0])
    elapsed=round(time.perf_counter()-started,2)

record('safety','Original ZIP unchanged',before,hashlib.sha256(source.read_bytes()).hexdigest())
summary={'catalog_products':len(catalog),'stores':len({p['store_id'] for p in catalog}),
         'checks':len(records),'passed':sum(r['passed'] for r in records),'failed':sum(not r['passed'] for r in records),
         'groups':dict(Counter(r['group'] for r in records)),'historical_first_messages':len(history),
         'historical_diagnostic':dict(replay),'elapsed_seconds':elapsed,
         'synthetic_products':[p['product_name'] for p in extras], 'external_model_calls':0,'original_zip_sha256':before}
(destination/'intensive-ad-results.json').write_text(json.dumps({'summary':summary,'checks':records},ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(summary,ensure_ascii=False,indent=2))
for failure in [r for r in records if not r['passed']][:18]: print(json.dumps(failure,ensure_ascii=False))
sys.exit(1 if summary['failed'] else 0)
