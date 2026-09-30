"""Opt-in live model evaluation. Never loads or messages real customers.

python -m account_app.tests.evaluate_sales --live --model MODEL --variant improved
Results are semantic fixtures, not a prediction of real conversion.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import tempfile
import time
from unittest.mock import patch


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--live',action='store_true')
    parser.add_argument('--model',default='google/gemini-3.8-flash')
    parser.add_argument('--variant',choices=['baseline','improved'],default='improved')
    parser.add_argument('--output',default='')
    parser.add_argument('--cases',default='')
    args=parser.parse_args()
    from .sales_scenarios import CASES,DRESS,CHEAPER,BUNDLE
    cases=[c for c in CASES if not args.cases or c['id'] in args.cases.split(',')]
    if not args.live:
        print(json.dumps([{'id':c['id'],'goal':c['goal']} for c in cases],ensure_ascii=False,indent=2)); return 0
    with tempfile.TemporaryDirectory() as directory:
        os.environ.update(DATA_DIR=directory,DB_PATH=str(Path(directory)/'evaluation.db'),BOOKINGS_FILE=str(Path(directory)/'bookings.jsonl'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
        import account_app.app as m
        if not m.OPENROUTER_KEY: raise RuntimeError('No model API key configured')
        import requests
        original=requests.sessions.Session.request
        def only_model(session,method,url,*a,**kw):
            if method.upper()!='POST' or url!=m.OPENROUTER_URL:
                raise RuntimeError('Evaluation blocks all non-model network calls')
            return original(session,method,url,*a,**kw)
        out=Path('outputs/sales-deep-evaluation');out.mkdir(parents=True,exist_ok=True)
        results=[]
        with m.app.app_context(), patch('requests.sessions.Session.request',new=only_model):
            db=m.get_db()
            m.set_store_setting(db,'ai_main_model',args.model)
            m.set_store_setting(db,'ai_main_max_tokens','800')
            if args.variant=='improved':m.set_store_setting(db,'ai_main_temperature','0.35')
            m.set_store_setting(db,'delivery_time','من يومين إلى ثلاثة أيام')
            for case in cases:
                sid='default::synthetic-'+case['id']
                customer={'sender_id':sid,'store_id':'default',**case.get('customer',{})}
                db.execute('INSERT INTO customers(sender_id,store_id,phone,province,address) VALUES(?,?,?,?,?)',(sid,'default',customer.get('phone',''),customer.get('province',''),customer.get('address','')));db.commit()
                products=copy.deepcopy([DRESS,CHEAPER,BUNDLE])
                product=products[2] if case.get('product')=='bundle' else products[0]
                if args.variant=='baseline':
                    for p in products:p.pop('notes',None)
                ev={'sender_id':sid,'text':case['text'],'image_url':None,'store_id':'default','platform':'facebook'}
                history=[dict(direction=direction,message_type='text',text=text,created_at=m.now_baghdad_iso()) for direction,text in case.get('history',[])]
                for h in history:m.save_message(db,sid,h['direction'],'text',h['text'],None,None,None,{})
                history.append(dict(direction='incoming',message_type='text',text=case['text'],created_at=m.now_baghdad_iso()))
                m.save_message(db,sid,'incoming','text',case['text'],None,None,None,{})
                started=time.monotonic()
                with patch.object(m,'load_products_from_file',return_value=products),patch.object(m,'send_order_to_telegram',return_value=False),patch.object(m,'save_booking_to_file'):
                    if args.variant=='baseline':
                        with patch.object(m.sales_strategy,'guidance',return_value=''),patch.object(m.sales_strategy,'reply_error',return_value=''):
                            result=m.call_main_ai(ev,'text',customer,history,products,product,None,'',[],customer_products=[product])
                    else:
                        result=m.call_main_ai(ev,'text',customer,history,products,product,None,'',[],customer_products=[product])
                reply=str(result.get('reply') or '')
                canonical=reply.translate(str.maketrans('٠١٢٣٤٥٦٧٨٩','0123456789')).replace(',','').replace('٬','')
                checks={}
                if 'create' in case:checks['order_decision']=bool(result.get('create_order'))==case['create']
                for term in case.get('must',[]):checks['contains_'+term]=term in canonical
                checks['not_failed']=not result.get('failed')
                checks['grounded_reply']=not m.sales_strategy.reply_error(reply,case['text'],product)
                checks['no_premature_close']=not m.sales_strategy.premature_close(reply,case['text'])
                if case['id']=='two_variants':
                    items=(result.get('order') or {}).get('items') or []
                    checks['separate_variants']=sorted((i.get('color'),str(i.get('size')),i.get('quantity',1)) for i in items)==sorted([('اسود','42',1),('كريمي','46',1)])
                if case['id']=='bundle_order':
                    items=(result.get('order') or {}).get('items') or []
                    try:
                        price=m.price_order(items,products,5000)
                        checks['correct_bundle_total']=price['product_total']==25000 and price['delivery_fee']==0 and sum(i['quantity'] for i in price['items'])==3
                    except Exception:checks['correct_bundle_total']=False
                row={'id':case['id'],'goal':case['goal'],'text':case['text'],'result':result,'checks':checks,'seconds':round(time.monotonic()-started,2)}
                results.append(row)
                (out/((args.output or args.variant)+'.json')).write_text(json.dumps({'model':args.model,'variant':args.variant,'results':results},ensure_ascii=False,indent=2),encoding='utf-8')
                print(json.dumps({'id':case['id'],'checks':checks,'seconds':row['seconds'],'failure_reason':result.get('failure_reason')},ensure_ascii=False),flush=True)
                if str(result.get('failure_reason','')).startswith(('provider_http_401','provider_http_402','provider_http_403','provider_http_404')):
                    break
            usage=[dict(r) for r in db.execute('''SELECT purpose,model,prompt_tokens,completion_tokens,
                cached_tokens,cache_write_tokens,cache_discount,cost,elapsed_ms FROM ai_usage_events''')]
            (out/((args.output or args.variant)+'-usage.json')).write_text(json.dumps(usage),encoding='utf-8')
    return 0


if __name__=='__main__':raise SystemExit(main())
