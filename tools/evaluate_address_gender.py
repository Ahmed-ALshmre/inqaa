"""Bounded real-provider address checks; synthetic conversations, isolated data.

Never sends customer messages or reads customer records from the snapshot.
Only catalogue facts and model settings are read; all writes use a temp database.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
from unittest.mock import patch

from evaluate_sales_dialogues import HTTPGate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--allow-live', action='store_true')
    args = parser.parse_args()
    if not args.allow_live:
        parser.error('--allow-live is required for billed provider calls')
    key = os.environ.get('QA_OPENROUTER_API_KEY')
    if not key:
        parser.error('QA_OPENROUTER_API_KEY is required')
    import requests
    catalog = json.loads((args.snapshot/'products.json').read_text(encoding='utf-8-sig'))
    product = next(p for p in catalog if p.get('store_id')=='al-fatena' and p['product_id']=='P003')
    source = sqlite3.connect((args.snapshot.resolve()/'sales.db').as_uri()+'?mode=ro',uri=True)
    settings = [(k,v) for k,v in source.execute('SELECT key,value FROM app_settings')
                if k.split(':')[-1] in {'ai_main_model','ai_main_temperature','ai_main_max_tokens'}]
    source.close()
    gate = HTTPGate('live',requests.sessions.Session.request,16,0.5)
    cases = [
        ('anonymous-womens', 'حساب خاص', '', ['شكد سعره؟','ومع التوصيل شكد؟'], 'female'),
        ('latin-name', 'its. heba', '', ['شكد سعره؟','شنو قماشه؟'], 'female'),
        ('self-description', 'محمد', '', ['اني محتارة بين الموديلات','شكد سعره؟'], 'female'),
        ('male-buyer', 'Hiba', '', ['اني رجل واشتري لزوجتي','شكد سعره؟'], 'male'),
        ('explicit-correction', 'محمد', 'male', ['اني بنت مو رجل','شكد سعره؟'], 'female'),
    ]
    observations = []
    args.output.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='lamsa-address-live-') as temporary:
        root=Path(temporary)
        (root/'products.json').write_text(json.dumps([product],ensure_ascii=False),encoding='utf-8')
        env=dict(DATA_DIR=temporary,DB_PATH=str(root/'sales.db'),PRODUCTS_FILE=str(root/'products.json'),
                 ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1',STORAGE_RESTORE_REQUIRED='0',
                 OPENROUTER_API_KEY=key,TELEGRAM_BOT_TOKEN='',MANYCHAT_API_KEY='')
        def http(session,method,url,**kwargs):
            return gate.request(session,method,url,**kwargs)
        with patch.dict(os.environ,env),patch('dotenv.load_dotenv',return_value=False), \
                patch('requests.sessions.Session.request',new=http):
            sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
            from account_app import app as m
            m.init_db()
            with m.app.app_context():
                m._current_store_id.set('al-fatena');db=m.get_db()
                db.executemany('INSERT OR REPLACE INTO app_settings(key,value) VALUES(?,?)',settings)
                db.commit()
                for case_id,name,initial,turns,expected in cases:
                    sender='al-fatena::qa-address-'+case_id
                    m.get_or_create_customer(db,sender,'synthetic-page','facebook')
                    db.execute('UPDATE customers SET name=?,gender=? WHERE sender_id=?',(name,initial or None,sender));db.commit()
                    history=[]; replies=[]
                    for text in turns:
                        m.update_customer_intelligence(db,sender,text)
                        m.save_message(db,sender,'incoming','text',text,None,None,None,{})
                        history.append({'direction':'incoming','text':text})
                        profile=dict(db.execute('SELECT * FROM customers WHERE sender_id=?',(sender,)).fetchone())
                        ev={'sender_id':sender,'text':text,'_staff_preview':True}
                        result=m.call_main_ai(ev,'text',profile,history,[product],product,None,'',[],customer_products=[product])
                        replies.append({'input':text,'reply':result.get('reply'),'gender':m.customer_address_gender(ev,profile,history)})
                        history.append({'direction':'outgoing','text':result.get('reply') or ''})
                    # Force a provider-backed advice turn, including effective
                    # address in the real main prompt and final surface guard.
                    text='هذا الموديل عاجبني بس شنو تنصحيني أنتبه له قبل ما أختار قياسي؟'
                    ev={'sender_id':sender,'text':text,'_staff_preview':True}
                    before=len(gate.calls)
                    raw=m._call_main_ai_once(ev,'text',profile,history,[product],product,None,'',[],customer_products=[product])
                    result=m.owner_result_surface(raw,ev,profile,history)
                    reply=result.get('reply') or ''
                    wrong_pattern = (r'(?<!\w)(?:تلبس|تحب|تشوف|قياسك|عليك|حاب)(?!\w)' if expected=='female'
                                     else r'(?<!\w)(?:تلبسين|تحبين|قياسج|حبيبتي)(?!\w)')
                    wrong = re.search(wrong_pattern,reply)
                    actual=m.customer_address_gender(ev,profile,history)
                    passed=bool(reply and len(gate.calls)>before and not wrong and actual==expected and not result.get('failed'))
                    passed=passed and all(t['reply'] and t['gender']==expected and not re.search(wrong_pattern,t['reply']) for t in replies)
                    stored_gender=db.execute('SELECT gender FROM customers WHERE sender_id=?',(sender,)).fetchone()[0]
                    if case_id in {'anonymous-womens','latin-name','self-description'}:
                        passed=passed and not stored_gender
                    if expected=='male':
                        passed=passed and all(not re.search(r'شكد\s+(?:قياس\s+)?تلبس\s*[؟?]',t['reply']) for t in replies)
                    row=dict(case=case_id,expected=expected,actual=actual,passed=passed,turns=replies,
                             provider_raw_reply=raw.get('reply'),provider_final_reply=reply,
                             provider_requests=len(gate.calls)-before,
                             stored_gender=stored_gender)
                    observations.append(row)
                    (args.output/'progress.json').write_text(json.dumps(observations,ensure_ascii=False,indent=2),encoding='utf-8')
                    print(json.dumps({k:row[k] for k in ('case','passed','actual','provider_requests')},ensure_ascii=False),flush=True)
                assert db.execute('SELECT COUNT(*) FROM orders').fetchone()[0]==0
    report=dict(scope='Synthetic multi-turn dialogues plus real main-model advice turns; no external message delivery.',
                passed=sum(c['passed'] for c in observations),total=len(observations),cases=observations,
                requests=gate.calls,cost_usd=gate.spent,blocked_external_requests=gate.blocked)
    (args.output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    return 0 if all(c['passed'] for c in observations) else 1


if __name__=='__main__':sys.exit(main())
