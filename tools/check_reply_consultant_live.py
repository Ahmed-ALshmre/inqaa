"""Live OpenRouter conformance with synthetic text only; no application writes.

Use QA_OPENROUTER_API_KEY or --ask-key (hidden stdin, never arguments/files).
Reports live plans separately from the mocked pipeline regression tests.
"""
import argparse
import getpass
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from account_app import reply_consultant as consultant, ai_transport


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--ask-key', action='store_true')
    args = parser.parse_args()
    key = getpass.getpass('OpenRouter test key (hidden): ') if args.ask_key else os.environ.get('QA_OPENROUTER_API_KEY') or os.environ.get('OPENROUTER_API_KEY')
    if not key:
        print('Set QA_OPENROUTER_API_KEY or use --ask-key.');return 2
    product = dict(product_id='QA1', store_id='default', product_name='قميص اختبار', price='12000',
        colors='أسود أزرق', sizes='38 إلى52', fabric='قطن', stock='متوفر', measurements='الطول80 سم')
    cases = [
        dict(id='lost_context', expected='assist', question='قلت لج الأسود44، ليش تطلبين صورة مرة ثانية؟',
             history=[{'direction':'incoming','text':'أريد الأسود قياس44'},
                      {'direction':'outgoing','text':'دزيلي صورة الموديل حتى أعرفه'}],
             memory={'color':'أسود','size':'44','product_id':'QA1'}),
        dict(id='true_missing_fact', expected='needs_human', question='بيه ليكرا؟', history=[], memory={}),
        dict(id='latest_correction', expected='assist', question='لا حبيبتي، بدلي الأزرق إلى أسود والقياس44 يبقى نفسه، قطعة وحدة',
             history=[{'direction':'incoming','text':'أريد الأزرق قياس44 قطعة وحدة'},
                      {'direction':'outgoing','text':'صار عيوني دزيلي الرقم والعنوان'}],
             memory={'color':'أزرق','size':'44','quantity':1,'product_id':'QA1'}),
    ]
    results = []
    for case in cases:
        with consultant.scope(True, store_id='default', ev={'sender_id':'synthetic-'+case['id'],'text':case['question']},
            matched_product=product, products=[product], customer={}, history=case['history'], memory=case['memory'],
            delivery={'baghdad_fee':5000}) as state:
            plan = consultant.advise(state, {'reply':'','requires_human':True}, [case['id']], key=key,
                endpoint='https://openrouter.ai/api/v1/chat/completions', post=ai_transport.post)
            decision = (plan or {}).get('decision') or ('needs_human' if state.status=='needs_human' else None)
            results.append(dict(case=case['id'], question=case['question'], expected=case['expected'],
                decision=decision, passed=decision==case['expected'], metadata=state.metadata(), advice=plan))
            print(json.dumps({'case':case['id'],'status':state.status,'decision':decision,'passed':decision==case['expected']},ensure_ascii=False),flush=True)
            if state.reason in {'provider_http_401','provider_http_402','provider_http_403'}:
                break
    output = ROOT/'outputs/reply-consultant-nov08';output.mkdir(exist_ok=True)
    report = dict(model=consultant.MODEL, cases=results, passed=sum(row['passed'] for row in results), total_planned=len(cases))
    (output/'live-consultations.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
    return 0 if len(results)==len(cases) and all(row['passed'] for row in results) else 1


if __name__=='__main__':raise SystemExit(main())
