"""Offline wording regression over a completed isolated replay, never live state.

Restores recorded post-decision state in a disposable database, then runs the
current Responses/copy guards with recorded writer output and grounded recovery.
This is NOT a fresh model-decision or checkout test. No API calls or sends occur.
"""
import argparse
import copy
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tools.evaluate_live_customer_dialogues import ROOT,load,save,initialize_test_db,RUNTIME_HASHES
from account_app.sales_runtime import Scope
from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.sales_runtime.orders import CONFIRMATION


class RecordedWriter:
    def __init__(self):self.parts=[];self.errors=[]
    def __call__(self,context):return copy.deepcopy(self.parts)
    def repair(self,context,previous,error):
        self.errors.append(error)
        return self(context)


def run(source):
    cases=[load(p) for p in sorted((ROOT/source).glob('*.json'))]
    if not cases or any('final_state' not in c for c in cases):raise ValueError('completed replay required')
    dataset=load(ROOT/'dataset.json');results=[]
    for case in cases:
        settings=load(ROOT/'raw'/f'settings-{case["store_id"]}.json')['items'][0]['values']
        def fee(sid,province):return int(settings.get('delivery_baghdad_fee' if province=='بغداد' else 'delivery_other_fee') or 0)
        with tempfile.TemporaryDirectory() as directory:
            db=initialize_test_db(str(Path(directory)/'copy.db'));writer=RecordedWriter()
            entry=Entrypoint(db,lambda sid:dataset['catalogue'][sid],fee,None,writer=writer,
                             confirmation=settings.get('inspection_message') or CONFIRMATION)
            entry.migrate();scope=Scope(case['store_id'],'chatwoot','copy-replay',str(case['case']))
            for index,turn in enumerate(case['turns'],1):
                event=entry.store.append(scope,str(index),kind='message',actor='customer',payload=dict(text=turn['customer']))
                if turn['action']!='continue':continue
                outcome=turn['outcome'];writer.parts=[dict(type='text',text=t) for t in turn['bot']];writer.errors=[]
                db.execute('UPDATE sales_v2_scopes SET version=?,data=? WHERE scope=?',
                           (outcome['state_version'],json.dumps(turn['state'],ensure_ascii=False),scope.key));db.commit()
                result=dict(case=case['case'],turn=index,customer=turn['customer'],original=turn['bot'])
                try:
                    response=entry.responses.prepare(scope,event['id'],outcome)
                    result.update(action=response['action'],bot=[],recovery_reasons=writer.errors)
                    while True:
                        part=entry.store.claim_reply(scope)
                        if not part:break
                        result['bot'].append(part['payload'].get('text',''))
                        entry.store.settle_reply(scope,part['id'],'accepted','recorded-copy')
                except Exception as exc:result.update(action='exception',error=type(exc).__name__+': '+str(exc))
                results.append(result)
            db.close()
    summary=dict(source=source,cases=len(cases),copy_turns=len(results),
                 recovered=sum(bool(r.get('recovery_reasons')) for r in results),
                 exceptions=[r for r in results if r['action']=='exception'],runtime_hashes=RUNTIME_HASHES)
    save(ROOT/'copy-regression.json',dict(summary=summary,turns=results))
    lines=['# فحص الصياغة المسجلة بعد آخر الحراسات','',
           'فحص محلي بلا استدعاء موديل جديد: استُعيدت حالة القرار المسجلة من الجولة المكتملة في قاعدة مؤقتة، ثم أُعيدت حراسة الصياغة والتعافي المحدود. لا يختبر هذا فهم الموديل من جديد أو الحجز من البداية ولا يرسل للزبائن.','',
           f'المحادثات: {len(cases)}؛ أدوار الصياغة المفحوصة: {len(results)}؛ صيغ استلزمت التعافي: {summary["recovered"]}؛ أخطاء تنفيذ: {len(summary["exceptions"])}.','']
    for r in results:
        lines.extend([f'## الحالة {r["case"]:02} / الرسالة {r["turn"]}','', '**الزبون:** '+r['customer'],''])
        for text in r.get('bot',[]):lines.extend(['**الرد بعد الفحص المحلي:** '+text,''])
        if r.get('recovery_reasons'):lines.extend(['**سبب إعادة الصياغة المحلية:** '+str(r['recovery_reasons']),''])
        if r.get('error'):lines.extend(['**الخطأ:** '+r['error'],''])
    (ROOT/'فحص-الصياغة-المحلي.md').write_text('\n'.join(lines),encoding='utf8')
    print(json.dumps({k:v for k,v in summary.items() if k!='runtime_hashes'},ensure_ascii=False))
    return 1 if summary['exceptions'] else 0


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--source',default='results-fixed2')
    raise SystemExit(run(parser.parse_args().source))
