"""Real model QA against disposable synthetic fixtures; no customer transport.

QA_OPENROUTER_API_KEY is read only from the environment. Reports contain no key.
The canonical order schema is taken from the offline integration fixture.
"""
import json
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from account_app.tests.test_sales_runtime_linking import LinkingTests
from account_app.sales_runtime.entrypoint import Entrypoint
from account_app.sales_runtime.gateway import DecisionGateway, WriterGateway
from account_app.sales_runtime.store import Scope


def main():
    key=os.environ.get('QA_OPENROUTER_API_KEY')
    if not key:
        raise SystemExit('QA_OPENROUTER_API_KEY required')
    model=os.environ.get('QA_DECISION_MODEL','google/gemini-3.8-flash')
    writer_model=os.environ.get('QA_WRITER_MODEL','google/gemini-3.8-flash')
    output=Path('outputs/sales-runtime-linking-live');output.mkdir(parents=True,exist_ok=True)
    scenarios=[('تصحيح اللون وبيانات متفرقة',[
        'هلا حبيبتي هذا شنو قماشه؟',
        'قياس 44 موجود؟',
        'اريد الماروني قياس 44 قطعة وحدة',
        'لا بدلي الماروني ازرق ونفس القياس',
        '07701234567',
        'بغداد حي الاختبار قرب مدرسة التجربة ثبتيلي الطلب']),
        ('استفسار السعر ليس شراء ثم حجز حقيبة',[
        'الحقيبة السوداء شكد سعرها؟',
        'اريد حقيبة سوداء وحدة',
        'تحتاجين وزني لو قياسي؟',
        'هذا رقمي 07701234567',
        'البصرة حي الاختبار قرب مدرسة التجربة ثبتيلي']),
        ('بيانات الحجز دون تأكيد ثان',[
        'هلا هذا شنو الوانه؟',
        'اريد الازرق قياس 44 قطعة وحدة',
        '07701234567',
        'بغداد حي الاختبار قرب مدرسة التجربة'])]
    reports=[]
    for index,(name,messages) in enumerate(scenarios):
        fixture=LinkingTests();fixture.setUp()
        fixture.products[0].update(fabric='قديفة شتوي',description='قميص شتوي')
        fixture.products[1].update(description='حقيبة نسائية',ad_ids=['bag-ad'])
        entry=Entrypoint(fixture.db,lambda sid:fixture.products,lambda sid,p:5000,
                         DecisionGateway(model,key),writer=WriterGateway(writer_model,key))
        entry.migrate()
        scope=Scope('khuyoot','chatwoot','qa-account',f'qa-customer-{index}')
        report=dict(name=name,turns=[],model=model,writer=writer_model)
        try:
            for number,text in enumerate(messages):
                turn=dict(customer=text);report['turns'].append(turn)
                try:
                    result=entry.receive(store_id=scope.store,channel=scope.channel,account_id=scope.account,
                        sender_id=scope.sender,message_id=f'm{number}',text=text,
                        ad={'ad_id':'bag-ad' if index==1 else '120251717777850095'})
                    turn['result']=result
                    turn['bot']=[]
                    while True:
                        part=entry.store.claim_reply(scope)
                        if not part:break
                        turn['bot'].append(part['payload'].get('text','[صورة]'))
                        entry.store.settle_reply(scope,part['id'],'accepted',f'qa-local-{part["id"]}')
                        if part['payload'].get('purpose')=='checkout_contacts':
                            entry.runtime.accept_checkout_prompt(scope,f'ask:{part["id"]}',
                                entry.store.snapshot(scope)['version'],part['id'])
                    if result['action']=='booked':
                        # Display the committed task; do not run any sender or fulfillment worker.
                        confirmation=fixture.db.execute("SELECT payload FROM sales_v2_order_tasks WHERE order_id=? AND kind='confirmation'",
                                                        (result['order_id'],)).fetchone()
                        turn['bot'].append(json.loads(confirmation[0])['text'])
                    turn['state']=entry.store.snapshot(scope)['data']
                    print(f'Scenario {index+1}, turn {number+1}: {result["action"]}',flush=True)
                except Exception as exc:
                    turn['error']=type(exc).__name__+': '+str(exc)
                    print(f'Scenario {index+1}, turn {number+1}: {type(exc).__name__}',flush=True)
                    break
            report['orders']=[dict(zip([d[0] for d in cur.description],row)) for cur in [fixture.db.execute('SELECT * FROM orders')] for row in cur.fetchall()]
            expected='B1' if index==1 else 'P1'
            orders=report['orders']
            checks=dict(completed=len(report['turns'])==len(messages) and not any(t.get('error') for t in report['turns']),
                        exactly_one_order=len(orders)==1,
                        product_correct=len(orders)==1 and orders[0]['product_id']==expected)
            if len(orders)==1:
                lines=json.loads(orders[0]['order_items'])
                checks['choice_correct']=(len(lines)==1 and lines[0]['quantity']==1 and
                    (lines[0]['size']=='' if index==1 else lines[0]['size']=='44' and lines[0]['color']=='أزرق'))
            report['checks']=checks
            report['passed']=all(checks.values())
            report['contract_diagnostics']=entry.runtime.decide.failures
            report['model_attempts']=[dict(stage=r[0],status=r[1],error_code=r[2]) for r in fixture.db.execute(
                'SELECT stage,status,error_code FROM sales_v2_model_attempts ORDER BY id')]
        finally:
            fixture.tearDown()
            reports.append(report)
            (output/'report.json').write_text(json.dumps(reports,ensure_ascii=False,indent=2),encoding='utf8')
    lines=['# محاكاة فعلية للموديل — بيانات مصطنعة وتسليم محجوب','',f'العقل: {model}؛ الكاتب: {writer_model}','']
    for report in reports:
        lines.extend([f'## {report["name"]}',''])
        for turn in report['turns']:
            lines.extend(['**الزبونة:** '+turn['customer'],''])
            for reply in turn.get('bot',[]):lines.extend(['**البوت:** '+reply,''])
            if turn.get('error'):lines.extend(['**فشل الاختبار:** '+turn['error'],''])
        lines.extend([f'الطلبات المحفوظة: {len(report.get("orders",[]))}',''])
        lines.extend([f'اجتياز حالة السلة والحجز: {report.get("passed",False)}',''])
    (output/'المحادثات.md').write_text('\n'.join(lines),encoding='utf8')


if __name__=='__main__':main()
