"""Validate and render the latest case results from isolated trial runs.

No network API calls. --browser uses local Edge with external requests blocked.
Later --runs entries override earlier cases; every source is disclosed.
"""
import argparse
import hashlib
import html
import json
from pathlib import Path
import re
import sqlite3
import sys
from collections import Counter

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from account_app import dual_sales,dual_sales_strategy as strategy
from tools import evaluate_dual_sales as evaluator


def inline(value):
    text=html.escape(value)
    text=re.sub(r'\[([^\]]+)\]\((https://[^\s)]+)\)',r'<a href="\2">\1</a>',text)
    text=re.sub(r'\*\*([^*]+)\*\*',r'<b>\1</b>',text)
    return re.sub(r'`([^`]+)`',r'<code>\1</code>',text)


def markdown_page(text):
    blocks=[];table=[];in_code=False;code=[]
    def flush_table():
        if table:
            rows=[]
            for i,row in enumerate(table):
                cells=row.strip().strip('|').split('|');tag='th' if i==0 else 'td'
                rows.append('<tr>'+''.join('<'+tag+'>'+inline(c.strip())+'</'+tag+'>' for c in cells)+'</tr>')
            blocks.append('<div class="table-wrap"><table>'+''.join(rows)+'</table></div>');table.clear()
    for line in text.splitlines():
        if line.startswith('```'):
            if in_code:blocks.append('<pre>'+html.escape('\n'.join(code))+'</pre>');code=[]
            in_code=not in_code;continue
        if in_code:code.append(line);continue
        if line.startswith('|'):
            if not re.fullmatch(r'[|\s:\-]+',line):table.append(line)
            continue
        flush_table()
        heading=re.match(r'^(#{1,3})\s+(.+)$',line)
        if heading:
            n=len(heading[1]);blocks.append('<h'+str(n)+'>'+inline(heading[2])+'</h'+str(n)+'>')
        elif line.strip():blocks.append('<p>'+inline(line)+'</p>')
    flush_table()
    return ''.join(blocks)


def check_case(folder,case,fixture):
    errors=[t.get('error') or t.get('adaptive_error') for t in case['turns'] if t.get('error') or t.get('adaptive_error')]
    assert not errors,(case['id'],errors)
    with sqlite3.connect(folder/'test-workspace.sqlite') as db:
        row=db.execute('SELECT data FROM orders WHERE case_id=?',(case['id'],)).fetchone()
        actual=json.loads(row[0]) if row else None
        assert (actual['total_amount'] if actual else None)==((case['final_state'].get('order') or {}).get('total_amount'))
        assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
    for t in case['turns']:
        parts=t['reply_parts'];assert len(parts)<=strategy.MAX_REPLY_PARTS
        if t.get('source')=='owner_confirmation_bypass':
            assert parts==[evaluator.CONFIRM] and t['after']['order']
        if t.get('source')=='gemini':
            generated=t.get('generated_reply_parts',parts)
            assert any(c.get('parts')==generated and not c['issues'] for c in t['candidates'])
            effects=set()
            if t['after'].get('order'):effects.add('saved')
            if any(a['name']=='update_order' and a['result'].get('ok') for a in t['tools']):effects.add('updated')
            if any(a['name']=='send_product_images' and a['result'].get('ok') for a in t['tools']):effects.add('images')
            issues=dual_sales.validate_parts(generated,message_count=t['plan']['message_count'],next_question=t['plan']['next_question'],effects=effects,allowed_numbers=evaluator.allowed_numbers(fixture,t['after'],t['quote'],t['tools']),known_contact=[k for k in ('phone','address') if t['after']['cart'].get(k)])
            assert not issues,(case['id'],t['step'],issues)
        if t.get('source') in ('silent_handoff','paused_for_staff'):assert parts==[]
        if t.get('dispatch_events'):
            assert [e['text'] for e in t['dispatch_events'] if e['kind']=='text']==parts
        if t.get('batch_cancelled'):
            assert len(parts)<len(t['generated_reply_parts'])
            assert not any(e['kind']=='images' for e in t['dispatch_events'])


def main():
    p=argparse.ArgumentParser();p.add_argument('--runs',nargs='+',type=Path,required=True);p.add_argument('--consultations',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--browser',action='store_true');args=p.parse_args()
    root=Path.cwd();out=args.output.resolve()
    if not out.is_relative_to((root/'outputs').resolve()):raise ValueError('Reports stay under ignored outputs')
    out.mkdir(exist_ok=True)
    cases={};sources={};fixtures={}
    for run in args.runs:
        run=run.resolve();json.loads((run/'summary.json').read_text(encoding='utf8'))
        for c in json.loads((run/'scenarios.json').read_text(encoding='utf8')):fixtures[c['id']]=c
        for file in run.glob('case-*.json'):
            c=json.loads(file.read_text(encoding='utf8'));cases[c['id']]=c;sources[c['id']]=run
    expected={'main':51000,'small':52000,'condition':None,'repeat':51000,'downgrade':29000,'implicit':51000,'single':None,'withdraw':None,'fulfillment':29000,'injection':51000,'complex':29000,'five_parts':None,'kids':63000,'interruption':29000}
    assert set(expected)|{'adaptive','adaptive_known'}==set(cases)
    for cid,c in cases.items():
        check_case(sources[cid],c,fixtures[cid])
        if cid in expected:assert ((c['final_state'].get('order') or {}).get('total_amount'))==expected[cid],cid
    main_case=cases['main']['final_state']['order'];assert main_case['order_version']==2
    assert {(r['color'],r['size'],r['quantity']) for r in main_case['items']}=={('اسود','44',1),('نيلي','46',1)}
    assert cases['small']['final_state']['order']['negotiated_discount']==1000
    assert cases['downgrade']['final_state']['order']['negotiated_discount']==0
    assert sum(r['quantity'] for r in cases['kids']['final_state']['order']['items'])==3
    interrupted=cases['interruption']['turns'][0];assert interrupted['batch_cancelled']
    with sqlite3.connect(sources['interruption']/'test-workspace.sqlite') as db:
        statuses=[r[0] for r in db.execute('SELECT status FROM media WHERE case_id=? ORDER BY id',('interruption',))]
        assert statuses[0]=='cancelled'
    results=list(cases.values());evaluator.render(results,out)
    counts=Counter(len(t['reply_parts']) for c in results for t in c['turns'] if t['reply_parts'])
    adaptive={cid:dict(saved=bool(cases[cid]['final_state']['order']),review_pending=cases[cid]['final_state'].get('review_pending'),turns=len(cases[cid]['turns']),customer_finished=cases[cid].get('adaptive_finished'),max_turns=cases[cid].get('adaptive_max_turns')) for cid in ('adaptive','adaptive_known')}
    notes=[
        'الخطة كُتبت بعد البحث وجولتي استشارة وقبل التنفيذ؛ أسلوبها صوت بنت عراقية تعمل بالمحل، حسب تصحيح المالك.',
        'السجل يجمع أحدث تشغيل لكل حالة بعد التصحيحات. المصدر ونسخة التشغيل لكل حالة محفوظان؛ ليس اختبارA/B أو عينة لقياس مبيعات فعلية.',
        'الألفاظ المعروضة من المصيغ حرفية. نص تأكيد الحجز المعتمد يتجاوز المصيغ ويخرج من الحفظ، وأي بديل حتمي أو صياغة مرفوضة يظهر في التفاصيل.',
        '1–5 حد تقسيم عملي حسب المعنى، وليس وعداً بأن خمس رسائل أفضل. سؤال السعر البسيط لا يعامل كاستبيان أو خمس فقرات.',
        'الصور أسماء ألوان بمرفقات تمثيلية فقط، ولم تُختبر رؤية المنتج أو تسليم Messenger/Zernio الحقيقي. المقاطعة اختُبرت في مُرسل معزول.',
        'فُصل اتفاق السعر عن اختلاف اللون بنفس التسعيرة، لمنع تأكيد زائد عند وصول البيانات. تغيير الكمية أو السعر أو الخصم يعيد التحقق من الاتفاق.',
        'فهم المناسبة والقياس والتفضيل يحفظ مستقلاً عن السلة، حتى تبقى المعلومة متاحة عند التحويل البشري، دون اعتبار ملخص الموديل مصدراً مالياً.',
        'المحاكاة التفاعلية ليست إنساناً: نتائجها تبين كيف يتصرف الحوار، ولا تثبت زيادة نسبة الحجز. المعلومة غير المسجلة تبقى للمراجعة الصامتة وفق سياسة المالك.',
        'ملفات الإنتاج الأصلية لم تتغير. جرى تصحيح تعارض اسم ملف التجربة وإعادة الملف واختباراته الأصليين؛ منطق التجربة في ملفاتdual_sales مستقلة. لم ينشر أي تعديل.'
    ]
    summary=dict(cases=len(results),customer_events=sum(len(c['turns']) for c in results),saved_order_cases=sum(bool(c['final_state']['order']) for c in results),displayed_text_parts=sum(sum(len(t['reply_parts']) for t in c['turns']) for c in results),parts_distribution=dict(counts),fallbacks=sum(bool(t.get('fallback')) for c in results for t in c['turns']),rejected_candidates=sum(bool(a['issues']) for c in results for t in c['turns'] for a in t['candidates']),adaptive=adaptive,source_runs={cid:str(p) for cid,p in sources.items()},notes=notes)
    (out/'validation.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf8')
    plan=root/'docs/updates/2026-11-08-warm-sales-plan.md'
    css='body{font:17px/1.9 Tahoma,Arial;background:#f4f6f5;color:#243c35;margin:0}main{max-width:1000px;margin:auto;padding:22px}article,header{background:white;padding:22px;border-radius:16px;margin:15px 0}p{overflow-wrap:anywhere}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit}table{border-collapse:collapse;width:100%;font-size:14px}td,th{border:1px solid #dbe5de;padding:10px;vertical-align:top}th{background:#e5f2eb}.table-wrap{overflow-x:auto}code{overflow-wrap:anywhere}a{color:#156b52}@media(max-width:500px){main{padding:10px}header,article{padding:14px}}'
    plan_doc='<!doctype html><html lang="ar" dir="rtl"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>خطة البيع العراقي</title><style>'+css+'</style><main><article>'+markdown_page(plan.read_text(encoding='utf8'))+'</article></main></html>'
    (out/'خطة-البيع.html').write_text(plan_doc,encoding='utf8')
    consultation=json.loads((args.consultations/'summary.json').read_text(encoding='utf8'))
    blocks=['<header><h1>نقاش خطة البيع قبل التنفيذ</h1><p>استشارتان جديدتان وجولة نقد لكل موديل، بعد ملاحظة المالك عن البرودة. النصوص حرفية وليست كل اقتراحاتها معتمدة؛ الخطة تذكر ما أخذناه وما رفضناه.</p></header>']
    for item in consultation['completed']:
        blocks.append('<article><h2 dir="ltr">'+html.escape(item['model'])+' — '+str(item['round'])+'</h2><pre>'+html.escape(item['reply'])+'</pre></article>')
    (out/'نقاش-الموديلين.html').write_text('<!doctype html><html lang="ar" dir="rtl"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>استشارة خطة البيع</title><style>'+css+'</style><main>'+''.join(blocks)+'</main></html>',encoding='utf8')
    file=out/'محادثات-البيع.html';doc=file.read_text(encoding='utf8')
    intro='<header id="results"><h2>الخطة ونتيجة الاختبار</h2><p><a href="خطة-البيع.html">الخطة كاملة بمصادر البحث</a> · <a href="نقاش-الموديلين.html">استشارة الموديلين ونقد المقترحات</a></p><p>'+str(summary['cases'])+' حالة · '+str(summary['customer_events'])+' رسالة زبون افتراضي · '+str(summary['saved_order_cases'])+' حالات حفظت طلباً اختبارياً.</p><p>الزبونة التفاعلية الأولى: '+('حفظت طلباً' if adaptive['adaptive']['saved'] else 'لم تحفظ طلباً')+'؛ الثانية: '+('حفظت طلباً' if adaptive['adaptive_known']['saved'] else 'لم تحفظ طلباً')+'. اقرأ المحادثتين والسبب بدلاً من تحويل العدد إلى نسبة بيع.</p><details><summary>حدود النتيجة ومصادر الحالات</summary><ul>'+''.join('<li>'+html.escape(n)+'</li>' for n in notes)+'</ul><pre>'+html.escape(json.dumps(summary['source_runs'],ensure_ascii=False,indent=2))+'</pre></details></header>'
    doc=doc.replace('</header>','</header>'+intro,1);file.write_text(doc,encoding='utf8')
    for file in out.glob('*'):
        if file.suffix in ('.html','.md','.json'):assert not re.search(r'sk-or-v1-[A-Za-z0-9]{32,}',file.read_text(encoding='utf8'))
    if args.browser:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            b=p.chromium.launch(channel='msedge',headless=True);page=b.new_page(viewport={'width':1280,'height':1000});page.route('https://**/*',lambda r:r.abort());page.goto((out/'محادثات-البيع.html').as_uri())
            assert page.locator('section').count()==len(results)
            assert page.locator('section article').count()==summary['customer_events']
            assert page.locator('.bot p').all_text_contents()==[part for c in results for t in c['turns'] for part in t['reply_parts']]
            page.screenshot(path=str(out/'report-desktop.png'));page.set_viewport_size({'width':390,'height':844});assert page.evaluate('document.documentElement.scrollWidth<=innerWidth');page.screenshot(path=str(out/'report-mobile.png'));page.locator('#main h2').evaluate('(el)=>el.scrollIntoView({block:"start"})');page.screenshot(path=str(out/'conversation-mobile.png'))
            for name in ('خطة-البيع.html','نقاش-الموديلين.html'):
                page.goto((out/name).as_uri());assert page.locator('h1').count()>=1;assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
            b.close()
    print('WARM_SALES_REPORT_PASS',json.dumps({k:summary[k] for k in ('cases','customer_events','saved_order_cases','displayed_text_parts','parts_distribution','fallbacks','adaptive')},ensure_ascii=False))


if __name__=='__main__':main()
