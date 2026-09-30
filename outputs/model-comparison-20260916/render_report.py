import html,json,statistics
from pathlib import Path
P=Path(__file__).resolve().parent
cases=json.loads((P/'cases.json').read_text(encoding='utf-8'))
results=json.loads((P/'results.json').read_text(encoding='utf-8'))
reviews=json.loads((P/'evaluation.json').read_text(encoding='utf-8'))
retries=json.loads((P/'retries.json').read_text(encoding='utf-8'))
models=list(dict.fromkeys(r['model'] for r in results))
rows=[]
for model in models:
    rr=[r for r in results if r['model']==model]
    ee=reviews[model]
    means=[statistics.mean(e['scores'][i] for e in ee) for i in range(3)]
    rows.append({'model':model,'sales':round(means[0],1),'understanding':round(means[1],1),'dialect':round(means[2],1),'total':round(means[0]*4+means[1]*3.5+means[2]*2.5,1),'seconds':round(statistics.mean(r['seconds'] for r in rr),2),'complete':sum(bool(r.get('reply')) and r.get('finish_reason')=='stop' for r in rr),'cost':sum((r.get('usage') or {}).get('cost',0) or 0 for r in rr)})
rows.sort(key=lambda x:x['total'],reverse=True)
(P/'summary.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2),encoding='utf-8')
intro='تم إرسال 10 حالات حقيقية إلى النماذج الخمسة، بنفس السؤال والسياق والكتالوج وتعليمة موحدة. لم تُرسل رسائل للزبائن. الدرجات تقديري التحليلي للردود وليست قياساً لنسبة المبيعات الفعلية. البيع 40%، الفهم 35%، اللهجة العراقية 25%. كل محور من 10. السرعة منفصلة ولا تدخل في الدرجة.'
limits='العينة منتقاة من 11–16 أيلول 2026 وليست عشوائية؛ تشغيل واحد لكل حالة، حرارة 0.3 وحد خرج 1600 توكن. الاختبار نصي مباشر عبر OpenRouter، وليس اختبار صور أو تشغيل مسار التطبيق كاملاً. بيانات الكتالوج والسياسة من لقطة النسخة الاحتياطية؛ لا يمكن إعادة بناء كل تعديل تاريخي. العناوين والهواتف والروابط حُجبت. النصوص أدناه ردود فعلية دون إعادة صياغة.'
md=['# مقارنة فعلية لنماذج المبيعات','',intro,'',limits,'','| النموذج | البيع /10 | الفهم /10 | العراقية /10 | الإجمالي /100 | متوسط الزمن بالثواني | ردود مكتملة /10 |','|---|---:|---:|---:|---:|---:|---:|']
for r in rows:md.append(f"| {r['model']} | {r['sales']} | {r['understanding']} | {r['dialect']} | {r['total']} | {r['seconds']} | {r['complete']} |")
md+=['','الأخطاء المتعلقة بالمخزون والقياس والإرجاع ومواعيد التوصيل تخفض الفهم والبيع؛ لا تُحسب الوعود المختلقة إقناعاً ناجحاً. الرد المبتور يُقيّم كما وصل، والرد الفارغ يحصل على صفر في جودة الرد. زمن الطلب الكامل يشمل الشبكة وليس زمن أول كلمة.','']
e=html.escape
body=['<h1>أي نموذج يفهم الزبون ويبيع بالعراقي؟</h1>',f'<p>{e(intro)}</p>',f'<p class="muted">{e(limits)}</p>','<div class="table"><table><tr><th>النموذج</th><th>البيع</th><th>الفهم</th><th>العراقية</th><th>الإجمالي</th><th>ثانية</th><th>مكتمل</th></tr>']
for r in rows:body.append(f"<tr><td dir=ltr>{e(r['model'])}</td><td>{r['sales']}</td><td>{r['understanding']}</td><td>{r['dialect']}</td><td><b>{r['total']}</b></td><td>{r['seconds']}</td><td>{r['complete']}/10</td></tr>")
body+=['</table></div>','<p>الوعود المختلقة تخفض درجة البيع والفهم. الرد المبتور يُقيّم كما وصل، والفارغ درجته صفر. افتح سياق كل سؤال لفهم سبب التقييم.</p>','<label>عرض نموذج: <select id="filter"><option value="all">جميع النماذج</option>'+''.join(f'<option value="{e(m)}">{e(m)}</option>' for m in models)+'</select></label>']
for c in cases:
    md += [f"## السؤال {c['case']}",'',c['question'],'',f"الرسالة {c['message_id']} — {c['date']}",'','السياق السابق:','']
    body.append(f"<section><h2>السؤال {c['case']}</h2><blockquote>{e(c['question'])}</blockquote><details><summary>سياق المحادثة والبيانات التي وصلت للنماذج</summary>")
    for h in c['history']:
        line=('الزبون: ' if h['role']=='customer' else 'الموظف السابق: ')+h['text']
        md.append('- '+line.replace('\n',' '));body.append('<p>'+e(line)+'</p>')
    body.append('<h3>سياسة التوصيل</h3><pre>'+e(json.dumps(c['delivery_policy'],ensure_ascii=False,indent=2))+'</pre><h3>الكتالوج</h3><pre>'+e(json.dumps(c['catalog'],ensure_ascii=False,indent=2))+'</pre></details><div class="cards">')
    for model in models:
        r=next(r for r in results if r['model']==model and r['case']==c['case'])
        review=reviews[model][c['case']-1];a,b,d=review['scores'];total=round(4*a+3.5*b+2.5*d,1)
        reply=r.get('reply') or '[لم يصل نص رد]'
        status='مكتمل' if r.get('finish_reason')=='stop' else 'غير مكتمل: '+str(r.get('finish_reason') or r.get('error'))
        metrics=f'البيع {a}/10 · الفهم {b}/10 · العراقية {d}/10 · الإجمالي {total}/100 · {r["seconds"]} ثانية · {status}'
        md+=['',f'### {model}','',metrics,'',reply,'','التقييم: '+review['note'],'']
        body.append(f'<article data-model="{e(model)}"><h3 dir="ltr">{e(model)}</h3><small>{e(metrics)}</small><div class="reply">{e(reply)}</div><p class="review">{e(review["note"])}</p></article>')
        retry=next((t for t in retries if t['model']==model and t['case']==c['case']),None)
        if retry:
            text=retry.get('reply') or '[لم يصل نص رد]'
            note=f"إعادة تشخيصية بسقف 4096 توكن — {retry['seconds']} ثانية — {retry.get('finish_reason')}. لا تدخل في الدرجات أعلاه."
            md+=['','إعادة الاختبار: '+note,'',text,'']
            body.append(f'<article data-model="{e(model)}"><h3 dir="ltr">{e(model)} — إعادة</h3><small>{e(note)}</small><div class="reply">{e(text)}</div></article>')
    body.append('</div></section>')
(P/'comparison.md').write_text('\n'.join(md),encoding='utf-8')
page='<!doctype html><html lang="ar" dir="rtl"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>مقارنة نماذج المبيعات</title><style>body{max-width:1250px;margin:40px auto;padding:0 24px;background:#f5f6f8;color:#172333;font:17px/1.8 system-ui}h1{font-size:32px}h2{margin-top:50px}h3{font-size:18px}blockquote{background:#102e48;color:white;padding:20px;border-radius:12px;margin:16px 0;white-space:pre-wrap}.muted,small{color:#566477}.table{overflow:auto}table{border-collapse:collapse;width:100%;background:white}td,th{padding:13px;border-bottom:1px solid #dce2e9;text-align:right;white-space:nowrap}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:16px;margin-top:20px}article{background:white;border:1px solid #dce2e9;border-radius:12px;padding:22px}.reply{white-space:pre-wrap;margin-top:20px}.review{border-top:1px solid #dce2e9;padding-top:14px;color:#805118}details{padding:12px;background:#eaf0f4;border-radius:8px}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:14px/1.7 system-ui}select{padding:10px;margin:20px}article[hidden]{display:none}@media print{select{display:none}.cards{display:block}article{break-inside:avoid}}</style><body>'+''.join(body)+'<script>document.getElementById("filter").onchange=function(){document.querySelectorAll("article[data-model]").forEach(a=>a.hidden=this.value!=="all"&&a.dataset.model!==this.value)}</script></body></html>'
(P/'comparison.html').write_text(page,encoding='utf-8')
print(json.dumps(rows,ensure_ascii=False,indent=2))
