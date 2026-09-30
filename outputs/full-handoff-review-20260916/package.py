from pathlib import Path
import json, zipfile, subprocess
out=Path('outputs/full-handoff-review-20260916')
before=json.loads((out/'before.json').read_text(encoding='utf-8'))['summary']
after=json.loads((out/'after.json').read_text(encoding='utf-8'))['summary']
comparison={'before':before['categories'],'after':after['categories']}
(out/'comparison.json').write_text(json.dumps(comparison,ensure_ascii=False,indent=2),encoding='utf-8')
report='''# إصلاح التوقف عند الأسئلة البسيطة

تم فحص سجلات التدخل البشري الـ759 الموجودة في النسخة الاحتياطية المقدمة، وتشغيل كل سجل مرتين قبل التعديل ومرتين بعده: بإعدادات لقطة النسخة، ثم مع تشغيل الوكيل افتراضياً في نسخة الاختبار فقط.
هذه محاكاة لمسارات البرنامج بإعادة نص المراجعة كرسالة متابعة اعتماداً على حالة النسخة الحالية، وليست إعادة بناء تاريخية لكل محادثة. لا تعيد إرسال الصور الأصلية كصور جديدة. تتوقف المحاكاة عند الحاجة للنموذج أو تنزيل الوسائط؛ لذلك هذه المراحل ليست أخطاء ولا إجابات ناجحة مؤكدة.

## التعديلات
- إجابة محلية من المنتج المرتبط لعبارات السعر والألوان والقياسات والقماش الواضحة، بصيغ عراقية أكثر.
- جمع الأسئلة غير المجابة قبل الرد، وعدم إسقاط سؤال سابق أو إجراء حجز أو إلغاء بسبب سؤال قصير أحدث.
- استخدام نتيجة الربط للصورة المعروفة والإجابة عن تعليقها البسيط دون استدعاء نموذج الرد مرة ثانية.
- طلب صورة المنتج المرتبط يستخدم صور الكتالوج. غياب الصورة يُذكر بوضوح؛ لا يُطلب من الزبون إعادة إرسالها لهذا الغرض.
- تبقى الأسئلة الغامضة والوزن وتغيير الطلب والمقارنة في المسار الكامل. لا يتحول الاهتمام إلى موافقة شراء.
- تشمل الحزمة الإصلاحات السابقة لتقليل التكرار ومحاولة التعرف الواحدة وسرعة اقتراح الرد وإغلاق حالات التدخل البشري.

## حدود النتيجة
نجح 286 اختباراً مع منع الشبكة. أرقام المقارنة في comparison.json تقيس تغير المسار المحلي فقط؛ لا تثبت نسبة خفض التوقف الفعلي أو تحسن لغة النموذج أو استهلاك التوكن الفعلي.
توجد أسباب تاريخية أخرى: فشل التعرف على الصور، أخطاء 402، انتهاء مهلة الخدمة، وردود غير صالحة. لا تعالج إجابة الأسئلة المحلية كل هذه الأسباب. حالات تعطيل الوكيل العامة أو اليدوية أو القديمة مجهولة السبب تبقى محترمة، ولا يعاد تفعيلها تلقائياً.
لم تُرسل رسائل للعملاء، ولم تُنشأ طلبات خارجية، ولم تُعدّل النسخة الأصلية. نتائج الاختبار التفصيلية محلية ولا تتضمنها حزمة الكود.

## التركيب
هذه حزمة كود تراكمية وليست نسخة لاستعادة قاعدة البيانات. احتفظ بنسخة من الكود والبيانات ثم انسخ الملفات إلى مساراتها داخل المشروع، وثبت requirements.txt وأعد تشغيل التطبيق. الحزمة لم تُركب على الخادم من هنا.
'''
for mode,label in [('snapshot','إعدادات النسخة'),('enabled_counterfactual','تشغيل الوكيل افتراضياً')]:
 report+=f"\n- {label}: الردود المحلية قبل {before['categories'][mode].get('local_reply',0)} وبعد {after['categories'][mode].get('local_reply',0)} من 759 حالة مراجعة.\n"
(out/'تقرير-المراجعة.md').write_text(report,encoding='utf-8')
files=subprocess.check_output(['git','diff','--name-only'],text=True,encoding='utf-8').splitlines()
files += ['account_app/ai_efficiency.py','account_app/tests/test_ai_efficiency.py','account_app/tests/test_close_human_attention.py']
archive=Path('outputs/lamsa-handoff-reduction-20260916.zip')
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for file in sorted(set(files)):z.write(file,file)
 for file in ['تقرير-المراجعة.md','comparison.json','tests.txt']:z.write(out/file,'validation/'+file)
with zipfile.ZipFile(archive) as z:
 assert z.testzip() is None
 assert not any(n.endswith(('.db','.env')) for n in z.namelist())
print(json.dumps(comparison,ensure_ascii=False))
print(archive.resolve())
