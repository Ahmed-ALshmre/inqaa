from pathlib import Path
import subprocess,zipfile
out=Path('outputs/ai-telegram-update-20260916');out.mkdir(exist_ok=True)
report='''تحديث الرد بالذكاء الاصطناعي وتفاصيل حجز التلكرام

- إلغاء اختصار الإجابات المحلية للأسئلة البسيطة: السؤال وبيانات المنتج وسياق المحادثة تمر إلى نموذج الرد الحالي. لم تتغير النماذج أو إعداداتها.
- حفظ منع إعادة التعرف على الصورة. نتيجة التعرف وربط المنتج تستخدم في سياق الرد.
- رسالة التلكرام تحتفظ بأول أسطر الحجز، وتضيف لكل قطعة الاسم والعدد واللون والقياس/الوزن المتوفرين؛ تظهر ملاحظات الطلب سواء كان مدفوعاً أو غير مدفوع، واسم الزبون إن كان مسجلاً.
- سبب غياب اللون: النموذج اليدوي يرسله ويحفظه ضمن قطع الطلب لكن منسق رسالة التلكرام لم يعرضه. الملاحظات كانت تعرض للطلب المدفوع فقط.
- التحقق يشمل إنشاء الطلب اليدوي وإعادة إرساله، وتعدد الألوان والقياسات والطلبات القديمة. الردود في الاختبارات محاكية، ولا تثبت جودة صياغة النموذج الحقيقية أو سرعة المزود.
- هذه الحزمة تحل محل الحزمة السابقة وتشمل التعديلات التراكمية. لا تعتمد أرقام الردود المحلية في التقرير السابق بعد هذا التغيير؛ الأسئلة تذهب الآن للذكاء الاصطناعي.

التركيب: احتفظ بنسخة من الكود والبيانات ثم انسخ الملفات حسب مساراتها داخل مشروع الخادم وثبت requirements.txt وأعد تشغيل التطبيق. هذا تحديث كود وليس ملف استعادة قاعدة بيانات. لم يتم نشره أو إرسال رسائل فعلية أثناء الاختبار.
'''
(out/'تعليمات.txt').write_text(report,encoding='utf-8')
files=subprocess.check_output(['git','diff','--name-only'],text=True,encoding='utf-8').splitlines()
files += ['account_app/ai_efficiency.py','account_app/tests/test_ai_efficiency.py','account_app/tests/test_close_human_attention.py']
archive=Path('outputs/lamsa-ai-replies-telegram-details-20260916.zip')
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
 for f in sorted(set(files)):z.write(f,f)
 z.write(out/'تعليمات.txt','تعليمات.txt')
 z.write('outputs/ai-telegram-tests.txt','validation/tests.txt')
with zipfile.ZipFile(archive) as z:
 assert z.testzip() is None
 assert not any(n.endswith(('.db','.env')) for n in z.namelist())
print(archive.resolve())
