from pathlib import Path
import zipfile
out=Path('outputs/rewrite-speed-20260916')
report='''# تحديث سرعة إعادة صياغة المسودة

زر hiBtnAskAI يرسل الآن mode=rewrite عندما يوجد نص مكتوب. الخادم يحافظ على المسودة ويستخدم استدعاءً واحداً للنموذج الرئيسي المضبوط للمتجر، مع سياق المحادثة والمنتج والتعليمات. لا يعيد جمع الصور القديمة أو تشغيل التعرف أو التصحيح التلقائي أو الحجز لهذا النوع من الطلب.
السياق لا يتضمن كتالوج المتجر الكامل ولا مخطط إنشاء الطلب الكبير الخاص بالرد الآلي. يرجع نص الاقتراح فقط للمراجعة؛ لا يرسله للزبون، ولا ينشئ طلباً. تُزال مسودة الحجز القديمة عند نجاح إعادة الصياغة لتجنب تطبيق إجراء قديم على النص الجديد.
إذا كان الحقل فارغاً، يبقى مسار تحليل المحادثة الكامل كما هو، وقد يحتاج وقتاً أطول. لم تتغير النماذج أو إعدادات التفكير أو حدود الإخراج. مهلة اتصال إعادة الصياغة 30 ثانية دون محاولة تلقائية إضافية؛ هذا حد انتظار للاتصال وليس ضماناً لزمن الخدمة الكامل.
الفشل أو النص المقطوع يعرض رسالة واضحة ويُبقي المسودة الأصلية. اختبارات المحاكاة تتحقق من استدعاء واحد وعدم تحليل الصور وعدم إرسال أو إنشاء طلب. لا يوجد قياس حي لسرعة المزود؛ لا يمكن ضمان عدد ثوان محدد.

## التركيب
الحزمة تحديث كود تشمل الإصلاحات السابقة. استبدل الملفات في مسارات المشروع، وثبّت requirements.txt وأعد تشغيل التطبيق. حُدث إصدار ملف الواجهة إلى v25 لتجنب استمرار استخدام النسخة المخزنة بالمتصفح. احتفظ بنسخة احتياطية أولاً. هذه ليست حزمة استعادة قاعدة بيانات.
لم يُنشر التحديث على الخادم من هذه المحادثة.
'''
(out/'تعليمات-التحديث.md').write_text(report,encoding='utf-8')
files=['account_app/app.py','account_app/ai_efficiency.py','account_app/static/js/dashboard.js','account_app/templates/dashboard.html','requirements.txt','account_app/tests/run_isolated.py','account_app/tests/test_ai_efficiency.py','account_app/tests/test_ai_configuration.py','account_app/tests/test_checkout_regressions.py','account_app/tests/test_conversation_context.py','account_app/tests/test_messaging_media.py','account_app/tests/test_handoff_recovery.py']
with zipfile.ZipFile('outputs/lamsa-ai-rewrite-speed-fix-20260916.zip','w',zipfile.ZIP_DEFLATED) as z:
 for f in files:z.write(f,f)
 z.write(out/'تعليمات-التحديث.md','تعليمات-التحديث.md')
 z.write(out/'tests.txt','validation/tests.txt')
with zipfile.ZipFile('outputs/lamsa-ai-rewrite-speed-fix-20260916.zip') as z:assert z.testzip() is None
print('Update package verified.')
