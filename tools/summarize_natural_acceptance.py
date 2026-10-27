"""Combine preserved live acceptance attempts without hiding failed attempts."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re


GROUPS = ('core', 'images', 'commerce', 'service', 'boundaries')
LABELS = {
    'natural-text-context': 'السعر والقماش والألوان والفحص ثم الحجز',
    'natural-hiba-landmark-before-province': 'عنوان هيبة قبل المحافظة وصيغة 3اكس وسمائي',
    'natural-haggling-then-booking': 'رفض التخفيض ثم استكمال الحجز',
    'natural-correct-size-before-checkout': 'تصحيح القياس مع بقاء الاختيار عبر أسئلة جانبية',
    'natural-anu-photo-followup': 'صورة anu88 الحقيقية ثم متابعة القياس والحجز',
    'natural-child-photo-and-booking': 'صورة أطفال حقيقية وحفظ العمر واللون',
    'natural-unknown-photo-then-clear-photo': 'صورة مجهولة ثم صورة واضحة واستئناف المحادثة',
    'natural-two-photo-selection': 'صورتان واختيار موديل واحد فقط',
    'natural-free-delivery-negotiation': 'رفض توصيل مجاني غير مسجل ثم الحجز',
    'natural-no-offer-no-invented-discount': 'مساومة على منتج بلا عرض دون اختراع خصم',
    'natural-conditional-price-with-contact': 'بيانات كاملة مع سعر مشروط ثم قبول السعر الفعلي',
    'natural-long-compound-replies': 'سؤال مركب وتقسيم الرد وحفظ السعر والتوصيل معاً',
    'natural-post-order-service': 'تعديل بعد الحجز دون إنشاء طلب ثان',
    'natural-refusal-without-pressure': 'رفض الشراء والشكر دون ضغط',
    'natural-sold-out-color-before-consent': 'لون نافد دون تبديل تلقائي ثم قبول البديل',
    'natural-address-is-not-courtesy': 'المجاملة واللون ليسا عنواناً',
    'natural-arrival-condition-is-not-guaranteed': 'موعد وصول غير مضمون دون وعد أو حجز خاطئ',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('outputs/natural-acceptance'))
    parser.add_argument('--offline-log', type=Path, default=Path('outputs/natural-offline-verified-final.log'))
    parser.add_argument('--version', default='v4')
    args = parser.parse_args()
    final = [(group, json.loads((args.root / (group + '-' + args.version) / 'report.json').read_text(encoding='utf-8'))) for group in GROUPS]
    initial = [(path.parent.name, json.loads(path.read_text(encoding='utf-8'))) for path in args.root.glob('*-v*/report.json') if not path.parent.name.endswith('-' + args.version)]
    reports = [data for _, data in final]
    cases = [case for data in reports for case in data['scenarios']]
    calls = [call for data in reports for call in data['http_requests']]
    kinds = Counter(call['kind'] for call in calls)
    statuses = Counter(str(call['status']) for call in calls)
    finish_reasons = Counter(reason for call in calls for reason in call.get('finish_reasons', []))
    checks = sum(data['checks_total'] for data in reports)
    passed = sum(data['checks_passed'] for data in reports)
    cost = sum(data['reported_cost_usd'] for data in reports)
    stable = all(not data['source_changed_during_run'] for data in reports) and len({json.dumps(data['source_file_hashes'], sort_keys=True) for data in reports}) == 1
    all_passed = all(data['deterministic_pass'] for data in reports) and stable
    offline = args.offline_log.read_text(encoding='utf-8-sig').splitlines()[-1]
    summary = dict(unique_conversations=len(cases), final_turns=sum(len(case['turns']) for case in cases),
                   final_requests=len(calls), requests_by_kind=dict(kinds), statuses=dict(statuses),
                   final_cost_usd=round(cost, 8), finish_reasons=dict(finish_reasons),
                   empty_provider_responses=sum(call.get('response_text_chars') == 0 for call in calls),
                   complete_selection_outputs=all(call.get('complete_selection_json') for call in calls if call['kind']=='selection'),
                   generation_failure_replies=sum(bool(re.search(r'فشل توليد|تعذر توليد|فشل إنشاء الرد', '\n'.join(turn['outgoing']))) for case in cases for turn in case['turns']),
                   unexplained_silent_turns=sum(not turn['outgoing'] and not turn.get('pending_human_review') for case in cases for turn in case['turns']),
                   checks_passed=passed, checks_total=checks,
                   all_passed=all_passed, same_unchanged_application_source=stable,
                   local_orders=sum(case['local_orders'] for case in cases), offline_result=offline,
                   all_attempt_cost_usd=round(cost + sum(data['reported_cost_usd'] for _, data in initial), 8),
                   all_attempt_conversations=len(cases) + sum(len(data['scenarios']) for _, data in initial),
                   all_attempt_turns=sum(len(case['turns']) for case in cases) + sum(len(case['turns']) for _, data in initial for case in data['scenarios']),
                   all_attempt_requests=len(calls) + sum(data['ai_http_requests'] for _, data in initial),
                   quality_review='Agent transcript review; no independent store-owner review recorded',
                   production_deployed=False)
    (args.root / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    lines = ['# نتيجة اختبار المحادثات الفعلية — 27 أكتوبر 2026', '',
             f"اختبار نهائي من {len(cases)} محادثة و{summary['final_turns']} دوراً: {passed}/{checks} فحصاً ناجحاً.",
             f"طلبات OpenRouter الفعلية في الجولة النهائية: {len(calls)}. التكلفة المبلغة: ${cost:.6f}.",
             f"جميع الطلبات حسب الحالة: {dict(statuses)}. أنواع الطلبات: {dict(kinds)}. أسباب انتهاء التوليد: {dict(finish_reasons)}.",
             f"الطلبات المحفوظة محلياً: {summary['local_orders']}. نتيجة اختبارات الانحدار المعزولة: {offline}.", '',
             '## ما الذي اختُبر فعلياً؟', '',
             'استُخدمت 26 صياغة من رسائل النسخة الاحتياطية ومراجعها الرقمية لتوجيه زبونة محاكاة عبر OpenRouter. '
             'رسائلها غير الثابتة مبنية على جواب النظام السابق. ثُبتت بعض رسائل حالات الانحدار وبيانات التواصل التجريبية لجعل النتائج قابلة للتحقق. '
             'مرّت المحادثات عبر الويب هوك، حفظ السياق، مطابقة الصور، إنشاء الطلب المحلي وتخطيط الرسائل. '
             'استُخدمت صور أصلية محفوظة لمنتجات الزبائن وصور الكتالوج فعلياً مع خدمة الرؤية؛ صورة فارغة ضابطة تختبر عدم التخمين.', '',
             'موديل المبيعات المسجل: google/gemini-3.8-flash. محاكاة الزبونة: google/gemini-2.5-flash. '
             'سجل HTTP التفصيلي يتضمن النماذج الفعلية لمطابقة الصور وإعادة المحاولات. '
             'الإجابات المحلية الحتمية لم تُستبدل بإجابات مصطنعة، ولذلك لا تستدعي كل رسالة موديل المبيعات.', '',
             'قراءة المصدر فقط، هوية ورقم وعنوان تجريبي، تخزين مؤقت، وحجب جميع قنوات إرسال الزبائن وتصدير الطلبات. '
             'لم تُفعّل خدمة الإنتاج ولم يُنشر أي تعديل. المفتاح من بيئة العملية ولا يوجد في التقرير أو الكود.', '',
             '## الحالات والنتيجة النهائية', '',
             '| الحالة | الأدوار | الطلبات المحلية | الفحوص | المحادثة الكاملة |',
             '|---|---:|---:|---|---|']
    for group, data in final:
        for case in data['scenarios']:
            values = list(case['checks'].values()) + [value for turn in case['turns'] for value in turn['checks'].values()]
            lines.append(f"| {LABELS[case['id']]} | {len(case['turns'])} | {case['local_orders']} | {sum(values)}/{len(values)} | [{case['id']}]({group}-{args.version}/report.md) |")
    lines += ['', '## ما ظهر في الجولة الأولى وما أُعيد اختباره', '',
              '- ظهر حشو «قبل حسم هالشرط» مرتين عند اشتراط وصول غداً. عُدلت الصياغة إلى توضيح طبيعي لعدم ضمان الموعد، وبقي منع الحجز الخاطئ. أضيف اختبار انحدار وأُعيدت المحادثات.',
              '- أعادت الزبونة المحاكاة «ثبتي» مرتين متتاليتين في حالة التوصيل، فمنع النظام إعادة إرسال جواب مكرر. تغير هدف الدور إلى قبول الأجور ثم طلب الحجز، دون تعطيل منع التكرار.',
              '- اعتبر فحص الترتيب «الثاني» خطأ رغم أن المحادثة احتوت صورتين، أولاهما مجهولة. صُحح الفحص ليحسب الصور الواردة السابقة. بقيت مطابقة الصورة والمنتج والسلة خاضعة للتحقق.', '',
              'أظهرت مراجعة سجل الجولة الثانية خمس إجابات داخلية لفهم اختيار المنتج انتهت عند حد التوليد 400 توكن. '
              'لم يظهر فشل للزبون، لكن الإجابات كانت مبتورة. رُفعت ميزانيتها إلى 1200 مع تفكير منخفض وإخراج JSON، '
              'ورُفض أي اختيار موسوم بأنه مبتور حتى لو أمكن تحليل بعضه. أضيف اختبار يمنع تغيير المنتجات المحفوظة من جواب مبتور، '
              'وأُعيدت جميع المحادثات مع فحص اكتمال جواب الاختيار الداخلي. '
              'احتساب التفكير ضمن ميزانية الإخراج موثق في [دليل OpenRouter](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens).', '',
              'التدقيق النصي للجولة الثالثة التقط «محجوز إلج» قبل إنشاء طلب. وُسّع حاجز تأكيد الحجز لالتقاطها مع صيغها المؤنثة والنفي، '
              'ويعيد جواب السؤال الفعلي بدلاً من ادعاء حجز غير منفذ. أضيف اختبار عبر الويب هوك قبل اكتمال بيانات الحجز. '
              'كذلك تبيّن أن الزبونة المحاكاة قد تختار قياس 46 من البداية رغم أن دورها الأول يطلب 44؛ '
              'ثُبت نص اختيار 44 وتصحيحه إلى 46 حتى يختبر السيناريو التبديل فعلاً. '
              'أعيدت المحادثات بعد هذين التعديلين وأضيف فحص مستقل لادعاء الحجز غير المنفذ.', '',
              'لم تُحذف نتائج المحاولات السابقة. توجد ملفات report.json وreport.md وpartial-results.json وسجلات الطلبات للجولات.', '',
              '## مراجعة النصوص وحدود النتيجة', '',
              'تُراجع المحادثات من حيث إجابة السؤال وحفظ الاختيار والمبالغ ورفض المساومة بصورة مفهومة واحترام الرفض. '
              'هذه مراجعة وكيل البرمجة؛ لا يوجد تسجيل قبول مستقل من مالك المتجر، ولا يقيس الاختبار نسبة تحويل المبيعات. '
              'العينة تثبت السلوك في هذه المحادثات، ولا تعطي ضماناً لكل صياغة أو صورة مستقبلية. '
              'الصورة المجهولة تحوّل للمراجعة وقد يبقى الرد معلقاً حتى تتحدد القطعة؛ لا تُعطى هوية أو أسعار مختلقة.', '',
              'طلبات OpenRouter في هذه المحادثات كانت فعلية. أخطاء الرصيد والرد الفارغ والازدحام لا تُدّعى كأحداث حية إذا لم تقع؛ '
              'تغطيها اختبارات الانحدار المعزولة بمحاكاة واضحة لحدود المزود. '
              f"تكلفة جميع الجولات المبلغة معاً: ${summary['all_attempt_cost_usd']:.6f}.", '',
              'تطابق بصمة ملفات التطبيق بين مجموعات الجولة النهائية ولم تتغير أثناء تشغيلها: ' + ('نعم.' if stable else 'لا؛ يجب إعادة التشغيل.')]
    (args.root / 'README.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=True))
    return 0 if all_passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
