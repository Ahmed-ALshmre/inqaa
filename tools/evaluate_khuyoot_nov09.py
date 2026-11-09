"""Real provider replay of anonymized Khuyoot question patterns, isolated writes.

The snapshot is an ignored directory with products.json and settings.db.
Only OpenRouter text requests are allowed by the existing HTTPGate; customer
and fulfillment delivery is intercepted. No historical order is replayed.
"""
import argparse
import getpass
import json
import os
import re
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(Path(__file__).parent))
import evaluate_sales_dialogues as h


class SyntheticResponseGate(h.HTTPGate):
    def request(self, session, method, url, **kwargs):
        response=super().request(session, method, url, **kwargs)
        if response.status_code==200:
            # Synthetic text conversations only. Never store request headers.
            self.calls[-1]['synthetic_response']=[(choice.get('message') or {}).get('content')
                for choice in response.json().get('choices',[])]
        return response


def scenarios(catalog):
    def case(name,product,questions,orders=0,**kwargs):
        return dict(id=name,store='khuyoot',product=product,expected_orders=orders,
            purpose=name,gender='female',turns=[h.step(q,'') for q in questions],**kwargs)
    return [
        case('unique_skirt_to_checkout','P007',[
            'السلام عليكم ما هي المواد المستخدمة في التنورة؟', 'قياسي هو ٤٤',
            'اريد النيلي', '07700000001', 'عنوان المنصور ١٤ رمضان قرب مدرسة التجربة'],1,unlinked=True),
        case('approximate_weight_two_colors','P007',[
            'حياتي وزني تقريبا ٥٨ او ٦٠ ما اعرف بالضبط', 'اريد نيلي وجوزي قطعتين',
            'قياسي هو ٣٨', 'اي عيني ثبتي', '07700000002', 'دهوك حي الاختبار قرب مدرسة التجربة'],1),
        case('courier_whatsapp','P007',[
            'اريد اسود قياس ٤٦', 'هو المندوب يتصل وانطيه الموقع ع الوات ساب',
            'ثبتي رقم 07700000003 بغداد حي الاختبار قرب مدرسة التجربة'],1),
        case('hesitation_then_selection','P007',[
            'شنو قماشها وشكد سعرها؟', 'اخاف تجي مو نفس الصورة مثل مرة اشتريت وندمت',
            'اذا بيها مشكلة ارفضها بوجود المندوب؟', 'تمام اريد الرصاصي قياس ٤٤',
            '07700000004', 'بغداد حي الاختبار قرب مدرسة التجربة'],1),
        case('correction_and_delivery','P007',[
            'اريد نيلي قياس ٤٤', 'لا خلي الجوزي بدل النيلي', 'شكد ويا التوصيل للبصرة؟',
            'ثبتي 07700000005 البصرة حي الاختبار قرب مدرسة التجربة'],1),
        case('price_inquiry_not_order','P011',[
            'شكد السوت وشلون القماش؟', 'اذا اخذ قطعتين شكد ويا التوصيل؟',
            'بس اقارن حاليا ما اريد احجز', 'شكرا حياتي']),
        case('unavailable_size_not_guess','P007',[
            'قياس ٥٤ متوفر؟ وزني ١٠٠', 'لا اريد تنورة بس', 'تمام شكرا']),
        case('true_missing_fact','P011',['شكد طول السوت بالسنتيمتر؟'])
    ]


def evaluate_turn(case,turn,index,prior,texts,orders,response,m,db,sender):
    reply='\n'.join(texts)
    if case['id']=='true_missing_fact':
        turn['allow_handoff']=True
        return dict(silent_missing_fact=not texts,
                    human_review_exists=bool(m.has_pending_human_review(db,sender)))
    checks={'no_exact_weight_loop':'برقم واحد' not in reply and 'النطاق وحده' not in reply}
    checks['no_dangling_condition']=not any(re.search(r'(?<!\w)(?:إذا|اذا|لو)(?!\w)',p) and p.rstrip().endswith('،') for p in texts)
    if case['id']=='courier_whatsapp':
        checks['no_false_closure_review']=not m.has_pending_human_review(db,sender)
    if case['id']=='unique_skirt_to_checkout' and index>0:
        checks['kept_product']=not m.reply_reasks_known_product(reply)
    if case['id']=='unique_skirt_to_checkout' and index==0:
        checks['actual_fabric']='دبل' in reply and 'ايطالي' in m.sales_strategy.normalized(reply)
        binding=m.get_active_product_binding(db,sender)
        checks['actual_binding']=(binding or {}).get('product_id')=='P007'
    if (case['id']=='unique_skirt_to_checkout' and index==2 or
        case['id']=='correction_and_delivery' and index in (1,2) or
        case['id']=='approximate_weight_two_colors' and index==2):
        checks['progresses_to_contact']='رقم' in reply and 'عنوان' in reply
        checks['no_redundant_confirmation']='قطعة من كل لون؟' not in reply and 'أثبتلج حجز؟' not in reply
    if case['id']=='unavailable_size_not_guess':
        checks['no_unimplemented_notification_offer']=not re.search('أبلغ|ابلغ|نبلغ',reply)
    if case['id']=='price_inquiry_not_order':checks['no_accidental_booking']=not orders
    if index==len(case['turns'])-1 and case['expected_orders']:
        checks['saved_before_confirmation']=len(orders)==1 and reply.startswith('تم تثبيت الطلب')
    return checks


def main():
    parser=argparse.ArgumentParser(__doc__)
    parser.add_argument('--snapshot',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--case',action='append')
    options=parser.parse_args()
    os.environ['QA_OPENROUTER_API_KEY']=os.environ.get('QA_OPENROUTER_API_KEY') or getpass.getpass('QA key (hidden): ')
    h.scenarios=scenarios
    h.HTTPGate=SyntheticResponseGate
    args=argparse.Namespace(mode='live',source_root=ROOT,catalog=options.snapshot/'products.json',
        settings_db=options.snapshot/'settings.db',env_file=ROOT/'outputs/absent.env',api_key_file=None,
        output=options.output,max_live_requests=100,max_live_cost_usd=3.0,scenario=options.case,
        repeat=1,copy_saved_prompts=True,save_partial_transcripts=True,require_complete_selection=True,
        turn_evaluator=evaluate_turn)
    try:report=h.run(args)
    finally:os.environ.pop('QA_OPENROUTER_API_KEY',None)
    options.output.mkdir(exist_ok=True,parents=True)
    (options.output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
    (options.output/'المحادثات.md').write_text(h.report_markdown(report),encoding='utf8')
    print(json.dumps({k:report[k] for k in ('checks_passed','checks_total','ai_http_requests','provider_error_status','source_changed_during_run')},ensure_ascii=False))


if __name__=='__main__':main()
