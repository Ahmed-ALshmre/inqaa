"""Long director/writer journeys through the isolated production webhook.

Use only an owner-authorized catalogue snapshot. Live mode sends product data
and synthetic dialogue to OpenRouter; messaging/fulfillment remain blocked.
"""
import argparse
import json
from pathlib import Path
import re
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
import evaluate_sales_dialogues as h


def scenarios(catalog):
    cases=[]
    def add(identity,store,product,texts,**extra):
        cases.append(dict(id=identity,store=store,product=product,purpose=identity,expected_orders=1,
            turns=[h.step(text,'') for text in texts],**extra))
    add('director-two-weights','default','P013',[
        'سلام حبيبتي شكد سعر هذا؟','إذا اخذ قطعتين شكد الحساب ويا التوصيل لبغداد؟',
        'اريد ازرق لوزن 130 وماروني لوزن 95 قطعة من كل لون',
        'بس شنو قماشه؟','خاف مو نفس الصورة، اكدر افحصه قبل الدفع؟',
        'لا خلي الرصاصي بدل الماروني نفس وزن 95 والازرق يبقى 130',
        'ثواني بعدني دا اشاور اختي لا تثبتين هسه','تمام وافقت ثبتي القطعتين بالعرض',
        h.PHONE,'بغداد حي الاختبار قرب مدرسة التجربة','صار الحجز؟','شكرا'],
        expected_lines=[('أزرق','130',1),('رصاصي','95',1)])
    add('director-side-questions-and-second-piece','khuyoot','P007',[
        'حبيبتي شكد التنورة؟','اريد نيلي قياس 44','شنو قماشها؟',
        'وشكد طول التنورة؟','التوصيل لبغداد شكد؟ واكدر افحص قبل الدفع؟',
        'ثواني دنطلب واحد ثاني لا تثبتين هسه','وخلي جوزي قياس 46 قطعة ثانية',
        'لا اعتمدي الاسود بدل النيلي يبقى 44 والجوزي 46',
        'تمام موافقة ثبتي القطعتين',h.PHONE,'بغداد حي الاختبار قرب مدرسة التجربة',
        'ثبت الطلب؟','تسلمين'],expected_lines=[('أسود','44',1),('جوزي','46',1)])
    return cases


def check_turn(case,turn,index,transcript,texts,orders,response,m,db,sender):
    reply='\n'.join(texts)
    clean=m.sales_strategy.normalized(reply)
    known=m.checkout_customer_data(db,sender,{})
    checks={}
    if known.get('phone'):
        checks['known_phone_not_requested_again']=not re.search(r'(?:دزي|ارسلي|انطي|باقي|ممكن).{0,20}(?:رقم|موبايل)',clean)
    if 'ثواني' in turn['text']:
        checks['pause_has_no_contact_pressure']=not re.search(r'(?:دزي|ارسلي|انطي|باقي).{0,30}(?:رقم|عنوان)',clean)
        checks['pause_does_not_book']=not orders
    checks['no_fake_confirmation']=bool(orders) or not m.claims_order_created(reply)
    return checks


def check_case(case,turns,orders,m,db,sender):
    lines=json.loads(orders[0]['order_items']) if len(orders)==1 else []
    actual=sorted((m.product_variants.colors(line.get('color'))[0],str(line['size']),int(line['quantity'])) for line in lines)
    return {'exact_final_cart':actual==sorted(case['expected_lines']), 'one_order_only':len(orders)==1}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--catalog',type=Path,required=True);p.add_argument('--settings-db',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--allow-live',action='store_true')
    p.add_argument('--case',action='append',help='Run a specific long journey only.')
    a=p.parse_args()
    if not a.allow_live:p.error('Explicit --allow-live and owner authorization are required.')
    h.scenarios=scenarios
    args=argparse.Namespace(mode='live',source_root=Path(__file__).resolve().parents[1],catalog=a.catalog,
        settings_db=a.settings_db,env_file=Path('outputs/absent.env'),api_key_file=None,output=a.output,
        max_live_requests=120,max_live_cost_usd=2.0,scenario=a.case,repeat=1,sales_director=True,
        director_model='openai/gpt-6.1-terra',writer_model='google/gemini-3.8-flash',
        save_partial_transcripts=True,require_complete_selection=True,turn_evaluator=check_turn,case_evaluator=check_case)
    report=h.run(args)
    a.output.mkdir(parents=True,exist_ok=True)
    (a.output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
    (a.output/'المحادثات.md').write_text(h.report_markdown(report),encoding='utf8')
    print(json.dumps({k:report[k] for k in ('checks_passed','checks_total','ai_http_requests','reported_cost_usd','source_changed_during_run')}))
    return 0 if report['deterministic_pass'] else 1


if __name__=='__main__':raise SystemExit(main())
