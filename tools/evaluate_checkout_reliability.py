"""Synthetic conversations only. Real main-provider HTTP, isolated DB, no delivery.

The settings JSON contains only public model IDs, temperature and token limits.
Prompts come from the repository defaults, never copied production settings.
The key is read from QA_OPENROUTER_API_KEY or hidden
terminal input. Billed calls require --allow-live and stop at 32 calls/$0.50.
"""
import argparse
import contextlib
import getpass
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'tools')]
from evaluate_sales_dialogues import HTTPGate
import requests


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--settings',type=Path,required=True)
    parser.add_argument('--output',type=Path,default=ROOT/'outputs/checkout-reliability-live')
    parser.add_argument('--allow-live',action='store_true')
    args=parser.parse_args()
    if not args.allow_live:parser.error('--allow-live is required for billed provider calls')
    key=os.environ.get('QA_OPENROUTER_API_KEY') or getpass.getpass('')
    gate=HTTPGate('live',requests.sessions.Session.request,32,0.5)
    settings=json.loads(args.settings.read_text(encoding='utf-8'))
    allowed={'main_model','main_temperature','main_max_tokens'}
    if any(set(fields)-allowed for fields in settings.values()):
        parser.error('Only public model IDs, temperature and token limits are accepted; no production prompts')
    out=args.output;out.mkdir(parents=True,exist_ok=True)
    gate.audit_path=out/'requests.jsonl'
    gate.audit_path.write_text('',encoding='utf-8')
    weight=dict(product_id='QA-W1',product_name='دشداشة قديفة اختبار',category='دشداشة نسائية',
                colors='ماروني رمادي',sizes='من 55 كيلو الى 90',price='18000',stock='متوفر',status='active')
    child=dict(product_id='QA-C1',product_name='فستان أطفال اختبار',category='ملابس أطفال',
               colors='وردي',sizes='من سنة الى 8 سنوات',price='15000',stock='متوفر',status='active')
    numbered=dict(weight,product_id='QA-N1',sizes='38 إلى 52',colors='اسود')
    cases=[
      ('weight-split','default',weight,['شتنصحيني أنتبه له حتى ما أغلط بالاختيار؟','وزني 80',
        'الماروني والرمادي','اي قطعة من كل لون','07701234567','بغداد حي الاختبار قرب مدرسة الاختبار'],1),
      ('different-counts','default',weight,['أريد أعرف شلون أختار الموديل المناسب إلي؟','وزني 80',
        'ثبتي ماروني قطعتين ورمادي قطعة','07701234567 بغداد حي الاختبار'],1),
      ('replace-color','al-fatena',weight,['شتنصحيني أنتبه له قبل الاختيار؟','وزني 80',
        'الماروني والرمادي قطعة من كل لون','خلي الماروني بدل الرمادي','07701234567 بغداد حي الاختبار'],1),
      ('child-age','baraah-kids',child,['شنو أنتبه له حتى أختار لبنتي فستان مناسب؟','عمرها 4 سنوات',
        'ثبتي الوردي قطعة وحدة','07701234567','البصرة حي الاختبار قرب مدرسة الاختبار'],1),
      ('numbered','khuyoot',numbered,['أريد أختار الموديل بس شنو أنتبه له؟','قياسي 44',
        'اريد قطعة اسود','07701234567 بغداد حي الاختبار'],1),
      ('discount','al-fatena',numbered,['غالي، نزليلي السعر شوي وأشتري','طيب شنو يميز هذا الموديل؟',
        'لا هسه ما اريد احجز'],0),
      ('declined-free-delivery','default',weight,['وزني 80','اذا التوصيل مجاني ثبتي ماروني قطعة ورمادي قطعة',
        '07701234567 بغداد حي الاختبار'],1),
      ('unresolved-arrival','default',weight,['وزني 80','اذا يوصل باجر ثبتي ماروني قطعة ورمادي قطعة',
        '07701234567 بغداد حي الاختبار'],0),
      ('identity-silent','baraah-kids',child,['أنتي روبوت لو موظفة؟'],0),
      ('ambiguous-count','default',weight,['وزني 80','الماروني والرمادي','الماروني قطعتين لو ثلاث مو متأكدة',
        '07701234567 بغداد حي الاختبار'],0),
      ('garment-size-last','default',weight,['شتنصحيني أنتبه له قبل اختيار الموديل؟',
        'الماروني والرمادي قطعة من كل لون','07701234567 بغداد حي الاختبار','ثلاث اكسات'],1),
      ('garment-size-first','default',weight,['شتنصحيني أنتبه له قبل اختيار الموديل؟',
        'ثبتي دبل اكس ماروني ورمادي قطعة من كل لون','07701234567','بغداد حي الاختبار'],1),
    ]
    observations=[]
    with tempfile.TemporaryDirectory(prefix='live-oct29-',dir=ROOT/'outputs') as temporary:
      base=Path(temporary)
      env=dict(DATA_DIR=temporary,DB_PATH=str(base/'sales.db'),PRODUCTS_FILE=str(base/'products.json'),
        BOOKINGS_FILE=str(base/'bookings.jsonl'),OPENROUTER_API_KEY=key,ENABLE_BACKGROUND_JOBS='0',
        DISABLE_CLIP='1',STORAGE_RESTORE_REQUIRED='0',TELEGRAM_BOT_TOKEN='',MANYCHAT_API_KEY='',
        ZERNIO_AL_FATENA_ENABLED='0',AI_MAX_CONCURRENT='1')
      original_exists=os.path.exists
      def exists(path):
        return False if str(path).replace('\\','/').endswith('/account_app/.env') else original_exists(path)
      def http(session,method,url,**kwargs):return gate.request(session,method,url,**kwargs)
      with contextlib.ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ,env))
        stack.enter_context(patch('dotenv.load_dotenv',return_value=False))
        stack.enter_context(patch('os.path.exists',side_effect=exists))
        stack.enter_context(patch('requests.sessions.Session.request',new=http))
        with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
          from account_app import app as m
          m.init_db()
        stack.enter_context(m.app.app_context());db=m.get_db()
        for name,value in [('send_order_to_telegram',True),('send_telegram_message',False),
                           ('save_booking_to_file',None),('send_text_to_facebook',True),
                           ('send_image_to_facebook',True),('is_ai_enabled',True),
                           ('is_customer_ai_enabled',True),('is_store_feature_enabled',False)]:
          stack.enter_context(patch.object(m,name,return_value=value))
        for case_id,store,product,texts,expected_orders in cases:
          token=m._current_store_id.set(store)
          for field,value in settings[store].items():
            target={'main_model':'ai_main_model','main_temperature':'ai_main_temperature',
                    'main_max_tokens':'ai_main_max_tokens'}.get(field,field)
            m.set_store_setting(db,target,value,store_id=store)
            assert str(m.get_app_setting(target,db=db))==str(value)
          sender=store+'::qa-oct29-'+case_id
          db.execute('INSERT INTO customers(sender_id,store_id) VALUES(?,?)',(sender,store));db.commit()
          m.bind_customer_to_product(db,sender,product,source='image_recognition')
          ev=dict(sender_id=sender,page_id='synthetic',platform='facebook',text='',image_url=None,
              ad_id=None,ref=None,store_id=store,attachments=[],timestamp=0,postback=None,
              quick_reply=None,referral_source=None,referral_type=None)
          turns=[];before=len(gate.calls)
          with patch.object(m,'load_products_from_file',return_value=[product]), \
               patch.object(m,'load_active_products',return_value=[product]):
            for text in texts:
              ev['text']=text
              gate.turn={'text':text}
              start=len(gate.calls)
              with patch.object(m,'extract_facebook_event',return_value=dict(ev)), \
                   contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
                result=m.process_webhook(db,{},use_debounce=False)
                m.send_webhook_result_to_facebook(result,sender)
              reply=result.get('reply') or ''
              turns.append(dict(input=text,reply=reply,parts=result.get('reply_parts'),
                  requires_human=result.get('requires_human'),failed=result.get('failed'),
                  meta=result.get('meta') or {},
                  calls=len(gate.calls)-start))
          orders=[dict(r) for r in db.execute('SELECT * FROM orders WHERE sender_id=?',(sender,))]
          items=json.loads(orders[0]['order_items']) if orders else []
          checks=dict(order_count=len(orders)==expected_orders,
            no_identity_disclosure=all(not m.sales_playbook.assistant_identity_statement(t['reply']) for t in turns),
            no_known_product_reask=all(not m.reply_reasks_known_product(t['reply']) for t in turns),
            no_duplicate_booking_noun=all('الحجز الحجز' not in t['reply'] for t in turns),
            no_failed_normal_turn=all(t['reply'] and not t['failed'] and not t['meta'].get('skipped') for t in turns) if case_id!='identity-silent' else True)
          if case_id=='weight-split':checks['both_colors']=len(items)==2 and all(i['quantity']==1 and i.get('weight')=='80' for i in items)
          if case_id=='different-counts':checks['counts']=sorted(i['quantity'] for i in items)==[1,2]
          if case_id=='replace-color':checks['replacement']=len(items)==1 and items[0]['quantity']==2 and items[0]['color']=='ماروني'
          if case_id=='child-age':checks['no_adult_weight']=all('كيلو' not in t['reply'] for t in turns)
          if case_id=='numbered':checks['real_size']=bool(items) and all(i['size']=='44' for i in items)
          if case_id in {'garment-size-last','garment-size-first'}:
            expected='3XL' if case_id=='garment-size-last' else 'XXL'
            checks['garment_labels_without_weight']=len(items)==2 and all(
                i['size']==expected and i['size_type']=='size' and not i.get('weight') for i in items)
            checks['no_mandatory_numeric_weight']=all('برقم واحد' not in t['reply'] for t in turns)
            checks['real_provider_used']=len(gate.calls)>before
          if case_id=='identity-silent':
            reviews=db.execute("SELECT COUNT(*) FROM human_reviews WHERE sender_id=? AND status='pending'",(sender,)).fetchone()[0]
            checks['silent_review']=turns[0]['reply']=='' and bool(turns[0]['meta'].get('customer_requested_human')) and reviews==1 and len(gate.calls)==before
          if case_id in {'weight-split','different-counts','replace-color','child-age','numbered','discount'}:
            checks['real_provider_used']=len(gate.calls)>before
          row=dict(case=case_id,store=store,passed=all(checks.values()),checks=checks,turns=turns,
                   provider_calls=len(gate.calls)-before,orders=len(orders),items=items)
          observations.append(row)
          (out/'progress.json').write_text(json.dumps(observations,ensure_ascii=False,indent=2),encoding='utf-8')
          print(json.dumps({k:row[k] for k in ('case','passed','provider_calls','checks')},ensure_ascii=True),flush=True)
          m._current_store_id.reset(token)
    report=dict(cases=observations,passed=sum(x['passed'] for x in observations),total=len(observations),
        requests=gate.calls,cost_usd=gate.spent,blocked=gate.blocked,customer_records_copied=0,external_deliveries=0,
        model_settings_scoped=True,prompt_source='public_repository_defaults',turns=sum(len(c['turns']) for c in observations))
    (out/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(dict(passed=report['passed'],total=report['total'],requests=len(gate.calls),cost_usd=gate.spent)),flush=True)
    return 0 if report['passed']==report['total'] else 1

if __name__=='__main__':raise SystemExit(main())
