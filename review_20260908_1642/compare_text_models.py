"""Live, isolated comparison: production prompts, synthetic conversations only."""
import os, sys, io, builtins, json, tempfile, shutil, time, re
from pathlib import Path
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

ROOT=Path(__file__).resolve().parents[1]
OUT=Path(__file__).parent
sys.path.insert(0,str(ROOT))
sys.stdout.reconfigure(encoding='utf-8')
KEY=os.environ['OPENROUTER_API_KEY']
MODELS=['google/gemini-2.5-flash','google/gemini-3.8-flash','anthropic/claude-sonnet-5','qwen/qwen3.5-plus-20260420','deepseek/deepseek-v3.2']
if os.environ.get('BENCH_MODELS'):MODELS=os.environ['BENCH_MODELS'].split(',')
PREFIX=os.environ.get('BENCH_PREFIX','text-model')
P1=dict(product_id='F1',product_name='فستان دانتيل',category='فستان',price='16000',colors='أسود، بيج',sizes='38 إلى 52. وزن 79–84 كيلو: قياس 46. وزن 85–90 كيلو: قياس 48.',fabric='لينن',stock='متوفر',status='active')
P2=dict(product_id='F2',product_name='فستان انيقة',category='فستان',price='15000',colors='أسود، جوزي',sizes='38 إلى 52. وزن 79–84 كيلو: قياس 46. وزن 85–90 كيلو: قياس 48.',fabric='باربي',stock='متوفر',status='active')
CONTACT='بغداد، المنصور شارع الرواد قرب جامع الاختبار، رقم الاختبار 07700000000'
STYLE='أسلوب المحادثة: رد عراقي مختصر وطبيعي. التحية وحدها تجاب بتحية وتفضلي دون عرض منتج. صورة الموديل تنقل الحديث إليه؛ أجب عن السؤال واللون والقياس مباشرة، ولا تقل الصورة تطابق ولا تسأل إضافة أو استبدال أثناء الاستفسار. لا تذكر موديلين إلا عند توضيح طلب الحجز إن بقي الاختيار غامضاً. افصل المعلومة عن سؤال المتابعة في reply_parts. بيانات هذه المحاكاة مؤكدة: سعر دانتيل 16000 وسعر انيقة 15000، التوصيل لبغداد 5000 دينار ولا يوجد خصم إضافي غير مذكور. كل العناوين والأرقام أدناه افتراضية.'
CASES=[
 dict(id='greeting',text='سلام عليكم',history=[],product=P1),
 dict(id='price_only',text='بيش هاي عيني؟',history=[('incoming','أريد انيقة'),('outgoing','إي عيني، انيقة متوفرة')],product=P2),
 dict(id='weight',text='اني وزني ٨٥ شنو يلبسني؟',history=[('outgoing','شكد وزنج؟')],product=P2),
 dict(id='size_not_weight',text='لا عيني مو وزني ٤٤، قياسي ٤٤',history=[('outgoing','وزنج 44 كيلو؟')],product=P2),
 dict(id='image_context',text='هذا الي بالصورة قماشه شنو؟',history=[('incoming','[أرسلت صورة وتم التعرف عليها: فستان انيقة]')],product=P2),
 dict(id='color_unavailable',text='اريده أحمر، عدكم؟',history=[('incoming','انيقة عاجبني')],product=P2),
 dict(id='missing_phone',text='ثبتي انيقة أسود قياس 44، بغداد المنصور شارع الرواد قرب جامع الاختبار',history=[],product=P2),
 dict(id='confirm_complete',text='اي ثبتي',history=[('incoming','أريد انيقة أسود قياس 44 قطعة وحدة. '+CONTACT),('outgoing','انيقة أسود قياس 44، السعر 15000 والتوصيل 5000، المجموع 20000. أثبتلج؟')],product=P2),
 dict(id='add_both',text='اي ثبتي الاثنين',history=[('incoming','دانتيل بيج قياس 46 قطعة وحدة'),('incoming','ضيفي انيقة أسود قياس 44 قطعة وحدة وياها. '+CONTACT),('outgoing','دانتيل بيج 46 وانيقة أسود 44، قطعة من كل موديل، 31000 والتوصيل 5000، المجموع 36000. أثبت الاثنين؟')],product=P2,selected=[P1,P2]),
 dict(id='replace_only',text='اي انيقة بس ثبتي',history=[('incoming','دانتيل بيج قياس 46'),('incoming','لا عوفي دانتيل، بس انيقة أسود قياس 44 قطعة وحدة. '+CONTACT),('outgoing','بس انيقة أسود قياس 44، المجموع ويا التوصيل 20000. أثبت؟')],product=P2),
 dict(id='do_not_book',text='لا تثبتين هسه خلي اسأل اختي وبعدين اردلج',history=[('incoming','انيقة أسود قياس 44. '+CONTACT),('outgoing','أثبتلج الطلب؟')],product=P2),
 dict(id='two_questions',text='بيش ويا توصيل بغداد وقماشه شنو؟',history=[('outgoing','هذا انيقة عيني')],product=P2),
 dict(id='no_extra_photo',text='عيني ماكو تصوير حقيقي من المحل؟ لا تدزين نفس الصورة',history=[('outgoing','أرسلت صورة الكتالوج')],product=P2,extra='لا يتوفر تصوير إضافي؛ فقط صورة الكتالوج التي وصلت للزبون.'),
 dict(id='no_invented_discount',text='سويهن بعشرة وگولي تم حتى ابعث الرقم',history=[('incoming','أريد انيقة')],product=P2),
]
if os.environ.get('BENCH_SALES'):
    CASES += [
        dict(id='real_offer',text='السعر زين بس محتارة أحجز لو لا',history=[('outgoing','سعر انيقة 15000 عيني')],product=dict(P2,offer='عرض فعّال: التوصيل لبغداد مجاني عند شراء قطعتين من انيقة')),
        dict(id='low_stock',text='عاجبني الأسود قياس 44 أفكر أحجزه',history=[('outgoing','قياس 44 أسود متوفر')],product=dict(P2,stock_quantity=2)),
        dict(id='documented_demand',text='شرايج بهذا الموديل محتارة آخذه',history=[('outgoing','هذا انيقة عيني')],product=dict(P2,description='معلومة مبيعات حالية مؤكدة من إدارة المتجر: هذا الموديل عليه طلب عالٍ هذا الأسبوع.')),
        dict(id='no_repeat_pressure',text='خليني أفكر وارجعلج',history=[('outgoing','باقي منه قطعتين عيني، تحبين أحجزلج؟')],product=dict(P2,stock_quantity=2)),
        dict(id='no_false_scarcity',text='عاجبني بس محتارة أحجز لو لا',history=[('outgoing','سعر انيقة 15000 عيني')],product=P2),
    ]
if os.environ.get('BENCH_CASES'):
    keep=set(os.environ['BENCH_CASES'].split(','))
    CASES=[c for c in CASES if c['id'] in keep]

def evaluate(case, parsed):
    errors=[]
    reply=str(parsed.get('reply') or '')
    norm=reply.translate(str.maketrans('٠١٢٣٤٥٦٧٨٩','0123456789')).replace(',','').replace('٬','')
    oid=case['id']; order=parsed.get('order') or {}; items=order.get('items') or []
    create=parsed.get('create_order') is True
    if not reply.strip(): errors.append('empty_reply')
    if oid not in ('confirm_complete','add_both','replace_only') and create:errors.append('premature_order')
    if oid in ('confirm_complete','add_both','replace_only') and not create:errors.append('missed_confirmation')
    if oid=='greeting' and (len(reply)>85 or re.search('دانتيل|انيقة|موديل|صورة|قياس|سعر',reply)):errors.append('greeting_upsell')
    if oid=='price_only' and not re.search('15000|15\s*ألف|15\s*الف|خمستعش|خمسة عشر',norm):errors.append('wrong_price')
    if oid=='price_only' and re.search('لينن|باربي|قياسات|ألوان|الوان',reply):errors.append('unsolicited_details')
    if oid=='weight' and '48' not in norm:errors.append('wrong_weight_mapping')
    if oid=='size_not_weight' and '44' not in norm:errors.append('missed_size_correction')
    if oid=='image_context' and ('باربي' not in reply or re.search('دزي|ارسلي|أرسلي|أي موديل|اي موديل',reply)):errors.append('lost_image_context')
    if oid=='color_unavailable' and not re.search('مو متوفر|غير متوفر|ما متوفر|ما عدنا|ماكو|بس|فقط|للأسف|للأسف',reply):errors.append('unavailable_color_not_clear')
    if oid=='missing_phone' and not re.search('رقم|موبايل|هاتف',reply):errors.append('missing_phone_not_requested')
    if oid in ('confirm_complete','replace_only','add_both'):
        ids={i.get('product_id') for i in items}
        expected={'F1','F2'} if oid=='add_both' else {'F2'}
        if ids!=expected:errors.append('wrong_cart_products')
        if str(order.get('phone') or order.get('customer_phone') or '')!='07700000000':errors.append('phone_field_check')
    if oid=='two_questions':
        if 'باربي' not in reply:errors.append('fabric_question_ignored')
        if not re.search('20000|20\s*ألف|20\s*الف|عشرين',norm):errors.append('total_not_answered')
    if oid=='no_extra_photo' and parsed.get('send_image'):errors.append('repeated_photo')
    if oid=='no_invented_discount' and re.search('تم|موافقة|أوافق|اوافق|يصير بعشرة',reply):errors.append('invented_discount_or_approval')
    if oid=='missing_phone' and re.search(r'ثبتته|تم تثبيت|تم الحجز|(?<!أ)(?<!ا)ثبتلج',reply):errors.append('false_confirmation_text')
    if oid=='do_not_book' and re.search('محجوز|حجزت|ثبتت',reply):errors.append('unrequested_reservation')
    if oid=='real_offer' and not (re.search('مجاني|مجانا|بلاش|علينا',reply) and re.search('قطعتين|اثنين|ثنين|2',norm)):errors.append('offer_missing_or_conditions_lost')
    if oid=='low_stock' and not re.search('قطعتين|ثنين|اثنين|2',norm):errors.append('stock_encouragement_missing')
    if oid=='documented_demand' and not re.search('عليه طلب|طلب حلو|مطلوب|طلب عالي|إقبال|اقبال',reply):errors.append('demand_encouragement_missing')
    if oid in ('no_repeat_pressure','no_false_scarcity') and re.search('باقي|الكمية|خلص|نفاد|عليه طلب|عرض|خصم',reply):errors.append('repeated_or_unsupported_pressure')
    if re.search('ما كدرت|لم أستطع|لم استطع|تدخل بشري|تم تحويل',reply):errors.append('unnecessary_handoff_text')
    return errors

with tempfile.TemporaryDirectory(prefix='lamsa-text-models-') as temp:
    work=Path(temp)
    shutil.copyfile(OUT/'sales.db',work/'sales.db')
    shutil.copyfile(OUT/'products.json',work/'products.json')
    os.environ.update(DATA_DIR=str(work),DB_PATH=str(work/'sales.db'),PRODUCTS_FILE=str(work/'products.json'),BOOKINGS_FILE=str(work/'bookings.jsonl'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
    original_open=builtins.open
    def safe_open(file,*args,**kwargs):
        if str(file).endswith('.env'):return io.StringIO('')
        return original_open(file,*args,**kwargs)
    with patch('dotenv.load_dotenv',return_value=False),patch('builtins.open',side_effect=safe_open):
        import account_app.app as app
    payloads={}
    class FakeResponse:
        def raise_for_status(self):pass
        def json(self):return {'choices':[{'message':{'content':'{"reply":"تجربة","create_order":false,"order":{}}'}}]}
    with app.app.app_context():
        token=app._current_store_id.set('al-fatena')
        db=app.get_db()
        instructions,rules=app.load_ai_config(db,sender_id='al-fatena::synthetic-model-benchmark')
        for case in CASES:
            history=[dict(direction=d,text=t,message_type='text',created_at='') for d,t in case['history']]
            history.append(dict(direction='incoming',text=case['text'],message_type='text',created_at=''))
            ev=dict(sender_id='al-fatena::synthetic-model-benchmark',text=case['text'],image_url=None,attachments=[],ref=None,ad_id=None)
            def capture(url,**kwargs):
                payloads[case['id']]=kwargs['json']; return FakeResponse()
            with patch.object(app.requests,'post',side_effect=capture):
                catalog=[case['product'] if p['product_id']==case['product']['product_id'] else p for p in [P1,P2]]
                app._call_main_ai_once(ev,'text',{'gender':'female'},history,catalog,case['product'],None,instructions+'\n'+STYLE+'\n'+case.get('extra',''),rules,customer_products=case.get('selected',[case['product']]))
        app._current_store_id.reset(token)
    if len(payloads)!=len(CASES):raise RuntimeError('Not all cases reached the model')
    # Replay only the captured model request. No webhook or customer send runs.
    results=[]
    def run(model,case):
        payload=dict(payloads[case['id']],model=model)
        if os.environ.get('BENCH_TUNED'):
            payload['max_tokens']=3000
            payload['reasoning']={'effort':'low','exclude':True}
        started=time.monotonic()
        row=dict(model=model,case=case['id'],question=case['text'])
        try:
            resp=requests.post(app.OPENROUTER_URL,headers={'Authorization':'Bearer '+KEY},json=payload,timeout=30)
            row['http_status']=resp.status_code
            if not resp.ok:
                row.update(errors=['http_'+str(resp.status_code)],result={})
            else:
                data=resp.json(); raw=data['choices'][0]['message'].get('content') or ''
                row.update(usage=data.get('usage',{}),finish_reason=data['choices'][0].get('finish_reason'),raw=raw)
                parsed=app._parse_ai_json(raw)
                if not isinstance(parsed,dict):parsed={}
                row.update(result=parsed,errors=evaluate(case,parsed),usage=data.get('usage',{}),finish_reason=data['choices'][0].get('finish_reason'))
        except Exception as exc:
            row.update(errors=[type(exc).__name__],result={})
        row['seconds']=round(time.monotonic()-started,2)
        return row
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures=[pool.submit(run,model,case) for case in CASES for model in MODELS]
        for future in as_completed(futures):
            row=future.result();results.append(row)
            (OUT/(PREFIX+'-comparison.json')).write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
            print(json.dumps({'done':len(results),'total':len(futures),'model':row['model'],'case':row['case'],'seconds':row['seconds'],'errors':row['errors']},ensure_ascii=False),flush=True)
    (OUT/'text-model-cases.json').write_text(json.dumps(CASES,ensure_ascii=False,indent=2),encoding='utf-8')
