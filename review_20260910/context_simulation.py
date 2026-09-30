import os,sys,tempfile,json,sqlite3
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
p=Path(__file__).parent
with tempfile.TemporaryDirectory() as root,patch('requests.sessions.Session.request',side_effect=AssertionError('Network disabled')):
 os.environ.update(DATA_DIR=root,DB_PATH=str(Path(root)/'test.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
 import account_app.app as m
 catalog=[x for x in json.loads((p/'products.json').read_text(encoding='utf-8')) if x['store_id']=='khuyoot']
 skirt=next(x for x in catalog if x['product_name']=='تنورة')
 results=[]
 with patch.object(m,'load_products_from_file',return_value=catalog),m.app.app_context():
  db=m.get_db();sender='khuyoot::context-audit'
  token=m._current_store_id.set('khuyoot')
  m.get_or_create_customer(db,sender,'test','facebook')
  m.complete_customer_product_link(db,sender,skirt,'manual',source='manual_admin')
  for i in range(100):
   m.save_message(db,sender,'incoming' if i%2==0 else 'outgoing','text',f'متابعة تجريبية {i}',None,None,None,{})
  results.append(dict(test='binding_after_100_messages',products=[x['product_name'] for x in m.load_customer_products(db,sender)],history_count=len(m.load_history(db,sender))))
  for text in ['شكد سعره؟','ست نفس الي بالصوره يوصل','قطعه وحده بس اذا ماعجبتني ترجع بيد المندوب بدون ما ادفع شيء','قماشها شنو\nاكو تصوير حقيقي\nللقطعة','لعد شنو كاتبه نحدد لون تنوره','اي التنوره\nدريت صورتها']:
   chosen=m.select_customer_context_product(text,[skirt])
   result=m._call_main_ai_once(dict(sender_id=sender,text=text),'text',{},[],catalog,chosen,None,'',[],customer_products=[skirt]) if not chosen else {}
   results.append(dict(test='single_bound_product',text=text,selected=chosen['product_name'] if chosen else None,request=m._extract_product_request(text),reply=result.get('reply')))
  for text in ['بس كوليلي شنو نوع القماش','لا حبي تسلمين','واذا مو نفس الصوره ارجعهه وكروه ماادفع']:
   with patch.object(m,'classify_contextual_selection',return_value=None):
    response=m.resolve_contextual_product_selection(db,dict(sender_id=sender,text=text),catalog)
   results.append(dict(test='classifier_unavailable',text=text,reply=(response or {}).get('reply'),binding=[x['product_name'] for x in m.load_customer_products(db,sender)]))
  for product in catalog:
   if product['product_id'] != skirt['product_id']:
    db.execute("INSERT INTO customer_product_interests(sender_id,product_id,product_name,status,source,match_method) VALUES(?,?,?,'suggested','catalog_search','catalog_search')",(sender,product['product_id'],product['product_name']))
  db.commit()
  with patch.object(m,'classify_contextual_selection',return_value=None):
   response=m.resolve_contextual_product_selection(db,dict(sender_id=sender,text='بس كوليلي شنو نوع القماش'),catalog)
  results.append(dict(test='suggestions_reenter_clarification',active=[x['product_name'] for x in m.load_customer_products(db,sender)],reply=(response or {}).get('reply')))
  m._current_store_id.reset(token)
(p/'context-simulation-results.json').write_text(json.dumps(results,ensure_ascii=False,indent=2,default=list),encoding='utf-8')
print(json.dumps(results,ensure_ascii=False,indent=2,default=list))
