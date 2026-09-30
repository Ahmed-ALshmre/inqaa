import os,sys,tempfile,pathlib,zipfile,sqlite3,json,re,collections
from contextlib import ExitStack
from unittest.mock import patch
ROOT=pathlib.Path.cwd(); OUT=ROOT/'outputs/full-handoff-review-20260916'
sys.path.insert(0,str(ROOT))
class ExternalStage(BaseException):
 pass
def stop(stage):
 def call(*a,**kw):raise ExternalStage(stage)
 return call
with tempfile.TemporaryDirectory(prefix='lamsa-simulation-') as directory:
 temp=pathlib.Path(directory)
 with zipfile.ZipFile(r'C:\Users\Ghost الشبح\Downloads\lamsa-store-backup-20260916-105632.zip') as archive:
  for name in ['sales.db','products.json']:temp.joinpath(name).write_bytes(archive.read(name))
 os.environ.update(DATA_DIR=directory,DB_PATH=str(temp/'sales.db'),PRODUCTS_FILE=str(temp/'products.json'),BOOKINGS_FILE=str(temp/'bookings.jsonl'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
 with patch('requests.sessions.Session.request',side_effect=stop('external_network')):
  from account_app import app as m
  m.init_db()
  source=sqlite3.connect(str(temp/'sales.db'));source.row_factory=sqlite3.Row
  reviews=[dict(r) for r in source.execute('select * from human_reviews order by id desc')]
  candidates=reviews
  output=[]
  for idx,review in enumerate(candidates):
   item={'review_id':review['id'],'store':review['store_id'],'question':review['message_text'],'historical_reason':review['reason'],'historical_date':review['created_at']}
   for mode in ['snapshot','enabled_counterfactual']:
    with m.app.app_context():
     db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row;source.backup(db);m.g.db=db
     token=m._current_store_id.set(review['store_id'] or 'default')
     customer=db.execute('select * from customers where sender_id=?',(review['sender_id'],)).fetchone()
     ev={'sender_id':review['sender_id'],'store_id':review['store_id'] or 'default','page_id':customer['page_id'] if customer else '', 'platform':customer['platform'] if customer else 'facebook','text':review['message_text'],'image_url':None,'attachments':[], 'ref':None,'ad_id':None,'timestamp':0,'postback':None,'quick_reply':None,'referral_source':None,'referral_type':None}
     if mode=='enabled_counterfactual':
      m.set_store_setting(db,'ai_enabled','1',ev['store_id']);m.set_customer_ai_enabled(db,ev['sender_id'],True)
     item[mode]={'store_enabled':m.is_ai_enabled(db),'conversation_enabled':m.is_customer_ai_enabled(db,ev['sender_id'])}
     linked=m.load_customer_products(db,ev['sender_id'])
     item[mode]['linked_products']=len(linked)
     item[mode]['direct_catalog_answer_available']=bool(m.known_product_question_reply(ev['text'],linked[0])) if len(linked)==1 else False
     item[mode]['pending_image_messages']=db.execute("SELECT count(*) FROM messages WHERE sender_id=? AND direction='incoming' AND (message_type='image' OR image_url IS NOT NULL AND image_url!='') AND id>COALESCE((SELECT max(id) FROM messages WHERE sender_id=? AND direction='outgoing'),0)",(ev['sender_id'],ev['sender_id'])).fetchone()[0]
     try:
      with ExitStack() as stack:
       stack.enter_context(patch.object(m,'extract_facebook_event',return_value=ev))
       stack.enter_context(patch.object(m,'is_recent_duplicate_incoming',return_value=False))
       for name in ['send_telegram_message','send_webhook_result_to_facebook','send_text_to_facebook','send_manychat_messages']:
        stack.enter_context(patch.object(m,name,return_value=True))
       for name in ['_call_main_ai_once','classify_contextual_selection','generate_first_message_reply']:
        stack.enter_context(patch.object(m,name,side_effect=stop(name)))
       stack.enter_context(patch.object(m,'_download_image_to_data_url',side_effect=stop('image_download_required')))
       result=m.process_webhook(db,{},use_debounce=False)
       item[mode].update(outcome='local_reply' if result.get('reply') else 'no_reply',meta=result.get('meta'),reply=result.get('reply'))
     except ExternalStage as exc:item[mode].update(outcome='requires_external_stage',stage=str(exc))
     except Exception as exc:item[mode].update(outcome='simulation_error',error=type(exc).__name__+':'+str(exc))
     finally:m._current_store_id.reset(token)
   item['question']=re.sub(r'[0-9٠-٩]{7,}', '[رقم محجوب]', item['question'] or '')
   output.append(item)
   if (idx+1)%25==0:print('completed',idx+1,flush=True)
  totals={'all_reviews':len(reviews),'pending_reviews':sum(r['status']=='pending' for r in reviews),'disabled_conversations':source.execute('select count(*) from customer_ai_settings where enabled=0').fetchone()[0], 'sample_size':len(output),'categories':{}}
  for mode in ['snapshot','enabled_counterfactual']:
   totals['categories'][mode]=dict(collections.Counter((r[mode].get('meta') or {}).get('reason') or r[mode].get('stage') or r[mode]['outcome'] for r in output))
  totals['reason_counts']=dict(collections.Counter(r['reason'] for r in reviews))
  OUT.joinpath(os.environ.get('SIMULATION_RESULT', 'before.json')).write_text(json.dumps({'summary':totals,'cases':output},ensure_ascii=False,indent=2),encoding='utf-8')
  print(json.dumps(totals['categories'],ensure_ascii=False))
  source.close()
