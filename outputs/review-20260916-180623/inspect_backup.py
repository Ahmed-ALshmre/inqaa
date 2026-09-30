import json,sqlite3,zipfile,hashlib
from pathlib import Path
from datetime import datetime,timedelta
OUT=Path(__file__).resolve().parent
ARCHIVE=Path.home()/'Downloads/lamsa-store-backup-20260916-180623.zip'
z=zipfile.ZipFile(ARCHIVE);info=json.loads(z.read('backup-info.json'))
end=datetime.fromisoformat(info['created_at']);start=end-timedelta(hours=2)
b=bytearray(z.read('sales.db'));b[18]=b[19]=1
db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row;db.deserialize(b)
def rows(q,args=()):return [dict(r) for r in db.execute(q,args)]
if __name__=='__main__':
 print('window',start.isoformat(),end.isoformat(),'integrity',db.execute('pragma quick_check').fetchone()[0])
 print('counts',rows("SELECT store_id,direction,count(*) n FROM messages WHERE julianday(created_at) BETWEEN julianday(?) AND julianday(?) GROUP BY store_id,direction",(start.isoformat(),end.isoformat())))
 targets=rows("SELECT sender_id,name,store_id FROM customers WHERE name IN ('ام احمد الراوي الراوي','shahraban_cake','احمد الشمري')")
 for c in targets:
  print('CUSTOMER',c)
  messages=rows("SELECT id,created_at,direction,message_type,text,image_url,raw_payload FROM messages WHERE sender_id=? AND julianday(created_at) BETWEEN julianday(?) AND julianday(?) ORDER BY id",(c['sender_id'],start.isoformat(),end.isoformat()))
  for m in messages:print(json.dumps({k:v for k,v in m.items() if k not in ['raw_payload','image_url']},ensure_ascii=False))
  for table in ['customer_product_interests','human_reviews','customer_image_positions','ai_jobs','conversation_pause_state']:
   found=rows('SELECT * FROM '+table+' WHERE sender_id=?',(c['sender_id'],))
   (OUT/(c['sender_id'].replace('::','_')+'_'+table+'.json')).write_text(json.dumps(found,ensure_ascii=False,indent=2),encoding='utf-8')
   if table!='ai_jobs':print(table,json.dumps(found,ensure_ascii=False)[:2400])
  (OUT/(c['sender_id'].replace('::','_')+'_messages.json')).write_text(json.dumps(messages,ensure_ascii=False,indent=2),encoding='utf-8')
 print('KHUYOOT',rows("SELECT c.name,m.sender_id,count(*) n FROM messages m LEFT JOIN customers c ON c.sender_id=m.sender_id WHERE m.store_id='khuyoot' AND julianday(m.created_at) BETWEEN julianday(?) AND julianday(?) GROUP BY m.sender_id",(start.isoformat(),end.isoformat())))
