import sqlite3,json,collections,statistics,datetime
from pathlib import Path
p=Path(__file__).parent;c=sqlite3.connect(f'file:{p / "sales.db"}?mode=ro',uri=True);c.row_factory=sqlite3.Row
rows=json.loads((p/'messages.json').read_text(encoding='utf-8'));groups=collections.defaultdict(list)
for r in rows:groups[r['sender_id']].append(r)
alias={sid:f'C{i:03}' for i,sid in enumerate(groups,1)};end=datetime.datetime.fromisoformat('2026-09-10T10:52:06+03:00')
orders=json.loads((p/'orders.json').read_text(encoding='utf-8'));reviews=json.loads((p/'human_reviews.json').read_text(encoding='utf-8'))
no_out=[];tails=[];latencies=[];delays=[];stores={}
for sid,ms in groups.items():
 a=alias[sid];pending=[]
 if not any(r['direction']=='outgoing' for r in ms):no_out.append(a)
 for r in ms:
  if r['direction']=='incoming':pending.append(r)
  elif pending:
   delay=(datetime.datetime.fromisoformat(r['created_at'])-datetime.datetime.fromisoformat(pending[-1]['created_at'])).total_seconds();latencies.append(delay)
   if delay>300:delays.append(dict(conversation=a,seconds=delay,incoming=pending[-1]['id'],outgoing=r['id']))
   pending=[]
 if pending:tails.append(dict(conversation=a,messages=len(pending),last_message=pending[-1]['id'],age_minutes=round((end-datetime.datetime.fromisoformat(pending[-1]['created_at'])).total_seconds()/60,1)))
 store=ms[0]['store_id'];st=stores.setdefault(store,dict(conversations=0,incoming=0,outgoing=0,orders=0));st['conversations']+=1
 for r in ms:st[r['direction']]+=1
for o in orders:stores[o['store_id']]['orders']+=1
flags={}
for name,term in [('unclear_model','الموديل المذكور مو واضح'),('generic_request','دزيلي صورة الموديل أو القياس'),('proposal','هذه القطع المقترحة'),('confirmation','تم تثبيت الطلب')]:
 hits=[r for r in rows if r['direction']=='outgoing' and term in (r['text'] or '')];flags[name]=dict(messages=len(hits),conversations=len(set(r['sender_id'] for r in hits)))
summary=dict(window=['2026-09-09T10:52:06+03:00','2026-09-10T10:52:06+03:00'],integrity=c.execute('pragma integrity_check').fetchone()[0],messages=len(rows),conversations=len(groups),stores=stores,orders=len(orders),ordering_customers=len(set(o['sender_id'] for o in orders)),order_statuses=dict(collections.Counter(o['status'] for o in orders)),reviews=len(reviews),review_statuses=dict(collections.Counter(r['status'] for r in reviews)),review_reasons=dict(collections.Counter(r['reason'] for r in reviews)),no_outgoing=no_out,unanswered_tails=tails,response_bursts=len(latencies),median_response_seconds=statistics.median(latencies),p90_response_seconds=sorted(latencies)[int(.9*(len(latencies)-1))],delays_over_5min=delays,flags=flags)
(p/'metrics.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps({k:v for k,v in summary.items() if k not in ['unanswered_tails','delays_over_5min','review_reasons']},ensure_ascii=False,indent=2))
print('pending tails',len(tails),'delays',len(delays));print('reasons',summary['review_reasons'])
for a in ['C007','C010','C021','C024','C107','C157','C185','C190','C204']:
 sid=next(s for s in groups if alias[s]==a)
 oo=[dict(r) for r in c.execute('select id,status,size,product_name,order_items,created_at from orders where sender_id=?',(sid,))]
 print(a,'orders',oo,'ai',tuple(c.execute('select enabled,updated_at from customer_ai_settings where sender_id=?',(sid,)).fetchone() or []))
