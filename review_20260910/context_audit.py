import sqlite3,json,collections,re
from pathlib import Path
p=Path('review_20260910');c=sqlite3.connect(f'file:{p / "sales.db"}?mode=ro',uri=True);c.row_factory=sqlite3.Row
window=json.loads((p/'messages.json').read_text(encoding='utf-8'));aliases={sid:f'C{i:03}' for i,sid in enumerate(dict.fromkeys(r['sender_id'] for r in window),1)}
patterns=['الموديل المذكور مو واضح','عندج أكثر من موديل','حتى أفهم قصدج','ألغيت الموديل السابق','ما واضح عندي أي موديل']
cases=[]
for r in window:
 if r['direction']!='outgoing' or not any(x in (r['text'] or '') for x in patterns):continue
 sid=r['sender_id'];prior=[dict(z) for z in c.execute('select id,direction,text,created_at from messages where sender_id=? and id<? order by id',(sid,r['id']))]
 if len(prior)<4:continue
 raw=c.execute('select raw_payload from messages where id=?',(r['id'],)).fetchone()[0]
 case=dict(conversation=aliases[sid],reply_id=r['id'],prior_messages=len(prior),reply=r['text'],preceding=prior[-5:],payload=json.loads(raw or '{}'))
 cases.append(case)
(p/'context-cases.json').write_text(json.dumps(cases,ensure_ascii=False,indent=2),encoding='utf-8')
for x in cases:
 print(x['conversation'],x['reply_id'],'prior',x['prior_messages'],x['reply'])
 for h in x['preceding']:
  txt=re.sub(r'https?://\S+','[مرفق]',h['text'] or '')
  print(h['id'],h['direction'],txt[:220])
 print('payload',json.dumps(x['payload'],ensure_ascii=False)[:500])
print('CASES',len(cases),'conversations',len(set(x['conversation'] for x in cases)))
