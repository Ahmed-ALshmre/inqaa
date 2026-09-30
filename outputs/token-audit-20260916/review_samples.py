import sqlite3,json,collections,re
c=sqlite3.connect('file:outputs/token-audit-20260916/sales.db?mode=ro',uri=True);c.row_factory=sqlite3.Row
r=list(c.execute('select id,store_id,message_text,reason,created_at from human_reviews order by id desc'))
print('range',min(x['created_at'] for x in r),max(x['created_at'] for x in r))
print('recent reasons',collections.Counter(x['reason'] for x in r if x['created_at']>='2026-09-15').most_common(12))
a=[dict(x) for x in r if len(x['message_text'] or '')<130 and re.search('سعر|شكد|قماش|قياس|لون|الوان|ألوان|توصيل|يوصل',x['message_text'] or '') and not re.search(r'\d{7}',x['message_text'] or '')]
print('simple candidates',len(a));print(json.dumps(a[:24],ensure_ascii=False))
