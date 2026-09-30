import sqlite3,json
c=sqlite3.connect('file:outputs/token-audit-20260916/sales.db?mode=ro',uri=True); c.row_factory=sqlite3.Row
for t in ['human_reviews','messages','customer_ai_settings','customers','customer_product_interests']:
 print(t,[r[1] for r in c.execute('pragma table_info('+t+')')])
print('reviews',c.execute('select count(*) from human_reviews').fetchone()[0])
print('reasons',json.dumps([dict(r) for r in c.execute('select reason,count(*) n from human_reviews group by reason order by n desc limit 20')],ensure_ascii=False))
print('statuses', [tuple(r) for r in c.execute('select status,count(*) from human_reviews group by status')])
print('paused',[tuple(r) for r in c.execute('select enabled,count(*) from customer_ai_settings group by enabled')])
