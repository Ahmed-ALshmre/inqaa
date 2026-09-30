import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

source = Path(__file__).with_name('live-audit.db')
db = sqlite3.connect(f'file:{source.as_posix()}?mode=ro', uri=True)
db.row_factory = sqlite3.Row
now = datetime.now(ZoneInfo('Asia/Baghdad')).replace(microsecond=0)
start = (now - timedelta(hours=24)).isoformat()
end = now.isoformat()

rows = db.execute('''WITH first_contacts AS (
    SELECT COALESCE(NULLIF(store_id,''),'default') store_id,
           sender_id, MIN(created_at) first_message
    FROM messages WHERE direction='incoming' AND COALESCE(sender_id,'')!=''
    GROUP BY COALESCE(NULLIF(store_id,''),'default'), sender_id
), cohort AS (
    SELECT * FROM first_contacts WHERE first_message>=? AND first_message<?
), converted AS (
    SELECT c.store_id, c.sender_id FROM cohort c WHERE EXISTS (
        SELECT 1 FROM orders o WHERE o.sender_id=c.sender_id
        AND COALESCE(NULLIF(o.store_id,''),'default')=c.store_id
        AND o.created_at>=c.first_message AND o.created_at>=? AND o.created_at<?
        AND LOWER(TRIM(COALESCE(o.status,'new'))) NOT IN ('cancelled','canceled')
    )
), booked AS (
    SELECT COALESCE(NULLIF(store_id,''),'default') store_id, COUNT(*) orders
    FROM orders WHERE created_at>=? AND created_at<?
    AND LOWER(TRIM(COALESCE(status,'new'))) NOT IN ('cancelled','canceled')
    GROUP BY COALESCE(NULLIF(store_id,''),'default')
)
SELECT c.store_id, COUNT(*) new_conversations,
       (SELECT COUNT(*) FROM converted v WHERE v.store_id=c.store_id) converted_people,
       COALESCE(b.orders,0) bookings
FROM cohort c LEFT JOIN booked b ON b.store_id=c.store_id
GROUP BY c.store_id ORDER BY new_conversations DESC''', (start, end, start, end, start, end)).fetchall()

print(json.dumps({'period_start': start, 'period_end': end,
                  'stores': [dict(row) for row in rows]}, ensure_ascii=False))
db.close()
