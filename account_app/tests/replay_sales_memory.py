"""Read-only backup replay; no model, bookings, customer delivery or credentials.

python -m account_app.tests.replay_sales_memory PATH_TO_SALES_DB OUTPUT_JSON
"""
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter

from account_app import sales_context


def replay(source):
    start = perf_counter()
    db = sqlite3.connect(Path(source).resolve().as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        end = db.execute('SELECT MAX(created_at) FROM messages').fetchone()[0]
        cutoff = (datetime.fromisoformat(end) - timedelta(hours=48)).isoformat()
        active = {(r[0], r[1]) for r in db.execute('SELECT DISTINCT store_id,sender_id FROM messages WHERE created_at>=?', (cutoff,))}
        histories = defaultdict(list)
        for row in db.execute('SELECT id,store_id,sender_id,direction,text,created_at,message_type,image_url FROM messages ORDER BY id'):
            key = row['store_id'], row['sender_id']
            if key in active:
                histories[key].append(dict(row))
        errors = []
        maximum = 0
        for key, messages in histories.items():
            full = sales_context.advance({}, messages)
            incremental = {}
            for offset in range(0, len(messages), 7):
                incremental = sales_context.advance(incremental, messages[offset:offset+7])
            if full != incremental:
                errors.append({'kind': 'incremental_mismatch', 'last_message_id': messages[-1]['id']})
            assert len(full['evidence']) <= 24
            assert len(full['last_staff_turn']) <= 6
            maximum = max(maximum, len(json.dumps(full, ensure_ascii=False)))
        resolved = []
        for sender, mid in [('al-fatena::cw-184605-4178', 42569), ('al-fatena::cw-184605-4575', 46609), ('al-fatena::cw-184605-4670', 51334)]:
            rows = [dict(r) for r in db.execute('SELECT id,direction,text,created_at,message_type,image_url FROM messages WHERE sender_id=? AND id<=? ORDER BY id', (sender, mid))]
            assert rows, mid
            intent = sales_context.continuation(rows[-1]['text'], sales_context.advance({}, rows))
            assert intent and intent['intent'] == 'browse', mid
            resolved.append({'message_id': mid, 'intent': intent['intent']})
        return {'window_start': cutoff, 'window_end': end, 'conversations_checked': len(histories),
                'historical_messages_checked': sum(map(len, histories.values())),
                'incremental_mismatches': errors, 'memory_size_max_chars': maximum,
                'real_short_reply_cases': resolved, 'elapsed_seconds': round(perf_counter()-start, 3),
                'read_only': True, 'model_calls': 0, 'customer_messages_sent': 0}
    finally:
        db.close()


if __name__ == '__main__':
    result = replay(sys.argv[1])
    Path(sys.argv[2]).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(bool(result['incremental_mismatches']))
