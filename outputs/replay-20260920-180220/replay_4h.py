"""Offline chronological routing audit; never invokes providers or customer delivery."""
import collections
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import zipfile
from contextlib import ExitStack
from datetime import datetime, timedelta
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding='utf-8')
ARCHIVE = Path.home() / 'Downloads/lamsa-store-backup-20260920-180220.zip'
OUT = Path(__file__).resolve().parent


class ExternalStage(BaseException):
    pass


def stop(stage):
    def halted(*args, **kwargs):
        raise ExternalStage(stage)
    return halted


def insert(db, table, row):
    fields = ','.join('"' + key + '"' for key in row)
    db.execute(f'INSERT OR REPLACE INTO {table} ({fields}) VALUES ({",".join("?" for _ in row)})', tuple(row.values()))


def main():
    digest = hashlib.sha256(ARCHIVE.read_bytes()).hexdigest()
    with zipfile.ZipFile(ARCHIVE) as archive, tempfile.TemporaryDirectory(prefix='lamsa-audit-') as directory:
        info = json.loads(archive.read('backup-info.json'))
        end = datetime.fromisoformat(info['created_at']); start = end - timedelta(hours=4)
        raw = bytearray(archive.read('sales.db')); raw[18] = raw[19] = 1
        source = sqlite3.connect(':memory:'); source.row_factory = sqlite3.Row; source.deserialize(raw)
        integrity = source.execute('PRAGMA quick_check').fetchone()[0]
        temp = Path(directory)
        (temp / 'products.json').write_bytes(archive.read('products.json'))
        os.environ.update(DATA_DIR=directory, DB_PATH=str(temp / 'audit.db'), PRODUCTS_FILE=str(temp / 'products.json'),
                          BOOKINGS_FILE=str(temp / 'bookings.jsonl'), ENABLE_BACKGROUND_JOBS='0', DISABLE_CLIP='1')
        with patch('requests.sessions.Session.request', side_effect=stop('external_network')):
            from account_app import app as m
            base = sqlite3.connect(str(temp / 'audit.db')); base.row_factory = sqlite3.Row
            # Only configuration is shared across events. No future customer state.
            for table in ('app_settings', 'ai_instructions', 'forbidden_rules', 'stores'):
                base.execute(f'DELETE FROM {table}')
                for row in source.execute(f'SELECT * FROM {table}'):
                    insert(base, table, dict(row))
            base.commit()
            events = [dict(row) for row in source.execute('''SELECT * FROM messages
                WHERE julianday(created_at)<=julianday(?) ORDER BY julianday(created_at),id''', (end.isoformat(),))]
            customers = {r['sender_id']: dict(r) for r in source.execute('SELECT * FROM customers')}
            histories = collections.defaultdict(list)
            snapshots = {}
            for table, date in [('orders', 'created_at'), ('human_reviews', 'created_at'), ('customer_product_interests', 'last_seen_at')]:
                grouped = collections.defaultdict(list)
                for row in source.execute(f'SELECT * FROM {table}'):
                    grouped[row['sender_id']].append(dict(row))
                snapshots[table] = (date, grouped)
            counts = collections.Counter(); cases = []; layout = collections.Counter(); violations = 0
            for message in events:
                histories[message['sender_id']].append(message)
                try:
                    at = datetime.fromisoformat(message['created_at'])
                    if at.tzinfo is None: at = at.replace(tzinfo=end.tzinfo)
                except (ValueError, TypeError):
                    continue
                if not start <= at <= end:
                    continue
                if message['direction'] == 'outgoing':
                    text = message.get('text') or ''
                    if text:
                        parts = m.approved_reply_parts({}, text)
                        layout[len(parts)] += 1
                        from account_app.reply_layout import compact
                        violations += compact(' '.join(parts)) != compact(text)
                    continue
                db = sqlite3.connect(':memory:'); db.row_factory = sqlite3.Row; base.backup(db)
                sender = message['sender_id']; store = message.get('store_id') or 'default'
                customer = dict(customers[sender])
                for field in ('phone', 'province', 'address', 'notes', 'gender'):
                    customer[field] = None
                insert(db, 'customers', customer)
                for old in histories[sender]: insert(db, 'messages', old)
                for table, (date, rows) in snapshots.items():
                    for original in rows.get(sender, []):
                        value = original.get(date)
                        if not value: continue
                        try:
                            when = datetime.fromisoformat(value)
                            if when.tzinfo is None: when = when.replace(tzinfo=end.tzinfo)
                        except ValueError: continue
                        if when <= at:
                            row = dict(original)
                            if table == 'human_reviews' and row.get('replied_at') and row['replied_at'] > message['created_at']:
                                row['status'], row['replied_at'] = 'pending', None
                            insert(db, table, row)
                db.commit()
                with m.app.app_context():
                    m.g.db = db; token = m._current_store_id.set(store)
                    m.set_store_setting(db, 'ai_enabled', '1', store)
                    m.set_customer_ai_enabled(db, sender, True)
                    ev = dict(sender_id=sender, store_id=store, page_id=customer.get('page_id') or '',
                              platform=customer.get('platform') or 'facebook', text=message.get('text') or '',
                              image_url=message.get('image_url'), attachments=m.message_media(message),
                              ref=message.get('ref'), ad_id=message.get('ad_id'), timestamp=0,
                              postback=None, quick_reply=None, postback_payload=None, quick_reply_payload=None,
                              referral_source=None, referral_type=None)
                    outcome = {}
                    try:
                        with ExitStack() as patches:
                            patches.enter_context(patch.object(m, 'extract_facebook_event', return_value=ev))
                            for function in ('_call_main_ai_once', 'classify_contextual_selection', 'generate_first_message_reply',
                                             '_download_image_to_data_url', '_image_ref_for_openrouter', 'match_customer_image_with_catalog'):
                                patches.enter_context(patch.object(m, function, side_effect=stop(function)))
                            for function in ('send_telegram_message', 'send_webhook_result_to_facebook', 'send_text_to_facebook',
                                             'send_manychat_messages', 'send_image_to_facebook'):
                                patches.enter_context(patch.object(m, function, return_value=True))
                            result = m.process_webhook(db, {}, use_debounce=False, incoming_message_id=message['id'])
                            outcome = {'outcome': 'local_reply' if result.get('reply') else 'no_reply',
                                       'reason': (result.get('meta') or {}).get('reason')}
                    except ExternalStage as exc:
                        outcome = {'outcome': 'external_required', 'stage': str(exc)}
                    except Exception as exc:
                        outcome = {'outcome': 'simulation_error', 'error': type(exc).__name__}
                    finally:
                        m._current_store_id.reset(token)
                    counts[outcome.get('stage') or outcome.get('reason') or outcome['outcome']] += 1
                    cases.append({'message_id': message['id'], 'store': store, **outcome})
                if len(cases) % 250 == 0: print('replayed', len(cases), flush=True)
            reviews = [dict(r) for r in source.execute('''SELECT * FROM human_reviews
                WHERE julianday(created_at) BETWEEN julianday(?) AND julianday(?)''', (start.isoformat(), end.isoformat()))]
            result = {'archive_sha256': digest, 'integrity': integrity, 'window': [start.isoformat(), end.isoformat()],
                      'incoming_events_replayed': len(cases), 'routing_counts': dict(counts),
                      'historical_review_count': len(reviews),
                      'historical_timeouts': sum('ReadTimeout' in r['reason'] for r in reviews),
                      'historical_image_no_match': sum('confidently match customer image' in r['reason'] for r in reviews),
                      'historical_billing_errors': sum('provider_http_402' in r['reason'] for r in reviews),
                      'layout_parts_distribution': dict(layout), 'layout_content_changes': violations,
                      'limitations': ['Offline routing, not live AI accuracy or latency measurement.',
                         'AI enabled counterfactually to exercise routing; the snapshot global AI switch is off.',
                         'Settings/product metadata are snapshot values; their historical revisions are unavailable.',
                         'Only links whose last_seen_at precedes each event are used; earlier overwritten links cannot be reconstructed.',
                         'No external provider requests or customer messages were sent.'], 'cases': cases}
            (OUT / 'replay-results.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
            print(json.dumps({k: v for k, v in result.items() if k != 'cases'}, ensure_ascii=False, indent=2))
            base.close(); source.close()
    assert hashlib.sha256(ARCHIVE.read_bytes()).hexdigest() == digest


if __name__ == '__main__':
    main()

