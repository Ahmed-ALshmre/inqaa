"""Durable order delivery. Payloads are frozen in the booking transaction."""
import json
import os
import re
import sqlite3
import threading
import time
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlparse

import requests

# Names verified against the receiving API; never route an unknown store to Lamsa.
REMOTE_STORE_NAMES = {
    'default': 'لمسة ستور',
    'khuyoot': 'خيوط ستور',
    'al-fatena': 'ملابس الفاتنة',
    'baraah-kids': 'عالم البراءة لملابس الأطفال',
}
EMPLOYEE_NAME = 'صوف'
TELEGRAM_ONLY_STORES = {'golden-threads'}

try:
    from .pricing import quote as price_order
except ImportError:
    from pricing import quote as price_order


def init_db(db):
    db.execute("""CREATE TABLE IF NOT EXISTS menger_store_settings (
        local_store TEXT PRIMARY KEY, api_url TEXT NOT NULL DEFAULT '',
        api_key TEXT NOT NULL DEFAULT '', store_id TEXT NOT NULL DEFAULT '',
        source TEXT NOT NULL DEFAULT 'lamsa-project', telegram_chat_id TEXT NOT NULL DEFAULT ''
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS menger_connection (
        id INTEGER PRIMARY KEY CHECK(id=1), api_url TEXT NOT NULL DEFAULT '',
        api_key TEXT NOT NULL DEFAULT '', source TEXT NOT NULL DEFAULT 'lamsa-project'
    )""")
    columns = {row[1] for row in db.execute('PRAGMA table_info(menger_store_settings)')}
    if 'store_name' not in columns:
        db.execute("ALTER TABLE menger_store_settings ADD COLUMN store_name TEXT NOT NULL DEFAULT ''")
    # Preserve an unambiguous previous connection; conflicting store connections
    # require selecting one shared connection in the settings screen.
    if not db.execute('SELECT 1 FROM menger_connection').fetchone():
        previous = db.execute("SELECT DISTINCT api_url,api_key,source FROM menger_store_settings WHERE api_url!='' AND api_key!=''").fetchall()
        if len(previous) == 1:
            db.execute('INSERT INTO menger_connection VALUES(1,?,?,?)', tuple(previous[0]))
    db.execute("""CREATE TABLE IF NOT EXISTS menger_deliveries (
        order_id INTEGER PRIMARY KEY, local_store TEXT NOT NULL,
        payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
        remote_order_id TEXT, last_error TEXT, synced_at REAL
    )""")
    columns = {row[1] for row in db.execute('PRAGMA table_info(menger_deliveries)')}
    for name, definition in {
        'remote_status': 'TEXT', 'remote_updated_at': 'TEXT', 'status_checked_at': 'REAL',
        'next_status_check': 'REAL NOT NULL DEFAULT 0', 'status_error': 'TEXT',
        'booking_actor': "TEXT NOT NULL DEFAULT 'unknown'",
    }.items():
        if name not in columns:
            db.execute(f'ALTER TABLE menger_deliveries ADD COLUMN {name} {definition}')
    # Owner selected the shared Telegram group only for this store.
    db.execute("UPDATE menger_deliveries SET status='telegram_only',last_error=NULL WHERE local_store='golden-threads' AND status NOT IN ('sent','cancelled','telegram_only')")


def connection(db, public=False):
    row = db.execute('SELECT api_url,api_key,source FROM menger_connection WHERE id=1').fetchone()
    base_url = os.environ.get('MENGER_BASE_URL', '').strip().rstrip('/')
    data = dict(row) if row else dict(api_url=os.environ.get('MENGER_ORDERS_API_URL', '').strip() or (base_url + '/api/v1/orders' if base_url else ''),
                                    api_key=os.environ.get('MENGER_ORDERS_API_KEY', '').strip(), source='lamsa-project')
    if public:
        data['key_configured'] = bool(data.pop('api_key'))
    return data


def save_connection(db, data):
    current = connection(db)
    first_activation = not current.get('api_key') and bool(data.get('api_key'))
    for field in ('api_url', 'source'):
        if field in data:
            current[field] = str(data[field] or '').strip()
    if data.get('api_key'):
        current['api_key'] = str(data['api_key']).strip()
    if data.get('clear_api_key') is True:
        current['api_key'] = ''
    if current['api_url']:
        parsed = urlparse(current['api_url'])
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.fragment or parsed.query:
            raise ValueError('أدخل رابط API صحيحاً يبدأ بـ https://')
        if parsed.path in ('', '/'):
            current['api_url'] = current['api_url'].rstrip('/') + '/api/v1/orders'
        elif parsed.path.rstrip('/') != '/api/v1/orders':
            raise ValueError('يجب أن ينتهي رابط مينجر بـ /api/v1/orders')
        current['api_url'] = current['api_url'].rstrip('/')
    if not current['source']:
        raise ValueError('اسم المشروع المرسل مطلوب')
    db.execute('INSERT INTO menger_connection VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET api_url=excluded.api_url,api_key=excluded.api_key,source=excluded.source',
               tuple(current[k] for k in ('api_url','api_key','source')))
    # The old per-store credential fields are no longer used.
    db.execute("UPDATE menger_store_settings SET api_url='',api_key=''")
    if first_activation:
        # Old Telegram imports may already exist in Menger. Do not replay a
        # historical unlinked backlog merely because a key has been added.
        for row in db.execute("SELECT order_id,payload FROM menger_deliveries WHERE status IN ('pending','retry','blocked','sending')").fetchall():
            try:
                attribution = json.loads(row['payload']).get('attribution') or {}
            except (TypeError, ValueError):
                attribution = {}
            if not attribution.get('conversation_id'):
                db.execute("UPDATE menger_deliveries SET status='review_required',last_error='legacy_booking_needs_reconciliation' WHERE order_id=?", (row['order_id'],))


def settings(db, store, public=False):
    row = db.execute('SELECT store_id,store_name,telegram_chat_id FROM menger_store_settings WHERE local_store=?', (store,)).fetchone()
    data = dict(row) if row else dict(store_id='', store_name='', telegram_chat_id='')
    if not data['store_id'] and not data['store_name']:
        data['store_name'] = REMOTE_STORE_NAMES.get(store, '')
    return data


def remote_stores(db):
    config = connection(db)
    parsed = urlparse(config['api_url'])
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or not config['api_key']):
        raise ValueError('احفظ رابط مينجر ومفتاح الربط أولاً')
    if not parsed.path.rstrip('/').endswith('/api/v1/orders'):
        raise ValueError('يجب أن ينتهي رابط مينجر بـ /api/v1/orders')
    url = config['api_url'].rstrip('/').rsplit('/', 1)[0] + '/stores'
    try:
        response = requests.get(url, headers={'Authorization': 'Bearer ' + config['api_key'], 'Accept': 'application/json'},
                                timeout=15, allow_redirects=False)
        if response.status_code != 200:
            raise ValueError(f'فشل الاتصال بمينجر (HTTP {response.status_code})')
        result = response.json()
        if not isinstance(result, dict) or result.get('ok') is not True or not isinstance(result.get('stores'), list):
            raise ValueError('استجابة المتاجر من مينجر غير صحيحة')
        return [dict(store_id=s['store_id'], name=s['name']) for s in result['stores']
                if isinstance(s, dict) and isinstance(s.get('store_id'), str) and isinstance(s.get('name'), str)]
    except requests.RequestException as exc:
        raise ValueError('تعذر الاتصال بمينجر؛ تحقق من الرابط والمفتاح') from exc


def save_settings(db, store, data):
    if any(field in data for field in ('api_url','api_key','source','clear_api_key')):
        raise ValueError('احفظ رابط API والمفتاح من إعدادات الربط الموحدة أعلى الصفحة')
    current = settings(db, store)
    for field in ('store_id', 'store_name', 'telegram_chat_id'):
        if field in data:
            current[field] = str(data[field] or '').strip()
    if 'store_id' in data and 'store_name' not in data:
        current['store_name'] = ''
    if current['store_id']:
        duplicate = db.execute('SELECT local_store FROM menger_store_settings WHERE store_id=? AND local_store!=?',
                               (current['store_id'], store)).fetchone()
        if duplicate:
            raise ValueError('متجر مينجر هذا مرتبط بمتجر آخر؛ اختر الوجهة الخاصة بهذا المتجر')
    db.execute('INSERT INTO menger_store_settings(local_store,store_id,store_name,telegram_chat_id) VALUES(?,?,?,?) ON CONFLICT(local_store) DO UPDATE SET store_id=excluded.store_id,store_name=excluded.store_name,telegram_chat_id=excluded.telegram_chat_id',
               (store, current['store_id'], current['store_name'], current['telegram_chat_id']))


def _normalized_store_name(value):
    value = unicodedata.normalize('NFKC', str(value or '')).casefold()
    value = re.sub(r'[\u064b-\u065f\u0670]', '', value)
    value = value.translate(str.maketrans({'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ى': 'ي', 'ة': 'ه'}))
    return re.sub(r'[^\w\u0600-\u06ff]+', ' ', value).strip()


def sync_stores(db, get=None):
    """Verify the shared API and bind local stores to Menger by stable name."""
    get = get or requests.get
    config = connection(db)
    parsed = urlparse(config.get('api_url') or '')
    if parsed.scheme != 'https' or not parsed.hostname or not config.get('api_key'):
        raise ValueError('احفظ رابط Menger ومفتاح API أولاً')
    base_path = parsed.path.rstrip('/')
    if base_path.endswith('/orders'):
        base_path = base_path[:-len('/orders')]
    stores_url = parsed._replace(path=f'{base_path}/stores', params='', query='', fragment='').geturl()
    try:
        response = get(stores_url, headers={
            'Authorization': f"Bearer {config['api_key']}",
            'Accept': 'application/json',
        }, timeout=20, allow_redirects=False)
    except requests.RequestException as exc:
        raise ValueError('تعذر الاتصال بـ Menger حالياً') from exc
    try:
        payload = response.json()
    except ValueError as exc:
        raise ValueError(f'استجابة Menger غير صالحة (HTTP {response.status_code})') from exc
    if response.status_code != 200 or not isinstance(payload, dict) or payload.get('ok') is not True:
        message = payload.get('error') if isinstance(payload, dict) else None
        raise ValueError(message or f'فشل اختبار الربط مع Menger (HTTP {response.status_code})')
    remote_stores = payload.get('stores')
    if not isinstance(remote_stores, list):
        raise ValueError('لم يُرجع Menger قائمة متاجر صالحة')
    remote_by_name = {}
    for store in remote_stores:
        if not isinstance(store, dict):
            continue
        name = _normalized_store_name(store.get('name'))
        store_id = str(store.get('store_id') or '').strip()
        if name and store_id:
            remote_by_name.setdefault(name, []).append({'store_id': store_id, 'store_name': str(store['name']).strip()})
    try:
        local_stores = db.execute('SELECT store_id,name FROM stores WHERE active=1 ORDER BY name').fetchall()
    except sqlite3.OperationalError as exc:
        raise ValueError('تعذر قراءة المتاجر المحلية') from exc
    matched, unmatched = [], []
    for local in local_stores:
        candidates = remote_by_name.get(_normalized_store_name(local['name']), [])
        if not candidates:
            candidates = remote_by_name.get(_normalized_store_name(REMOTE_STORE_NAMES.get(local['store_id'], '')), [])
        if len(candidates) != 1:
            unmatched.append(local['name'])
            continue
        save_settings(db, local['store_id'], candidates[0])
        matched.append({'local_store': local['store_id'], 'name': local['name'], **candidates[0]})
    db.commit()
    return {'matched': matched, 'unmatched': unmatched, 'remote_count': len(remote_stores)}


def snapshot_prices(order, catalog):
    products = {p['product_id']: p for p in catalog}
    if any('unit_price' not in item for item in order['items']):
        order['items'][:] = price_order(order['items'], catalog, order.get('delivery_fee', 0))['items']
    for item in order['items']:
        product = products.get(item.get('product_id'), {})
        item.pop('send_to', None)
        item['order_name'] = str(product.get('order_name') or '').strip() or item['product_name']
        item['unit_price'] = int(item['unit_price'])


def enqueue(db, order_id, order, catalog):
    init_db(db)
    if (order.get('store_id') or 'default') in TELEGRAM_ONLY_STORES:
        return
    if any('unit_price' not in item for item in order['items']):
        snapshot_prices(order, catalog)
    products = {p['product_id']: p for p in catalog}
    items = []
    for item in order['items']:
        product = products.get(item.get('product_id'), {})
        if product.get('store_id') and product['store_id'] != (order.get('store_id') or 'default'):
            raise ValueError('لا يمكن إرسال منتج من متجر آخر ضمن هذا الطلب')
        price = str(item['unit_price'])
        items.append(dict(product_name=item.get('order_name') or item['product_name'],
                          quantity=int(item.get('quantity') or 1),
                          color=item.get('color') or '', size=item.get('size') or '',
                          size_type=item.get('size_type') or 'size', weight=item.get('weight') or '',
                          unit_price=int(price) if price else 0))
    if not items:
        return
    config = connection(db)
    local_store = order.get('store_id') or 'default'
    store_name = settings(db, local_store)['store_name']
    notes = [str(order.get('notes') or '').strip()]
    for item in order['items']:
        if str(item.get('notes') or '').strip():
            notes.append(f"{item.get('order_name') or item['product_name']} ({item.get('color') or ''} / {item.get('size') or ''}): {item['notes']}")
    # The receiver stores one final order price, including shipping exactly once.
    total = order.get('total_amount')
    if total is None:
        subtotal = order.get('product_total')
        if subtotal is None:
            subtotal = sum(i['unit_price'] * i['quantity'] for i in items)
        total = int(subtotal) + int(order.get('delivery_fee') or 0)
    payload = dict(source=config['source'], external_order_id=str(order_id),
                   store_name=store_name,
                   created_at=order['created_at'],
                   customer=dict(name=order.get('customer_name') or '', phone=order['phone'],
                                 province=order['province'], address=order['address']),
                   items=items, total_price=int(total), employee_name=EMPLOYEE_NAME,
                   notes='\n'.join(note for note in notes if note))
    payload['attribution'] = order_attribution(db, order)
    # Telegram does not deliver one bot's messages to another bot. The API
    # registers the booking; this project owns the single group notification.
    payload['telegram_notification_owner'] = 'source'
    if order.get('is_paid'):
        payload['total_price'] = 0
        payload['notes'] = paid_notes(payload['notes'])
    # Older receivers use `total_price or total_amount`; provide both so a paid
    # order's explicit zero never falls back to the nonzero sum of its items.
    payload['total_amount'] = payload['total_price']
    if order.get('delivery_fee') is not None:
        payload['delivery_fee'] = int(order['delivery_fee'])
    db.execute('INSERT OR IGNORE INTO menger_deliveries(order_id,local_store,payload,booking_actor) VALUES(?,?,?,?)',
               (order_id, local_store, json.dumps(payload, ensure_ascii=False), payload['attribution']['booking_actor']))


def order_attribution(db, order):
    """Freeze the real conversation identity, never infer AI credit from a name."""
    sender = str(order.get('sender_id') or '').strip()
    store = order.get('store_id') or 'default'
    actor = order.get('_booking_actor') or 'unknown'
    actor = actor if actor in {'ai', 'staff', 'unknown'} else 'unknown'
    result = dict(source_store_id=store, conversation_id=sender, booking_actor=actor,
                  telegram_chat_id=settings(db, store).get('telegram_chat_id') or '')
    try:
        customer = db.execute('SELECT platform FROM customers WHERE sender_id=?', (sender,)).fetchone()
        if customer:
            result['platform'] = customer['platform'] or ''
        moments = []
        for row in db.execute("SELECT created_at FROM messages WHERE sender_id=? AND COALESCE(store_id,'default')=? AND direction='incoming'", (sender, store)):
            try:
                stamp = datetime.fromisoformat(str(row['created_at']).replace('Z', '+00:00'))
                moments.append(stamp.replace(tzinfo=ZoneInfo('Asia/Baghdad')) if stamp.tzinfo is None else stamp)
            except (ValueError, TypeError):
                pass
        if moments:
            result['first_contact_at'] = min(moments).astimezone(timezone.utc).isoformat()
    except sqlite3.OperationalError:
        pass  # Small standalone delivery databases have no conversation tables.
    return result


def paid_notes(notes):
    text = str(notes or '').strip()
    marker = 'مدفوع بالكامل شاملاً التوصيل — لا يُحصّل أي مبلغ من الزبون'
    return text if marker in text else '\n'.join(filter(None, [text, marker]))


def retry_after_seconds(response, now):
    """Read a finite server delay, bounded to one hour for the durable queue."""
    headers = getattr(response, 'headers', None)
    value = headers.get('Retry-After') if hasattr(headers, 'get') else None
    if not isinstance(value, str) or not value.strip():
        return 0
    try:
        delay = int(value.strip())
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            delay = date.timestamp() - now
        except (ValueError, TypeError, OverflowError):
            return 0
    return max(0, min(3600, delay))


def deliver_due(db, post=None, now=None, order_id=None):
    post = post or requests.post
    now = time.time() if now is None else now
    shared = connection(db)
    url, key = shared['api_url'], shared['api_key']
    try:
        stores = json.loads(os.environ.get('MENGER_STORE_MAP', '{}'))
        if not isinstance(stores, dict):
            stores = {}
    except ValueError:
        stores = {}
    default = os.environ.get('MENGER_STORE_ID', '').strip()
    if default:
        stores.setdefault('default', default)
    rows = db.execute("SELECT * FROM menger_deliveries WHERE status IN ('pending','retry','sending') AND next_attempt<=? AND (? IS NULL OR order_id=?) ORDER BY order_id", (now, order_id, order_id)).fetchall()
    delivered = 0
    for row in rows:
        if delivered >= 20:
            break
        config = settings(db, row['local_store'])
        configured = db.execute('SELECT 1 FROM menger_store_settings WHERE local_store=?', (row['local_store'],)).fetchone()
        target_url, target_key = url, key
        parsed = urlparse(target_url)
        if not target_key or parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
            continue
        payload = json.loads(row['payload'])
        destination = payload.get('store_id')
        if not row['attempts']:
            destination = destination or (config['store_id'] if configured else stores.get(row['local_store']))
        if row['attempts']:
            store_name = str(payload.get('store_name') or '').strip()
        else:
            store_name = config['store_name'] or str(payload.get('store_name') or '').strip()
        # An explicit legacy ID has no verified fallback name. Do not guess one.
        if destination and not config['store_name'] and not row['attempts']:
            store_name = ''
        if (not isinstance(destination, str) or not destination.strip()) and not store_name:
            continue
        delivered += 1
        if isinstance(destination, str) and destination.strip():
            payload['store_id'] = destination.strip()
        if store_name:
            payload['store_name'] = store_name
        else:
            payload.pop('store_name', None)
        # Atomic lease supports several server processes; destination stays fixed on retry.
        claim = db.execute("UPDATE menger_deliveries SET status='sending', next_attempt=?, attempts=attempts+1,payload=? WHERE order_id=? AND status IN ('pending','retry','sending') AND next_attempt<=?",
                           (now + 120, json.dumps(payload, ensure_ascii=False), row['order_id'], now))
        db.commit()
        if not claim.rowcount:
            continue
        status, remote, error, server_delay = 'retry', None, 'network_error', 0
        try:
            response = post(target_url, json=payload, headers={'Authorization': f'Bearer {target_key}',
                            'Content-Type': 'application/json', 'Accept': 'application/json'}, timeout=20, allow_redirects=False)
            if response.status_code in (200, 201):
                try:
                    result = response.json()
                except ValueError:
                    result = {}
                if isinstance(result, dict) and result.get('ok') is True and isinstance(result.get('order_id'), str) and result['order_id'].strip():
                    status, remote, error = 'sent', result['order_id'], None
                else:
                    error = 'invalid_success_response'
            else:
                # The receiver contract deduplicates source + external_order_id
                # + store. The frozen payload retains all three across retries,
                # including a timeout after a possibly accepted request.
                status = 'retry' if response.status_code in (408, 429) or 500 <= response.status_code < 600 else 'blocked'
                if status == 'retry':
                    server_delay = retry_after_seconds(response, now)
                error = f'http_{response.status_code}'
                try:
                    detail = response.json()
                    if isinstance(detail, dict):
                        messages = [detail.get('error')]
                        if isinstance(detail.get('details'), list):
                            messages.extend(detail['details'])
                        explanation = '؛ '.join(s for s in messages if isinstance(s, str))
                        if explanation:
                            error += ': ' + explanation.replace(target_key, '[hidden]')[:600]
                except ValueError:
                    pass
        except requests.RequestException:
            pass
        delay = max(server_delay, min(3600, 60 * 2 ** min(row['attempts'], 6)))
        db.execute('UPDATE menger_deliveries SET status=?,remote_order_id=?,last_error=?,next_attempt=?,synced_at=? WHERE order_id=?',
                   (status, remote, error, now + delay, now if status == 'sent' else None, row['order_id']))
        db.commit()


def sync_statuses(db, get=None, now=None):
    """Read the receiver's outcome; never overwrite a local cancellation."""
    get, now = get or requests.get, time.time() if now is None else now
    config = connection(db)
    parsed = urlparse(config.get('api_url') or '')
    if not config.get('api_key') or parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
        return 0
    rows = db.execute("""SELECT * FROM menger_deliveries WHERE status='sent'
        AND COALESCE(remote_order_id,'')!='' AND next_status_check<=? ORDER BY next_status_check,order_id LIMIT 20""", (now,)).fetchall()
    updated = 0
    for row in rows:
        claimed = db.execute("UPDATE menger_deliveries SET next_status_check=? WHERE order_id=? AND next_status_check<=?",
                             (now + 120, row['order_id'], now)).rowcount
        db.commit()
        if not claimed:
            continue
        payload = json.loads(row['payload'])
        params = {k: payload[k] for k in ('source', 'external_order_id', 'store_id', 'store_name') if payload.get(k)}
        error = None
        try:
            response = get(config['api_url'].rstrip('/') + '/status', params=params,
                           headers={'Authorization': 'Bearer ' + config['api_key'], 'Accept': 'application/json'},
                           timeout=15, allow_redirects=False)
            data = response.json()
            if (response.status_code != 200 or not isinstance(data, dict) or data.get('ok') is not True
                    or data.get('order_id') != row['remote_order_id']
                    or data.get('source') != payload['source']
                    or data.get('external_order_id') != payload['external_order_id']
                    or not isinstance(data.get('status'), str) or not data['status'].strip()):
                error = f'invalid_status_response_http_{response.status_code}'
            else:
                db.execute('''UPDATE menger_deliveries SET remote_status=?,remote_updated_at=?,status_checked_at=?,
                    status_error=NULL,next_status_check=? WHERE order_id=?''',
                    (data['status'], data.get('updated_at'), now, now + 300, row['order_id']))
                updated += 1
        except (requests.RequestException, ValueError, TypeError):
            error = 'status_connection_failed'
        if error:
            db.execute('UPDATE menger_deliveries SET status_error=?,next_status_check=? WHERE order_id=?',
                       (error, now + 300, row['order_id']))
        db.commit()
    return updated


def configured(db):
    try:
        config = connection(db)
    except sqlite3.OperationalError:
        return False
    return bool(config.get('api_url') and config.get('api_key'))


def recorded_order_condition(alias='orders'):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', alias):
        raise ValueError('Invalid order table alias')
    return (f"EXISTS (SELECT 1 FROM menger_deliveries md WHERE md.order_id={alias}.id "
            "AND md.status='sent' AND COALESCE(md.remote_order_id,'')!='' "
            "AND LOWER(TRIM(COALESCE(md.remote_status,'new'))) NOT IN "
            "('cancelled','canceled','incomplete_data','draft'))")


def reconcile_legacy(db, post=None, now=None):
    """Link historical bookings only when Menger finds one exact existing order.

    This dedicated endpoint never creates orders or sends group notifications.
    An older server responds 404, rather than treating this as a new booking.
    """
    post, now = post or requests.post, time.time() if now is None else now
    config = connection(db)
    parsed = urlparse(config.get('api_url') or '')
    if not config.get('api_key') or parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
        return 0
    matched = 0
    rows = db.execute("SELECT * FROM menger_deliveries WHERE status='review_required' AND next_attempt<=? ORDER BY order_id LIMIT 5", (now,)).fetchall()
    for row in rows:
        if not db.execute("UPDATE menger_deliveries SET next_attempt=? WHERE order_id=? AND status='review_required' AND next_attempt<=?", (now+120, row['order_id'], now)).rowcount:
            continue
        db.commit()
        error = 'legacy_booking_needs_reconciliation'
        try:
            order = db.execute("SELECT * FROM orders WHERE id=? AND COALESCE(store_id,'default')=?", (row['order_id'], row['local_store'])).fetchone()
            if not order:
                raise ValueError('missing_local_order')
            payload = json.loads(row['payload'])
            payload['attribution'] = order_attribution(db, dict(order, _booking_actor='unknown'))
            destination = settings(db, row['local_store'])
            if not payload.get('store_id') and destination.get('store_id'):
                payload['store_id'] = destination['store_id']
            if not payload.get('store_name') and destination.get('store_name'):
                payload['store_name'] = destination['store_name']
            response = post(config['api_url'].rstrip('/') + '/reconcile', json=payload,
                headers={'Authorization': 'Bearer ' + config['api_key'], 'Accept': 'application/json'},
                timeout=15, allow_redirects=False)
            result = response.json()
            if (response.status_code == 200 and isinstance(result, dict) and result.get('ok') is True
                    and result.get('source') == payload['source']
                    and result.get('external_order_id') == payload['external_order_id']
                    and isinstance(result.get('order_id'), str) and result['order_id']):
                db.execute('''UPDATE menger_deliveries SET status='sent',booking_actor='unknown',remote_order_id=?,remote_status=?,
                    remote_updated_at=?,status_checked_at=?,next_status_check=?,last_error=NULL,
                    payload=?,synced_at=? WHERE order_id=?''',
                    (result['order_id'], result.get('status'), result.get('updated_at'), now, now+300,
                     json.dumps(payload, ensure_ascii=False), now, row['order_id']))
                matched += 1
                error = None
            else:
                error = f'legacy_reconciliation_http_{response.status_code}'
        except (requests.RequestException, sqlite3.OperationalError, ValueError, TypeError, KeyError):
            error = 'legacy_reconciliation_unavailable'
        if error:
            db.execute('UPDATE menger_deliveries SET last_error=?,next_attempt=? WHERE order_id=?', (error, now+3600, row['order_id']))
        db.commit()
    return matched


def delivery_summary(db, store=None):
    scope, params = (' WHERE local_store=?', [store]) if store and store != 'all' else ('', [])
    rows = db.execute('SELECT status,COUNT(*) n FROM menger_deliveries' + scope + ' GROUP BY status', params).fetchall()
    return {row['status']: row['n'] for row in rows}


def start_worker(db_path):
    def loop():
        while True:
            db = None
            try:
                db = sqlite3.connect(db_path, timeout=30)
                db.row_factory = sqlite3.Row
                deliver_due(db)
                sync_statuses(db)
                reconcile_legacy(db)
            except Exception:
                # The durable lease expires so a subsequent cycle can recover.
                pass
            finally:
                if db is not None:
                    db.close()
            time.sleep(5)
    threading.Thread(target=loop, daemon=True, name='menger-orders').start()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Inspect Menger delivery status or retry a corrected blocked order.')
    parser.add_argument('--db', required=True, help='Existing sales.db path')
    parser.add_argument('--retry-order', type=int)
    args = parser.parse_args()
    if not os.path.isfile(args.db):
        parser.error('Database does not exist')
    with sqlite3.connect(args.db, timeout=30) as db:
        db.row_factory = sqlite3.Row
        init_db(db)
        if args.retry_order is not None:
            db.execute("UPDATE menger_deliveries SET status='pending',next_attempt=0 WHERE order_id=? AND status='blocked'", (args.retry_order,))
        rows = db.execute('SELECT order_id,local_store,status,attempts,remote_order_id,last_error FROM menger_deliveries ORDER BY order_id DESC LIMIT 100').fetchall()
        print(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2))
