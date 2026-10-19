"""Deterministic timing and cohort reporting for the 30% conversion plan."""
import math
import re
from datetime import timedelta

try:
    from .sales_engagement import timestamp
except ImportError:
    from sales_engagement import timestamp


def reminder_minutes(text):
    text = str(text or '').translate(str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789'))
    if not re.search(r'ذكرني|ذكريني|راسلني|راسليني|ارجعلي|ارجعيلي', text):
        return None
    if re.search(r'لا\s*(?:تذكر|تراسل)|ما\s*تراسل', text):
        return None
    match = re.search(r'بعد\s+(ساعة|ساعه|ساعتين|نص ساعة|نصف ساعة|\d+\s*(?:ساعات|ساعة|ساعه|دقيقة|دقايق))', text)
    if not match:
        return None
    value = match.group(1)
    if value in ('ساعة', 'ساعه'):
        return 60
    if value == 'ساعتين':
        return 120
    if value in ('نص ساعة', 'نصف ساعة'):
        return 30
    number = int(re.search(r'\d+', value).group())
    return number if 'دق' in value else number * 60


def during_contact_hours(moment, start=9, end=21):
    if start == end:  # Explicitly configured 24-hour operation.
        return moment
    hour = moment.hour
    allowed = start <= hour < end if start < end else hour >= start or hour < end
    if allowed:
        return moment
    next_open = moment.replace(hour=start, minute=0, second=0, microsecond=0)
    return next_open if next_open > moment else next_open + timedelta(days=1)


def followup_plan(db, sender_id, now, settings, delay_minutes=None):
    current = timestamp(now)
    incoming = db.execute("SELECT id,text,created_at FROM messages WHERE sender_id=? AND direction='incoming' ORDER BY id DESC LIMIT 1", (sender_id,)).fetchone()
    outgoing = db.execute("SELECT id,text,created_at FROM messages WHERE sender_id=? AND direction='outgoing' ORDER BY id DESC LIMIT 1", (sender_id,)).fetchone()
    if not incoming or not outgoing or outgoing['id'] < incoming['id']:
        return None
    received = timestamp(incoming['created_at'])
    answered = timestamp(outgoing['created_at'])
    if not received or not answered or not current:
        return None
    turn = db.execute("SELECT text,message_type,image_url FROM messages WHERE sender_id=? AND direction='incoming' AND id>COALESCE((SELECT MAX(id) FROM messages WHERE sender_id=? AND direction='outgoing' AND id<?),0) ORDER BY id", (sender_id, sender_id, incoming['id'])).fetchall()
    text = ' '.join(row['text'] or '' for row in turn)
    requested = reminder_minutes(text)
    reason = 'general_interest'
    delay = settings.get('default_delay_minutes', 120)
    base = answered
    if requested is not None:
        reason, delay, base = 'requested_reminder', requested, received
    elif delay_minutes is not None:
        reason, delay = 'custom_delay', max(1, int(delay_minutes))
    elif settings.get('adaptive_timing', True):
        customer = db.execute('SELECT phone,address,lead_stage FROM customers WHERE sender_id=?', (sender_id,)).fetchone()
        if re.search(r'ثبت|احجز|أحجز|اطلب|أطلب', text) or (customer and customer['phone'] and customer['address']):
            reason, delay = 'checkout', settings.get('checkout_delay_minutes', 60)
        elif re.search(r'قياس|مقاس|لون|عمر|وزن', text) or any(row['message_type'] == 'image' or row['image_url'] for row in turn):
            reason, delay = 'selection', settings.get('selection_delay_minutes', 120)
        else:
            delay = settings.get('general_delay_minutes', 240)
    due = max(current, base + timedelta(minutes=max(1, delay)))
    due = during_contact_hours(due, settings.get('contact_start_hour', 9), settings.get('contact_end_hour', 21))
    deadline = received + timedelta(hours=23)
    if due >= deadline:
        return None
    return {'scheduled_at': due.isoformat(), 'reason': reason, 'delay_minutes': delay,
            'incoming_id': incoming['id'], 'outgoing_id': outgoing['id'], 'expires_at': deadline.isoformat()}


def _instant_sql(column):
    """Compare real instants; historical naive timestamps use Baghdad time."""
    value = f"TRIM({column})"
    aware = f"(UPPER(SUBSTR({value},-1))='Z' OR (SUBSTR({value},-6,1) IN ('+','-') AND SUBSTR({value},-3,1)=':'))"
    return f"CASE WHEN {aware} THEN julianday({value}) ELSE julianday({value},'-3 hours') END"


def _period_boundary(value, label):
    if value is None:
        return None
    parsed = timestamp(value)
    if parsed is None:
        raise ValueError(f"Invalid {label} timestamp")
    return parsed


def cohort_metrics(db, start=None, end=None, store_id=None, target=30):
    """New contacts who book within the same half-open reporting period.

    A person is the pair (store, sender), first contacted at their earliest
    incoming message. Repeat messages, returning buyers, manual-only orders and
    cancelled orders cannot raise this cohort's conversion. Activity and all
    valid order totals remain separate so changing the definition never hides
    existing activity. This is booking conversion, not delivered-order revenue
    or a claim that any reply/follow-up caused the purchase.
    """
    start_at = _period_boundary(start, 'start')
    end_at = _period_boundary(end, 'end')
    if start_at is not None and end_at is not None and start_at >= end_at:
        raise ValueError('The reporting start must precede its end')
    try:
        goal = float(target)
    except (ValueError, TypeError):
        raise ValueError('The conversion target must be between 0 and 100') from None
    if not math.isfinite(goal) or not 0 <= goal <= 100:
        raise ValueError('The conversion target must be between 0 and 100')
    store = str(store_id).strip() if store_id is not None else None
    store = None if store in (None, '', 'all') else store
    scope = " AND COALESCE(NULLIF(TRIM(store_id),''),'default')=?" if store else ''
    params = [start_at.isoformat() if start_at else None,
              end_at.isoformat() if end_at else None]
    params += ([store] if store else []) * 2
    sql = f"""WITH bounds AS (
        SELECT julianday(?) starts_at,julianday(?) ends_at
    ), incoming AS (
        SELECT COALESCE(NULLIF(TRIM(store_id),''),'default') store_id,
               sender_id,{_instant_sql('created_at')} instant
        FROM messages WHERE direction='incoming' AND COALESCE(TRIM(sender_id),'')!=''{scope}
    ), first_contacts AS (
        SELECT store_id,sender_id,MIN(instant) first_message FROM incoming
        WHERE instant IS NOT NULL GROUP BY store_id,sender_id
    ), cohort AS (
        SELECT f.* FROM first_contacts f,bounds b
        WHERE (b.starts_at IS NULL OR f.first_message>=b.starts_at)
          AND (b.ends_at IS NULL OR f.first_message<b.ends_at)
    ), active_contacts AS (
        SELECT DISTINCT i.store_id,i.sender_id FROM incoming i,bounds b
        WHERE i.instant IS NOT NULL
          AND (b.starts_at IS NULL OR i.instant>=b.starts_at)
          AND (b.ends_at IS NULL OR i.instant<b.ends_at)
    ), valid_orders AS (
        SELECT COALESCE(NULLIF(TRIM(store_id),''),'default') store_id,
               sender_id,{_instant_sql('created_at')} instant
        FROM orders
        WHERE LOWER(TRIM(COALESCE(status,'new'))) NOT IN ('cancelled','canceled'){scope}
    ), period_orders AS (
        SELECT o.* FROM valid_orders o,bounds b
        WHERE o.instant IS NOT NULL
          AND (b.starts_at IS NULL OR o.instant>=b.starts_at)
          AND (b.ends_at IS NULL OR o.instant<b.ends_at)
    ), period_buyers AS (
        SELECT store_id,sender_id,MAX(instant) last_order FROM period_orders
        WHERE COALESCE(TRIM(sender_id),'')!='' GROUP BY store_id,sender_id
    ) SELECT
        (SELECT COUNT(*) FROM cohort) people,
        (SELECT COUNT(*) FROM cohort c JOIN period_buyers p
         ON p.store_id=c.store_id AND p.sender_id=c.sender_id
         WHERE p.last_order>=c.first_message) buyers,
        (SELECT COUNT(*) FROM period_orders) orders,
        (SELECT COUNT(*) FROM period_buyers) period_order_buyers,
        (SELECT COUNT(*) FROM active_contacts) active_contacts,
        (SELECT COUNT(*) FROM active_contacts a JOIN period_buyers p
         ON p.store_id=a.store_id AND p.sender_id=a.sender_id
         JOIN first_contacts f ON f.store_id=a.store_id AND f.sender_id=a.sender_id
         WHERE p.last_order>=f.first_message) active_buyers"""
    row = db.execute(sql, params).fetchone()
    people, buyers, orders, period_buyers, active, active_buyers = tuple(row)
    conversion = round(100 * buyers / people, 2) if people else 0
    needed = math.ceil(people * goal / 100)
    return {
        'start': start_at.isoformat() if start_at else None,
        'end': end_at.isoformat() if end_at else None,
        'end_exclusive': True, 'store_id': store or 'all',
        'people': people, 'buyers': buyers, 'orders': orders,
        'conversion': conversion,
        'cohort_people': people, 'cohort_buyers': buyers,
        'cohort_conversion_rate': conversion,
        'orders_booked_people': period_buyers,
        'active_contacts': active, 'active_buyers': active_buyers,
        'active_conversion_rate': round(100 * active_buyers / active, 2) if active else 0,
        'target': target, 'target_buyers': needed,
        'additional_buyers': max(0, needed - buyers),
        'measurement_definition': 'new_store_contacts_to_non_cancelled_booking_in_same_period',
    }


def metrics(db, store_id, now, target=30):
    end = _period_boundary(now, 'now')
    if end is None:
        raise ValueError('A current timestamp is required')
    start = end - timedelta(hours=24)
    result = cohort_metrics(db, start.isoformat(), end.isoformat(), store_id, target)
    params = (store_id, start.isoformat(), end.isoformat())
    queue = db.execute("""SELECT f.status,COUNT(*) AS n FROM followups f JOIN customers c ON c.sender_id=f.sender_id
        WHERE COALESCE(c.store_id,'default')=? AND f.created_at>=? AND f.created_at<? GROUP BY f.status""", params).fetchall()
    # Timestamp attribution means 'after follow-up', never a claim of causation.
    recovered = db.execute("""SELECT COUNT(DISTINCT o.sender_id) FROM orders o JOIN followups f ON f.sender_id=o.sender_id
        WHERE COALESCE(o.store_id,'default')=? AND o.created_at>=? AND o.created_at<?
        AND LOWER(TRIM(COALESCE(o.status,'new'))) NOT IN ('cancelled','canceled')
        AND f.status='sent' AND f.sent_at IS NOT NULL AND f.sent_at<o.created_at AND f.sent_at>=?""", (*params, start.isoformat())).fetchone()[0]
    urgent = db.execute("""SELECT c.sender_id,c.name,c.lead_score,MIN(r.created_at) AS waiting_since,COUNT(*) AS reviews,
        CASE WHEN COALESCE(c.phone,'')!='' AND COALESCE(c.address,'')!='' THEN 1 ELSE 0 END AS contact_ready
        FROM human_reviews r JOIN customers c ON c.sender_id=r.sender_id
        WHERE COALESCE(c.store_id,'default')=? AND r.status='pending'
        AND NOT EXISTS(SELECT 1 FROM orders o WHERE o.sender_id=c.sender_id)
        GROUP BY c.sender_id ORDER BY contact_ready DESC,c.lead_score DESC,waiting_since LIMIT 20""", (store_id,)).fetchall()
    return dict(result,
            followups={r['status']: r['n'] for r in queue}, buyers_after_followup=recovered,
            urgent=[dict(r, waiting_minutes=max(0, int((end-timestamp(r['waiting_since'])).total_seconds()/60))) for r in urgent if timestamp(r['waiting_since'])])
