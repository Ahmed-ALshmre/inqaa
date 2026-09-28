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
    turn = db.execute("SELECT text FROM messages WHERE sender_id=? AND direction='incoming' AND id>COALESCE((SELECT MAX(id) FROM messages WHERE sender_id=? AND direction='outgoing' AND id<?),0) ORDER BY id", (sender_id, sender_id, incoming['id'])).fetchall()
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
        elif re.search(r'قياس|مقاس|لون|عمر|وزن', text):
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


def metrics(db, store_id, now, target=30):
    end = timestamp(now)
    start = end - timedelta(hours=24)
    params = (store_id, start.isoformat(), end.isoformat())
    people = {r[0] for r in db.execute("SELECT DISTINCT sender_id FROM messages WHERE COALESCE(store_id,'default')=? AND direction='incoming' AND created_at>=? AND created_at<=?", params)}
    orders = db.execute("SELECT sender_id FROM orders WHERE COALESCE(store_id,'default')=? AND created_at>=? AND created_at<=?", params).fetchall()
    buyers = people & {r[0] for r in orders}
    needed = math.ceil(len(people) * target / 100)
    queue = db.execute("""SELECT f.status,COUNT(*) AS n FROM followups f JOIN customers c ON c.sender_id=f.sender_id
        WHERE COALESCE(c.store_id,'default')=? AND f.created_at>=? AND f.created_at<=? GROUP BY f.status""", params).fetchall()
    # Timestamp attribution means 'after follow-up', never a claim of causation.
    recovered = db.execute("""SELECT COUNT(DISTINCT o.sender_id) FROM orders o JOIN followups f ON f.sender_id=o.sender_id
        WHERE COALESCE(o.store_id,'default')=? AND o.created_at>=? AND o.created_at<=?
        AND f.status='sent' AND f.sent_at IS NOT NULL AND f.sent_at<o.created_at AND f.sent_at>=?""", (*params, start.isoformat())).fetchone()[0]
    urgent = db.execute("""SELECT c.sender_id,c.name,c.lead_score,MIN(r.created_at) AS waiting_since,COUNT(*) AS reviews,
        CASE WHEN COALESCE(c.phone,'')!='' AND COALESCE(c.address,'')!='' THEN 1 ELSE 0 END AS contact_ready
        FROM human_reviews r JOIN customers c ON c.sender_id=r.sender_id
        WHERE COALESCE(c.store_id,'default')=? AND r.status='pending'
        AND NOT EXISTS(SELECT 1 FROM orders o WHERE o.sender_id=c.sender_id)
        GROUP BY c.sender_id ORDER BY contact_ready DESC,c.lead_score DESC,waiting_since LIMIT 20""", (store_id,)).fetchall()
    return {'start': start.isoformat(), 'end': end.isoformat(), 'people': len(people), 'buyers': len(buyers),
            'orders': len(orders), 'conversion': round(100*len(buyers)/len(people), 2) if people else 0,
            'target': target, 'target_buyers': needed, 'additional_buyers': max(0, needed-len(buyers)),
            'followups': {r['status']: r['n'] for r in queue}, 'buyers_after_followup': recovered,
            'urgent': [dict(r, waiting_minutes=max(0, int((end-timestamp(r['waiting_since'])).total_seconds()/60))) for r in urgent if timestamp(r['waiting_since'])]}
