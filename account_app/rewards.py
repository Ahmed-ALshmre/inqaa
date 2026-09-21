"""Immutable employee credit ledger; awards share the resolution transaction."""
from datetime import datetime, timedelta, timezone
import secrets
from flask import has_request_context, jsonify, redirect, render_template, request, session

BAGHDAD = timezone(timedelta(hours=3))
DAILY_GOAL = 5


def tables(db):
    db.execute('''CREATE TABLE IF NOT EXISTS staff_rewards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        review_id INTEGER NOT NULL UNIQUE, staff_id INTEGER NOT NULL,
        amount INTEGER NOT NULL, duration_seconds REAL,
        multiplier REAL NOT NULL, created_at TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS staff_rewards_person ON staff_rewards(staff_id,id)')
    db.execute('''CREATE TABLE IF NOT EXISTS staff_reward_withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, staff_id INTEGER NOT NULL,
        amount INTEGER NOT NULL CHECK(amount>0), status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL, resolved_at TEXT)''')
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_pending_withdrawal ON staff_reward_withdrawals(staff_id) WHERE status='pending'")


def award_pending(db, sender_id, person, cutoff=None, now=None):
    """Caller holds the write transaction and closes these reviews atomically."""
    if not person or person.get('owner'):
        return
    now = now or datetime.now(BAGHDAD)
    rows = db.execute("SELECT id,created_at FROM human_reviews WHERE sender_id=? AND status='pending' AND id<=?",
                      (sender_id, cutoff if cutoff is not None else 9223372036854775807)).fetchall()
    previous = db.execute('''SELECT duration_seconds FROM staff_rewards
        WHERE staff_id=? AND duration_seconds IS NOT NULL ORDER BY id DESC LIMIT 10''', (person['id'],)).fetchall()
    baseline = sum(r[0] for r in previous) / len(previous) if previous else None
    for row in rows:
        duration = None
        try:
            started = datetime.fromisoformat(row['created_at'])
            if started.tzinfo is None:
                started = started.replace(tzinfo=BAGHDAD)
            seconds = (now - started).total_seconds()
            if seconds >= 0:
                duration = seconds
        except (ValueError, TypeError):
            pass
        fast = duration is not None and baseline is not None and duration < baseline
        db.execute('''INSERT OR IGNORE INTO staff_rewards
            (review_id,staff_id,amount,duration_seconds,multiplier,created_at) VALUES(?,?,?,?,?,?)''',
            (row['id'], person['id'], 60 if fast else 50, duration, 1.2 if fast else 1, now.isoformat()))


def summary(db, staff_id):
    today = datetime.now(BAGHDAD).date().isoformat()
    total = db.execute('''SELECT COUNT(*) solved, COALESCE(SUM(amount),0) balance,
        COALESCE(SUM(amount-50),0) bonus FROM staff_rewards WHERE staff_id=?''', (staff_id,)).fetchone()
    daily = db.execute('''SELECT COUNT(*) solved,COALESCE(SUM(amount),0) earned
        FROM staff_rewards WHERE staff_id=? AND substr(created_at,1,10)=?''', (staff_id,today)).fetchone()
    history = [dict(r) for r in db.execute('SELECT * FROM staff_rewards WHERE staff_id=? ORDER BY id DESC LIMIT 50', (staff_id,))]
    durations = [r[0] for r in db.execute('SELECT duration_seconds FROM staff_rewards WHERE staff_id=? AND duration_seconds IS NOT NULL ORDER BY id DESC LIMIT 10', (staff_id,))]
    withdrawals = [dict(r) for r in db.execute('SELECT * FROM staff_reward_withdrawals WHERE staff_id=? ORDER BY id DESC', (staff_id,))]
    reserved = sum(r['amount'] for r in withdrawals if r['status'] == 'pending')
    paid = sum(r['amount'] for r in withdrawals if r['status'] == 'paid')
    return dict(total, available=max(0,total['balance']-reserved-paid), reserved=reserved, paid=paid,
                withdrawals=withdrawals, day=today, today=dict(daily), goal=DAILY_GOAL, history=history,
                baseline_seconds=sum(durations)/len(durations) if durations else None)


def install(app, get_db, current):
    @app.post('/api/rewards/withdraw')
    def withdraw():
        person = current()
        if not person: return jsonify(error='سجل الدخول أولاً'), 401
        if person['owner']: return jsonify(error='السحب مخصص لرصيد الموظف'), 403
        db = get_db(); tables(db); db.commit()
        with db:
            db.execute('BEGIN IMMEDIATE')
            data = summary(db, person['id'])
            if data['reserved']: return jsonify(error='لديك طلب سحب قيد المراجعة'), 409
            if data['available'] <= 0: return jsonify(error='لا يوجد رصيد متاح للسحب'), 409
            db.execute('INSERT INTO staff_reward_withdrawals(staff_id,amount,created_at) VALUES(?,?,?)',
                       (person['id'],data['available'],datetime.now(BAGHDAD).isoformat()))
        return jsonify(ok=True, amount=data['available'])

    @app.post('/api/rewards/withdrawals/<int:withdrawal_id>')
    def settle_withdrawal(withdrawal_id):
        person = current()
        if not person: return jsonify(error='سجل الدخول أولاً'), 401
        if not person['owner']: return jsonify(error='هذا الإجراء للمالك فقط'), 403
        if not secrets.compare_digest(request.headers.get('X-CSRF-Token',''), session.get('csrf_token') or secrets.token_hex(32)):
            return jsonify(error='حدّث الصفحة ثم حاول مجدداً'), 403
        status = (request.get_json(silent=True) or {}).get('status')
        if status not in {'paid','rejected'}: return jsonify(error='حالة غير صحيحة'), 400
        db = get_db(); tables(db); db.commit()
        with db:
            db.execute('BEGIN IMMEDIATE')
            changed = db.execute("UPDATE staff_reward_withdrawals SET status=?,resolved_at=? WHERE id=? AND status='pending'",
                                 (status,datetime.now(BAGHDAD).isoformat(),withdrawal_id)).rowcount
            if not changed: return jsonify(error='تمت معالجة الطلب مسبقاً أو غير موجود'), 409
        return jsonify(ok=True)

    @app.get('/rewards')
    def rewards_page():
        if not current():
            return redirect('/login')
        return render_template('rewards.html')

    @app.get('/api/rewards')
    def rewards_data():
        person = current()
        if not person:
            return jsonify(error='سجل الدخول أولاً'), 401
        db = get_db()
        tables(db)
        if person['owner']:
            people = db.execute('SELECT id,name FROM staff_accounts ORDER BY name').fetchall() if db.execute("SELECT 1 FROM sqlite_master WHERE name='staff_accounts'").fetchone() else []
            return jsonify(owner=True, employees=[dict(id=p['id'], name=p['name'], **summary(db,p['id'])) for p in people])
        return jsonify(owner=False, **summary(db,person['id']))
