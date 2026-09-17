"""Immutable employee credit ledger; awards share the resolution transaction."""
from datetime import datetime, timedelta, timezone
from flask import has_request_context, jsonify, redirect, render_template

BAGHDAD = timezone(timedelta(hours=3))
DAILY_GOAL = 5


def tables(db):
    db.execute('''CREATE TABLE IF NOT EXISTS staff_rewards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        review_id INTEGER NOT NULL UNIQUE, staff_id INTEGER NOT NULL,
        amount INTEGER NOT NULL, duration_seconds REAL,
        multiplier REAL NOT NULL, created_at TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS staff_rewards_person ON staff_rewards(staff_id,id)')


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
    return dict(total, today=dict(daily), goal=DAILY_GOAL, history=history,
                baseline_seconds=sum(durations)/len(durations) if durations else None)


def install(app, get_db, current):
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
