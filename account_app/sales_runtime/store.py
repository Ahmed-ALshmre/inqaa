"""Durable, tenant-scoped events, state and reply delivery primitives.

No model, network or application imports. Own transactions are deliberately short;
callers must finish unrelated transactions before invoking mutations.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import time


class Conflict(ValueError):
    """The caller must reread state or reconcile a previous operation."""


@dataclass(frozen=True)
class Scope:
    store: str
    channel: str
    account: str
    sender: str

    def __post_init__(self):
        if any(not isinstance(v, str) or not v.strip() for v in self.values()):
            raise ValueError('Every scope component is required')

    def values(self):
        return self.store, self.channel, self.account, self.sender

    @property
    def key(self):
        return hashlib.sha256(encode(self.values()).encode()).hexdigest()


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def initial_state():
    return dict(schema_version=1, cart=[], contact={}, purchase_event=None,
                conditions=[], pending_questions=[], control_mode='active')


class Store:
    def __init__(self, db):
        self.db = db

    def migrate(self):
        if self.db.in_transaction:
            raise Conflict('migration_requires_clean_transaction')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS sales_v2_scopes (
                scope TEXT PRIMARY KEY, store TEXT NOT NULL, channel TEXT NOT NULL,
                account TEXT NOT NULL, sender TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 0, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sales_v2_events (
                id INTEGER PRIMARY KEY, scope TEXT NOT NULL, external_id TEXT NOT NULL,
                kind TEXT NOT NULL, actor TEXT NOT NULL, occurred_at REAL NOT NULL,
                received_at REAL NOT NULL, payload TEXT NOT NULL, digest TEXT NOT NULL,
                UNIQUE(scope,external_id));
            CREATE INDEX IF NOT EXISTS sales_v2_events_scope ON sales_v2_events(scope,id);
            CREATE TABLE IF NOT EXISTS sales_v2_operations (
                scope TEXT NOT NULL, operation_key TEXT NOT NULL, digest TEXT NOT NULL,
                result TEXT NOT NULL, created_at REAL NOT NULL,
                PRIMARY KEY(scope,operation_key));
            CREATE TABLE IF NOT EXISTS sales_v2_evidence (
                id INTEGER PRIMARY KEY, scope TEXT NOT NULL, event_id INTEGER NOT NULL,
                target TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL,
                state_version INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS sales_v2_outbox (
                id INTEGER PRIMARY KEY, scope TEXT NOT NULL, reply_key TEXT NOT NULL,
                part_index INTEGER NOT NULL, state_version INTEGER NOT NULL,
                payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
                provider_id TEXT, UNIQUE(scope,reply_key,part_index));
            CREATE TABLE IF NOT EXISTS sales_v2_model_attempts (
                id INTEGER PRIMARY KEY, scope TEXT NOT NULL, event_id INTEGER NOT NULL,
                stage TEXT NOT NULL, status TEXT NOT NULL, error_code TEXT,
                created_at REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS sales_v2_model_attempts_event
                ON sales_v2_model_attempts(scope,event_id);
            CREATE TABLE IF NOT EXISTS sales_v2_incidents (
                id INTEGER PRIMARY KEY, scope TEXT NOT NULL, event_id INTEGER NOT NULL,
                category TEXT NOT NULL, error_code TEXT NOT NULL,
                review_id INTEGER, created_at REAL NOT NULL,
                UNIQUE(scope,event_id,category));
        ''')

    @contextmanager
    def transaction(self):
        if self.db.in_transaction:
            raise Conflict('transaction_already_open')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _ensure(self, scope):
        self.db.execute('INSERT OR IGNORE INTO sales_v2_scopes VALUES(?,?,?,?,?,0,?)',
                        (scope.key, *scope.values(), encode(initial_state())))

    def snapshot(self, scope):
        row = self.db.execute('SELECT version,data FROM sales_v2_scopes WHERE scope=?',
                              (scope.key,)).fetchone()
        state = json.loads(row[1]) if row else initial_state()
        if state.get('schema_version') != 1:
            raise Conflict('unsupported_state_schema')
        return dict(version=row[0] if row else 0, data=state)

    def append(self, scope, external_id, *, kind, actor, payload, occurred_at=None):
        if not external_id or kind not in {'message', 'referral', 'receipt'}:
            raise ValueError('invalid_event')
        if actor not in {'customer', 'staff', 'system'}:
            raise ValueError('invalid_actor')
        wire = dict(kind=kind, actor=actor, payload=payload)
        fingerprint = digest(wire)
        with self.transaction():
            self._ensure(scope)
            old = self.db.execute('SELECT id,digest FROM sales_v2_events WHERE scope=? AND external_id=?',
                                   (scope.key, external_id)).fetchone()
            if old:
                if old[1] != fingerprint:
                    raise Conflict('event_id_reused_with_different_content')
                return dict(id=old[0], duplicate=True)
            now = time.time()
            cur = self.db.execute('''INSERT INTO sales_v2_events
                (scope,external_id,kind,actor,occurred_at,received_at,payload,digest)
                VALUES(?,?,?,?,?,?,?,?)''',
                (scope.key, external_id, kind, actor, occurred_at if occurred_at is not None else now,
                 now, encode(payload), fingerprint))
            # Any new conversational evidence invalidates a draft already in flight.
            # Receipts only describe transport; they must not stale a valid reply.
            if kind != 'receipt':
                self.db.execute('UPDATE sales_v2_scopes SET version=version+1 WHERE scope=?', (scope.key,))
            return dict(id=cur.lastrowid, duplicate=False)

    def evidence(self, scope, event_id):
        row = self.db.execute('SELECT kind,actor,payload FROM sales_v2_events WHERE scope=? AND id=?',
                              (scope.key, event_id)).fetchone()
        if not row or row[0] != 'message' or row[1] not in {'customer', 'staff'}:
            raise Conflict('selection_requires_scoped_customer_or_staff_evidence')
        return dict(actor=row[1], payload=json.loads(row[2]))

    def apply(self, scope, key, expected_version, command, handler):
        """Atomically mutate application state and journal the result exactly once.

        handler(state) runs LOCAL SQL only, using this connection if needed. It
        returns a JSON-serializable result; exceptions roll everything back.
        """
        if not isinstance(key, str) or not key:
            raise ValueError('operation_key_required')
        fingerprint = digest(command)
        with self.transaction():
            self._ensure(scope)
            old = self.db.execute('SELECT digest,result FROM sales_v2_operations WHERE scope=? AND operation_key=?',
                                   (scope.key, key)).fetchone()
            if old:
                if old[0] != fingerprint:
                    raise Conflict('operation_key_reused_with_different_content')
                return dict(result=json.loads(old[1]), replayed=True)
            snap = self.snapshot(scope)
            if snap['version'] != expected_version:
                raise Conflict('state_version_changed')
            state = snap['data']
            result = handler(state)
            version = expected_version + 1
            self.db.execute('UPDATE sales_v2_scopes SET version=?,data=? WHERE scope=?',
                            (version, encode(state), scope.key))
            result = dict(result, state_version=version)
            self.db.execute('INSERT INTO sales_v2_operations VALUES(?,?,?,?,?)',
                            (scope.key, key, fingerprint, encode(result), time.time()))
            return dict(result=result, replayed=False)

    def enqueue_reply(self, scope, reply_key, version, parts):
        if not reply_key or not parts or len(parts) > 5:
            raise ValueError('invalid_reply')
        with self.transaction():
            old = self.db.execute('''SELECT payload,state_version FROM sales_v2_outbox
                WHERE scope=? AND reply_key=? ORDER BY part_index''', (scope.key, reply_key)).fetchall()
            wire = [encode(part) for part in parts]
            if old:
                if [(r[0], r[1]) for r in old] != [(p, version) for p in wire]:
                    raise Conflict('reply_key_reused_with_different_content')
                return
            snap = self.snapshot(scope)
            if snap['version'] != version or snap['data']['control_mode'] != 'active':
                raise Conflict('reply_superseded_or_paused')
            self.db.executemany('''INSERT INTO sales_v2_outbox
                (scope,reply_key,part_index,state_version,payload) VALUES(?,?,?,?,?)''',
                [(scope.key, reply_key, i, version, part) for i, part in enumerate(wire)])

    def claim_reply(self, scope):
        """Persist sending BEFORE external I/O. Unknown sends block later parts."""
        with self.transaction():
            snap = self.snapshot(scope)
            self.db.execute('''UPDATE sales_v2_outbox SET status='superseded'
                WHERE scope=? AND status='queued' AND state_version<>?''', (scope.key, snap['version']))
            if snap['data']['control_mode'] != 'active':
                return None
            rows = self.db.execute('''SELECT id,payload,status FROM sales_v2_outbox
                WHERE scope=? AND status NOT IN ('accepted','superseded') ORDER BY id''', (scope.key,)).fetchall()
            if not rows or rows[0][2] != 'queued':
                return None
            row = rows[0]
            self.db.execute("UPDATE sales_v2_outbox SET status='sending' WHERE id=?", (row[0],))
            return dict(id=row[0], payload=json.loads(row[1]))

    def settle_reply(self, scope, row_id, status, provider_id=None):
        if status not in {'accepted', 'rejected', 'unknown'}:
            raise ValueError('invalid_delivery_status')
        with self.transaction():
            updated = self.db.execute('''UPDATE sales_v2_outbox SET status=?,provider_id=?
                WHERE scope=? AND id=? AND status='sending' ''',
                (status, provider_id, scope.key, row_id)).rowcount
            if not updated:
                raise Conflict('delivery_not_sending_in_scope')

    def set_control(self, scope, key, version, event_id, mode):
        if mode not in {'active', 'human'}:
            raise ValueError('invalid_control_mode')
        def handle(state):
            if self.evidence(scope, event_id)['actor'] != 'staff':
                raise Conflict('staff_control_required')
            state['control_mode'] = mode
            return dict(control_mode=mode)
        return self.apply(scope, key, version, dict(event_id=event_id, mode=mode), handle)

    def recover(self):
        """Only call at exclusive worker startup, never while another worker sends."""
        with self.transaction():
            self.db.execute("UPDATE sales_v2_outbox SET status='unknown' WHERE status='sending'")
