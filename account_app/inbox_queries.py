"""Indexes and narrow state reads used by the staff inbox."""

INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_reviews_sender_status ON human_reviews(sender_id,status,id)",
    "CREATE INDEX IF NOT EXISTS idx_problems_sender_status ON problem_reports(sender_id,status,id)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_sender_status_created ON ai_jobs(sender_id,status,created_at)",
    "CREATE INDEX IF NOT EXISTS idx_interests_sender_seen ON customer_product_interests(sender_id,last_seen_at)",
    "CREATE INDEX IF NOT EXISTS idx_orders_sender_created ON orders(sender_id,created_at)",
    "CREATE INDEX IF NOT EXISTS idx_messages_incoming_image ON messages(sender_id,id) WHERE direction='incoming' AND (message_type='image' OR image_url IS NOT NULL)",
    "CREATE INDEX IF NOT EXISTS idx_messages_ad ON messages(sender_id,id) WHERE ad_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_messages_ref ON messages(sender_id,id) WHERE ref IS NOT NULL",
)

TECHNICAL_REVIEW = "(COALESCE(hr.reason,'') LIKE '%exception:%' OR COALESCE(hr.reason,'') LIKE '%provider_http_%' OR COALESCE(hr.reason,'') LIKE '%Image analysis service failure:%' OR COALESCE(hr.reason,'') LIKE '%empty_reply%' OR COALESCE(hr.reason,'') LIKE '%could not produce a reply%')"


def system_issue_sql(sender, last_time, reviewed_id):
    return f"""((SELECT COUNT(*) FROM human_reviews hr WHERE hr.sender_id={sender} AND hr.status='pending' AND {TECHNICAL_REVIEW}) +
         (SELECT COUNT(*) FROM ai_jobs j WHERE j.sender_id={sender} AND j.status='failed') +
         CASE WHEN (SELECT COUNT(*) FROM messages issue WHERE issue.sender_id={sender} AND issue.direction='outgoing'
             AND issue.id>COALESCE({reviewed_id},0)
             AND (issue.text LIKE 'هذه القطع المقترحة للحجز%' OR issue.text LIKE 'باقي نحدد لون%' OR issue.text LIKE 'أثبتلج%وحده، لو وياه%')
             AND datetime(issue.created_at)>=datetime({last_time},'-1 day')
             AND NOT EXISTS (SELECT 1 FROM orders done WHERE done.sender_id={sender} AND datetime(done.created_at)>=datetime(issue.created_at)))>=3 THEN 1 ELSE 0 END)"""


def init_db(db):
    for statement in INDEXES:
        db.execute(statement)


def attention_state(db, sender_id):
    """Read authoritative post-mutation badges without reloading the whole inbox."""
    issues = system_issue_sql('c.sender_id', "COALESCE((SELECT created_at FROM messages WHERE sender_id=c.sender_id ORDER BY id DESC LIMIT 1),c.last_seen_at,'')", 'c.inbox_reviewed_message_id')
    row = db.execute(f"""SELECT
        (SELECT COUNT(*) FROM human_reviews WHERE sender_id=? AND status='pending') pending_reviews_count,
        (SELECT COUNT(*) FROM problem_reports WHERE sender_id=? AND COALESCE(status,'open') IN ('open','needs_attention')) problem_count,
        (SELECT reason FROM human_reviews WHERE sender_id=? AND status='pending' ORDER BY id DESC LIMIT 1) human_review_reason,
        (SELECT reason FROM problem_reports WHERE sender_id=? AND COALESCE(status,'open') IN ('open','needs_attention') ORDER BY id DESC LIMIT 1) problem_reason,
        (SELECT COUNT(*) FROM ai_jobs WHERE sender_id=? AND status='failed') failed_jobs,
        (SELECT json_extract(result,'$.error') FROM ai_jobs WHERE sender_id=? AND status='failed' ORDER BY created_at DESC LIMIT 1) ai_job_error,
        {issues} system_issue_count
        FROM customers c WHERE c.sender_id=?
        """, (sender_id,) * 7).fetchone()
    if row is None:
        return {}
    state = dict(row)
    state['human_attention_count'] = state['pending_reviews_count'] + state['problem_count']
    state['human_service_count'] = state['human_attention_count'] - db.execute(
        f"SELECT COUNT(*) FROM human_reviews hr WHERE hr.sender_id=? AND hr.status='pending' AND {TECHNICAL_REVIEW}", (sender_id,)).fetchone()[0]
    return state
