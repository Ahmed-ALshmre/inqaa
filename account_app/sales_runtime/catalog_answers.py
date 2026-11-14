"""Read approved teaching without committing SQL or turning guidance into facts."""
from copy import deepcopy
import sqlite3
from ..product_questions import DASHBOARD_ANSWER_TOPICS, apply_fact


def enrich(db,store,products):
    result=deepcopy(products)
    try:
        cursor=db.execute("SELECT product_id,topic,question_key,answer,guidance FROM product_questions WHERE store_id=? AND status='answered' ORDER BY updated_at,id",(store,))
        rows=[dict(zip([d[0] for d in cursor.description],row)) for row in cursor.fetchall()]
    except sqlite3.OperationalError as exc:
        if 'no such table' not in str(exc):raise
        return result
    for product in result:
        if product.get('store_id','default')!=store:continue
        for row in rows:
            if row['product_id']!=product['product_id']:continue
            if row['answer'] and row['topic'] in DASHBOARD_ANSWER_TOPICS:
                apply_fact(product, row['topic'], row['answer'])
            if row['guidance'] and row['topic']!='service':
                product.setdefault('_staff_guidance',[]).append({k:row[k] for k in ('topic','question_key','guidance')})
    return result
