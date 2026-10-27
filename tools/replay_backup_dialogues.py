"""Replay actual backup questions through the webhook with synthetic identities.

The source database is opened read-only. Only OpenRouter inference can leave
the machine in live mode; final messaging and order-export adapters are blocked
by evaluate_sales_dialogues. Phones and delivery locations are replaced before
provider submission. No live customer record or real order is modified.
"""
import argparse
import json
from pathlib import Path
import re
import sqlite3

import evaluate_sales_dialogues as harness


def source_cases(snapshot):
    db = sqlite3.connect(snapshot.as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    def message(identity):
        row = db.execute('SELECT text FROM messages WHERE id=?', (identity,)).fetchone()
        if not row:
            raise ValueError('Source message missing: ' + str(identity))
        return row[0]
    def incoming(identity, **checks):
        text = message(identity)
        if harness.PHONE != text:
            text = re.sub(r'(?<!\d)(?:\+?964|0)?7[\d\s-]{9,}(?!\d)', harness.PHONE, text)
        return harness.step(text, '', checks=checks, source_message_id=identity)
    def history(*identities):
        rows = []
        for identity in identities:
            row = db.execute('SELECT direction,text FROM messages WHERE id=?', (identity,)).fetchone()
            rows.append(dict(row))
        return rows
    cases = [dict(id='backup-anu-size', store='al-fatena', product='P017', expected_orders=0,
                  purpose='Actual size 42 follow-up on black remaining after cream sold out.',
                  turns=[incoming(150523, contains=['42'])]),
             dict(id='backup-split-booking', store='baraah-kids', product='P009', expected_orders=1,
                  purpose='Actual split checkout sequence; contact identity/location replaced with synthetic values.',
                  history=history(150471, 150476, 150482),
                  turns=[incoming(150484), incoming(150492), incoming(150493),
                         harness.step(harness.PHONE, '', source_message_id=150495, checks=dict(no_phone_reask=True)),
                         harness.step('مجمع الاختبار حولي', '', source_message_id=150497,
                                      checks=dict(no_phone_reask=True))]),
             dict(id='backup-price-booking', store='baraah-kids', product='P010', expected_orders=0,
                  purpose='Remember the pink/four-year choice but report its current stock change without silently substituting black.',
                  history=history(149219, 149229, 149234) + [dict(direction='incoming', text=harness.PHONE + ' ديالى ناحية الاختبار')],
                  turns=[incoming(149236)])]
    # Genuine short product questions with a uniquely linked product. Multi-model
    # and address messages are excluded here instead of guessed during replay.
    selected = [150505, 150457, 150346, 150333, 150314, 150227, 150085, 150070, 150053,
                150048, 150031, 149950, 149932, 149909, 149861, 149826]
    for identity in selected:
        row = db.execute('SELECT sender_id,store_id,text FROM messages WHERE id=?', (identity,)).fetchone()
        linked = db.execute("SELECT product_id FROM customer_product_interests WHERE sender_id=? AND status='active'",
                            (row['sender_id'],)).fetchall()
        if len(linked) != 1:
            continue
        cases.append(dict(id='backup-question-' + str(identity), store=row['store_id'], product=linked[0][0],
                          expected_orders=0, purpose='Actual catalog question from backup message ' + str(identity),
                          turns=[incoming(identity)]))
    db.close()
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=['offline', 'live'], default='offline')
    parser.add_argument('--allow-live', action='store_true')
    parser.add_argument('--max-live-requests', type=int, default=50)
    args = parser.parse_args()
    if args.mode == 'live' and not args.allow_live:
        parser.error('--allow-live required')
    snapshot = args.snapshot.resolve()
    cases = source_cases(snapshot / 'sales.db')
    harness.scenarios = lambda catalog: cases
    config = argparse.Namespace(mode=args.mode, source_root=Path(__file__).resolve().parents[1],
        catalog=snapshot / 'products.json', settings_db=snapshot / 'sales.db', env_file=Path('absent.env'),
        api_key_file=None, output=args.output.resolve(), max_live_requests=args.max_live_requests,
        max_live_cost_usd=1.0, scenario=None, repeat=1)
    report = harness.run(config)
    report['data_scope'] = 'Actual backup product questions and source message IDs; synthetic sender IDs, phone numbers and delivery addresses. Original database is read-only.'
    report['source_database_messages'] = [turn.get('source_message_id') for case in cases for turn in case['turns']]
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    (args.output / 'report.md').write_text(harness.report_markdown(report), encoding='utf-8')
    print(json.dumps(dict(passed=report['checks_passed'], total=report['checks_total'],
                         scenarios=len(report['scenarios']), requests=report['ai_http_requests'],
                         cost=report['reported_cost_usd'], pass_all=report['deterministic_pass']), ensure_ascii=True))
    return 0 if report['deterministic_pass'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
