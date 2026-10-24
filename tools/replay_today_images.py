"""Replay clothing-only crops from today's archived photos with isolated state.

Only OpenRouter is live. No customer names, phone numbers, raw sender IDs,
remote image URLs or conversation transcripts are loaded into the test DB.
The original snapshot remains read-only. Identity labels come from a separate
visual review, never from the result under test.
"""
import argparse
import contextlib
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import time
from unittest.mock import patch

from PIL import Image, ImageOps
from evaluate_sales_dialogues import HTTPGate, source_file_hashes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--candidates', type=Path, required=True)
    parser.add_argument('--groundtruth', type=Path, required=True)
    parser.add_argument('--env-file', type=Path, default=Path('account_app/.env'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--allow-live', action='store_true')
    parser.add_argument('--max-live-requests', type=int, default=100)
    parser.add_argument('--max-live-cost-usd', type=float, default=2)
    parser.add_argument('--message-id', type=int, action='append', help='Repeat only this already-reviewed input; may be repeated.')
    args = parser.parse_args()
    if not args.allow_live:
        parser.error('--allow-live is required; billed model requests will be sent')
    import requests
    from dotenv import dotenv_values
    key = dotenv_values(args.env_file).get('OPENROUTER_API_KEY') or ''
    if not key:
        raise ValueError('Missing local test key')
    snapshot = args.snapshot.resolve()
    catalog = json.loads((snapshot / 'products.json').read_text(encoding='utf-8-sig'))
    candidates = json.loads(args.candidates.read_text(encoding='utf-8'))['candidates']
    truth = json.loads(args.groundtruth.read_text(encoding='utf-8'))
    labels = {(row['store_id'],row['image_message_id']): row for row in truth['cases']}
    cases = [row for row in candidates if (row['store_id'],row['image_message_id']) in labels]
    case_numbers = {(row['store_id'],row['image_message_id']): index for index,row in enumerate(cases,1)}
    if args.message_id:
        cases = [row for row in cases if row['image_message_id'] in args.message_id]
        if not cases:
            raise ValueError('No matching reviewed input')
    args.output.mkdir(parents=True, exist_ok=True)
    source = args.source_root.resolve()
    hashes = source_file_hashes(source)
    gate = HTTPGate('live', requests.sessions.Session.request, args.max_live_requests, args.max_live_cost_usd)
    original_gate_request = gate.request
    def http(session, method, url, **kwargs):
        before = len(gate.calls)
        try:
            return original_gate_request(session, method, url, **kwargs)
        finally:
            if len(gate.calls) > before:
                payload = kwargs.get('json') or {}
                gate.calls[-1]['request_bytes'] = len(json.dumps(payload, ensure_ascii=True).encode('utf-8'))
                gate.calls[-1]['image_count'] = sum(p.get('type') == 'image_url' for msg in payload.get('messages', [])
                    if isinstance(msg.get('content'), list) for p in msg['content'])
    db_source = sqlite3.connect((snapshot/'sales.db').as_uri()+'?mode=ro&immutable=1', uri=True)
    allowed = {'vision_enabled','catalog_match_enabled','ai_vision_model','ai_catalog_match_model',
               'ai_main_model','ai_main_max_tokens','ai_main_temperature','delivery_time',
               'delivery_baghdad_fee','delivery_other_fee','delivery_inspection_message'}
    settings = [(k,v) for k,v in db_source.execute('SELECT key,value FROM app_settings') if k.split(':')[-1] in allowed]
    db_source.close()
    results = []
    capture = []
    with tempfile.TemporaryDirectory(prefix='lamsa-today-photo-qa-') as temporary:
        temp = Path(temporary)
        (temp/'products.json').write_text(json.dumps(catalog,ensure_ascii=False),encoding='utf-8')
        for kind, folder in [('uploads','images/uploads'),('catalog','images/catalog')]:
            shutil.copytree(snapshot/folder,temp/kind)
        incoming = temp/'media'/'incoming'; incoming.mkdir(parents=True)
        environment = dict(ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1',DATA_DIR=str(temp),
            DB_PATH=str(temp/'sales.db'),PRODUCTS_FILE=str(temp/'products.json'),
            UPLOADS_DIR=str(temp/'uploads'),CATALOG_IMAGE_DIR=str(temp/'catalog'),INCOMING_MEDIA_DIR=str(incoming),
            OPENROUTER_API_KEY=key,TELEGRAM_BOT_TOKEN='',MANYCHAT_API_KEY='',
            STORAGE_RESTORE_REQUIRED='0',AI_MAX_CONCURRENT='1')
        original_exists = os.path.exists
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ,environment))
            stack.enter_context(patch('dotenv.load_dotenv',return_value=False))
            stack.enter_context(patch('os.path.exists',side_effect=lambda p:False if Path(p).name=='.env' else original_exists(p)))
            stack.enter_context(patch('requests.sessions.Session.request',new=http))
            sys.path.insert(0,str(source))
            m = importlib.import_module('account_app.app')
            stack.enter_context(m.app.app_context())
            db=m.get_db()
            db.executemany('INSERT INTO app_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',settings)
            db.commit()
            def text_send(sender,text,*args,**kwargs):
                capture.append(dict(type='text',text=text)); return True
            def image_send(sender,url,*args,**kwargs):
                capture.append(dict(type='image')); return True
            stack.enter_context(patch.object(m,'send_text_to_facebook',side_effect=text_send))
            stack.enter_context(patch.object(m,'send_image_to_facebook',side_effect=image_send))
            for name in ['send_order_to_telegram','send_telegram_message','send_problem_to_telegram','save_booking_to_file']:
                stack.enter_context(patch.object(m,name,return_value=False))
            stack.enter_context(patch.object(m.menger,'deliver_due',return_value=dict(sent=0,blocked=0)))
            for case in cases:
                index=case_numbers[(case['store_id'],case['image_message_id'])]
                label=labels[(case['store_id'],case['image_message_id'])]
                assert label.get('customer_name_phone_account_ui_excluded') and label.get('interface_removed'), 'Unscreened image must not be transmitted'
                crop=label.get('crop_box_exif_transposed_xyxy')
                source_image=snapshot/case['local_relative_path']
                assert source_image.resolve().is_relative_to(snapshot/'media'/'incoming')
                approved_image=(args.groundtruth.resolve().parent/label['replay_crop_relative_path']).resolve()
                assert approved_image.is_relative_to(args.groundtruth.resolve().parent/'private-replay-crops')
                content=approved_image.read_bytes()
                assert hashlib.sha256(content).hexdigest()==label['replay_crop_sha256']
                with Image.open(io.BytesIO(content)) as picture:
                    picture.verify()
                filename=hashlib.md5(('synthetic-case-'+str(index)).encode()).hexdigest()+'.jpg'
                (incoming/filename).write_bytes(content)
                store=case['store_id']; m._current_store_id.set(store)
                for name,value in [('ai_enabled','1'),('auto_product_enabled','0'),('followup_enabled','0'),('checker_enabled','0')]:
                    m.set_store_setting(db,name,value)
                sender=store+'::synthetic-today-image-'+str(index)
                m.get_or_create_customer(db,sender,'synthetic-page','facebook')
                url='https://lookaside.fbsbx.com/ig_messaging_cdn/?asset_id=synthetic-case-'+str(index)
                def body(text='',photo=False):
                    return {'entry':[{'id':'synthetic-page','_store_id':store,'messaging':[{'sender':{'id':sender},
                        'message':{'mid':'synthetic-'+str(index)+'-'+str(photo),'text':text,
                                   'attachments':[{'type':'image','payload':{'url':url}}] if photo else []}}]}]}
                first=body('شكد سعره؟',True)
                message_id=m.save_message(db,sender,'incoming','image','شكد سعره؟',url,None,None,first)
                db.execute('UPDATE messages SET media_json=? WHERE id=?',(json.dumps([dict(type='image',url=url,local_file=filename,mime='image/jpeg')]),message_id))
                db.commit()
                m.save_conversation_message(db,sender,'user','شكد سعره؟')
                start_calls,start_capture,start_blocked=len(gate.calls),len(capture),len(gate.blocked)
                started=time.monotonic()
                error=None; response={}
                try:
                    response=m.process_webhook(db,first,use_debounce=False,incoming_message_id=message_id)
                    if response.get('reply'):
                        m.send_webhook_result_to_facebook(response,sender)
                except Exception as exc:
                    error=type(exc).__name__
                positions=m.customer_image_positions(db,sender)
                recognized=[p['product_id'] for p in positions if p.get('product_found')]
                linked=[p['product_id'] for p in m.load_customer_products(db,sender)]
                image_calls=len(gate.calls)-start_calls
                image_text=[p['text'] for p in capture[start_capture:] if p['type']=='text']
                follow_start=len(capture); follow_calls=len(gate.calls)
                try:
                    follow=m.process_webhook(db,body('وشنو قياساته؟'),use_debounce=False)
                    if follow.get('reply'):
                        m.send_webhook_result_to_facebook(follow,sender)
                except Exception as exc:
                    error=type(exc).__name__
                follow_text=[p['text'] for p in capture[follow_start:] if p['type']=='text']
                all_text='\n'.join(image_text+follow_text)
                checks=dict(no_pipeline_exception=error is None,
                    no_service_error=not any(p.get('service_error') for p in positions),
                    response_for_received_image=bool(image_text),response_to_followup=bool(follow_text),
                    no_remote_customer_image_download=len(gate.blocked)==start_blocked,
                    no_order=db.execute('SELECT COUNT(*) FROM orders WHERE sender_id=?',(sender,)).fetchone()[0]==0,
                    no_same_photo_reask=not __import__('re').search(r'(?:دزي|ارسل|أرسل|ابعث).{0,25}(?:صوره|صورة)',all_text),
                    bubble_limit=len(image_text)<=3 and len(follow_text)<=3)
                expected=(label.get('expected_product_ids')
                          if label.get('include_in_identity_accuracy_denominator') else None)
                if expected is not None:
                    checks['visual_identity_matches_review']=set(recognized)==set(expected)
                if recognized:
                    checks['identity_persisted']=set(recognized).issubset(linked)
                    checks['known_product_not_reasked']=not m.reply_reasks_known_product('\n'.join(follow_text))
                    checks['no_followup_vision_request']=not any(c.get('image_count') for c in gate.calls[follow_calls:])
                result=dict(case_id='photo-'+str(index),store=store,prior_state=case['previous_recognition_state'],
                    expected_product_ids=expected,recognized_product_ids=recognized,linked_product_ids=linked,
                    review_confidence=label.get('visual_confidence'),input_sha256=hashlib.sha256(content).hexdigest(),
                    source_image_sha256=hashlib.sha256(source_image.read_bytes()).hexdigest(),input_was_cropped=bool(crop),
                    image_reply=image_text,followup_reply=follow_text,checks=checks,error_type=error,
                    elapsed_seconds=round(time.monotonic()-started,2),image_stage_requests=image_calls,
                    requests=gate.calls[start_calls:])
                results.append(result)
                try:
                    (args.output/'checkpoint.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
                except OSError:
                    pass
                print(json.dumps(dict(case_id=result['case_id'],store=store,recognized=recognized,
                    passed=sum(checks.values()),total=len(checks),seconds=result['elapsed_seconds'])),flush=True)
    report=dict(scope='Today archived garment crops; isolated synthetic conversations; OpenRouter only is live.',
        label_scope=truth.get('label_scope','Assistant visual review; not human-certified ground truth'),cases=results,
        checks_passed=sum(sum(c['checks'].values()) for c in results),checks_total=sum(len(c['checks']) for c in results),
        provider_requests=len(gate.calls),known_provider_reported_cost_usd=gate.spent,
        blocked_external_requests=gate.blocked,source_file_hashes=hashes,
        source_changed_during_run=hashes!=source_file_hashes(source),production_modified=False)
    (args.output/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:report[k] for k in ['checks_passed','checks_total','provider_requests','known_provider_reported_cost_usd','source_changed_during_run']}))
    return 0 if report['checks_passed']==report['checks_total'] else 1


if __name__=='__main__':
    sys.exit(main())
