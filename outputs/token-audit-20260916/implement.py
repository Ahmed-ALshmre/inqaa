from pathlib import Path
p=Path('account_app/app.py')
s=p.read_text(encoding='utf-8')
s=s.replace('from . import menger, chatwoot, ad_attribution','from . import menger, chatwoot, ad_attribution, ai_efficiency',1).replace('    import menger\n','    import ai_efficiency\n    import menger\n',1)
s=s.replace('    ad_attribution.init_db(db)\n','    ad_attribution.init_db(db)\n    ai_efficiency.init_db(db)\n',1)
a=s.index('    attempts = 3 if current_store_id() == DEFAULT_STORE_ID else 1',s.index('def match_customer_image_with_catalog('))
b=s.index('\n\ndef _match_customer_image_with_catalog_once',a)
s=s[:a]+'    return dict(_match_customer_image_with_catalog_once(customer_image_url, products), attempts=1)\n'+s[b:]
s=s.replace('        for attempt in range(2):','        for attempt in range(1):',1)
s=s.replace('        catalog_result = match_customer_image_with_catalog(image_url, products)', '''        try:
            image_data = (_download_image_to_data_url(image_url)
                          if image_url.startswith(('https://', 'http://'))
                          else _image_ref_for_openrouter(image_url))
            image_key = ai_efficiency.fingerprint(image_data)
            catalog_result = ai_efficiency.recognize_once(
                db, current_store_id(), str(sender_id or ''), image_key, products,
                lambda: match_customer_image_with_catalog(image_data, products))
        except Exception:
            catalog_result = {'product_found': False, 'service_error': True,
                              'reason': 'Image unavailable; manual review required',
                              'error_code': 'image_unavailable'}''',1)
s=s.replace('if not matched and image_url and products and current_store_id() == DEFAULT_STORE_ID:', 'if not matched and image_url and products:',1)
a=s.index('    if not matched and image_url and products and is_store_feature_enabled("vision_enabled"',s.index('def _match_single_product'))
b=s.index('\n    if matched:',a)
s=s[:a]+s[b:]
# Preserve full supplied history and all product fields; remove duplicate transport history only.
a=s.index('def _call_main_ai_once(')
b=s.index('\n# ── Reply checker',a)
t=s[a:b]
t=t.replace('json.dumps(matched_short, ensure_ascii=False, indent=2)','ai_efficiency.dumps(matched_short)').replace('json.dumps(customer_products_short, ensure_ascii=False, indent=2)','ai_efficiency.dumps(customer_products_short)').replace('json.dumps(products_short, ensure_ascii=False, indent=2)','ai_efficiency.dumps(products_short)').replace('json.dumps(customer_profile, ensure_ascii=False, indent=2)','ai_efficiency.dumps(customer_profile)')
# Use a single chronological transcript, marking unanswered rows instead of copying them.
t=t.replace('    for m in history:\n', '    last_outgoing_index = max((i for i, m in enumerate(history) if m["direction"] != "incoming"), default=-1)\n    for history_index, m in enumerate(history):\n',1)
t=t.replace('        line_prefix = f"{speaker}"','        line_prefix = f"{speaker}" + (" [غير مجابة]" if is_in and history_index > last_outgoing_index else "")',1)
t=t.replace('f"{unanswered_block}"','"الرسائل المعلمة [غير مجابة] في السجل أعلاه؛ أجب عنها جميعاً."')
# Keep event text only if it is not already represented by the supplied transcript.
t=t.replace('f"النص: {ev.get(\'text\') or \'[لا يوجد نص]\'}\\n"','f"النص: {ev.get(\'text\') if not any((m.get(\'text\') or \'\').strip() == (ev.get(\'text\') or \'\').strip() for m in history if m.get(\'direction\') == \'incoming\') else \'موجود في السجل أعلاه\'}\\n"')
x=t.index('    customer_images = ')
y=t.index('\n    try:',x)
t=t[:x]+'''    # Recognition is performed only by the dedicated image pipeline. Never resend
    # original images to the reply model (including retries and manual links).
    ai_messages = [{"role": "system", "content": system_prompt},
                   {"role": "user", "content": user_content}]
''' +t[y:]
t=t.replace('        resp = requests.post(', '        started_at = time.monotonic()\n        resp = requests.post(',1)
t=t.replace('        raw = resp.json()["choices"][0]["message"]["content"]','''        payload = resp.json()
        try:
            ai_efficiency.record_usage(db, current_store_id(),
                "main_retry" if fix_instruction else "main",
                get_ai_model(db, "main_model", MAIN_MODEL), payload,
                round((time.monotonic() - started_at) * 1000))
        except (sqlite3.Error, TypeError, ValueError):
            pass
        raw = payload["choices"][0]["message"]["content"]''',1)
s=s[:a]+t+s[b:]
p.write_text(s,encoding='utf-8')
p=Path('requirements.txt');s=p.read_text();p.write_text(s+'pillow>=10.4.0\n')
