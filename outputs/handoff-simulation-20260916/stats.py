import json,collections,pathlib
p=pathlib.Path('outputs/handoff-simulation-20260916/results.json');d=json.loads(p.read_text(encoding='utf-8'))
counts=d['summary']['reason_counts']
for term in ['402','ReadTimeout','invalid_ai_response','ValueError','empty_reply','handoff/apology','confidently match']:
 print(term,sum(v for k,v in counts.items() if term in k))
print('disabled direct',sum(r['snapshot']['outcome']=='no_reply' and r['snapshot']['direct_catalog_answer_available'] for r in d['cases']))
print('image linked',sum(r['enabled_counterfactual'].get('stage')=='image_download_required' and r['enabled_counterfactual']['linked_products']>0 for r in d['cases']))
print('examples',json.dumps([{k:r[k] for k in ['review_id','question','historical_reason','snapshot','enabled_counterfactual']} for r in d['cases'] if r['review_id'] in [853,850,837,828,815,854]],ensure_ascii=False))
