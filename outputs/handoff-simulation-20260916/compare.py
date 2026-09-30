from pathlib import Path
import json
p=Path('outputs/handoff-simulation-20260916')
before=json.loads((p/'results-before.json').read_text(encoding='utf-8'));after=json.loads((p/'results.json').read_text(encoding='utf-8'))
assert [r['review_id'] for r in before['cases']]==[r['review_id'] for r in after['cases']]
changes=[]
for a,b in zip(before['cases'],after['cases']):
 if a['snapshot'].get('stage')!=b['snapshot'].get('stage') or a['snapshot']['outcome']!=b['snapshot']['outcome']:
  changes.append({'review_id':b['review_id'],'question':b['question'],'before':a['snapshot'].get('stage') or a['snapshot']['outcome'],'after':b['snapshot'].get('stage') or b['snapshot']['outcome'],'reply':b['snapshot'].get('reply')})
print(json.dumps(changes,ensure_ascii=False,indent=2))
(p/'comparison.json').write_text(json.dumps({'before':before['summary']['categories'],'after':after['summary']['categories'],'changed_cases':changes},ensure_ascii=False,indent=2),encoding='utf-8')
