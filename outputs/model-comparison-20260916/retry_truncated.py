import concurrent.futures,json,os,time
from pathlib import Path
import requests
P=Path(__file__).resolve().parent
cases=json.loads((P/'cases.json').read_text(encoding='utf-8'))
method=json.loads((P/'method.json').read_text(encoding='utf-8'))
results=json.loads((P/'results.json').read_text(encoding='utf-8'))
def run(r):
 c=cases[r['case']-1];start=time.monotonic();out={'model':r['model'],'case':r['case'],'max_tokens':4096}
 try:
  response=requests.post('https://openrouter.ai/api/v1/chat/completions',headers={'Authorization':'Bearer '+os.environ['OPENROUTER_API_KEY']},json={'model':r['model'],'temperature':0.3,'max_tokens':4096,'messages':[{'role':'system','content':method['system']},{'role':'user','content':json.dumps({k:v for k,v in c.items() if k not in ['message_id','date','case']},ensure_ascii=False)}]},timeout=(10,90))
  data=response.json();out['http_status']=response.status_code
  if response.ok and data.get('choices'):
   ch=data['choices'][0];out.update(reply=ch['message'].get('content') or '',finish_reason=ch.get('finish_reason'),usage=data.get('usage'),actual_model=data.get('model'))
  else:out['error']='Provider error'
 except Exception as exc:out['error']=type(exc).__name__
 out['seconds']=round(time.monotonic()-start,2)
 print(out['model'],out['case'],out.get('finish_reason'),out['seconds'],flush=True)
 return out
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
 data=list(pool.map(run,[r for r in results if r.get('finish_reason')=='length']))
(P/'retries.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
