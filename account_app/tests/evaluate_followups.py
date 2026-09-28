"""Explicit live follow-up rehearsal; never delivers a message."""
import json
import os
from pathlib import Path
import tempfile
from datetime import datetime,timedelta
from unittest.mock import patch


def main():
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--live',action='store_true');args=parser.parse_args()
    if not args.live:print('Use --live to generate synthetic follow-ups without sending.');return
    with tempfile.TemporaryDirectory() as directory:
        os.environ.update(DATA_DIR=directory,DB_PATH=str(Path(directory)/'followups.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
        import account_app.app as m
        from .sales_scenarios import DRESS,CHEAPER
        import requests
        original=requests.sessions.Session.request
        diagnostics=[]
        def only_model(session,method,url,*a,**kw):
            if method.upper()!='POST' or url!=m.OPENROUTER_URL:raise RuntimeError('Non-model network blocked')
            response=original(session,method,url,*a,**kw)
            try:
                data=response.json();choice=(data.get('choices') or [{}])[0]
                diagnostics.append({'status':response.status_code,'finish_reason':choice.get('finish_reason'),'content':choice.get('message',{}).get('content'),'usage':data.get('usage')})
            except (ValueError,TypeError):pass
            return response
        cases=[
            ('missing_contact','ثبتي الاسود قياس 42','متوفر، بس رقم الهاتف والمحافظة والعنوان حتى نكمل الحجز.',DRESS,False),
            ('measuring','اقيس بالفيته وادزلج','طول الفستان 135 سم، اعتمدي قياس القطعة قبل الحجز.',DRESS,False),
            ('budget','غالي علي','متوفر فستان ساده قطن بـ13 ألف، تحب تشوف تفاصيله؟',CHEAPER,False),
            ('requested','مو هسه ذكريني بعد ساعتين','براحتك، القياس 42 متوفر.',DRESS,False),
            ('declined','ما اريد لا تراسلوني','تدلل، شكراً لتواصلك.',DRESS,False),
            ('human_pending','اريد فيديو حقيقي','حالياً الفيديو مو متوفر عندي.',DRESS,True),
        ]
        results=[]
        with m.app.app_context(),patch('requests.sessions.Session.request',new=only_model):
            db=m.get_db();m.set_store_setting(db,'ai_main_model','google/gemini-3.8-flash')
            now=datetime.now(m.BAGHDAD_TZ)
            for name,question,answer,product,pending in cases:
                sid='default::synthetic-'+name
                db.execute("INSERT INTO customers(sender_id,store_id,lead_score) VALUES(?,'default',80)",(sid,))
                for direction,text,delta in [('incoming',question,180),('outgoing',answer,175)]:
                    db.execute("INSERT INTO messages(sender_id,store_id,direction,message_type,text,created_at) VALUES(?,'default',?,'text',?,?)",(sid,direction,text,(now-timedelta(minutes=delta)).isoformat()))
                if pending:db.execute("INSERT INTO human_reviews(sender_id,status,store_id,created_at) VALUES(?,'pending','default',?)",(sid,now.isoformat()))
                db.commit()
                reason=m._followup_block_reason(db,sid)
                message=m.generate_ai_followup_message(db,sid,'conversation',product)
                results.append({'id':name,'block_reason':reason,'message':message,'delivered':False})
                print(json.dumps(results[-1],ensure_ascii=False),flush=True)
        output=Path('outputs/sales-deep-evaluation');output.mkdir(parents=True,exist_ok=True)
        (output/'followups.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
        (output/'followup-diagnostics.json').write_text(json.dumps(diagnostics,ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__':main()
