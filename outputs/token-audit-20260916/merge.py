from pathlib import Path
p=Path('account_app/app.py');s=p.read_text(encoding='utf-8');a=s.index('def _call_main_ai_once(');b=s.index('    transcript_lines = []',a);s=s[:b]+'    history = ai_efficiency.merge_history(history, conversation_history)\n'+s[b:];p.write_text(s,encoding='utf-8')
