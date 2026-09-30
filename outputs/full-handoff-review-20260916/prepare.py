from pathlib import Path
s=Path('outputs/handoff-simulation-20260916/simulate.py').read_text(encoding='utf-8')
s=s.replace("OUT=ROOT/'outputs/handoff-simulation-20260916'","OUT=ROOT/'outputs/full-handoff-review-20260916'")
a=s.index('  candidates=');b=s.index('\n  output=[]',a)
s=s[:a]+'  candidates=reviews'+s[b:]
s=s.replace("OUT.joinpath('results.json')","OUT.joinpath(os.environ.get('SIMULATION_RESULT', 'before.json'))")
s=s.replace("   output.append(item)","   item['question']=re.sub(r'[0-9٠-٩]{7,}', '[رقم محجوب]', item['question'] or '')\n   output.append(item)")
Path('outputs/full-handoff-review-20260916/simulate.py').write_text(s,encoding='utf-8')
