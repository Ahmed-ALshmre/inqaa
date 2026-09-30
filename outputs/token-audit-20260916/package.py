from pathlib import Path
import zipfile,hashlib,json
files=['account_app/app.py','account_app/ai_efficiency.py','requirements.txt','account_app/tests/run_isolated.py','account_app/tests/test_ai_efficiency.py','account_app/tests/test_ai_configuration.py','account_app/tests/test_checkout_regressions.py','account_app/tests/test_conversation_context.py','account_app/tests/test_messaging_media.py']
output=Path('outputs/lamsa-token-reduction-phase1-20260916.zip')
with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as z:
 for f in files:z.write(f,f)
 z.write('outputs/token-audit-20260916/تعليمات-التحديث.md','تعليمات-التحديث.md')
 z.write('outputs/token-audit-20260916/tests.txt','validation/tests.txt')
with zipfile.ZipFile(output) as z:
 assert z.testzip() is None
 assert not any(x.endswith('.db') or x.endswith('.env') for x in z.namelist())
print(str(output.resolve()))
print('bytes',output.stat().st_size)
