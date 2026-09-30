import pathlib,subprocess,shutil,tempfile,os
root=pathlib.Path(tempfile.mkdtemp(prefix='lamsa-baseline-'))
shutil.copytree('account_app',root/'account_app',ignore=shutil.ignore_patterns('.env','*.db','*.db-*','__pycache__','images','runtime','node_modules'))
(root/'account_app/app.py').write_bytes(subprocess.check_output(['git','show','HEAD:account_app/app.py']))
with open('outputs/token-audit-20260916/baseline-tests.txt','w',encoding='utf-8') as out:
 result=subprocess.run([os.sys.executable,'-m','account_app.tests.run_isolated'],cwd=root,stdout=out,stderr=subprocess.STDOUT)
print('baseline_exit',result.returncode)
