import os, sys, json, shutil, time
from pathlib import Path
root=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(root))
source=root/'review_20260913'
work=source/'live_test_runtime';work.mkdir(exist_ok=True)
shutil.copy2(source/'sales.db',work/'sales.db')
os.environ.update(DB_PATH=str(work/'sales.db'),DATA_DIR=str(work),PRODUCTS_FILE=str(source/'products.json'),CATALOG_IMAGE_DIR=str(source/'images/catalog'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
import account_app.app as m
m.CATALOG_IMAGE_PATH=''
products=[p for p in json.loads((source/'products.json').read_text(encoding='utf-8')) if p.get('store_id','default')=='default' and p.get('status')=='active']
results=[]
with m.app.app_context():
 token=m._current_store_id.set('default')
 try:
  for pid in ['P004','P005']:
   product=next(p for p in products if p['product_id']==pid)
   # Actual uploaded product photograph is the customer test input, not a reference.
   refs=product['image_url']
   ref=(refs[0] if isinstance(refs,list) else refs.split('\n')[0]).strip()
   path=source/'images/uploads'/ref.rsplit('/',1)[-1]
   if not path.exists():
    paths=list((source/'images/uploads').glob('product_1789230984105424584_0.jpg')) if pid=='P005' else []
    path=paths[0] if paths else path
   customer=m._file_to_data_url(str(path))
   started=time.monotonic()
   result=m.match_customer_image_with_catalog(customer,products)
   row={'expected':pid,'actual':result.get('product_id'),'passed':result.get('product_id')==pid,'seconds':round(time.monotonic()-started,2),'catalog_images':len(m._resolve_catalog_image_paths()),'result':result}
   results.append(row)
   sys.stdout.write(json.dumps(row,ensure_ascii=False)+'\n');sys.stdout.flush()
   if result.get('error_code')=='authentication': break
 finally:m._current_store_id.reset(token)
(root/'outputs/live-catalog-test-20260913.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
