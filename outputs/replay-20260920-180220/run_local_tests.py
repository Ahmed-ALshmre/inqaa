import os,sys,unittest
from unittest.mock import patch
os.environ['ENABLE_BACKGROUND_JOBS']='0'
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import account_app.app as m
modules=['account_app.tests.test_conversation_context','account_app.tests.test_handoff_recovery','account_app.tests.test_checkout_regressions','account_app.tests.test_sales_engagement','account_app.tests.test_baraah']
with patch.object(m.ai_efficiency,'fingerprint',side_effect=lambda data:__import__('hashlib').sha256(data.encode()).hexdigest()), patch.object(m,'_resolve_catalog_image_paths',return_value=[]), patch.object(m,'_download_image_to_data_url',side_effect=lambda url:'data:image/png;base64,'+__import__('base64').b64encode(url.encode()).decode()):
    result=unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromNames(modules))
sys.exit(not result.wasSuccessful())



