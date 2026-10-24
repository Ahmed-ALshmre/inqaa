"""Live vision sanity checks using catalogue assets, never customer photos.

All catalogue references go through the actual matching/batching/parser code.
Inputs are re-encoded copies of product assets plus a blank negative control.
This checks obvious regressions; it is not a held-out accuracy estimate.
"""
import argparse
import base64
import contextlib
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from unittest.mock import patch
from urllib.parse import urlsplit

from PIL import Image, ImageOps
from evaluate_sales_dialogues import HTTPGate, source_file_hashes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--snapshot', type=Path, default=Path('analysis_20261024'))
    parser.add_argument('--api-key-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--allow-live', action='store_true')
    args = parser.parse_args()
    if not args.allow_live:
        parser.error('--allow-live is required; provider usage is billed')
    import requests
    args.output.mkdir(parents=True, exist_ok=True)
    snapshot = args.snapshot.resolve()
    catalog = json.loads((snapshot / 'products.json').read_text(encoding='utf-8-sig'))
    original_request = requests.sessions.Session.request
    gate = HTTPGate('live', original_request, 35, 1)
    measurements = []
    def http(session, method, url, **kwargs):
        payload = kwargs.get('json') or {}
        measurements.append(dict(bytes=len(json.dumps(payload, ensure_ascii=True).encode('utf-8')),
            images=sum(part.get('type') == 'image_url' for msg in payload.get('messages', [])
                       if isinstance(msg.get('content'), list) for part in msg['content'])))
        return gate.request(session, method, url, **kwargs)
    def local_ref(url):
        if str(url).startswith('data:'):
            return url
        asset = snapshot / 'images' / 'uploads' / Path(urlsplit(str(url)).path).name
        if not asset.is_file():
            raise FileNotFoundError('Catalogue reference missing locally')
        return 'data:image/' + ('png' if asset.suffix == '.png' else 'jpeg') + ';base64,' + base64.b64encode(asset.read_bytes()).decode('ascii')
    settings_db = sqlite3.connect((snapshot / 'sales.db').as_uri() + '?mode=ro', uri=True)
    settings = [(key, value) for key, value in settings_db.execute('SELECT key,value FROM app_settings')
                if key.split(':')[-1] in {'ai_vision_model', 'ai_catalog_match_model', 'vision_enabled', 'catalog_match_enabled'}]
    settings_db.close()
    hashes = source_file_hashes(args.source_root.resolve())
    results = []
    with tempfile.TemporaryDirectory(prefix='lamsa-vision-qa-') as temporary:
        temp = Path(temporary)
        (temp / 'products.json').write_text(json.dumps(catalog, ensure_ascii=False), encoding='utf-8')
        (temp / 'uploads').mkdir()
        # Existing-asset eligibility is checked by production code. References
        # themselves are read only from the supplied catalogue snapshot.
        import shutil
        for asset in (snapshot / 'images' / 'uploads').glob('*'):
            if asset.is_file():
                shutil.copy2(asset, temp / 'uploads' / asset.name)
        environment = dict(ENABLE_BACKGROUND_JOBS='0', DISABLE_CLIP='1', DATA_DIR=str(temp),
            DB_PATH=str(temp / 'sales.db'), PRODUCTS_FILE=str(temp / 'products.json'),
            OPENROUTER_API_KEY=args.api_key_file.read_text(encoding='utf-8-sig').strip(),
            TELEGRAM_BOT_TOKEN='', MANYCHAT_API_KEY='', STORAGE_RESTORE_REQUIRED='0')
        exists = os.path.exists
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, environment))
            stack.enter_context(patch('dotenv.load_dotenv', return_value=False))
            stack.enter_context(patch('os.path.exists', side_effect=lambda p: False if Path(p).name == '.env' else exists(p)))
            stack.enter_context(patch('requests.sessions.Session.request', new=http))
            sys.path.insert(0, str(args.source_root.resolve()))
            m = importlib.import_module('account_app.app')
            stack.enter_context(m.app.app_context())
            db = m.get_db()
            db.executemany('INSERT INTO app_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', settings)
            db.commit()
            stack.enter_context(patch.object(m, '_image_ref_for_openrouter', side_effect=local_ref))
            cases = [('default', 'P003'), ('khuyoot', 'P002'), ('al-fatena', 'P003'), ('baraah-kids', 'P010'), ('al-fatena', None)]
            for store, expected in cases:
                m._current_store_id.set(store)
                products = m.load_active_products(db)
                if expected:
                    product = next(item for item in products if item['product_id'] == expected)
                    url = product['image_url']; url = url[0] if isinstance(url, list) else url
                    original = snapshot / 'images' / 'uploads' / Path(urlsplit(url).path).name
                    with Image.open(original) as source:
                        picture = ImageOps.exif_transpose(source).convert('RGB')
                        picture.thumbnail((720, 720), Image.Resampling.LANCZOS)
                    input_hash = hashlib.sha256(original.read_bytes()).hexdigest()
                else:
                    picture = Image.new('RGB', (600, 600), '#e5e5e5')
                    input_hash = None
                output = io.BytesIO(); picture.save(output, format='JPEG', quality=76)
                customer = 'data:image/jpeg;base64,' + base64.b64encode(output.getvalue()).decode('ascii')
                folder = snapshot / 'images' / 'catalog'
                if store != 'default':
                    folder /= store
                sheets = [str(path) for path in folder.glob('*') if path.is_file()]
                start_calls, start_measurements = len(gate.calls), len(measurements)
                started = time.monotonic()
                with patch.object(m, '_resolve_catalog_image_paths', return_value=sheets):
                    result = m.match_customer_image_with_catalog(customer, products)
                passed = not result.get('service_error') and (result.get('product_id') == expected if expected else not result.get('product_found'))
                row = dict(store=store, expected=expected, actual=result.get('product_id'), passed=passed,
                    service_error=bool(result.get('service_error')), error_code=result.get('error_code'),
                    seconds=round(time.monotonic()-started, 2), input_asset_sha256=input_hash,
                    reference_source=result.get('reference_source', 'catalog_sheets'),
                    requests=gate.calls[start_calls:], payloads=measurements[start_measurements:])
                results.append(row)
                (args.output / 'progress.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
                print(json.dumps({key: row[key] for key in ('store','expected','actual','passed','service_error','seconds')}), flush=True)
    report = dict(scope='Re-encoded catalogue positives and blank negative; no independent customer-image accuracy measurement.',
        passed=sum(row['passed'] for row in results), total=len(results), cases=results,
        source_file_hashes=hashes, source_changed_during_run=hashes != source_file_hashes(args.source_root.resolve()),
        requests=len(gate.calls), known_cost_usd=gate.spent, blocked_external_requests=gate.blocked)
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return 0 if all(row['passed'] for row in results) else 1


if __name__ == '__main__':
    sys.exit(main())
