import os, sys, io, json, sqlite3, tempfile, zipfile, builtins, time
from pathlib import Path
from unittest.mock import patch
from dotenv import dotenv_values
root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
sys.stdout.reconfigure(encoding="utf-8")
key = os.environ.get("OPENROUTER_API_KEY") or dotenv_values(root / "account_app/.env").get("OPENROUTER_API_KEY", "")
results = []
with tempfile.TemporaryDirectory(prefix="lamsa-live-vision-") as temp:
    work = Path(temp)
    with zipfile.ZipFile(next((Path.home()/"Downloads").glob("lamsa-store-backup-20260908-164252.zip"))) as bundle:
        for name in ["sales.db", "products.json"]:
            (work/name).write_bytes(bundle.read(name))
        for name in bundle.namelist():
            if name.startswith("images/") and not name.endswith("/"):
                dest = (work/name).resolve()
                if work.resolve() not in dest.parents: raise ValueError("Invalid archive path")
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(bundle.read(name))
    os.environ.update(DATA_DIR=str(work), DB_PATH=str(work/"sales.db"), PRODUCTS_FILE=str(work/"products.json"), CATALOG_IMAGE_DIR=str(work/"images/catalog"), UPLOADS_DIR=str(work/"images/uploads"), BOOKINGS_FILE=str(work/"bookings.jsonl"), ENABLE_BACKGROUND_JOBS="0", DISABLE_CLIP="1", OPENROUTER_API_KEY=key)
    original_open = builtins.open
    def safe_open(file, *args, **kwargs):
        if str(file).endswith(".env"): return io.StringIO("")
        return original_open(file, *args, **kwargs)
    with patch("dotenv.load_dotenv", return_value=False), patch("builtins.open", side_effect=safe_open):
        import account_app.app as app
    import requests
    original_request = requests.sessions.Session.request
    def restricted_request(self, method, url, **kwargs):
        if method.upper() == "POST" and url != app.OPENROUTER_URL:
            raise RuntimeError("Customer messaging disabled in simulation")
        return original_request(self, method, url, **kwargs)
    with app.app.app_context(), patch("requests.sessions.Session.request", new=restricted_request), patch.object(app, "send_telegram_message", return_value=False):
        db = app.get_db()
        token = app._current_store_id.set("al-fatena")
        try:
            products = app.load_active_products(db)
            candidates = app.build_product_vision_candidates(products, limit=len(products))
            samples = [("instagram_screenshot", str(Path.home()/"Downloads/photo_2026-09-08_11-40-30.jpg"), "P006")]
            for pid in ("P003", "P006"):
                product = next(p for p in products if p["product_id"] == pid)
                refs = app.product_image_urls(product)
                if refs: samples.append(("catalog_original_"+pid, refs[0], pid))
            original_model = app.get_ai_model
            for model in ("google/gemini-2.5-flash",):
                def selected_model(db, setting, default):
                    return model if setting in ("vision_model", "catalog_match_model") else original_model(db, setting, default)
                with patch.object(app, "get_ai_model", side_effect=selected_model):
                    for label, ref, expected in samples:
                        started = time.monotonic()
                        result = app.match_customer_image_with_catalog(ref, products)
                        row = {"sample":label,"model":model,"expected":expected,"result":result,"seconds":round(time.monotonic()-started,2),"correct":result.get("product_id")==expected}
                        results.append(row)
                        print(json.dumps(row,ensure_ascii=False),flush=True)
                        (root/"review_20260908_1642/live-catalog-results.json").write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding="utf-8")
        finally:
            app._current_store_id.reset(token)
