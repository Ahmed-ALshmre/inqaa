import os, sys, io, json, sqlite3, tempfile, zipfile, builtins
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
    with zipfile.ZipFile(next((Path.home()/"Downloads").glob("lamsa-store-backup-20260908-103859.zip"))) as bundle:
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
        sample = db.execute("SELECT id,sender_id,image_url FROM human_reviews WHERE id=191").fetchone()
        token = app._current_store_id.set("al-fatena")
        try:
            products = app.load_active_products(db)
            result = app.match_customer_image_with_catalog(sample["image_url"], products)
            results.append({"sample": "customer_review_191", "expected_from_saved_link": "P001", "result": result})
            print(json.dumps({"sample": "customer_review_191", "product_found": result.get("product_found"), "product_id": result.get("product_id"), "reason": result.get("reason")}, ensure_ascii=False), flush=True)
        finally:
            app._current_store_id.reset(token)
(root/"review_20260908/live-vision-result.json").write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding="utf-8")
