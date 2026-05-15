import base64
import builtins
import io
import json
import mimetypes
import os
import re
import sqlite3
import sys
import tempfile
import threading
import time
import unicodedata
from collections import Counter
from datetime import UTC, datetime, timedelta
from functools import wraps
from urllib.parse import urlencode, urlparse
from zoneinfo import ZoneInfo

import requests
from flask import Flask, g, jsonify, redirect, render_template, request, send_file, send_from_directory

_ORIGINAL_PRINT = builtins.print


def print(*args, **kwargs):
    return None


def _trace_value(value):
    if isinstance(value, str):
        value = value.replace("\n", " ").strip()
        return value if len(value) <= 220 else value[:217] + "..."
    return value


def image_flow(stage, **data):
    record = {
        "time": datetime.now().replace(microsecond=0).isoformat(),
        "stage": stage,
    }
    record.update({k: _trace_value(v) for k, v in data.items()})
    _ORIGINAL_PRINT("[ImageFlow] " + json.dumps(record, ensure_ascii=False, default=str), flush=True)

# تحميل .env المبكر حتى يمكن تعطيل مكتبات ML الثقيلة قبل استيرادها
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                _key = _k.strip()
                _value = _v.strip().strip('"\'')
                if _key == "OPENROUTER_API_KEY":
                    os.environ[_key] = _value
                else:
                    os.environ.setdefault(_key, _value)

# قيم افتراضية غير سرية فقط — لا تضع مفاتيح API أو توكنات هنا (استخدم .env أو متغيرات الاستضافة).
for _k, _v in {
    "MAIN_MODEL":              "google/gemini-3-flash-preview",
    "IMPROVE_MODEL":           "google/gemini-3-flash-preview",
    "CHECKER_MODEL":           "disabled",
    "VISION_MODEL":            "disabled",
    "CHECKER_ENABLED":         "0",
    "VISION_ENABLED":          "0",
    "DISABLE_CLIP":            "1",
    "DEBOUNCE_DELAY":          "35",
    "HUMAN_REVIEW_ALL_IMAGES": "1",
}.items():
    if not os.environ.get(_k):
        os.environ[_k] = _v

# مكتبات CLIP — اختيارية، يتم تحميلها عند أول استخدام
if os.environ.get("DISABLE_CLIP", "0") == "1":
    CLIP_AVAILABLE = False
    print("[CLIP] Disabled by DISABLE_CLIP=1", flush=True)
else:
    try:
        import numpy as np
        import torch
        from PIL import Image
        from transformers import CLIPModel, CLIPProcessor
        CLIP_AVAILABLE = True
    except ImportError:
        CLIP_AVAILABLE = False
        print("[CLIP] ⚠️  المكتبات غير مثبتة — شغّل: pip install torch transformers pillow numpy", flush=True)
 
# متغيرات CLIP العامة (تُحمَّل مرة واحدة عند التشغيل)
_clip_model: object = None
_clip_processor: object = None
_sender_locks = {}
_sender_locks_guard = threading.Lock()

# تحميل .env: override=False حتى لا يمسح متغيرات المضيف (Railway) عند MANYCHAT_API_KEY= فارغ في الملف
try:
    from dotenv import load_dotenv
    load_dotenv(_env_path, override=False)
except ImportError:
    pass

# قراءة .env يدوياً: لا نفرّغ متغيرات Railway إذا كان الملف يحتوي MANYCHAT_API_KEY=
if os.path.exists(_env_path):
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                _key = _k.strip()
                _val = _v.strip().strip('"\'')
                if not _val and _key in os.environ and os.environ.get(_key):
                    continue
                os.environ[_key] = _val

# إصلاح encoding للتيرمنال على Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ── Logger ────────────────────────────────────────────────────────────────────

def log(step, label, msg="", data=None):
    """طباعة منسقة مع رقم الخطوة والوقت."""
    now  = datetime.now().strftime("%H:%M:%S")
    line = f"[{now}] ── STEP {step:02d} │ {label}"
    if msg:
        line += f" │ {msg}"
    print(line, flush=True)
    if data is not None:
        print(json.dumps(data, ensure_ascii=False, indent=2), flush=True)


def log_sep(title=""):
    border = "━" * 55
    print(f"\n{border}", flush=True)
    if title:
        print(f"  {title}", flush=True)
        print(border, flush=True)


def _parse_ai_json(raw: str) -> dict:
    """تحليل JSON من ردود AI مع معالجة code blocks والأسطر المكسورة والـ JSON الناقص."""
    # إزالة markdown code blocks
    text = re.sub(r"```(?:json)?", "", raw).strip()

    # محاولة 1: تحليل مباشر
    try:
        return json.loads(text)
    except Exception:
        pass

    # محاولة 2: استخراج أول كتلة JSON
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        json_str = match.group()
        # محاولة 3: تحليل مباشر
        try:
            return json.loads(json_str)
        except Exception:
            pass
        # محاولة 4: إصلاح الأسطر الحرفية
        json_fixed = re.sub(r'(?<!\\)\n', r"\\n", json_str)
        json_fixed = re.sub(r'(?<!\\)\t', r"\\t", json_fixed)
        try:
            return json.loads(json_fixed)
        except Exception:
            pass

    # محاولة 5: استخراج حقل reply فقط من JSON ناقص (عند قطع الـ tokens)
    reply_match = re.search(r'"reply"\s*:\s*"((?:[^"\\]|\\.)*)"', text, re.DOTALL)
    if reply_match:
        reply_text = reply_match.group(1).replace("\\n", "\n")
        print(f"[JSON] تعذّر تحليل JSON كامل، تم استخراج reply فقط", flush=True)
        return {
            "reply": reply_text,
            "intent": "unknown",
            "create_order": False,
            "order": {},
            "confidence": 50,
        }

    raise ValueError(f"Could not parse JSON from AI response: {raw[:300]}")


def _normalize_manychat_key_value(value: str = "") -> str:
    key = str(value or "").strip().strip('"\'')
    if key.lower().startswith("bearer "):
        key = key[7:].strip()
    return key


def _manychat_key_from_environ() -> str:
    """
    مفتاح ManyChat: Settings → API → Generate (الخطة المدفوعة).
    في Swagger يُلصق كقيمة Authorize / Bearer فقط — انظر help.manychat.com و api.manychat.com/swagger
    """
    for name in ("MANYCHAT_API_KEY", "MANYCHAT_KEY", "MC_API_KEY"):
        raw = os.environ.get(name)
        if not raw:
            continue
        key = _normalize_manychat_key_value(raw)
        if key:
            if name != "MANYCHAT_API_KEY":
                os.environ["MANYCHAT_API_KEY"] = key
            return key
    return ""


# ── Configuration ─────────────────────────────────────────────────────────────
API_SECRET_KEY = os.environ.get("API_SECRET_KEY", "")
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY", "")

if OPENROUTER_KEY:
    print("[Config] ✅ OPENROUTER_API_KEY loaded", flush=True)
else:
    print("[Config] ❌ OPENROUTER_API_KEY is MISSING — AI calls will fail!", flush=True)
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

MAIN_MODEL      = os.environ.get("MAIN_MODEL",    "google/gemini-3-flash-preview")
IMPROVE_MODEL   = os.environ.get("IMPROVE_MODEL", MAIN_MODEL)
CHECKER_MODEL   = os.environ.get("CHECKER_MODEL", "google/gemini-3.1-pro-preview")
VISION_MODEL    = os.environ.get("VISION_MODEL",  "google/gemini-3.1-pro-preview")
CHECKER_ENABLED = os.environ.get("CHECKER_ENABLED", "0") == "1"
VISION_ENABLED  = os.environ.get("VISION_ENABLED", "1") == "1"
CATALOG_MATCH_MODEL = os.environ.get("CATALOG_MATCH_MODEL", "google/gemini-2.5-flash")
CATALOG_MATCH_ENABLED = os.environ.get("CATALOG_MATCH_ENABLED", "1") != "0"
CATALOG_IMAGE_PATH = os.environ.get(
    "CATALOG_IMAGE_PATH",
    os.path.join(os.path.dirname(__file__), "Gemini_Generated_Image_efqo6zefqo6zefqo.png")
)
CATALOG_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ORDERS_CHAT_ID = (
    os.environ.get("TELEGRAM_ORDERS_CHAT_ID", "")
    or os.environ.get("ORDER_TELEGRAM_CHAT_ID", "")
).strip()
TELEGRAM_PROBLEMS_CHAT_ID = os.environ.get("TELEGRAM_PROBLEMS_CHAT_ID", "").strip()
TELEGRAM_NOTIFICATION_HEADER = os.environ.get("TELEGRAM_NOTIFICATION_HEADER", "أنيقة").strip()
MANYCHAT_API_KEY = _manychat_key_from_environ()
MANYCHAT_API_URL = "https://api.manychat.com"
HUMAN_REPLY_WEBHOOK_URL = os.environ.get("HUMAN_REPLY_WEBHOOK_URL", "")
HUMAN_REVIEW_ALL_IMAGES = os.environ.get("HUMAN_REVIEW_ALL_IMAGES", "1") == "1"
PUBLIC_URL         = os.environ.get("PUBLIC_URL", "").rstrip("/")
if MANYCHAT_API_KEY:
    print(f"[Config] ✅ MANYCHAT_API_KEY loaded ({MANYCHAT_API_KEY[:12]}...)", flush=True)
else:
    print("[Config] ❌ MANYCHAT_API_KEY is MISSING — customer messages will NOT be sent!", flush=True)
if PUBLIC_URL:
    print(f"[Config] ✅ PUBLIC_URL = {PUBLIC_URL}", flush=True)
else:
    print("[Config] ⚠️  PUBLIC_URL not set — image URLs may resolve to localhost", flush=True)
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "admin123")
DEBOUNCE_DELAY     = int(os.environ.get("DEBOUNCE_DELAY", "35"))   # ثواني انتظار قبل الرد (لجمع كل رسائل الزبون قبل تشغيل الموديل)
ASYNC_WEBHOOK      = os.environ.get("ASYNC_WEBHOOK", "1") == "1"

DB_PATH           = os.path.join(os.path.dirname(__file__), "sales.db")
PRODUCTS_FILE     = os.path.join(os.path.dirname(__file__), "products.json")
PRODUCT_IMAGE_DIR = os.path.join(os.path.dirname(__file__), "product_image")
CATALOG_IMAGE_DIR = os.environ.get("CATALOG_IMAGE_DIR", os.path.join(PRODUCT_IMAGE_DIR, "catalog"))
if not os.path.isabs(CATALOG_IMAGE_DIR):
    CATALOG_IMAGE_DIR = os.path.join(os.path.dirname(__file__), CATALOG_IMAGE_DIR)
AUD_DIR           = os.path.join(os.path.dirname(__file__), "aud")
BOOKINGS_FILE     = os.path.join(os.path.dirname(__file__), "bookings.jsonl")
INCOMING_REQUESTS_FILE = os.environ.get(
    "INCOMING_REQUESTS_FILE",
    os.path.join(os.path.dirname(__file__), "incoming_requests.jsonl"),
)
AD_TRACKING_FILE = os.path.join(os.path.dirname(__file__), "ad_tracking.jsonl")
MAX_HISTORY  = 20
FALLBACK_REPLY = (
    "حبيبتي ممكن توضحين أكثر شنو الموديل المطلوب؟ "
    "حتى أتأكدلج من التوفر والسعر 🌸"
)
FIXED_DELIVERY_TEXT = "أجور التوصيل ثابتة: 4 آلاف لكل محافظات العراق"

app = Flask(__name__)
_request_log_lock = threading.Lock()
_ad_tracking_lock = threading.Lock()
_products_file_lock = threading.Lock()
_catalog_image_lock = threading.Lock()
BAGHDAD_TZ = ZoneInfo("Asia/Baghdad")


def now_baghdad_iso():
    return datetime.now(BAGHDAD_TZ).replace(microsecond=0).isoformat()


# ── HTTP request/response logging ─────────────────────────────────────────────

def _safe_headers_for_log(headers):
    hidden = {"authorization", "x-api-key", "cookie"}
    return {
        key: ("***" if key.lower() in hidden else value)
        for key, value in headers.items()
    }


def _safe_body_for_log():
    if request.is_json:
        return request.get_json(silent=True)
    if request.files:
        return "<multipart/form-data>"
    return request.get_data(as_text=True) or ""


def _incoming_request_log_record():
    return {
        "timestamp": now_baghdad_iso(),
        "method": request.method,
        "path": request.path,
        "query": request.args.to_dict(flat=False),
        "remote_addr": request.remote_addr,
        "headers": _safe_headers_for_log(request.headers),
        "form": request.form.to_dict(flat=False),
        "files": {
            key: {
                "filename": file.filename,
                "content_type": file.content_type,
                "content_length": file.content_length,
            }
            for key, file in request.files.items()
        },
        "body": _safe_body_for_log(),
    }


def _append_incoming_request_log(record):
    with _request_log_lock:
        with open(INCOMING_REQUESTS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def _extract_ad_info_from_body(body):
    """Extract all ad/referral info from a Facebook webhook body and save to ad_tracking.jsonl."""
    if not isinstance(body, dict):
        return
    entries = body.get("entry")
    if not isinstance(entries, list):
        return

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        messaging = entry.get("messaging")
        if not isinstance(messaging, list):
            continue
        for event in messaging:
            if not isinstance(event, dict):
                continue

            sender_id = (event.get("sender") or {}).get("id")
            page_id = (entry.get("id") or "")

            ref = ad_id = referral_source = referral_type = None
            referral_nodes = [
                event.get("referral"),
                (event.get("message") or {}).get("referral"),
                (event.get("postback") or {}).get("referral"),
            ]
            for node in referral_nodes:
                if node and isinstance(node, dict):
                    ref = node.get("ref") or ref
                    ad_id = node.get("ad_id") or ad_id
                    referral_source = node.get("source") or referral_source
                    referral_type = node.get("type") or referral_type

            message = event.get("message") or {}
            text = (message.get("text") or "").strip()
            mid = message.get("mid", "")

            attachments = message.get("attachments") or []
            image_url = None
            for att in attachments:
                if att.get("type") == "image":
                    image_url = (att.get("payload") or {}).get("url")
                    break

            postback_payload = (event.get("postback") or {}).get("payload")

            record = {
                "timestamp": now_baghdad_iso(),
                "page_id": page_id,
                "ad_id": ad_id,
            }

            with _ad_tracking_lock:
                with open(AD_TRACKING_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

            print(f"[AdTrack] page_id={page_id} ad_id={ad_id}", flush=True)


@app.before_request
def log_incoming_http_request():
    record = _incoming_request_log_record()
    _append_incoming_request_log(record)
    print("\n" + "═" * 55, flush=True)
    print("[HTTP IN] Incoming request", flush=True)
    print(json.dumps(record, ensure_ascii=False, indent=2, default=str), flush=True)
    print(f"[HTTP IN] Saved to {INCOMING_REQUESTS_FILE}", flush=True)


@app.after_request
def log_outgoing_http_response(response):
    # Skip logging for static files and binary/streaming responses to avoid consuming their stream
    path = request.path
    ct   = response.content_type or ""
    if (path.startswith("/static/") or path.startswith("/product_image/")
            or "text/html" in ct or response.direct_passthrough):
        return response
    try:
        body_text = response.get_data(as_text=True) or ""
    except Exception:
        body_text = "<unreadable>"
    print("[HTTP OUT] Outgoing response", flush=True)
    print(json.dumps({
        "status_code": response.status_code,
        "content_type": ct,
        "body": body_text[:4000],
    }, ensure_ascii=False, indent=2), flush=True)
    print("═" * 55 + "\n", flush=True)
    return response


# ── Database ──────────────────────────────────────────────────────────────────

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, timeout=30)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA busy_timeout=30000")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db():
    """Create all tables if they don't exist."""
    db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS processed_messages (
            mid        TEXT PRIMARY KEY,
            processed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS sender_processing_locks (
            sender_id TEXT PRIMARY KEY,
            locked_at TEXT
        );

        CREATE TABLE IF NOT EXISTS customers (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id    TEXT UNIQUE NOT NULL,
            page_id      TEXT,
            platform     TEXT DEFAULT 'facebook',
            first_seen_at TEXT,
            last_seen_at  TEXT,
            name         TEXT,
            phone        TEXT,
            province     TEXT,
            address      TEXT,
            notes        TEXT
        );

        CREATE TABLE IF NOT EXISTS customer_product_interests (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id    TEXT NOT NULL,
            product_id   TEXT NOT NULL,
            product_name TEXT,
            match_method TEXT,
            confidence   REAL DEFAULT 0,
            last_seen_at TEXT,
            UNIQUE(sender_id, product_id)
        );

        CREATE TABLE IF NOT EXISTS messages (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id    TEXT NOT NULL,
            direction    TEXT NOT NULL,
            message_type TEXT,
            text         TEXT,
            image_url    TEXT,
            ad_id        TEXT,
            ref          TEXT,
            raw_payload  TEXT,
            created_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS conversation_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
            content TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_conv_memory_sender
        ON conversation_memory(sender_id, timestamp DESC);

        CREATE TABLE IF NOT EXISTS products (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id         TEXT UNIQUE NOT NULL,
            ref                TEXT,
            ad_id              TEXT,
            product_name       TEXT,
            keywords           TEXT,
            category           TEXT,
            description        TEXT,
            visual_description TEXT,
            price              TEXT,
            offer              TEXT,
            colors             TEXT,
            sizes              TEXT,
            stock              TEXT,
            delivery           TEXT,
            image_url          TEXT,
            image_embedding    TEXT,
            status             TEXT DEFAULT 'active',
            notes              TEXT
        );

        CREATE TABLE IF NOT EXISTS ai_instructions (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            title   TEXT,
            content TEXT,
            active  INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS forbidden_rules (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            rule   TEXT,
            active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS orders (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id     TEXT,
            customer_name TEXT,
            phone         TEXT,
            province      TEXT,
            address       TEXT,
            product_id    TEXT,
            product_name  TEXT,
            color         TEXT,
            size          TEXT,
            notes         TEXT,
            status        TEXT DEFAULT 'new',
            created_at    TEXT
        );

        CREATE TABLE IF NOT EXISTS human_reviews (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id     TEXT,
            message_text  TEXT,
            image_url     TEXT,
            candidates_json TEXT,
            reason        TEXT,
            status        TEXT DEFAULT 'pending',
            admin_reply   TEXT,
            created_at    TEXT,
            replied_at    TEXT
        );

        CREATE TABLE IF NOT EXISTS customer_instructions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id    TEXT,
            instructions TEXT,
            apply_to_all INTEGER DEFAULT 0,
            created_at   TEXT,
            updated_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS customer_ai_settings (
            sender_id  TEXT PRIMARY KEY,
            enabled    INTEGER DEFAULT 1,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS app_settings (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS followups (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id      TEXT NOT NULL,
            stage          TEXT,
            message_text   TEXT,
            status         TEXT DEFAULT 'pending',
            scheduled_at   TEXT,
            sent_at        TEXT,
            created_at     TEXT,
            order_after_id INTEGER,
            meta_json      TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_followups_due
        ON followups(status, scheduled_at);

        CREATE INDEX IF NOT EXISTS idx_followups_sender_day
        ON followups(sender_id, created_at);

        CREATE TABLE IF NOT EXISTS customer_followup_messages (
            sender_id        TEXT PRIMARY KEY,
            message_template TEXT,
            updated_at       TEXT
        );

        CREATE TABLE IF NOT EXISTS problem_reports (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id     TEXT,
            customer_name TEXT,
            message_text  TEXT,
            reason        TEXT,
            product_id    TEXT,
            product_name  TEXT,
            status        TEXT DEFAULT 'open',
            created_at    TEXT,
            updated_at    TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_problem_reports_status
        ON problem_reports(status, created_at);

        CREATE TABLE IF NOT EXISTS evaluation_suggestions (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            suggestion_type          TEXT,
            title                    TEXT NOT NULL,
            content                  TEXT NOT NULL,
            metric_name              TEXT,
            metric_value             TEXT,
            conversion_rate          REAL,
            reason                   TEXT,
            status                   TEXT DEFAULT 'draft',
            admin_notes              TEXT,
            date_range_start         TEXT,
            date_range_end           TEXT,
            metrics_json             TEXT,
            created_at               TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at               TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS active_ai_rules (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            source_suggestion_id INTEGER,
            rule_type            TEXT,
            rule_text            TEXT NOT NULL,
            priority             INTEGER DEFAULT 5,
            active               INTEGER DEFAULT 1,
            created_at           TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at           TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS daily_learning_runs (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            date_range_start     TEXT,
            date_range_end       TEXT,
            metrics_json         TEXT,
            report_text          TEXT,
            suggestions_count    INTEGER DEFAULT 0,
            status               TEXT DEFAULT 'completed',
            error_text           TEXT,
            created_at           TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    db.commit()

    for key, value in (
        ("followup_enabled", "0"),
        ("followup_max_per_day", "2"),
        ("followup_stop_on_order", "1"),
        ("followup_stop_on_rejection", "1"),
        ("followup_default_delay_minutes", "20"),
        ("followup_message_template", ""),
    ):
        db.execute(
            "INSERT OR IGNORE INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, now_baghdad_iso()),
        )
    db.commit()

    # Migration: أضف العمود إذا لم يكن موجوداً (للقواعد القديمة)
    def _add_column_if_missing(table, column, definition):
        columns = {
            row[1]
            for row in db.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column in columns:
            return
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        db.commit()
        print(f"[DB] Migration: added {table}.{column} column.", flush=True)

    for column, definition in (
        ("source", "TEXT DEFAULT 'unknown'"),
        ("status", "TEXT DEFAULT 'active'"),
        ("rejected_at", "DATETIME"),
        ("notes", "TEXT"),
        ("image_sent", "INTEGER DEFAULT 0"),
    ):
        try:
            _add_column_if_missing("customer_product_interests", column, definition)
        except Exception as exc:
            print(f"[DB] Could not add customer_product_interests.{column}: {exc}", flush=True)

    try:
        db.execute("ALTER TABLE products ADD COLUMN image_embedding TEXT")
        db.commit()
        print("[DB] Migration: added image_embedding column.", flush=True)
    except Exception:
        pass  # العمود موجود مسبقاً

    try:
        db.execute("ALTER TABLE customers ADD COLUMN platform TEXT DEFAULT 'facebook'")
        db.commit()
        print("[DB] Migration: added customer platform column.", flush=True)
    except Exception:
        pass  # العمود موجود مسبقاً

    try:
        db.execute("ALTER TABLE customers ADD COLUMN gender TEXT")
        db.commit()
        print("[DB] Migration: added customer gender column.", flush=True)
    except Exception:
        pass  # العمود موجود مسبقاً

    try:
        db.execute("DELETE FROM products")
        db.commit()
        print("[DB] Cleared products table; products are loaded from products.json.", flush=True)
    except Exception as exc:
        print(f"[DB] Could not clear products table: {exc}", flush=True)

    db.close()
    print("[DB] Database initialized.", flush=True)


# ── Products file ──────────────────────────────────────────────────────────────

PRODUCT_FIELDS = (
    "product_id", "ref", "ad_id", "product_name", "keywords", "category",
    "description", "visual_description", "price", "offer", "colors", "sizes",
    "stock", "delivery", "image_url", "image_embedding", "status", "notes",
)


def _normalize_product(raw_product):
    product = {field: raw_product.get(field, "") for field in PRODUCT_FIELDS}
    product["product_id"] = str(product.get("product_id") or "").strip()
    product["status"] = str(product.get("status") or "active").strip() or "active"
    return product


def _normalize_product_image_value(value):
    if isinstance(value, list):
        urls = [str(item or "").strip() for item in value if str(item or "").strip()]
    else:
        text = str(value or "").strip()
        urls = [line.strip() for line in re.split(r"[\r\n]+", text) if line.strip()]
    if not urls:
        return ""
    return urls[0] if len(urls) == 1 else urls


def product_payload(product):
    payload = _normalize_product(product)
    payload["image_url"] = _normalize_product_image_value(payload.get("image_url"))
    payload["image_urls"] = product_image_urls(payload)
    return payload


def load_products_from_file():
    """تحميل المنتجات من products.json فقط بدل قاعدة البيانات."""
    try:
        with open(PRODUCTS_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"[ProductsFile] Missing file: {PRODUCTS_FILE}", flush=True)
        return []
    except Exception as exc:
        print(f"[ProductsFile] Failed to read products.json: {exc}", flush=True)
        return []

    if not isinstance(data, list):
        print("[ProductsFile] products.json must contain a JSON array.", flush=True)
        return []

    return [
        _normalize_product(item)
        for item in data
        if isinstance(item, dict) and str(item.get("product_id") or "").strip()
    ]


def save_products_to_file(products):
    temp_path = PRODUCTS_FILE + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(temp_path, PRODUCTS_FILE)


def get_setting(db, key, default=""):
    row = db.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    if not row or row["value"] is None:
        return default
    return row["value"]


def set_setting(db, key, value):
    now = now_baghdad_iso()
    db.execute(
        """INSERT INTO app_settings (key, value, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(key) DO UPDATE SET
               value=excluded.value,
               updated_at=excluded.updated_at""",
        (key, str(value), now),
    )
    db.commit()


def _setting_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _setting_int(value, default=0, minimum=None, maximum=None):
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default
    if minimum is not None:
        number = max(minimum, number)
    if maximum is not None:
        number = min(maximum, number)
    return number


def get_followup_settings(db=None):
    db = db or get_db()
    return {
        "enabled": _setting_bool(get_setting(db, "followup_enabled", "0")),
        "max_per_day": _setting_int(get_setting(db, "followup_max_per_day", "2"), 2, 1, 10),
        "stop_on_order": _setting_bool(get_setting(db, "followup_stop_on_order", "1"), True),
        "stop_on_rejection": _setting_bool(get_setting(db, "followup_stop_on_rejection", "1"), True),
        "default_delay_minutes": _setting_int(
            get_setting(db, "followup_default_delay_minutes", "20"),
            20,
            1,
            10080,
        ),
        "message_template": get_setting(db, "followup_message_template", ""),
    }


def save_followup_settings(db, data):
    enabled = bool(data.get("enabled"))
    max_per_day = _setting_int(data.get("max_per_day"), 2, 1, 10)
    stop_on_order = bool(data.get("stop_on_order", True))
    stop_on_rejection = bool(data.get("stop_on_rejection", True))
    delay = _setting_int(data.get("default_delay_minutes"), 20, 1, 10080)
    message_template = str(data.get("message_template") or "")
    for key, value in (
        ("followup_enabled", "1" if enabled else "0"),
        ("followup_max_per_day", str(max_per_day)),
        ("followup_stop_on_order", "1" if stop_on_order else "0"),
        ("followup_stop_on_rejection", "1" if stop_on_rejection else "0"),
        ("followup_default_delay_minutes", str(delay)),
        ("followup_message_template", message_template),
    ):
        set_setting(db, key, value)
    return get_followup_settings(db)


def _format_followup_message_template(template, customer_name, product_name, stage):
    if not template or not template.strip():
        return None
    result = str(template)
    result = result.replace("{customer_name}", customer_name or "حبيبتي")
    result = result.replace("{product_name}", product_name or "الموديل")
    result = result.replace("{stage}", stage or "")
    return result.strip()


def get_customer_followup_template(db, sender_id):
    if not sender_id:
        return ""
    try:
        row = db.execute(
            "SELECT message_template FROM customer_followup_messages WHERE sender_id=?",
            (sender_id,),
        ).fetchone()
        return (row["message_template"] or "").strip() if row else ""
    except Exception as exc:
        print(f"[FollowUp] customer template fetch error for {sender_id}: {exc}", flush=True)
        return ""


def build_followup_message(db, sender_id, stage, product=None):
    customer_name = _customer_name_from_db(db, sender_id) or "حبيبتي"
    product_name = (product or {}).get("product_name") or "الموديل"
    for template in (
        get_customer_followup_template(db, sender_id),
        get_setting(db, "followup_message_template", "").strip(),
    ):
        custom_message = _format_followup_message_template(template, customer_name, product_name, stage)
        if custom_message:
            return custom_message
    if stage in {"price", "asked_price"}:
        return f"حبيبتي بعدج مهتمة بـ {product_name}؟ أكدر أحجزه إلج إذا تحبين."
    if stage in {"availability", "asked_availability"}:
        return f"حبيبتي أتابع وياج بخصوص {product_name}، تحبين أحجزه قبل ما يخلص؟"
    if stage in {"order", "waiting_for_customer_info"}:
        return "حبيبتي بقي بس ترسلين العنوان ورقم الهاتف حتى أثبت الحجز."
    return f"حبيت أتابع وياج بخصوص {product_name}، تحبين أكمل وياج الحجز؟"


def _followup_day_bounds():
    today = datetime.now(BAGHDAD_TZ).date()
    start = datetime.combine(today, datetime.min.time(), tzinfo=BAGHDAD_TZ).isoformat()
    end = (datetime.combine(today, datetime.min.time(), tzinfo=BAGHDAD_TZ) + timedelta(days=1)).isoformat()
    return start, end


def schedule_followup_if_needed(db, sender_id, stage="conversation", product=None, delay_minutes=None, meta=None):
    settings = get_followup_settings(db)
    if not settings["enabled"]:
        print(f"[FollowUp] Disabled; not scheduling for {sender_id}", flush=True)
        return None
    if not sender_id:
        return None
    day_start, day_end = _followup_day_bounds()
    today_count = db.execute(
        """SELECT COUNT(*) FROM followups
           WHERE sender_id=? AND created_at >= ? AND created_at < ?
             AND status IN ('pending', 'sent')""",
        (sender_id, day_start, day_end),
    ).fetchone()[0]
    if today_count >= settings["max_per_day"]:
        return None
    pending = db.execute(
        "SELECT id FROM followups WHERE sender_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (sender_id,),
    ).fetchone()
    if pending:
        return pending["id"]
    delay = delay_minutes if delay_minutes is not None else settings["default_delay_minutes"]
    scheduled_at = (datetime.now(BAGHDAD_TZ) + timedelta(minutes=max(1, int(delay)))).isoformat()
    now = now_baghdad_iso()
    message = build_followup_message(db, sender_id, stage, product)
    cur = db.execute(
        """INSERT INTO followups
           (sender_id, stage, message_text, status, scheduled_at, created_at, meta_json)
           VALUES (?, ?, ?, 'pending', ?, ?, ?)""",
        (
            sender_id,
            stage,
            message,
            scheduled_at,
            now,
            json.dumps(meta or {}, ensure_ascii=False),
        ),
    )
    db.commit()
    return cur.lastrowid


def cancel_followups_for_sender(db, sender_id, reason="cancelled"):
    now = now_baghdad_iso()
    cur = db.execute(
        """UPDATE followups
           SET status='cancelled', sent_at=?, meta_json=COALESCE(meta_json, '') || ?
           WHERE sender_id=? AND status='pending'""",
        (now, f"\n{reason}", sender_id),
    )
    db.commit()
    return cur.rowcount


def list_customer_followup_messages(db, limit=500):
    rows = db.execute(
        """SELECT
               c.sender_id,
               c.name,
               c.phone,
               c.province,
               c.last_seen_at,
               COALESCE(cfm.message_template, '') AS message_template,
               cfm.updated_at AS template_updated_at,
               lf.id AS pending_followup_id,
               lf.message_text AS pending_message_text,
               lf.stage AS pending_stage,
               lf.scheduled_at AS pending_scheduled_at,
               lm.text AS last_message,
               lm.created_at AS last_message_at
           FROM customers c
           LEFT JOIN customer_followup_messages cfm ON cfm.sender_id = c.sender_id
           LEFT JOIN followups lf ON lf.id = (
               SELECT id FROM followups
               WHERE sender_id = c.sender_id AND status='pending'
               ORDER BY scheduled_at ASC, id ASC LIMIT 1
           )
           LEFT JOIN messages lm ON lm.id = (
               SELECT id FROM messages
               WHERE sender_id = c.sender_id
               ORDER BY id DESC LIMIT 1
           )
           ORDER BY COALESCE(lf.scheduled_at, c.last_seen_at, lm.created_at, '') DESC
           LIMIT ?""",
        (int(limit),),
    ).fetchall()
    return [dict(row) for row in rows]


def save_customer_followup_template(db, sender_id, message_template, update_pending=True):
    sender_id = str(sender_id or "").strip()
    message_template = str(message_template or "").strip()
    if not sender_id:
        return {"ok": False, "error": "sender_id required"}

    exists = db.execute("SELECT sender_id FROM customers WHERE sender_id=?", (sender_id,)).fetchone()
    if not exists:
        return {"ok": False, "error": "customer not found"}

    now = now_baghdad_iso()
    if message_template:
        db.execute(
            """INSERT INTO customer_followup_messages (sender_id, message_template, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(sender_id) DO UPDATE SET
                   message_template=excluded.message_template,
                   updated_at=excluded.updated_at""",
            (sender_id, message_template, now),
        )
    else:
        db.execute("DELETE FROM customer_followup_messages WHERE sender_id=?", (sender_id,))

    updated_pending = 0
    if update_pending:
        pending_rows = db.execute(
            "SELECT id, stage FROM followups WHERE sender_id=? AND status='pending'",
            (sender_id,),
        ).fetchall()
        product = db.execute(
            """SELECT product_id, product_name FROM customer_product_interests
               WHERE sender_id=? AND COALESCE(status, 'active')='active'
               ORDER BY last_seen_at DESC LIMIT 1""",
            (sender_id,),
        ).fetchone()
        product_payload = dict(product) if product else None
        for row in pending_rows:
            refreshed_message = build_followup_message(db, sender_id, row["stage"] or "conversation", product_payload)
            db.execute(
                "UPDATE followups SET message_text=? WHERE id=?",
                (refreshed_message, row["id"]),
            )
            updated_pending += 1
    db.commit()
    return {"ok": True, "sender_id": sender_id, "updated_pending": updated_pending}


def send_due_followups(db=None, limit=25):
    db = db or get_db()
    settings = get_followup_settings(db)
    if not settings["enabled"]:
        return {"sent": 0, "skipped": 0, "reason": "followup_disabled"}
    now = now_baghdad_iso()
    rows = db.execute(
        """SELECT f.*, COALESCE(c.platform, 'facebook') AS platform, c.page_id
           FROM followups f
           LEFT JOIN customers c ON c.sender_id=f.sender_id
           WHERE f.status='pending' AND f.scheduled_at <= ?
           ORDER BY f.scheduled_at ASC
           LIMIT ?""",
        (now, int(limit)),
    ).fetchall()
    sent = 0
    skipped = 0
    for row in rows:
        sender_id = row["sender_id"]
        if settings["stop_on_order"]:
            order = db.execute(
                "SELECT id FROM orders WHERE sender_id=? AND created_at >= ? ORDER BY id DESC LIMIT 1",
                (sender_id, row["created_at"]),
            ).fetchone()
            if order:
                db.execute(
                    "UPDATE followups SET status='cancelled', order_after_id=? WHERE id=?",
                    (order["id"], row["id"]),
                )
                skipped += 1
                continue
        ok = send_reply_via_manychat(sender_id, row["message_text"], row["platform"] or "facebook", page_id=row["page_id"])
        if ok:
            save_message(
                db, sender_id, "outgoing", "text",
                row["message_text"], None, None, None,
                {"followup_id": row["id"], "automatic_followup": True},
            )
            save_conversation_message(db, sender_id, "assistant", row["message_text"])
            db.execute("UPDATE followups SET status='sent', sent_at=? WHERE id=?", (now, row["id"]))
            sent += 1
        else:
            skipped += 1
    db.commit()
    return {"sent": sent, "skipped": skipped, "reason": None}


def is_ai_enabled(db):
    return get_setting(db, "ai_enabled", "1") != "0"


def is_customer_ai_enabled(db, sender_id):
    row = db.execute(
        "SELECT enabled FROM customer_ai_settings WHERE sender_id=?",
        (sender_id,),
    ).fetchone()
    if not row:
        return True
    return int(row["enabled"] or 0) == 1


def set_customer_ai_enabled(db, sender_id, enabled):
    now = now_baghdad_iso()
    db.execute(
        """INSERT INTO customer_ai_settings (sender_id, enabled, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(sender_id) DO UPDATE SET
               enabled=excluded.enabled,
               updated_at=excluded.updated_at""",
        (sender_id, 1 if enabled else 0, now),
    )
    db.commit()


def find_product_by_id(product_id, include_inactive=False):
    product_id = str(product_id or "").strip()
    for product in load_products_from_file():
        if product.get("product_id") != product_id:
            continue
        if include_inactive or product.get("status") == "active":
            return product
    return None


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_auto_product_settings(db):
    enabled = str(get_setting(db, "auto_product_enabled", "0")).strip().lower() in {
        "1", "true", "yes", "on",
    }
    product_id = str(get_setting(db, "auto_product_id", "") or "").strip()
    send_image = str(get_setting(db, "auto_product_send_image", "0")).strip().lower() in {
        "1", "true", "yes", "on",
    }
    product = find_product_by_id(product_id) if enabled and product_id else None
    if enabled and product_id and not product:
        print(f"[AutoProduct] Configured product is missing or inactive: {product_id}", flush=True)
    return {
        "enabled": enabled and bool(product),
        "product_id": product_id,
        "send_image": send_image,
        "product": product,
    }


def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not API_SECRET_KEY:
            return jsonify({"error": "API_SECRET_KEY not configured on server"}), 503
        if request.headers.get("X-API-Key") != API_SECRET_KEY:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


def extract_sender_id_from_body(body):
    try:
        return body["entry"][0]["messaging"][0]["sender"]["id"]
    except Exception:
        return None


def split_facebook_event_bodies(body):
    """Return one Facebook webhook body per messaging event.

    Facebook may batch multiple entries/messages in one webhook request. The
    processing pipeline intentionally handles one event at a time, so this
    prevents silently dropping entry[1] or messaging[1:].
    """
    entries = body.get("entry") if isinstance(body, dict) else None
    if not isinstance(entries, list):
        return [body]

    result = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        messaging = entry.get("messaging")
        if not isinstance(messaging, list) or not messaging:
            continue
        for event in messaging:
            single_entry = dict(entry)
            single_entry["messaging"] = [event]
            single_body = dict(body)
            single_body["entry"] = [single_entry]
            result.append(single_body)

    return result or [body]


def acquire_sender_lock(db, sender_id, wait_seconds=90, stale_seconds=180):
    """منع معالجة أكثر من رسالة لنفس الزبون بنفس الوقت بدون لمس SQLite."""
    with _sender_locks_guard:
        lock = _sender_locks.get(sender_id)
        if lock is None:
            lock = threading.Lock()
            _sender_locks[sender_id] = lock

    if lock.locked():
        print(f"[Lock] Waiting for sender lock: {sender_id}", flush=True)

    acquired = lock.acquire(timeout=wait_seconds)
    if acquired:
        print(f"[Lock] Acquired sender lock: {sender_id}", flush=True)
    else:
        print(f"[Lock] Timeout waiting for sender lock: {sender_id}", flush=True)
    return acquired


def release_sender_lock(db, sender_id):
    if not sender_id:
        return
    with _sender_locks_guard:
        lock = _sender_locks.get(sender_id)
    if lock and lock.locked():
        lock.release()
        print(f"[Lock] Released sender lock: {sender_id}", flush=True)


# ── Customer ──────────────────────────────────────────────────────────────────

def get_or_create_customer(db, sender_id, page_id, platform=None):
    platform = (platform or "").strip().lower()
    now = now_baghdad_iso()
    row = db.execute(
        "SELECT * FROM customers WHERE sender_id=?", (sender_id,)
    ).fetchone()

    if row is None:
        platform = platform or "facebook"
        db.execute(
            "INSERT INTO customers (sender_id, page_id, platform, first_seen_at, last_seen_at) VALUES (?,?,?,?,?)",
            (sender_id, page_id, platform, now, now),
        )
        db.commit()
        print(f"[Customer] New: {sender_id}", flush=True)
        row = db.execute("SELECT * FROM customers WHERE sender_id=?", (sender_id,)).fetchone()
    else:
        current = dict(row)
        platform = platform or current.get("platform") or "facebook"
        db.execute(
            "UPDATE customers SET last_seen_at=?, page_id=COALESCE(?, page_id), platform=? WHERE sender_id=?",
            (now, page_id or None, platform, sender_id),
        )
        db.commit()
        print(f"[Customer] Returning: {sender_id}", flush=True)

    return dict(row)


def remember_customer_product(
    db, sender_id, product, match_method, confidence=0,
    source=None, status="active", notes=None,
):
    """حفظ كل موديل تعرّف عليه النظام لهذا الزبون كقائمة اهتمامات."""
    if not product:
        return
    now = now_baghdad_iso()
    binding_source = source or match_method or "unknown"
    db.execute(
        """INSERT INTO customer_product_interests
           (sender_id, product_id, product_name, match_method, confidence, last_seen_at, source, status, notes)
           VALUES (?,?,?,?,?,?,?,?,?)
           ON CONFLICT(sender_id, product_id) DO UPDATE SET
             product_name=excluded.product_name,
             match_method=excluded.match_method,
             confidence=excluded.confidence,
             last_seen_at=excluded.last_seen_at,
             source=excluded.source,
             status=excluded.status,
             rejected_at=NULL,
             notes=COALESCE(excluded.notes, notes)""",
        (
            sender_id,
            product.get("product_id", ""),
            product.get("product_name", ""),
            match_method or "",
            confidence or 0,
            now,
            binding_source,
            status or "active",
            notes,
        ),
    )
    db.commit()
    print(
        f"[CustomerProduct] Remembered {product.get('product_id')} for {sender_id}",
        flush=True,
    )


def complete_customer_product_link(db, sender_id, product, match_method, confidence=100, source=""):
    """Persist a product match and clear pending human review for this customer."""
    if not sender_id or not product or not product.get("product_id"):
        return {"linked": False, "closed_reviews": 0, "reason": "missing_sender_or_product"}
    now = now_baghdad_iso()
    product_id = product.get("product_id")
    existing = db.execute(
        """SELECT match_method, source FROM customer_product_interests
           WHERE sender_id=? AND product_id=?""",
        (sender_id, product_id),
    ).fetchone()
    existing_method = existing["match_method"] if existing else ""
    existing_source = existing["source"] if existing and "source" in existing.keys() else ""
    weak_methods = {"auto_product_link", "matched_product_context", ""}
    effective_method = match_method or existing_method or ""
    if existing_method and effective_method in weak_methods:
        effective_method = existing_method
    valid_sources = {"auto_default_product", "image_recognition", "ad_ref", "manual_admin", "unknown"}
    requested_source = source if source in valid_sources else ""
    effective_source = requested_source or existing_source or effective_method or "unknown"
    if effective_source not in valid_sources:
        if effective_method in {"ref", "ad_id"}:
            effective_source = "ad_ref"
        elif effective_method in {"manual", "telegram_human"}:
            effective_source = "manual_admin"
        elif effective_method in {"image_recognition", "openrouter_catalog", "vision_product_id"}:
            effective_source = "image_recognition"
        elif effective_method == "auto_default_product":
            effective_source = "auto_default_product"
        else:
            effective_source = "unknown"
    remember_customer_product(
        db, sender_id, product, effective_method,
        confidence=confidence, source=effective_source, status="active",
    )
    cur = db.execute(
        "UPDATE human_reviews SET status='linked', replied_at=? "
        "WHERE sender_id=? AND status='pending'",
        (now, sender_id),
    )
    closed = cur.rowcount if cur.rowcount is not None else 0
    db.commit()
    set_customer_ai_enabled(db, sender_id, True)
    image_flow(
        "product_link_completed",
        sender_id=sender_id,
        product_id=product_id,
        match_method=effective_method,
        confidence=confidence,
        closed_pending_reviews=closed,
        source=source,
    )
    return {"linked": True, "closed_reviews": closed}


def load_customer_products(db, sender_id, limit=5):
    rows = db.execute(
        """SELECT product_id, product_name, match_method, confidence, last_seen_at,
                  source, status, rejected_at, notes, image_sent
           FROM customer_product_interests
           WHERE sender_id=?
             AND COALESCE(status, 'active')='active'
           ORDER BY last_seen_at DESC, id DESC
           LIMIT ?""",
        (sender_id, limit),
    ).fetchall()
    products_by_id = {
        p.get("product_id"): p
        for p in load_products_from_file()
        if p.get("product_id")
    }
    result = []
    for row in rows:
        memory = dict(row)
        product = dict(products_by_id.get(memory.get("product_id"), {}))
        product.update({
            "product_id": memory.get("product_id"),
            "product_name": product.get("product_name") or memory.get("product_name"),
            "match_method": memory.get("match_method"),
            "confidence": memory.get("confidence"),
            "last_seen_at": memory.get("last_seen_at"),
            "source": memory.get("source") or memory.get("match_method") or "unknown",
            "status": memory.get("status") or "active",
            "rejected_at": memory.get("rejected_at"),
            "binding_notes": memory.get("notes"),
            "image_sent": memory.get("image_sent") or 0,
        })
        result.append(product)
    return result


def get_active_product_binding(db, sender_id):
    row = db.execute(
        """SELECT *
           FROM customer_product_interests
           WHERE sender_id=? AND COALESCE(status, 'active')='active'
           ORDER BY last_seen_at DESC, id DESC
           LIMIT 1""",
        (sender_id,),
    ).fetchone()
    return dict(row) if row else None


def bind_customer_to_product(
    db, sender_id, product, source="unknown", confidence=100, notes=None,
    match_method=None,
):
    if not product or not product.get("product_id"):
        return None
    method = match_method or source or "unknown"
    remember_customer_product(
        db, sender_id, product, method,
        confidence=confidence, source=source or method,
        status="active", notes=notes,
    )
    return get_active_product_binding(db, sender_id)


def reject_current_binding(db, sender_id, reason=""):
    binding = get_active_product_binding(db, sender_id)
    if not binding or (binding.get("source") or binding.get("match_method")) != "auto_default_product":
        return False
    now = now_baghdad_iso()
    note = str(reason or "").strip()
    db.execute(
        """UPDATE customer_product_interests
           SET status='rejected', rejected_at=?, notes=?
           WHERE id=?""",
        (now, note, binding["id"]),
    )
    db.commit()
    print(f"[AutoProduct] Rejected auto binding for {sender_id}: {binding.get('product_id')}", flush=True)
    return True


_PRODUCT_OBJECTION_KEYWORDS = (
    "لا مو هذا",
    "مو هذا",
    "مو هاذا",
    "مو نفس",
    "هذا غير",
    "هاذا غير",
    "اريد غيره",
    "أريد غيره",
    "اقصد هذا",
    "أقصد هذا",
    "عندي صورة",
    "اريد مثل الصورة",
    "أريد مثل الصورة",
    "ارسلت صورة",
    "أرسلت صورة",
    "مو المطلوب",
    "المنتج غير",
)


def is_product_objection(text):
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    if normalized in {"لا", "لاا", "كلا", "no"}:
        return True
    return any(keyword in normalized for keyword in _PRODUCT_OBJECTION_KEYWORDS)


def should_use_auto_product(db, sender_id, ev, message_type, customer_products):
    if message_type == "image" or ev.get("image_url"):
        return False
    if customer_products or get_active_product_binding(db, sender_id):
        return False
    if ev.get("ref") or ev.get("ad_id"):
        return False
    text = (ev.get("text") or "").strip()
    if not text or is_product_objection(text):
        return False
    return bool(get_auto_product_settings(db).get("enabled"))


def _is_contextual_product_question(text):
    """هل الرسالة تشير إلى موديل سبق ذكره بدون تسمية المنتج صراحة؟"""
    if not text:
        return False
    normalized = text.strip().lower()
    words = set(re.findall(r"[A-Za-z0-9_\u0621-\u064A\u0660-\u0669]+", normalized))
    contextual_words = {
        "هذا", "هذه", "هاذا", "هاي", "هذي", "هذاك", "الموديل",
        "موديل", "القطعة", "الطقم", "بيها", "بيها", "موجود",
        "موجوده", "متوفر", "متوفرة", "خلص", "خالص", "السعر",
        "سعره", "سعرها", "سعر", "بكم", "شكد", "كدش", "كم",
        "اريد", "أريد", "اخذ", "آخذ",
    }
    return bool(words & contextual_words)


def _requires_linked_product_for_details(text):
    """أسئلة السعر/التوفر/الحجز لا يجوز جوابها بتفاصيل منتج قبل ربط منتج واضح."""
    if not text:
        return False
    words = set(re.findall(r"[A-Za-z0-9_\u0621-\u064A\u0660-\u0669]+", text.strip().lower()))
    detail_words = {
        "سعر", "السعر", "سعره", "سعرها", "بكم", "شكد", "كم",
        "متوفر", "متوفرة", "موجود", "موجوده", "موجودة",
        "قياس", "مقاس", "مقاسات", "سايز", "لون", "الوان", "ألوان",
        "احجز", "حجز", "اريد", "أريد", "اطلب", "طلب", "اخذ", "آخذ",
        "هذا", "هذه", "هاي", "هذي", "الموديل", "موديل", "المنتج",
    }
    return bool(words & detail_words)


def _has_demonstrative_reference(text):
    if not text:
        return False
    words = set(re.findall(r"[A-Za-z0-9_\u0621-\u064A\u0660-\u0669]+", text.strip().lower()))
    return bool(words & {"هذا", "هذه", "هاذا", "هاي", "هذي", "هذاك"})


def previous_incoming_message_type(db, sender_id):
    rows = db.execute(
        """SELECT message_type FROM messages
           WHERE sender_id=? AND direction='incoming'
           ORDER BY id DESC LIMIT 2""",
        (sender_id,),
    ).fetchall()
    if len(rows) < 2:
        return None
    return rows[1]["message_type"]


def clear_customer_product_memory(db, sender_id, reason=""):
    db.execute("DELETE FROM customer_product_interests WHERE sender_id=?", (sender_id,))
    db.commit()
    print(f"[CustomerProduct] Cleared memory for {sender_id}: {reason}", flush=True)


def has_pending_image_review(db, sender_id):
    row = db.execute(
        """SELECT id FROM human_reviews
           WHERE sender_id=? AND status='pending' AND image_url IS NOT NULL AND image_url != ''
           ORDER BY id DESC LIMIT 1""",
        (sender_id,),
    ).fetchone()
    return row["id"] if row else None


def has_any_customer_image(db, sender_id):
    row = db.execute(
        """SELECT id FROM messages
           WHERE sender_id=? AND direction='incoming'
             AND (
               message_type='image'
               OR (image_url IS NOT NULL AND image_url != '')
               OR raw_payload LIKE '%"type": "image"%'
               OR raw_payload LIKE '%"image_url"%'
               OR raw_payload LIKE '%"last_input_image%'
               OR raw_payload LIKE '%"attachment_url"%'
               OR raw_payload LIKE '%"photo_url"%'
               OR raw_payload LIKE '%"picture_url"%'
             )
           ORDER BY id DESC LIMIT 1""",
        (sender_id,),
    ).fetchone()
    return row is not None


def incoming_message_count(db, sender_id):
    row = db.execute(
        "SELECT COUNT(*) AS count FROM messages WHERE sender_id=? AND direction='incoming'",
        (sender_id,),
    ).fetchone()
    return int(row["count"] or 0) if row else 0


def has_sent_pending_image_reply(db, sender_id, review_id):
    row = db.execute(
        """SELECT id FROM messages
           WHERE sender_id=? AND direction='outgoing'
             AND raw_payload LIKE ?
             AND raw_payload LIKE ?
           ORDER BY id DESC LIMIT 1""",
        (
            sender_id,
            f'%"human_review_id": {int(review_id)}%',
            '%"pending_image_%',
        ),
    ).fetchone()
    return row is not None


def has_pending_human_review(db, sender_id):
    row = db.execute(
        """SELECT id FROM human_reviews
           WHERE sender_id=? AND status='pending'
           ORDER BY id DESC LIMIT 1""",
        (sender_id,),
    ).fetchone()
    return row["id"] if row else None


def _is_out_of_stock(product):
    return _stock_state(product) == "out"


def _stock_state(product):
    """إرجاع حالة المخزون بوضوح: available/out/unknown."""
    stock = str((product or {}).get("stock") or "").strip().lower()
    status = str((product or {}).get("status") or "").strip().lower()
    if status in ("inactive", "disabled"):
        return "out"
    if any(w in stock for w in ("نفذ", "خلص", "غير متوفر", "not available", "out")):
        return "out"
    if "متوفر" in stock or "available" in stock:
        return "available"
    return "unknown"


def local_reply_validation(reply, matched_product, customer_products):
    """قواعد محلية سريعة قبل/بعد Checker حتى لا يمر رد يخالف بيانات المنتج."""
    reply_text = (reply or "").strip().lower()
    if not reply_text:
        return {
            "approved": False,
            "problem": "الرد فارغ.",
            "fix_instruction": "اكتب رداً قصيراً ومفيداً للزبونة بناءً على المنتج المطابق.",
        }

    stock_state = _stock_state(matched_product) if matched_product else "unknown"

    if matched_product and stock_state == "out":
        available_words = (
            "متوفر", "موجود", "عدنا", "اكدر احجز", "أكدر أحجز",
            "تقدرين تطلبين", "تگدرين تطلبين", "اطلبيه",
        )  
        negative_phrases = ("غير متوفر", "ما متوفر", "مو متوفر", "ما موجود", "مو موجود", "خلص", "نفذ")
        has_negative = any(phrase in reply_text for phrase in negative_phrases)
        if not has_negative and any(word.lower() in reply_text for word in available_words):
            return {
                "approved": False,
                "problem": "الرد يقول أو يلمّح أن المنتج متوفر بينما stock=نفذ.",
                "fix_instruction": (
                    "المنتج المطابق غير متوفر/خلص حالياً. أعد صياغة الرد باللهجة العراقية، "
                    "اذكر أنه خلص حالياً بلطف، ولا تعرض شراءه كأنه متوفر."
                ),
            }

    if matched_product and stock_state == "available":
        sold_out_words = ("خلص", "نفذ", "غير متوفر", "ما متوفر")
        if any(word in reply_text for word in sold_out_words):
            return {
                "approved": False,
                "problem": "الرد يقول إن المنتج غير متوفر بينما stock يقول متوفر.",
                "fix_instruction": (
                    "المنتج المطابق متوفر. أعد الرد وأخبر الزبونة بتوفره مع السعر والتفاصيل الموجودة فقط."
                ),
            }

    return {"approved": True, "problem": "", "fix_instruction": ""}


def _text_has_any(text, terms):
    text = (text or "").strip().lower()
    return any(term in text for term in terms)


PENDING_IMAGE_REPLY = (
    "هلا حبيبتي 🌸 وصلتني الصورة، حتى أتأكدلج من الموديل والتوفر. "
    "شنو القياس أو اللون المطلوب؟"
)

FIRST_MESSAGE_REPLY = (
    "يا هلا بيج عيني 🌸 دزيلي صورة الموديل أو القياس حتى أتأكدلج."
)


def _detect_child_gender(text: str) -> str:
    """حقل قديم للتوافق؛ متجر أنيقة لا يستخدم تصنيف أطفال هنا."""
    return "unknown"


def _customer_name_from_db(db, sender_id: str) -> str:
    """يجلب اسم الزبون المخزّن من قاعدة البيانات إن وُجد."""
    try:
        row = db.execute(
            "SELECT name FROM customers WHERE sender_id=?",
            (sender_id,),
        ).fetchone()
        return (row["name"] or "").strip() if row else ""
    except Exception:
        return ""


def _customer_gender_from_db(db, sender_id: str) -> str:
    """يجلب جنس الزبون المخزّن من قاعدة البيانات إن وُجد."""
    try:
        row = db.execute(
            "SELECT gender FROM customers WHERE sender_id=?",
            (sender_id,),
        ).fetchone()
        return ((row["gender"] or "").strip().lower()) if row else ""
    except Exception:
        return ""


def generate_first_message_reply(db, ev, products, instructions_text, rules_list):
    """
    صياغة رد ديناميكي على أول رسالة من الزبون:
    - يتفاعل مع ما قاله الزبون.
    - الموديل يقرر صيغة الترحيب المناسبة ولا يُلزَم بصيغة محددة.
    - يسأل عن العمر/القياس وسياق الموديل (ولد/بنت) إذا لم يذكر.
    - لا يلتزم بمنتج معين (لأن المنتج سيُربط من الإدارة).
    """
    customer_text = (ev.get("text") or "").strip()
    detected_child = _detect_child_gender(customer_text)
    customer_name = _customer_name_from_db(db, ev["sender_id"])
    customer_gender = _customer_gender_from_db(db, ev["sender_id"])  # 'male' | 'female' | ''

    if not OPENROUTER_KEY:
        return FIRST_MESSAGE_REPLY, detected_child

    rules_text = "\n".join(f"- {r}" for r in rules_list) if rules_list else "- لا توجد قواعد محظورة."

    name_context = (
        f"اسم الزبون من المنصة (للسياق فقط، ممنوع ذكره في الرد): {customer_name}"
        if customer_name else
        "اسم الزبون غير متوفر."
    )

    if customer_gender == "male":
        gender_rule = (
            "جنس الزبون: ذكر. خاطبه بصيغة المذكر بأسلوب محترم ورسمي (أستاذ/تفضّل/تأمر). "
            "⚠️ ممنوع منعاً باتاً استخدام 'حبيبي' أو 'عيني' أو أي كلمة عاطفية. استبدلها دائماً بـ 'أستاذ'."
        )
    elif customer_gender == "female":
        gender_rule = (
            "جنس الزبون: أنثى. خاطبيها بصيغة المؤنث (حبيبتي/عيني/تأمرين/تدللين)."
        )
    else:
        gender_rule = (
            "جنس الزبون غير محدد. استخدم صياغة محايدة (تفضّل/أهلاً)؛ لا تستخدم 'حبيبي' للذكور ولا تفترض الجنس."
        )

    system_prompt = (
        "أنت موظفة مبيعات قصيرة الكلام، عملية، باللهجة العراقية الودودة، في متجر أنيقة للموديلات والقطع النسائية المحتشمة.\n"
        "هذا أول تواصل من الزبون. هدفك من هذا الرد: ترحيب خفيف + سؤال قصير يفتح المحادثة ويوصلنا للحجز.\n\n"
        f"{name_context}\n"
        f"{gender_rule}\n\n"
        "قواعد صارمة:\n"
        "1) صيغة الترحيب متروكة لك (مثلاً: هلا حبيبتي، يا هلا، أهلين، أستاذ). لا تستخدم قوالب طويلة.\n"
        "2) ⚠️ طول الرد إلزامي: من جملة إلى جملتين قصيرتين فقط (≤ 20 كلمة). ممنوع الإطالة.\n"
        "3) ممنوع منعاً باتاً ذكر اسم الزبون أو أي جزء منه في نص الرد.\n"
        "4) لا تذكر اسم منتج معين أو سعر أو قياس (المنتج لم يُحدد بعد).\n"
        "5) لو الزبون سأل عن شيء، تفاعل معه بإيجاز ودون ادعاء.\n"
        "6) اسأل فقط عن صورة الموديل أو القياس أو اطلب الصورة. ممنوع منعاً باتاً السؤال عن سياق الموديل.\n"
        "7) إذا الزبون ذكر سياق الموديل من نفسه، استخدمه طبيعياً.\n"
        "8) ⚠️ ممنوع الأدعية أو المجاملات الزائدة (تسلمين، فدوة لعمرج، يرزقج، تدللين بأي وقت...). كلمة ودّ واحدة خفيفة فقط (تأمرين/من عيوني/تفضّل) ضمن نفس الجملة.\n"
        "9) اختم بسؤال قصير واحد يحفّز الزبون للرد (مثلاً: شنو القياس؟ أو دزّيلي صورة الموديل).\n\n"
        f"سياق الموديل المستنتج من رسالة الزبون: {detected_child} (لا تسأل عنه، فقط استخدمه في الصياغة لو كان معروفاً).\n\n"
        "تعليمات الإدارة:\n"
        f"{instructions_text or 'لا توجد تعليمات إضافية.'}\n\n"
        "القواعد المحظورة:\n"
        f"{rules_text}\n\n"
        "أخرج JSON فقط: {\"reply\":\"النص\", \"gender\":\"boy|girl|unknown\"}"
    )

    user_content = (
        "أول رسالة من الزبون في هذه المحادثة:\n"
        f"{customer_text or '[رسالة فارغة أو تحية فقط]'}\n\n"
        "صُغ ردك الترحيبي والاستفساري وفق القواعد أعلاه."
    )

    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": MAIN_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_content},
                ],
                "max_tokens": 250,
                "temperature": 0.4,
            },
            timeout=20,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        parsed = _parse_ai_json(raw) if isinstance(raw, str) else (raw or {})
        reply = (parsed.get("reply") or "").strip()
        gender = (parsed.get("gender") or detected_child or "unknown").strip().lower()
        if gender not in ("boy", "girl", "unknown"):
            gender = detected_child
        if not reply:
            return FIRST_MESSAGE_REPLY, detected_child
        print(f"[FirstMsgAI] name={customer_name!r} child={gender} | reply={reply[:80]}", flush=True)
        return reply, gender
    except Exception as exc:
        print(f"[FirstMsgAI] Error: {exc} → falling back to default greeting.", flush=True)
        return FIRST_MESSAGE_REPLY, detected_child


def build_safe_fallback_reply(matched_product, customer_text=""):
    """رد احتياطي واعي بحالة المنتج بدل سؤال عام ينسى الموديل."""
    if matched_product:
        name = matched_product.get("product_name") or "هذا الموديل"
        price = matched_product.get("price") or ""
        sizes = matched_product.get("sizes") or ""
        colors = matched_product.get("colors") or ""
        notes = matched_product.get("notes") or matched_product.get("description") or ""
        stock_state = _stock_state(matched_product)
        if stock_state == "out":
            return f"حبيبتي {name} خلص حالياً للأسف 🌸 إذا تحبين أگدر أقترحلج موديل مشابه متوفر."
        if stock_state == "unknown":
            return f"حبيبتي هذا هو {name}، بس حالة التوفر مو واضحة عندي حالياً. أراجعها إلج وأرجعلج 🌸"

        if _text_has_any(customer_text, ("قياس", "مقاس", "سايز", "عمر", "سنة", "سنوات", "يلبس")):
            size_part = f"قياساته {sizes}" if sizes else "القياسات مو محددة عندي حالياً"
            return f"حبيبتي {name} متوفر، {size_part} 🌸"

        if _text_has_any(customer_text, ("لون", "الوان", "ألوان")):
            color_part = f"ألوانه {colors}" if colors else "ألوانه مثل الصورة"
            return f"إي عيني {name} متوفر، {color_part} 🌸"

        if _text_has_any(customer_text, ("توصيل", "محافظة", "بغداد", "شحن")):
            return f"إي حبيبتي التوصيل متوفر لكل المحافظات 🌸 {FIXED_DELIVERY_TEXT}."

        if _text_has_any(customer_text, ("سعر", "السعر", "شكد", "كم", "بكم")):
            price_part = f"سعره {price}" if price else "سعره مو محدد حالياً"
            return f"{price_part} حبيبتي 🌸 {FIXED_DELIVERY_TEXT}."

        if _text_has_any(customer_text, ("خام", "نوعية", "جودة", "فحص", "يرجع", "ارجاع", "ثقة")):
            notes_part = f" {notes}" if notes else ""
            return (
                f"حبيبتي {name} متوفر.{notes_part} "
                "وعدنا شرط الفحص عند الاستلام بوجود المندوب، وإذا ما طابق يرجع مجاناً 🌸"
            )

        if _text_has_any(customer_text, ("احجز", "حجز", "اريد", "أريد", "اخذ", "آخذ", "اطلب", "طلب")):
            return f"تدللين حبيبتي، {name} متوفر 🌸 دزيلي رقم الموبايل والمحافظة والعنوان الكامل حتى أثبتلج الطلب."

        price_part = f" وسعره {price}" if price else ""
        size_part = f" والقياسات {sizes}" if sizes else ""
        return f"إي حبيبتي {name} متوفر حالياً{price_part}{size_part} 🌸 شنو تحبين تعرفين عنه؟"
    return FALLBACK_REPLY


def save_booking_to_file(booking_data):
    """حفظ الحجز في ملف JSONL مستقل لسهولة المراجعة خارج قاعدة البيانات."""
    with open(BOOKINGS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(booking_data, ensure_ascii=False) + "\n")
    print(f"[BookingFile] Saved booking to {BOOKINGS_FILE}", flush=True)


def send_telegram_message(text, chat_id=None, label="notification"):
    target_chat_id = chat_id or TELEGRAM_CHAT_ID
    if label == "notification" and TELEGRAM_NOTIFICATION_HEADER:
        header = TELEGRAM_NOTIFICATION_HEADER
        if not str(text or "").startswith(header):
            text = f"{header}\n\n{text}"
    if not TELEGRAM_BOT_TOKEN or not target_chat_id:
        print(f"[Telegram] TELEGRAM_BOT_TOKEN/{label} chat_id not configured.", flush=True)
        print(f"[Telegram] Message would be:\n{text}", flush=True)
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": target_chat_id,
                "text": text,
                "disable_web_page_preview": False,
            },
            timeout=15,
        )
        if not resp.ok:
            print(f"[Telegram] Error {resp.status_code}: {resp.text}", flush=True)
            return False
        print(f"[Telegram] Sent {label} message.", flush=True)
        return True
    except Exception as exc:
        print(f"[Telegram] Send error: {exc}", flush=True)
        return False


ORDER_CONFIRMATION_TEXT = (
    "تم تثبيت الطلب 📍📍\n"
    "يرجى فحص الطلب بحضور المندوب والتأكد من الموديل والقياس "
    "اي اشكال ترجع مع المندوب بدون متدفع ولا فلس 📍📍\n"
    "اهم شي تفحصين الطلب قبل دفع المبلغ\n"
    "التوصيل خلال يومين من تاريخ الحجز 🔥💫"
)


def format_order_for_telegram(order):
    store_name = TELEGRAM_NOTIFICATION_HEADER or "أنيقة"
    return (
        f"🧾 طلب جديد - {store_name}\n"
        f"الوقت: {order.get('created_at') or '-'}\n"
        f"Sender: {order.get('sender_id') or '-'}\n"
        f"الاسم: {order.get('customer_name') or '-'}\n"
        f"الهاتف: {order.get('phone') or '-'}\n"
        f"المحافظة: {order.get('province') or '-'}\n"
        f"العنوان: {order.get('address') or '-'}\n"
        f"المنتج: {order.get('product_name') or '-'}\n"
        f"Product ID: {order.get('product_id') or '-'}\n"
        f"اللون: {order.get('color') or '-'}\n"
        f"القياس: {order.get('size') or '-'}\n"
        f"ملاحظات: {order.get('notes') or '-'}\n"
        f"الحالة: {order.get('status') or 'new'}"
    )


def send_order_to_telegram(order):
    chat_id = TELEGRAM_ORDERS_CHAT_ID or TELEGRAM_CHAT_ID
    sent = send_telegram_message(
        format_order_for_telegram(order),
        chat_id=chat_id,
        label="order",
    )
    try:
        product = find_product_by_id(order.get("product_id")) if order.get("product_id") else None
        image_urls = product_image_urls(product) if product else []
        if image_urls:
            caption = (
                f"صور موديل الطلب - {TELEGRAM_NOTIFICATION_HEADER or 'أنيقة'}\n"
                f"المنتج: {order.get('product_name') or order.get('product_id') or '-'}\n"
                f"اللون: {order.get('color') or (product or {}).get('colors') or '-'}\n"
                f"القياس: {order.get('size') or '-'}"
            )
            for image_url in image_urls:
                send_telegram_photo(image_url, caption=caption, chat_id=chat_id, label="order_photo")
    except Exception as exc:
        print(f"[Telegram] Could not send order product images: {exc}", flush=True)
    return sent


def create_problem_report(db, ev, reason, matched_product=None):
    customer_name = None
    try:
        row = db.execute("SELECT name FROM customers WHERE sender_id=?", (ev.get("sender_id"),)).fetchone()
        customer_name = row["name"] if row else None
    except Exception:
        customer_name = None

    product_id = None
    product_name = None
    if matched_product:
        product_id = matched_product.get("product_id")
        product_name = matched_product.get("product_name")

    now = now_baghdad_iso()
    existing = db.execute(
        """SELECT id FROM problem_reports
           WHERE sender_id=? AND status='open' AND message_text=? AND reason=?
           ORDER BY id DESC LIMIT 1""",
        (ev.get("sender_id"), ev.get("text") or "", reason),
    ).fetchone()
    if existing:
        return existing["id"], False

    db.execute(
        "INSERT INTO problem_reports (sender_id, customer_name, message_text, reason, product_id, product_name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?)",
        (
            ev.get("sender_id"),
            customer_name,
            ev.get("text") or "",
            reason,
            product_id,
            product_name,
            now,
            now,
        ),
    )
    db.commit()
    return db.execute("SELECT last_insert_rowid()").fetchone()[0], True


def send_problem_to_telegram(problem):
    chat_id = TELEGRAM_ORDERS_CHAT_ID or TELEGRAM_PROBLEMS_CHAT_ID or TELEGRAM_CHAT_ID
    if not chat_id:
        print("[Telegram] No problems chat configured. Problem message skipped.", flush=True)
        return False

    text = (
        "🚨 مشكلة جديدة\n"
        f"الزبون: {problem.get('customer_name') or problem.get('sender_id') or '-'}\n"
        f"Sender ID: {problem.get('sender_id') or '-'}\n"
        f"سبب المشكلة: {problem.get('reason') or '-'}\n"
        f"الرسالة: {problem.get('message_text') or '-'}\n"
    )
    if problem.get('product_name') or problem.get('product_id'):
        text += f"\nالمنتج: {problem.get('product_name') or problem.get('product_id')}"

    return send_telegram_message(text, chat_id=chat_id, label="problem")


def get_problem_reports(db, status=None, limit=200):
    query = "SELECT * FROM problem_reports"
    params = []
    if status:
        query += " WHERE status=?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def _norm_order_value(value):
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def find_duplicate_order(db, sender_id, phone="", product_id="", address="", minutes=120):
    """Return a recent matching order so repeated submits/webhooks do not create duplicates."""
    sender_id = str(sender_id or "").strip()
    if not sender_id:
        return None
    cutoff = datetime.fromtimestamp(time.time() - (minutes * 60), BAGHDAD_TZ).replace(microsecond=0).isoformat()
    rows = db.execute(
        """SELECT * FROM orders
           WHERE sender_id=? AND created_at >= ?
           ORDER BY id DESC LIMIT 30""",
        (sender_id, cutoff),
    ).fetchall()
    wanted_phone = _norm_order_value(phone)
    wanted_product = _norm_order_value(product_id)
    wanted_address = _norm_order_value(address)
    for row in rows:
        row_phone = _norm_order_value(row["phone"])
        row_product = _norm_order_value(row["product_id"])
        row_address = _norm_order_value(row["address"])
        phone_match = wanted_phone and row_phone == wanted_phone
        product_match = wanted_product and row_product == wanted_product
        address_match = wanted_address and row_address == wanted_address
        if phone_match and (product_match or address_match):
            return dict(row)
    return None


def send_telegram_photo(photo_url, caption="", chat_id=None, label="notification_photo"):
    if not photo_url:
        return False
    target_chat_id = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target_chat_id:
        print(f"[Telegram] TELEGRAM_BOT_TOKEN/{label} chat_id not configured.", flush=True)
        print(f"[Telegram] Photo would be: {photo_url}\nCaption: {caption}", flush=True)
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
            json={
                "chat_id": target_chat_id,
                "photo": photo_url,
                "caption": caption[:1024],
            },
            timeout=20,
        )
        if not resp.ok:
            print(f"[TelegramPhoto] Error {resp.status_code}: {resp.text}", flush=True)
            return False
        print(f"[TelegramPhoto] Sent {label}.", flush=True)
        return True
    except Exception as exc:
        print(f"[TelegramPhoto] Send error: {exc}", flush=True)
        return False


def create_human_review(db, ev, reason, candidates=None):
    now = now_baghdad_iso()
    candidates = candidates or []
    db.execute(
        """INSERT INTO human_reviews
           (sender_id, message_text, image_url, candidates_json, reason, status, created_at)
           VALUES (?,?,?,?,?,'pending',?)""",
        (
            ev.get("sender_id"),
            ev.get("text", ""),
            ev.get("image_url", ""),
            json.dumps(candidates, ensure_ascii=False),
            reason,
            now,
        ),
    )
    db.commit()
    review_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    candidate_lines = []
    for c in candidates[:10]:
        candidate_lines.append(
            f"- {c.get('product_id')} | {c.get('product_name')} | score={c.get('score', 0)} | stock={c.get('stock')}\n"
            f"  image: {c.get('image_url') or '-'}"
        )

    msg = (
        "⚠️ مراجعة بشرية مطلوبة\n"
        f"Review ID: {review_id}\n"
        f"Sender: {ev.get('sender_id')}\n"
        f"Reason: {reason}\n"
        f"Text: {ev.get('text') or '-'}\n"
        f"Image: {ev.get('image_url') or '-'}\n\n"
        "Candidates:\n"
        + ("\n".join(candidate_lines) if candidate_lines else "لا توجد مرشحات")
        + "\n\nإذا عرفت المنتج اكتب:\n"
        f"/product {review_id} P001\n"
        f"أو إذا غير موجود:\n/product {review_id} NONE\n\n"
        "للرد اليدوي المباشر اكتب:\n"
        f"/reply {review_id} نص الرد"
    )
    send_telegram_message(msg)

    if ev.get("image_url"):
        send_telegram_photo(
            ev.get("image_url"),
            f"صورة الزبون للمراجعة #{review_id}\nSender: {ev.get('sender_id')}",
        )

    for c in candidates[:10]:
        send_telegram_photo(
            c.get("image_url"),
            (
                f"Candidate for review #{review_id}\n"
                f"ID: {c.get('product_id')}\n"
                f"Name: {c.get('product_name')}\n"
                f"Stock: {c.get('stock')}\n"
                f"Score: {c.get('score', 0)}"
            ),
        )
    return review_id


def handle_human_product_selection(db, review_id, product_id):
    row = db.execute("SELECT * FROM human_reviews WHERE id=?", (review_id,)).fetchone()
    if not row:
        return {"ok": False, "error": "review not found"}

    product_id = (product_id or "").strip()
    if product_id.upper() in ("NONE", "NO", "NO_MATCH", "غير_موجود"):
        reply = "No matching product selected by human reviewer."
        status = "no_product"
        selected_product = None
    else:
        selected_product = find_product_by_id(product_id)
        if not selected_product:
            return {"ok": False, "error": f"product {product_id} not found"}
        remember_customer_product(
            db,
            row["sender_id"],
            selected_product,
            "telegram_human",
            100,
            source="manual_admin",
        )
        reply = f"Linked image review to product {selected_product.get('product_id')}"
        status = "product_selected"

    now = now_baghdad_iso()
    db.execute(
        "UPDATE human_reviews SET status=?, admin_reply=?, replied_at=? WHERE id=?",
        (status, reply, now, review_id),
    )
    db.commit()
    return {
        "ok": True,
        "review_id": review_id,
        "sender_id": row["sender_id"],
        "product_id": product_id,
        "sent_to_customer": False,
        "reply": "",
        "note": "Product linked only. No automatic customer reply was sent.",
    }


def send_human_reply_to_customer(sender_id, reply):
    """إرسال رد الإدارة للزبون مباشرة عبر Facebook Messenger."""
    page_id, platform = get_customer_send_context(sender_id)
    sent = send_text_to_facebook(sender_id, reply, page_id, platform)
    print(f"[HumanReply] Direct Facebook send={sent}", flush=True)
    return sent


def get_customer_send_context(sender_id):
    try:
        db = get_db()
        row = db.execute(
            "SELECT page_id, COALESCE(platform, 'facebook') AS platform FROM customers WHERE sender_id=?",
            (sender_id,),
        ).fetchone()
        if row:
            return row["page_id"] or "", row["platform"] or "facebook"
    except Exception as exc:
        print(f"[SendContext] Could not load platform for {sender_id}: {exc}", flush=True)
    return "", "facebook"


def _is_product_info_request(text):
    if not text:
        return False
    words = set(re.findall(r"[\w\u0600-\u06FF]+", text.strip().lower()))
    image_keywords = {
        # صورة variants
        "صورة", "صوره", "صور", "الصورة", "الصوره", "الصور", "صورتها", "صورته",
        # send variants
        "ارسلي", "ارسل", "ارسلها", "ترسلين", "ترسل", "ترسلي",
        "ارسلو", "ابعث", "ابعثي", "ابعثيها",
        # info
        "تفاصيل", "معلومات", "وصف", "شوف", "شوفيها",
        "موديل", "المنتج", "عنه", "عنها",
        # can you
        "تكدر", "تقدر", "ممكن",
    }
    return bool(words & image_keywords)


def build_product_info_reply(product):
    name = product.get("product_name") or "الموديل"
    price = product.get("price") or "غير محدد"
    sizes = product.get("sizes") or "غير محدد"
    available_text = "متوفر" if _stock_state(product) == "available" else "غير متوفر حالياً"
    # Image is sent separately via attachments/product_image_url — do NOT embed URL in text
    reply = (
        f"تدللين عيني 🌸\n"
        f"هذا {name}\n"
        f"الحالة: {available_text}\n"
        f"السعر: {price}\n"
        f"المقاسات: {sizes}\n"
        f"{FIXED_DELIVERY_TEXT}"
    )
    return reply


def product_image_urls(product):
    raw = (product or {}).get("image_url") or []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        raw = []

    urls = []
    seen = set()
    for value in raw:
        raw_url = str(value or "").strip()
        parsed = urlparse(raw_url)
        path = parsed.path if parsed.scheme and parsed.netloc else raw_url
        if "/product_image/" in path.replace("\\", "/"):
            local_rel = path.replace("\\", "/").split("/product_image/", 1)[1]
            if not os.path.isfile(os.path.join(PRODUCT_IMAGE_DIR, local_rel)):
                print(f"[Catalog] Skipping missing product image: {path}", flush=True)
                continue
        url = build_public_image_url(raw_url)
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def build_product_image_payload(product):
    image_urls = product_image_urls(product)
    if not image_urls:
        return {
            "image_url": "",
            "product_image_url": "",
            "image_urls": [],
            "product_image_urls": [],
            "send_image": False,
            "attachments": [],
            "messages": [],
        }
    first_image = image_urls[0]
    return {
        "image_url": first_image,
        "product_image_url": first_image,
        "image_urls": image_urls,
        "product_image_urls": image_urls,
        "send_image": True,
        "attachments": [
            {
                "type": "image",
                "url": image_url,
            }
            for image_url in image_urls
        ],
        "messages": [
            {
                "type": "image",
                "url": image_url,
            }
            for image_url in image_urls
        ],
    }


def _should_send_image(db, sender_id: str, product: dict, ev: dict) -> bool:
    """إرسال صورة المنتج فقط عند طلبها صراحة أو عند دخول الزبون من إعلان المنتج."""
    if not product or not product.get("image_url"):
        return False

    # طلب صريح للصورة/التفاصيل
    binding = get_active_product_binding(db, sender_id)
    if (
        binding
        and binding.get("source") == "auto_default_product"
        and int(binding.get("image_sent") or 0) == 1
    ):
        return False

    if _is_product_info_request(ev.get("text", "")):
        return True

    # أول اتصال عبر ref/ad_id (إعلان)
    if ev.get("ref") or ev.get("ad_id"):
        return True

    return False


def attach_product_image_payload(final, product, reply):
    image_payload = build_product_image_payload(product)
    final.update({
        "image_url": image_payload["image_url"],
        "product_image_url": image_payload["product_image_url"],
        "image_urls": image_payload["image_urls"],
        "product_image_urls": image_payload["product_image_urls"],
        "send_image": image_payload["send_image"],
        "attachments": image_payload["attachments"],
        "messages": [{"type": "text", "text": reply}],
    })
    final["messages"].extend(image_payload["messages"])
    final["debug"]["product_image_url"] = image_payload["product_image_url"]
    final["debug"]["product_image_urls"] = image_payload["product_image_urls"]
    final["debug"]["send_image"] = image_payload["send_image"]
    return final


def send_product_image_if_available(db, sender_id, page_id, platform, product, binding=None):
    settings = get_auto_product_settings(db)
    if not settings.get("send_image") or not product:
        return False
    active_binding = binding or get_active_product_binding(db, sender_id)
    if active_binding and int(active_binding.get("image_sent") or 0) == 1:
        return False
    image_urls = product_image_urls(product)
    if not image_urls:
        return False

    sent_any = False
    for image_url in image_urls:
        sent = send_image_to_facebook(sender_id, image_url, page_id, platform)
        sent_any = sent_any or sent
        save_message(
            db, sender_id, "outgoing", "image",
            None, image_url, None, None,
            {"auto_default_product_image": True, "sent": sent},
        )

    if active_binding and sent_any:
        db.execute(
            "UPDATE customer_product_interests SET image_sent=1 WHERE id=?",
            (active_binding["id"],),
        )
        db.commit()
    return sent_any


def build_public_image_url(image_url):
    if not image_url:
        return ""

    parsed = urlparse(image_url)
    path = parsed.path if parsed.scheme and parsed.netloc else image_url
    if "/product_image/" not in path.replace("\\", "/"):
        return image_url

    # أولوية: PUBLIC_URL من .env (ثابت دائماً)
    if PUBLIC_URL:
        return f"{PUBLIC_URL}{path}"

    try:
        forwarded_host = request.headers.get("X-Forwarded-Host")
        forwarded_proto = request.headers.get("X-Forwarded-Proto") or request.scheme
        host = forwarded_host or request.host
        # تجاهل localhost/127.0.0.1 لأن فيسبوك لا يستطيع الوصول إليه
        if host and not host.startswith("127.") and not host.startswith("localhost"):
            return f"{forwarded_proto}://{host}{path}"
    except RuntimeError:
        pass

    return image_url


def build_catalog_reply(products):
    active = [
        p for p in products
        if _stock_state(p) == "available" and product_image_urls(p)
    ]
    if not active:
        return "حالياً ماكو منتجات متوفرة بالكتالوك 🌸", []

    lines = ["كتالوك المنتجات المتوفرة 🌸", FIXED_DELIVERY_TEXT]
    messages = []
    for product in active:
        name = product.get("product_name") or product.get("product_id") or "موديل"
        price = product.get("price") or "غير محدد"
        colors = product.get("colors") or "كما في الصورة"
        sizes = product.get("sizes") or "غير محدد"
        lines.append(f"• {name}\n  السعر: {price}\n  الألوان: {colors}\n  المقاسات: {sizes}")

        for image_url in product_image_urls(product):
            messages.append({"type": "image", "url": image_url})

    return "\n\n".join(lines), messages


def build_catalog_image_messages(products):
    messages = []
    for product in products:
        if _stock_state(product) != "available":
            continue
        for image_url in product_image_urls(product):
            messages.append({"type": "image", "url": image_url})
    return messages


def send_catalog_to_customer(db, sender_id, page_id="", platform="facebook"):
    products = load_active_products(db)
    image_messages = build_catalog_image_messages(products)
    reply = ""
    send_messages = image_messages
    sent = send_manychat_messages(sender_id, send_messages, platform)

    for msg in image_messages:
        save_message(
            db, sender_id, "outgoing", "image", None, msg["url"], None, None,
            {"catalog": True, "image_url": msg["url"]},
        )
    return sent, reply, image_messages


def send_catalog_to_customer_background(sender_id, page_id="", platform="facebook"):
    try:
        with app.app_context():
            db = get_db()
            sent, _, image_messages = send_catalog_to_customer(db, sender_id, page_id, platform)
            print(
                f"[Catalog] Background send to {sender_id}: sent={sent} images={len(image_messages)}",
                flush=True,
            )
    except Exception as exc:
        print(f"[Catalog] Background send error for {sender_id}: {exc}", flush=True)


def is_ai_handoff_reply(reply: str) -> bool:
    text = (reply or "").strip()
    if not text:
        return False
    lowered = text.lower()
    handoff_markers = [
        "أحول رسالتج",
        "احول رسالتج",
        "أحول الرسالة",
        "احول الرسالة",
        "للإدارة",
        "للاادارة",
        "للادارة",
        "الإدارة",
        "الادارة",
        "ثواني وأحول",
        "ثواني واحول",
    ]
    apology_markers = ["أعتذر", "اعتذر", "ما اكدر", "ما أقدر", "لم أتمكن"]
    return any(marker in text for marker in handoff_markers) or (
        any(marker in text for marker in apology_markers)
        and ("الإدارة" in text or "الادارة" in text or "تفاصيل أدق" in text)
    ) or "management" in lowered


# ── ManyChat customer sender ──────────────────────────────────────────────────

def is_instagram_platform(platform: str = "") -> bool:
    return str(platform or "").strip().lower() == "instagram"


def manychat_content_type(platform: str = "") -> str:
    return "instagram" if is_instagram_platform(platform) else "messenger"


def normalize_manychat_api_key(value: str = "") -> str:
    return _normalize_manychat_key_value(value)


def current_manychat_api_key() -> str:
    """آخر قيمة مفتاح ManyChat (بيئة + .env غير المفرغة)."""
    global MANYCHAT_API_KEY
    key = _manychat_key_from_environ()
    if not key:
        key = normalize_manychat_api_key(MANYCHAT_API_KEY)
    if os.path.exists(_env_path):
        try:
            with open(_env_path, encoding="utf-8") as env_file:
                for line in env_file:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    name, value = line.split("=", 1)
                    n = name.strip()
                    if n in ("MANYCHAT_API_KEY", "MANYCHAT_KEY", "MC_API_KEY"):
                        parsed = normalize_manychat_api_key(value)
                        if parsed:
                            key = parsed
                            break
        except Exception as exc:
            print(f"[ManyChat] Could not reload MANYCHAT_API_KEY from .env: {exc}", flush=True)
    if key:
        MANYCHAT_API_KEY = key
        os.environ["MANYCHAT_API_KEY"] = key
    return key


_manychat_page_key_map = None
_manychat_page_key_raw = ""


def _manychat_page_id_to_key_map():
    """تحويل page_id (فيسبوك/قناة ManyChat) إلى مفتاح API منفصل — لعدة صفحات/حسابات."""
    global _manychat_page_key_map, _manychat_page_key_raw
    raw = (os.environ.get("MANYCHAT_KEYS_BY_PAGE") or "").strip()
    if raw == _manychat_page_key_raw and _manychat_page_key_map is not None:
        return _manychat_page_key_map
    _manychat_page_key_raw = raw
    _manychat_page_key_map = {}
    if not raw:
        return _manychat_page_key_map
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"[ManyChat] تعذّر تحليل MANYCHAT_KEYS_BY_PAGE: {exc}", flush=True)
        return _manychat_page_key_map
    if not isinstance(data, dict):
        print("[ManyChat] MANYCHAT_KEYS_BY_PAGE يجب أن يكون كائناً: {\"PAGE_ID\":\"api_key\",...}", flush=True)
        return _manychat_page_key_map
    for pk, val in data.items():
        pid = str(pk or "").strip()
        key = normalize_manychat_api_key(str(val or ""))
        if pid and key:
            _manychat_page_key_map[pid] = key
    if _manychat_page_key_map:
        print(
            f"[ManyChat] تم تحميل مفاتيح منفصلة لـ {len(_manychat_page_key_map)} page_id (عدة حسابات).",
            flush=True,
        )
    return _manychat_page_key_map


def manychat_api_key_for_page(page_id) -> str:
    """مفتاح الإرسال: خاص بالصفحة إن وُجد في MANYCHAT_KEYS_BY_PAGE وإلا المفتاح العام."""
    pid = str(page_id or "").strip()
    if pid:
        m = _manychat_page_id_to_key_map()
        key = m.get(pid)
        if key:
            return key
    return current_manychat_api_key()


def detect_manychat_platform(data):
    ig_keys = (
        "ig_id", "ig_username", "ig_last_interaction", "ig_last_seen",
        "instagram_id", "instagram_username",
    )
    for key in ig_keys:
        if (data or {}).get(key):
            return "instagram"

    live_chat_url = str((data or {}).get("live_chat_url") or "").strip().lower()
    if "/ig" in live_chat_url or "instagram" in live_chat_url:
        return "instagram"

    for key in ("platform", "channel", "source", "social_channel", "messenger_type"):
        value = str((data or {}).get(key) or "").strip().lower()
        if "instagram" in value or value == "ig":
            return "instagram"

    # If ManyChat sends IG-specific custom fields, treat it as Instagram.
    custom_fields = (data or {}).get("custom_fields")
    if isinstance(custom_fields, dict):
        for key, value in custom_fields.items():
            if str(key).lower().startswith(("ig_", "instagram_")) and value:
                return "instagram"

    return "facebook"


def _looks_like_image_url(value):
    text = str(value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    return (
        lowered.startswith("http")
        and (
            any(ext in lowered for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif"))
            or "scontent" in lowered
            or "cdn" in lowered
            or "image" in lowered
            or "photo" in lowered
        )
    )


def extract_image_url_from_manychat_data(data):
    keys = (
        "last_input_image_url", "image_url", "last_input_image",
        "last_input_attachment_url", "attachment_url", "photo_url",
        "picture_url", "last_image_url", "url", "file_url", "media_url",
        "ig_last_input_image_url", "instagram_image_url",
    )
    for key in keys:
        value = (data or {}).get(key)
        if _looks_like_image_url(value):
            return str(value).strip()

    for value in (data or {}).values():
        if _looks_like_image_url(value):
            return str(value).strip()
        if isinstance(value, dict):
            nested = extract_image_url_from_manychat_data(value)
            if nested:
                return nested
        if isinstance(value, list):
            for item in value:
                if _looks_like_image_url(item):
                    return str(item).strip()
                if isinstance(item, dict):
                    nested = extract_image_url_from_manychat_data(item)
                    if nested:
                        return nested
    return ""


def send_image_to_facebook(sender_id: str, image_url: str, page_id: str = "", platform: str = "facebook") -> bool:
    """Compatibility wrapper: all customer sending goes through ManyChat."""
    if not image_url:
        print("[FB] No image_url provided — skipping send", flush=True)
        return False
    return send_image_via_manychat(sender_id, image_url, "", platform, page_id)


# ── ManyChat API ──────────────────────────────────────────────────────────────

MANYCHAT_DEFAULT_MESSAGE_TAG = os.environ.get("MANYCHAT_MESSAGE_TAG", "").strip()


def _build_manychat_content(content_type: str, messages: list, message_tag: str = "") -> dict:
    content = {"type": content_type, "messages": messages}
    if message_tag and content_type == "messenger":
        content["message_tag"] = message_tag
    return content


def _post_manychat_send(subscriber_id: str, messages: list, platform: str = "facebook",
                        label: str = "send", message_tag: str = "", page_id: str = "") -> dict:
    api_key = manychat_api_key_for_page(page_id)
    if not api_key:
        msg = "MANYCHAT_API_KEY not set"
        print(f"[ManyChat] {msg}", flush=True)
        return {"ok": False, "status_code": 0, "status": "missing_key", "message": msg, "response": None}
    if not subscriber_id:
        return {"ok": False, "status_code": 0, "status": "missing_subscriber", "message": "subscriber_id is empty", "response": None}
    clean_messages = [m for m in messages if isinstance(m, dict) and m.get("type")]
    if not clean_messages:
        return {"ok": False, "status_code": 0, "status": "empty_messages", "message": "no messages to send", "response": None}

    content_type = manychat_content_type(platform)
    if content_type == "messenger" and message_tag:
        print(
            "[ManyChat] Ignoring message_tag for Facebook Messenger; tags are no longer supported.",
            flush=True,
        )
        message_tag = ""

    def _do_send(tag: str) -> tuple:
        payload = {
            "subscriber_id": subscriber_id,
            "data": {
                "version": "v2",
                "content": _build_manychat_content(content_type, clean_messages, tag),
            },
        }
        if tag:
            payload["message_tag"] = tag
        resp = requests.post(
            f"{MANYCHAT_API_URL}/fb/sending/sendContent",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=20,
        )
        try:
            body = resp.json()
        except Exception:
            body = {"status": "http_error", "body": resp.text}
        return resp, body

    try:
        resp, body = _do_send(message_tag or "")
        status = body.get("status") if isinstance(body, dict) else None
        ok = resp.ok and status == "success"
        retried_with_tag = ""
        if (
            not ok
            and content_type != "messenger"
            and not message_tag
            and MANYCHAT_DEFAULT_MESSAGE_TAG
            and isinstance(body, dict)
            and "24" in (body.get("message") or "")
        ):
            retried_with_tag = MANYCHAT_DEFAULT_MESSAGE_TAG
            resp, body = _do_send(retried_with_tag)
            status = body.get("status") if isinstance(body, dict) else None
            ok = resp.ok and status == "success"

        message = ""
        if not ok and isinstance(body, dict):
            top_message = (body.get("message") or body.get("error") or body.get("body") or "").strip()
            details = body.get("details") or body.get("data")
            detail_text = ""
            if isinstance(details, dict):
                raw_messages = details.get("messages")
                if isinstance(raw_messages, list):
                    parts = []
                    for item in raw_messages:
                        if isinstance(item, dict):
                            entry = (item.get("message") or item.get("error") or "").strip()
                            if entry:
                                parts.append(entry)
                        elif isinstance(item, str) and item.strip():
                            parts.append(item.strip())
                    if parts:
                        detail_text = " | ".join(parts)
                if not detail_text:
                    raw_field = details.get("messages") or details.get("error")
                    if isinstance(raw_field, str) and raw_field.strip():
                        detail_text = raw_field.strip()
                if not detail_text:
                    detail_text = json.dumps(details, ensure_ascii=False)[:300]
            elif isinstance(details, list) and details:
                detail_text = json.dumps(details, ensure_ascii=False)[:300]
            if top_message and detail_text and detail_text not in top_message:
                message = f"{top_message} — {detail_text}"
            else:
                message = top_message or detail_text
        print(
            f"[ManyChat][{label}] subscriber={subscriber_id} type={content_type} tag={message_tag or retried_with_tag or '-'} "
            f"http={resp.status_code} ok={ok} status={status} response={body}",
            flush=True,
        )
        return {
            "ok": ok,
            "status_code": resp.status_code,
            "status": status or "http_error",
            "message": message,
            "response": body,
            "message_tag": message_tag or retried_with_tag or "",
        }
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        print(f"[ManyChat][{label}] Exception: {err}", flush=True)
        return {"ok": False, "status_code": 0, "status": "exception", "message": err, "response": None}


def send_text_via_manychat_detailed(subscriber_id: str, reply_text: str, platform: str = "facebook",
                                    message_tag: str = "", page_id: str = "") -> dict:
    return _post_manychat_send(
        subscriber_id,
        [{"type": "text", "text": reply_text}],
        platform=platform,
        label="text",
        message_tag=message_tag,
        page_id=page_id,
    )


def send_reply_via_manychat(subscriber_id: str, reply_text: str, platform: str = "facebook",
                            page_id: str = "") -> bool:
    return send_text_via_manychat_detailed(
        subscriber_id, reply_text, platform, message_tag="", page_id=page_id,
    ).get("ok", False)


def send_image_via_manychat_detailed(subscriber_id: str, image_url: str, caption: str = "",
                                     platform: str = "facebook", message_tag: str = "", page_id: str = "") -> dict:
    messages = [{"type": "image", "url": image_url}]
    if caption:
        messages.append({"type": "text", "text": caption})
    return _post_manychat_send(
        subscriber_id, messages, platform=platform, label="image", message_tag=message_tag, page_id=page_id,
    )


def send_image_via_manychat(subscriber_id: str, image_url: str, caption: str = "", platform: str = "facebook",
                            page_id: str = "") -> bool:
    return send_image_via_manychat_detailed(
        subscriber_id, image_url, caption, platform, message_tag="", page_id=page_id,
    ).get("ok", False)


def send_manychat_messages_detailed(subscriber_id: str, messages: list, platform: str = "facebook",
                                    page_id: str = "") -> dict:
    return _post_manychat_send(subscriber_id, messages, platform=platform, label="batch", page_id=page_id)


def send_manychat_messages(subscriber_id: str, messages: list, platform: str = "facebook",
                           page_id: str = "") -> bool:
    return send_manychat_messages_detailed(subscriber_id, messages, platform, page_id=page_id).get("ok", False)


def get_subscriber_info(subscriber_id: str, page_id: str = "") -> dict:
    """
    جلب معلومات الزبون من ManyChat
    endpoint: GET /fb/subscriber/getInfo
    """
    api_key = current_manychat_api_key()
    if not api_key:
        return {}
    try:
        resp = requests.get(
            f"{MANYCHAT_API_URL}/fb/subscriber/getInfo",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"subscriber_id": subscriber_id},
            timeout=10,
        )
        data = resp.json()
        if data.get("status") == "success":
            return data.get("data", {})
        return {}
    except Exception:
        return {}


# ── Messages ──────────────────────────────────────────────────────────────────

def save_message(db, sender_id=None, direction=None, message_type=None, text=None,
                 image_url=None, ad_id=None, ref=None, raw_payload=None):
    if not hasattr(db, "execute"):
        try:
            memory_db = get_db()
            return save_conversation_message(memory_db, db, sender_id, direction)
        except RuntimeError:
            memory_db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
            memory_db.row_factory = sqlite3.Row
            try:
                return save_conversation_message(memory_db, db, sender_id, direction)
            finally:
                memory_db.close()

    now = now_baghdad_iso()
    db.execute(
        """INSERT INTO messages
           (sender_id, direction, message_type, text, image_url, ad_id, ref, raw_payload, created_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            sender_id, direction, message_type, text, image_url,
            ad_id, ref, json.dumps(raw_payload, ensure_ascii=False), now,
        ),
    )
    db.commit()
    print(f"[Message] Saved {direction}/{message_type} for {sender_id}", flush=True)


def load_history(db, sender_id, limit=MAX_HISTORY):
    rows = db.execute(
        "SELECT direction, message_type, text, image_url, created_at "
        "FROM messages WHERE sender_id=? ORDER BY id DESC LIMIT ?",
        (sender_id, limit),
    ).fetchall()
    return [dict(r) for r in reversed(rows)]


def get_conversation_history(db, sender_id, limit=10):
    """
    Return the latest messages from the same customer in AI message format.
    Uses the existing SQLite connection style.
    """
    rows = db.execute(
        "SELECT role, content "
        "FROM conversation_memory "
        "WHERE sender_id=? "
        "ORDER BY timestamp DESC, id DESC "
        "LIMIT ?",
        (sender_id, limit),
    ).fetchall()
    history = []
    for row in reversed(rows):
        try:
            role = row["role"]
            content = row["content"]
        except (TypeError, KeyError, IndexError):
            role = row[0]
            content = row[1]
        history.append({"role": role, "content": content})
    return history


def save_conversation_message(db, sender_id, role, content):
    """
    Save a user or assistant message inside conversation_memory.
    """
    if role not in {"user", "assistant"}:
        return False
    text = str(content or "").strip()
    if not sender_id or not text:
        return False
    db.execute(
        "INSERT INTO conversation_memory (sender_id, role, content) VALUES (?, ?, ?)",
        (sender_id, role, text),
    )
    db.commit()
    return True


def latest_incoming_message(db, sender_id):
    row = db.execute(
        """SELECT message_type, text, image_url, ad_id, ref, raw_payload, created_at
           FROM messages
           WHERE sender_id=? AND direction='incoming'
           ORDER BY id DESC LIMIT 1""",
        (sender_id,),
    ).fetchone()
    return dict(row) if row else {}


# ── Referral extraction ───────────────────────────────────────────────────────

def extract_referral(event):
    ref = ad_id = referral_source = referral_type = None
    nodes = [
        event.get("referral"),
        event.get("message", {}).get("referral"),
        (event.get("postback") or {}).get("referral"),
    ]
    for node in nodes:
        if node:
            ref             = node.get("ref")            or ref
            ad_id           = node.get("ad_id")          or ad_id
            referral_source = node.get("source")         or referral_source
            referral_type   = node.get("type")           or referral_type
    return ref, ad_id, referral_source, referral_type


# ── Facebook / Instagram event extraction ─────────────────────────────────────

def detect_message_platform(body):
    obj = str((body or {}).get("object") or "").strip().lower()
    if "instagram" in obj:
        return "instagram"
    return "facebook"


def extract_facebook_event(body):
    entry     = body["entry"][0]
    event     = entry["messaging"][0]
    sender_id = event["sender"]["id"]
    page_id   = entry["id"]
    platform  = detect_message_platform(body)
    timestamp = event.get("timestamp")
    message   = event.get("message", {})
    text      = (message.get("text") or "").strip()
    postback  = event.get("postback", {})
    quick_reply = message.get("quick_reply", {})

    attachments = message.get("attachments", [])
    image_url   = None
    for att in attachments:
        if att.get("type") == "image":
            image_url = att.get("payload", {}).get("url")
            break
    if image_url:
        image_flow(
            "01_received_from_customer",
            sender_id=sender_id,
            page_id=page_id,
            platform=platform,
            image_url=image_url,
            text_present=bool(text),
        )

    ref, ad_id, referral_source, referral_type = extract_referral(event)

    print(f"[Extract] platform={platform} sender={sender_id} text={text!r} image={bool(image_url)} "
          f"ref={ref} ad_id={ad_id}", flush=True)

    return {
        "sender_id"      : sender_id,
        "page_id"        : page_id,
        "platform"       : platform,
        "timestamp"      : timestamp,
        "text"           : text,
        "attachments"    : attachments,
        "image_url"      : image_url,
        "quick_reply"    : quick_reply,
        "postback"       : postback,
        "ref"            : ref,
        "ad_id"          : ad_id,
        "referral_source": referral_source,
        "referral_type"  : referral_type,
    }


# ── Message type detection ────────────────────────────────────────────────────

def _is_emoji_only(text):
    if not text:
        return False
    for ch in text:
        if ch in (" ", "\n", "\r", "\t"):
            continue
        cat = unicodedata.category(ch)
        is_emoji = (
            cat in ("So", "Sm", "Sk")
            or "\U0001F300" <= ch <= "\U0001FAFF"
            or "\u2600"     <= ch <= "\u27BF"
            or "\uFE00"     <= ch <= "\uFE0F"
        )
        if not is_emoji:
            return False
    return True


def detect_message_type(ev):
    if ev["postback"]:
        return "postback"
    if ev["image_url"]:
        return "image"
    if ev["attachments"] and not ev["image_url"]:
        return "attachment"
    if ev["text"]:
        return "emoji" if _is_emoji_only(ev["text"]) else "text"
    return "unknown"


# ── Product matching ──────────────────────────────────────────────────────────

def load_active_products(db):
    return [
        product
        for product in load_products_from_file()
        if product.get("status") == "active"
    ]


def _strip_arabic_prefix(word):
    """إزالة أدوات التعريف والحروف الزائدة من الكلمة العربية."""
    for prefix in ("وال", "بال", "فال", "كال", "لل", "ال", "و", "ف", "ب", "ل", "ك"):
        if word.startswith(prefix) and len(word) > len(prefix) + 1:
            return word[len(prefix):]
    return word


def _text_match_product(text, products):
    text_lower = text.lower()
    raw_words = [w for w in re.split(r"\s+", text_lower) if len(w) > 1]
    # أضف النسخة بدون أداة التعريف لكل كلمة
    words = set(raw_words) | {_strip_arabic_prefix(w) for w in raw_words}
    best_product, best_score = None, 0
    for p in products:
        haystack = " ".join(filter(None, [
            p.get("product_name", ""),
            p.get("keywords", ""),
            p.get("description", ""),
        ])).lower()
        score = sum(1 for w in words if w in haystack and len(w) > 1)
        if score > best_score:
            best_score, best_product = score, p
    return best_product if best_score > 0 else None


_ARABIC_DIGIT_TRANS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")

_AGE_WORDS = {
    "سنة": 1, "سنه": 1, "واحد": 1, "واحدة": 1, "وحدة": 1,
    "سنتين": 2, "اثنين": 2, "اثنان": 2, "ثنين": 2, "ثنتين": 2,
    "ثلاث": 3, "ثلاثة": 3, "اربعة": 4, "أربعة": 4, "اربع": 4, "أربع": 4,
    "خمسة": 5, "خمس": 5, "ستة": 6, "ست": 6, "سبعة": 7, "سبع": 7,
    "ثمانية": 8, "ثمان": 8, "تسعة": 9, "تسع": 9, "عشرة": 10, "عشر": 10,
    "احدعش": 11, "احد عشر": 11, "إحدى عشر": 11, "اثنعش": 12, "اثنا عشر": 12,
}

_PRODUCT_TYPE_TERMS = {
    "طقم", "قميص", "بدلة", "بدله", "فستان", "تراك", "تلبيسة", "تلبيسه",
    "قاط", "رسمي", "كاجوال", "صيفي", "شتوي", "بنطلون", "شورت", "ملابس",
}

_GENERIC_PRODUCT_TERMS = {"ملابس", "موديل", "موديلات", "منتج", "منتجات"}

_COLOR_TERMS = {
    "اسود", "أسود", "ابيض", "أبيض", "احمر", "أحمر", "ازرق", "أزرق",
    "وردي", "بنفسجي", "اصفر", "أصفر", "رصاصي", "كحلي", "اخضر", "أخضر",
}

_PRODUCT_SEARCH_WORDS = {
    "اريد", "أريد", "اكو", "أكو", "عندكم", "عندج", "متوفر", "متوفرة",
    "موجود", "موجودة", "قياس", "مقاس", "مقاسات", "عمر", "سنوات", "سنة",
    "ولادي", "بناتي", "ولد", "بنت", "ملابس", "موديل", "موديلات", "منتج",
}

_ORDINAL_WORDS = {
    1: ("الاول", "الأول", "اول", "أول", "واحد", "1"),
    2: ("الثاني", "ثاني", "اثنين", "اثنان", "2"),
    3: ("الثالث", "ثالث", "ثلاثة", "3"),
    4: ("الرابع", "رابع", "اربعة", "أربعة", "4"),
    5: ("الخامس", "خامس", "خمسة", "5"),
}


def _catalog_text(value):
    text = str(value or "").translate(_ARABIC_DIGIT_TRANS)
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)
    return text.strip()


def _catalog_words(text):
    text = _catalog_text(text).lower()
    words = re.findall(r"[A-Za-z0-9_\u0621-\u064A]+", text)
    expanded = set(words)
    expanded.update(_strip_arabic_prefix(w) for w in words)
    return {w for w in expanded if w}


def _product_search_haystack(product):
    return _catalog_text(" ".join(str(product.get(field) or "") for field in (
        "product_name", "keywords", "category", "description",
        "visual_description", "colors", "sizes", "notes",
    ))).lower()


def _extract_age_years(text):
    normalized = _catalog_text(text).lower()
    matches = re.findall(r"(\d{1,2})\s*(?:سن(?:ة|ه|وات|ين)?|سنه|سنوات|عمر|اعوام|أعوام|عام)", normalized)
    if matches:
        return int(matches[0])
    matches = re.findall(r"(?:عمر|قياس|مقاس)\s*(\d{1,2})", normalized)
    if matches:
        return int(matches[0])
    for phrase, value in sorted(_AGE_WORDS.items(), key=lambda item: len(item[0]), reverse=True):
        if re.search(rf"(?:عمر|قياس|مقاس)?\s*{re.escape(phrase)}\s*(?:سن(?:ة|ه|وات)?|اعوام|أعوام|عام)?", normalized):
            return value
    return None


def _extract_size_age_range(sizes_text):
    normalized = _catalog_text(sizes_text).lower()
    values = []
    for match in re.finditer(r"(\d{1,2})\s*(اشهر|أشهر|شهر|سن(?:ة|ه|وات|ين)?|سنه|سنوات|اعوام|أعوام|عام)", normalized):
        num = int(match.group(1))
        unit = match.group(2)
        values.append(num / 12 if "شهر" in unit or "اشهر" in unit or "أشهر" in unit else float(num))
    if any(unit in normalized for unit in ("سنة", "سنه", "سنوات", "سنين", "عام", "اعوام", "أعوام")):
        for match in re.finditer(r"(\d{1,2})(?!\s*(?:اشهر|أشهر|شهر))", normalized):
            values.append(float(match.group(1)))
    for phrase, value in _AGE_WORDS.items():
        if phrase in normalized and any(unit in normalized for unit in ("سنة", "سنه", "سنوات", "عام", "اعوام", "أعوام")):
            values.append(float(value))
    if not values:
        return None
    return min(values), max(values)


def _age_fits_product(age_years, product):
    if age_years is None:
        return True, ""
    age_range = _extract_size_age_range(product.get("sizes") or "")
    if not age_range:
        return False, "no_size_range"
    low, high = age_range
    if low <= float(age_years) <= high:
        return True, f"age:{low:g}-{high:g}"
    return False, f"age_mismatch:{low:g}-{high:g}"


def _product_gender(product):
    haystack = _product_search_haystack(product)
    girl_terms = ("بناتي", "بنات", "بنت", "فستان", "بنوتة", "بنوت")
    boy_terms = ("ولادي", "اولاد", "أولاد", "ولد", "صبي", "قاط")
    has_girl = any(term in haystack for term in girl_terms)
    has_boy = any(term in haystack for term in boy_terms)
    if has_girl and not has_boy:
        return "girl"
    if has_boy and not has_girl:
        return "boy"
    return "unknown"


def _request_gender(text):
    normalized = _catalog_text(text).lower()
    words = _catalog_words(text)
    girl_terms = {"بناتي", "بنات", "بنت", "بنوتة", "بنوت", "فستان"}
    boy_terms = {"ولادي", "اولاد", "أولاد", "ولد", "صبي", "قاط"}
    has_girl = bool(words & girl_terms) or any(term in normalized for term in girl_terms)
    has_boy = bool(words & boy_terms) or any(term in normalized for term in boy_terms)
    if has_girl and not has_boy:
        return "girl"
    if has_boy and not has_girl:
        return "boy"
    return "unknown"


def _extract_product_request(text):
    words = _catalog_words(text)
    gender = _request_gender(text)
    age_years = _extract_age_years(text)
    product_terms = sorted(words & _PRODUCT_TYPE_TERMS)
    color_terms = sorted(words & _COLOR_TERMS)
    has_search_language = bool(words & _PRODUCT_SEARCH_WORDS)
    has_specific_filter = bool(age_years or gender != "unknown" or product_terms or color_terms)
    has_catalog_phrase = any(term in _catalog_text(text).lower() for term in (
        "كتالوج", "كاتالوج", "كل الموديلات", "كل البضاعة", "شنو الموجود", "شنو موجود",
    ))
    return {
        "age_years": age_years,
        "gender": gender,
        "product_terms": product_terms,
        "color_terms": color_terms,
        "has_search_language": has_search_language,
        "has_specific_filter": has_specific_filter,
        "is_search": has_specific_filter or has_catalog_phrase or (has_search_language and bool(product_terms)),
    }


def _product_matches_request(product, request_info):
    if _stock_state(product) != "available":
        return None
    score = 0
    reasons = []
    age_ok, age_reason = _age_fits_product(request_info.get("age_years"), product)
    if not age_ok:
        return None
    if request_info.get("age_years") is not None:
        score += 40
        reasons.append(age_reason)

    requested_gender = request_info.get("gender") or "unknown"
    product_gender = _product_gender(product)
    if requested_gender != "unknown":
        if product_gender != "unknown" and product_gender != requested_gender:
            return None
        score += 25 if product_gender == requested_gender else 6
        reasons.append(f"gender:{product_gender}")

    haystack = _product_search_haystack(product)
    specific_terms = [term for term in (request_info.get("product_terms") or []) if term not in _GENERIC_PRODUCT_TERMS]
    if specific_terms and not any(term in haystack for term in specific_terms):
        return None
    for term in request_info.get("product_terms") or []:
        if term in haystack:
            score += 8
            reasons.append(f"type:{term}")
    for term in request_info.get("color_terms") or []:
        if term in haystack:
            score += 6
            reasons.append(f"color:{term}")

    if not request_info.get("has_specific_filter"):
        score += 1
    return {"product": product, "score": score, "reasons": reasons}


def search_available_products_for_request(text, products, limit=5):
    request_info = _extract_product_request(text)
    if not request_info.get("is_search"):
        return {"request": request_info, "matches": []}
    matches = []
    for product in products or []:
        match = _product_matches_request(product, request_info)
        if match:
            matches.append(match)
    matches.sort(key=lambda item: (
        item["score"],
        1 if product_image_urls(item["product"]) else 0,
        item["product"].get("product_id") or "",
    ), reverse=True)
    return {"request": request_info, "matches": matches[:limit]}


def _product_search_summary(request_info):
    parts = []
    if request_info.get("age_years") is not None:
        parts.append(f"للقياس المطلوب")
    if request_info.get("gender") == "boy":
        parts.append("ولادي")
    elif request_info.get("gender") == "girl":
        parts.append("بناتي")
    product_terms = [term for term in (request_info.get("product_terms") or []) if term not in _GENERIC_PRODUCT_TERMS]
    if product_terms:
        parts.append(" / ".join(product_terms))
    if request_info.get("color_terms"):
        parts.append("لون " + " / ".join(request_info["color_terms"]))
    return " ".join(parts) or "حسب طلبك"


def build_product_recommendation_reply(matches, request_info):
    products = [item["product"] for item in matches]
    summary = _product_search_summary(request_info)
    if not products:
        return f"ما لقيت حالياً موديل متوفر مطابق لـ {summary}. ممكن ترسلين عمر/قياس ثاني حتى أبحث لك؟"
    lines = [f"لقيت لك {len(products)} موديل متوفر {summary}:"]
    for idx, product in enumerate(products, 1):
        name = product.get("product_name") or "موديل"
        price = product.get("price") or "السعر غير محدد"
        sizes = product.get("sizes") or "المقاسات غير محددة"
        colors = product.get("colors") or "الألوان حسب الصورة"
        lines.append(f"{idx}. {name} - السعر: {price} - القياسات: {sizes} - الألوان: {colors}")
    lines.append("أرسل لك الصور بالترتيب، اختاري الرقم اللي يعجبك وأحجزه لك.")
    return "\n".join(lines)


def build_product_recommendation_image_messages(matches, max_images_per_product=1):
    messages = []
    for item in matches:
        product = item["product"]
        for image_url in product_image_urls(product)[:max_images_per_product]:
            messages.append({"type": "image", "url": image_url})
    return messages


def remember_product_search_results(db, sender_id, matches, request_info):
    summary = _product_search_summary(request_info)
    for rank, item in reversed(list(enumerate(matches, 1))):
        remember_customer_product(
            db,
            sender_id,
            item["product"],
            "catalog_search",
            confidence=item.get("score") or 70,
            source="catalog_search",
            status="active",
            notes=f"catalog_search_rank={rank}; query={summary}; reasons={','.join(item.get('reasons') or [])}",
        )


def _catalog_search_rank(product):
    notes = str((product or {}).get("binding_notes") or "")
    match = re.search(r"catalog_search_rank=(\d+)", notes)
    return int(match.group(1)) if match else 999


def _customer_products_display_order(customer_products):
    return sorted(
        customer_products or [],
        key=lambda product: (_catalog_search_rank(product), product.get("last_seen_at") or ""),
    )


def select_customer_context_product(text, customer_products):
    if not customer_products:
        return None
    ordered = _customer_products_display_order(customer_products)
    words = _catalog_words(text)
    for index, aliases in _ORDINAL_WORDS.items():
        if any(alias in words for alias in aliases) and len(ordered) >= index:
            return ordered[index - 1]

    normalized = _catalog_text(text).lower()
    for product in ordered:
        if product.get("product_id") and str(product.get("product_id")).lower() in normalized:
            return product
        name = _catalog_text(product.get("product_name") or "").lower()
        if name and name in normalized:
            return product

    request_info = _extract_product_request(text)
    if request_info.get("has_specific_filter"):
        matches = []
        for product in ordered:
            match = _product_matches_request(product, request_info)
            if match:
                matches.append(match)
        if matches:
            matches.sort(key=lambda item: item["score"], reverse=True)
            return matches[0]["product"]
    return None


def analyze_image_with_ai(image_url, candidate_products):
    if not VISION_ENABLED:
        print("[Vision] Disabled by VISION_ENABLED=0.", flush=True)
        return {"product_found": False, "confidence": 0, "reason": "Vision disabled"}
    if not OPENROUTER_KEY:
        print("[Vision] No API key, skipping.", flush=True)
        return {"product_found": False, "confidence": 0, "reason": "No API key"}

    candidates_json = json.dumps([
        {k: p.get(k) for k in [
            "product_id", "product_name", "visual_description",
            "image_url", "colors", "price", "stock", "status",
        ]}
        for p in candidate_products
    ], ensure_ascii=False)

    system_prompt = (
        "You are a product matching AI. Match the customer image against candidate products.\n"
        "Reply with JSON ONLY:\n"
        '{"product_found":true/false,"product_id":"","product_name":"",'
        '"confidence":0,"reason":""}'
    )
    user_content = [
        {
            "type": "text",
            "text": f"Candidate products:\n{candidates_json}\n\nMatch the customer image:",
        },
        {"type": "image_url", "image_url": {"url": image_url}},
    ]

    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": VISION_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_content},
                ],
                "max_tokens": 300,
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        print(f"[Vision] {raw}", flush=True)
        return _parse_ai_json(raw)
    except Exception as exc:
        print(f"[Vision] Error: {exc}", flush=True)
        return {"product_found": False, "confidence": 0, "reason": str(exc)}


# ── CLIP image recognition ────────────────────────────────────────────────────

def load_clip_model():
    """تحميل نموذج CLIP مرة واحدة عند بدء التشغيل."""
    global _clip_model, _clip_processor
    if _clip_model is not None:
        return
    if not CLIP_AVAILABLE:
        raise RuntimeError("CLIP libraries not installed.")
    print("[CLIP] Loading openai/clip-vit-base-patch32...", flush=True)
    _clip_processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
    _clip_model     = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
    _clip_model.eval()
    print("[CLIP] ✅ Model loaded successfully.", flush=True)


def get_image_embedding(image_url: str) -> list:
    """تحميل صورة من URL وإرجاع L2-normalized embedding كـ list[float]."""
    if not CLIP_AVAILABLE or _clip_model is None:
        raise RuntimeError("CLIP model not loaded.")
    resp = requests.get(image_url, timeout=20)
    resp.raise_for_status()
    image       = Image.open(io.BytesIO(resp.content)).convert("RGB")
    inputs      = _clip_processor(images=image, return_tensors="pt")
    pixel_values = inputs["pixel_values"]

    with torch.no_grad():
        # نستخدم vision_model + visual_projection مباشرة لتجنب اختلافات إصدارات transformers
        vision_out = _clip_model.vision_model(pixel_values=pixel_values)
        emb        = _clip_model.visual_projection(vision_out.pooler_output)

    emb = emb / emb.norm(dim=-1, keepdim=True)   # L2 normalization
    return emb[0].tolist()


def cosine_similarity(v1: list, v2: list) -> float:
    """حساب cosine similarity بين vectorين (L2-normalized → dot product)."""
    a = np.array(v1, dtype=np.float32)
    b = np.array(v2, dtype=np.float32)
    return float(np.dot(a, b))


def index_product_image(product_id: str, image_url: str):
    """حساب وتخزين embedding صورة المنتج داخل products.json."""
    try:
        embedding = get_image_embedding(image_url)
        products = load_products_from_file()
        updated = False
        for product in products:
            if product.get("product_id") == product_id:
                product["image_url"] = image_url or product.get("image_url", "")
                product["image_embedding"] = embedding
                updated = True
                break
        if not updated:
            raise ValueError(f"product {product_id} not found in products.json")
        save_products_to_file(products)
        print(f"[CLIP] ✅ Indexed: {product_id}", flush=True)
        return True
    except Exception as exc:
        print(f"[CLIP] ❌ Failed to index {product_id}: {exc}", flush=True)
        return False


def find_top_candidates(db, customer_image_url: str, top_k: int = 3) -> list:
    """إيجاد أفضل top_k منتجات مشابهة لصورة الزبون باستخدام CLIP."""
    customer_emb = get_image_embedding(customer_image_url)
    products = [
        product
        for product in load_active_products(db)
        if product.get("image_embedding")
    ]

    scored = []
    for product in products:
        if not product.get("image_url"):
            continue
        try:
            raw_embedding = product.get("image_embedding")
            product_emb = (
                raw_embedding
                if isinstance(raw_embedding, list)
                else json.loads(raw_embedding)
            )
            score = cosine_similarity(customer_emb, product_emb)
            scored.append({
                "product_id"         : product.get("product_id"),
                "product_name"       : product.get("product_name"),
                "image_url"          : product.get("image_url"),
                "visual_description" : product.get("visual_description"),
                "description"        : product.get("description"),
                "price"              : product.get("price"),
                "stock"              : product.get("stock"),
                "colors"             : product.get("colors"),
                "sizes"              : product.get("sizes"),
                "status"             : product.get("status"),
                "score"              : round(score, 4),
            })
        except Exception as exc:
            print(f"[CLIP] Error scoring {product.get('product_id')}: {exc}", flush=True)

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:top_k]


def build_product_vision_candidates(products, limit=10):
    """بناء قائمة منتجات مرئية كاملة عندما لا تكفي ترشيحات CLIP."""
    candidates = []
    for p in products:
        if not p.get("image_url"):
            continue
        candidates.append({
            "product_id"         : p.get("product_id"),
            "product_name"       : p.get("product_name"),
            "image_url"          : p.get("image_url"),
            "visual_description" : p.get("visual_description"),
            "description"        : p.get("description"),
            "price"              : p.get("price"),
            "stock"              : p.get("stock"),
            "colors"             : p.get("colors"),
            "sizes"              : p.get("sizes"),
            "status"             : p.get("status"),
            "score"              : p.get("score", 0),
        })
    return candidates[:limit]


def merge_vision_candidates(primary, fallback, limit=10):
    merged = []
    seen = set()
    for item in (primary or []) + (fallback or []):
        pid = item.get("product_id")
        if not pid or pid in seen:
            continue
        merged.append(item)
        seen.add(pid)
        if len(merged) >= limit:
            break
    return merged


def _extract_product_id_only(raw, candidates):
    text = (raw or "").strip()
    candidate_ids = {str(c.get("product_id")) for c in candidates if c.get("product_id")}

    try:
        data = _parse_ai_json(text)
        value = (
            data.get("product_id")
            or data.get("id")
            or data.get("matched_product_id")
            or ""
        )
        value = str(value).strip()
        if value in candidate_ids:
            return value
        if value.upper() in ("NONE", "NO_MATCH", "NULL", "FALSE", ""):
            return ""
    except Exception:
        pass

    cleaned = re.sub(r"```(?:json)?|```", "", text).strip().strip('"\'')
    if cleaned in candidate_ids:
        return cleaned
    for pid in candidate_ids:
        if re.search(rf"\b{re.escape(pid)}\b", cleaned):
            return pid
    return ""


def confirm_with_vision(customer_image_url: str, candidates: list) -> dict:
    """إرجاع product_id فقط من Vision بعد مقارنة صورة الزبون بصور المنتجات."""
    if not VISION_ENABLED:
        return {
            "product_found": False, "product_id": "", "confidence": 0,
            "available": False, "reason": "Vision disabled",
        }
    if not OPENROUTER_KEY:
        return {
            "product_found": False, "product_id": "", "confidence": 0,
            "reason": "No API key", "available": False,
        }

    content = [
        {"type": "text",      "text": "صورة الزبون:"},
        {"type": "image_url", "image_url": {"url": customer_image_url}},
        {
            "type": "text",
            "text": (
                "قارن صورة الزبون مع صور المنتجات التالية. "
                "مهمتك الوحيدة اختيار product_id المطابق أو NONE."
            ),
        },
    ]
    for c in candidates:
        product_info = {
            "product_id": c.get("product_id"),
            "product_name": c.get("product_name"),
            "visual_description": c.get("visual_description"),
            "clip_score": c.get("score"),
        }
        content.append({
            "type": "text",
            "text": "PRODUCT_CANDIDATE:\n" + json.dumps(product_info, ensure_ascii=False, indent=2),
        })
        if c.get("image_url"):
            content.append({"type": "image_url", "image_url": {"url": c["image_url"]}})

    content.append({
        "type": "text",
        "text": (
            "اختر المنتج المطابق فقط إذا كانت صورة الزبون وصورة المنتج نفس القطعة بوضوح. "
            "إذا يوجد اختلاف مهم في القصة أو اللون أو الخامة أو التفاصيل، أو لم تكن متأكد، أجب NONE. "
            "لا تكتب شرحاً. أجب فقط بمعرف المنتج مثل P001 أو NONE."
        ),
    })

    system_prompt = (
        "أنت خبير تطابق صور منتجات موديلات وقطع نسائية محتشمة لمتجر.\n"
        "ستستلم صورة الزبون ثم مجموعة منتجات، كل منتج معه product_id وvisual_description وصورته.\n"
        "قارن بصرياً بدقة عالية جداً: نوع القطعة، اللون، القصة، الأكمام، الياقة، البنطال/الشورت، الجيوب، الأزرار، الإكسسوارات.\n"
        "إذا صورة الزبون ليست نفس المنتج تماماً أو تشبهه فقط، أجب NONE.\n"
        "لا تشرح ولا ترجع JSON. أجب بسطر واحد فقط: product_id أو NONE."
    )

    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": VISION_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": content},
                ],
                "max_tokens": 20,
                "temperature": 0,
            },
            timeout=45,
        )
        resp.raise_for_status()
        payload = resp.json()
        if "choices" not in payload:
            print(f"[VisionID] Bad response: {json.dumps(payload, ensure_ascii=False)[:800]}", flush=True)
            return {
                "product_found": False, "product_id": "", "confidence": 0,
                "available": False, "reason": "Vision response missing choices",
            }
        raw = payload["choices"][0]["message"]["content"]
        pid = _extract_product_id_only(raw, candidates)
        print(f"[VisionID] raw={raw!r} selected={pid or 'NONE'}", flush=True)
        return {
            "product_found": bool(pid),
            "product_id": pid,
            "confidence": 100 if pid else 0,
            "available": False,
            "reason": "Vision selected product_id only" if pid else "Vision returned NONE",
        }
    except Exception as exc:
        print(f"[VisionConfirm] Error: {exc}", flush=True)
        return {
            "product_found": False, "product_id": "", "confidence": 0,
            "available": False, "reason": str(exc),
        }


def _resolve_catalog_image_path():
    path = str(CATALOG_IMAGE_PATH or "").strip()
    if not path:
        return ""
    if os.path.isabs(path):
        return path
    return os.path.join(os.path.dirname(__file__), path)


def _is_catalog_image_file(filename):
    return os.path.splitext(str(filename or "").strip())[1].lower() in CATALOG_IMAGE_EXTENSIONS


def _catalog_image_paths_from_dir():
    if not os.path.isdir(CATALOG_IMAGE_DIR):
        return []
    paths = []
    for name in os.listdir(CATALOG_IMAGE_DIR):
        path = os.path.join(CATALOG_IMAGE_DIR, name)
        if os.path.isfile(path) and _is_catalog_image_file(name):
            paths.append(path)
    return sorted(paths, key=lambda item: (os.path.getmtime(item), os.path.basename(item).lower()))


def _resolve_catalog_image_paths():
    paths = []
    seen = set()
    legacy_path = _resolve_catalog_image_path()
    for path in [legacy_path, *_catalog_image_paths_from_dir()]:
        if not path or not os.path.isfile(path):
            continue
        real = os.path.realpath(path)
        if real in seen:
            continue
        seen.add(real)
        paths.append(path)
    return paths


def _catalog_image_id(path):
    real_path = os.path.realpath(path)
    legacy_path = _resolve_catalog_image_path()
    if legacy_path and os.path.realpath(legacy_path) == real_path:
        return "__legacy__"
    return os.path.basename(path)


def _catalog_image_public_url(path):
    try:
        rel = os.path.relpath(path, PRODUCT_IMAGE_DIR).replace("\\", "/")
    except ValueError:
        return ""
    if rel.startswith(".."):
        return ""
    return build_public_image_url(f"/product_image/{rel}")


def _catalog_image_meta(path):
    stat = os.stat(path)
    return {
        "id": _catalog_image_id(path),
        "filename": os.path.basename(path),
        "managed": _catalog_image_id(path) != "__legacy__",
        "url": _catalog_image_public_url(path),
        "size": stat.st_size,
        "updated_at": datetime.fromtimestamp(stat.st_mtime, BAGHDAD_TZ).isoformat(),
    }


def _safe_catalog_filename(filename):
    base = os.path.basename(str(filename or "").replace("\\", "/")).strip()
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    stem, ext = os.path.splitext(base)
    ext = ext.lower()
    if ext not in CATALOG_IMAGE_EXTENSIONS:
        return ""
    stem = stem.strip("._-") or "catalog"
    return f"{stem[:80]}_{int(time.time() * 1000)}{ext}"


def _file_to_data_url(path):
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(path or "missing image path")
    with open(path, "rb") as f:
        content = f.read()
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        mime_type = "image/png"
    elif content.startswith(b"\xff\xd8\xff"):
        mime_type = "image/jpeg"
    elif content.startswith(b"RIFF") and content[8:12] == b"WEBP":
        mime_type = "image/webp"
    else:
        mime_type = mimetypes.guess_type(path)[0] or "image/png"
    encoded = base64.b64encode(content).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _download_image_to_data_url(url):
    resp = requests.get(url, timeout=25)
    resp.raise_for_status()
    mime_type = (resp.headers.get("Content-Type") or "").split(";", 1)[0].strip()
    if not mime_type.startswith("image/"):
        mime_type = mimetypes.guess_type(urlparse(url).path)[0] or "image/jpeg"
    encoded = base64.b64encode(resp.content).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _local_image_path_from_reference(image_ref):
    image_ref = str(image_ref or "").strip()
    if not image_ref:
        return ""
    parsed = urlparse(image_ref)
    path = parsed.path if parsed.scheme and parsed.netloc else image_ref
    normalized = path.replace("\\", "/")
    if "/product_image/" in normalized:
        rel = normalized.split("/product_image/", 1)[1].lstrip("/")
        return os.path.join(PRODUCT_IMAGE_DIR, rel)
    if os.path.isabs(image_ref):
        return image_ref
    return os.path.join(os.path.dirname(__file__), image_ref.lstrip("/"))


def _image_ref_for_openrouter(image_ref):
    image_ref = str(image_ref or "").strip()
    if not image_ref:
        raise ValueError("missing image url")
    if image_ref.startswith("data:"):
        return image_ref
    if image_ref.startswith("https://"):
        return image_ref
    if image_ref.startswith("http://"):
        return _download_image_to_data_url(image_ref)
    return _file_to_data_url(_local_image_path_from_reference(image_ref))


def _clean_catalog_product_id(raw, products):
    product_ids = {str(p.get("product_id") or "").strip() for p in products if p.get("product_id")}
    text = re.sub(r"```(?:json)?|```", "", str(raw or ""), flags=re.IGNORECASE).strip().strip('"\'')
    first_line = (text.splitlines() or [""])[0].strip().strip('"\'')
    if first_line.upper() in {"NONE", "NO", "NO_MATCH", "NULL", "FALSE", ""}:
        return ""
    if first_line in product_ids:
        return first_line
    for pid in product_ids:
        if re.fullmatch(re.escape(pid), first_line):
            return pid
    return ""


def match_customer_image_with_catalog(customer_image_url, products):
    image_flow(
        "04_catalog_match_start",
        customer_image_url=customer_image_url,
        products_count=len(products or []),
        enabled=CATALOG_MATCH_ENABLED,
        model=CATALOG_MATCH_MODEL,
        catalog_images_count=len(_resolve_catalog_image_paths()),
    )
    if not CATALOG_MATCH_ENABLED:
        image_flow("04_catalog_match_skipped", reason="disabled")
        print("[CatalogVision] returned NONE, human review required", flush=True)
        return {
            "product_found": False,
            "product_id": "",
            "confidence": 0,
            "reason": "Catalog match disabled",
        }
    if not OPENROUTER_KEY:
        image_flow("04_catalog_match_skipped", reason="missing_openrouter_key")
        print("[CatalogVision] error=missing OPENROUTER_API_KEY", flush=True)
        return {
            "product_found": False,
            "product_id": "",
            "confidence": 0,
            "reason": "No API key",
        }
    if not products:
        image_flow("04_catalog_match_skipped", reason="no_products")
        print("[CatalogVision] returned NONE, human review required", flush=True)
        return {
            "product_found": False,
            "product_id": "",
            "confidence": 0,
            "reason": "No products",
        }

    try:
        catalog_image_urls = [
            _file_to_data_url(path)
            for path in _resolve_catalog_image_paths()
        ]
        if not catalog_image_urls:
            image_flow("04_catalog_match_skipped", reason="no_catalog_images")
            print("[CatalogVision] returned NONE, no catalog images configured", flush=True)
            return {
                "product_found": False,
                "product_id": "",
                "confidence": 0,
                "reason": "No catalog images",
            }
        customer_openrouter_url = _image_ref_for_openrouter(customer_image_url)
        image_flow(
            "05_openrouter_catalog_request",
            model=CATALOG_MATCH_MODEL,
            customer_image_mode="url" if str(customer_openrouter_url).startswith("https://") else "data_url",
            catalog_image_mode="data_url",
            catalog_images_count=len(catalog_image_urls),
        )
        prompt = (
            "أنت خبير في مطابقة المنتجات. قارن قطعة الملابس في صورة الزبون الأولى مع الموديلات الموجودة في صور الكتالوج التالية. "
            "استخرج رقم المعرف product_id للموديل المطابق تماماً. أرجع رقم الـ ID فقط دون أي شرح. "
            "إذا لم تجد تطابقاً واضحاً أرجع NONE فقط."
        )
        print("[CatalogVision] sending customer image to OpenRouter", flush=True)
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": CATALOG_MATCH_MODEL,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": {"url": customer_openrouter_url}},
                            *[
                                {"type": "image_url", "image_url": {"url": catalog_image_url}}
                                for catalog_image_url in catalog_image_urls
                            ],
                        ],
                    }
                ],
                "temperature": 0.1,
                "max_tokens": 20,
            },
            timeout=45,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        selected = _clean_catalog_product_id(raw, products)
        image_flow(
            "06_openrouter_catalog_response",
            status_code=resp.status_code,
            raw=raw,
            selected=selected or "NONE",
        )
        print(f"[CatalogVision] raw={raw!r} selected={selected or 'NONE'}", flush=True)
        if not selected:
            image_flow("07_catalog_match_failed", reason="none_or_unknown", raw=raw)
            print("[CatalogVision] returned NONE, human review required", flush=True)
            return {
                "product_found": False,
                "product_id": "",
                "confidence": 0,
                "reason": "Catalog returned NONE or unknown product_id",
                "raw": raw,
            }
        print(f"[CatalogVision] matched product {selected}", flush=True)
        image_flow("07_catalog_match_success", product_id=selected, confidence=100)
        return {
            "product_found": True,
            "product_id": selected,
            "confidence": 100,
            "reason": "Matched by OpenRouter catalog",
        }
    except Exception as exc:
        image_flow("06_openrouter_catalog_error", error=str(exc))
        print(f"[CatalogVision] error={exc}", flush=True)
        return {
            "product_found": False,
            "product_id": "",
            "confidence": 0,
            "reason": str(exc),
        }


def match_product(db, ev, products):
    ref       = ev.get("ref")
    ad_id     = ev.get("ad_id")
    text      = ev.get("text", "")
    image_url = ev.get("image_url")
    sender_id = ev.get("sender_id")

    matched      = None
    match_method = None
    image_result = None

    if ref:
        matched = next((p for p in products if p.get("ref") == ref), None)
        if matched:
            match_method = "ref"

    if not matched and ad_id:
        ad_id_str = str(ad_id).strip()
        matched = next(
            (p for p in products
             if str(p.get("ad_id") or "").strip() == ad_id_str
             or ad_id_str in str(p.get("ad_id") or "")
             or str(p.get("ad_id") or "") in ad_id_str),
            None,
        )
        if matched:
            match_method = "ad_id"
            print(f"[Product] Matched via ad_id={ad_id} → {matched.get('product_name')}", flush=True)

    if not matched and text:
        matched = _text_match_product(text, products)
        if matched:
            match_method = "text"

    if not matched and text and sender_id and _is_contextual_product_question(text):
        prev_type = previous_incoming_message_type(db, sender_id)
        if _has_demonstrative_reference(text) and prev_type != "image":
            print(
                "[Product] Context text looks like it refers to a new image, "
                "but previous message was not image; skipping customer memory.",
                flush=True,
            )
            return None, None, {
                "product_found": False,
                "confidence": 0,
                "reason": "Text likely refers to an image that has not arrived yet",
                "waiting_for_image": True,
            }
        remembered_products = load_customer_products(db, sender_id, limit=1)
        if remembered_products:
            last_product_id = remembered_products[0].get("product_id")
            matched = next((p for p in products if p.get("product_id") == last_product_id), None)
            if matched:
                match_method = "customer_memory"
                image_result = {
                    "product_found": True,
                    "product_id": last_product_id,
                    "confidence": remembered_products[0].get("confidence") or 70,
                    "reason": "Matched from customer's recent product memory",
                }

    if not matched and image_url and products:
        candidates = build_product_vision_candidates(products, limit=20)
        image_flow(
            "04_match_product_image_branch",
            sender_id=sender_id,
            image_url=image_url,
            candidates_count=len(candidates),
        )
        catalog_result = match_customer_image_with_catalog(image_url, products)
        image_result = dict(catalog_result or {})
        if catalog_result.get("product_found"):
            pid = catalog_result.get("product_id")
            matched = next((p for p in products if p.get("product_id") == pid), None)
            if matched:
                match_method = "image_recognition"
                image_result["available"] = _stock_state(matched) == "available"
                image_result["stock"] = matched.get("stock")
                image_result["price"] = matched.get("price")
                complete_customer_product_link(
                    db,
                    sender_id,
                    matched,
                    match_method,
                    confidence=100,
                    source="image_recognition",
                )
                print(f"[CatalogVision] matched product {pid}", flush=True)
                image_flow(
                    "08_product_linked_automatically",
                    sender_id=sender_id,
                    product_id=pid,
                    match_method=match_method,
                    stock=matched.get("stock"),
                    price=matched.get("price"),
                )
                return matched, match_method, image_result

        image_result["human_review_candidates"] = candidates
        image_flow(
            "08_product_not_matched",
            sender_id=sender_id,
            reason=image_result.get("reason"),
            human_review_candidates=len(candidates),
        )
        return None, None, image_result

    if not matched and image_url and products and VISION_ENABLED:
        # ── Vision product-id pipeline ────────────────────────────────────────
        candidates = []
        if CLIP_AVAILABLE and _clip_model is not None:
            try:
                candidates = find_top_candidates(db, image_url, top_k=5)
                print(
                    "[CLIP] Top candidates: "
                    + json.dumps(
                        [{"product_id": c["product_id"], "product_name": c["product_name"],
                          "score": c["score"]} for c in candidates],
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            except Exception as exc:
                print(f"[CLIP] Error: {exc}", flush=True)

        all_image_candidates = build_product_vision_candidates(products, limit=10)
        candidates = merge_vision_candidates(candidates, all_image_candidates, limit=10)
        if not candidates:
            print(
                "[VisionID] No products with images available for comparison.",
                flush=True,
            )
        else:
            print(
                "[VisionID] Sending candidates to vision: "
                + json.dumps(
                    [{"product_id": c["product_id"], "product_name": c["product_name"], "score": c.get("score", 0)}
                     for c in candidates],
                    ensure_ascii=False,
                ),
                flush=True,
            )

        vision = confirm_with_vision(image_url, candidates)
        image_result = vision
        pid = vision.get("product_id")
        if pid:
            matched = next((p for p in products if p.get("product_id") == pid), None)
            if matched:
                match_method = "image_recognition"
        else:
            print("[Product] Vision returned NONE; no product match.", flush=True)
            image_result["human_review_candidates"] = candidates
    elif not matched and image_url and products:
        image_result = {
            "product_found": False,
            "confidence": 0,
            "reason": "Vision disabled",
            "human_review_candidates": build_product_vision_candidates(products, limit=10),
        }
        print("[Product] Vision disabled; skipping image model matching.", flush=True)

    if matched:
        if image_result is None:
            image_result = {}
        image_result["available"] = _stock_state(matched) == "available"
        image_result["stock"] = matched.get("stock")
        image_result["price"] = matched.get("price")
        print(f"[Product] Matched via '{match_method}': {matched.get('product_name')}", flush=True)
    else:
        print("[Product] No match found.", flush=True)

    return matched, match_method, image_result


# ── AI config loader ──────────────────────────────────────────────────────────

_INSTRUCTIONS_FILE  = os.path.join(os.path.dirname(__file__), "instructions.txt")
_PLAYBOOK_FILE      = os.path.join(os.path.dirname(__file__), "gemini_sales_playbook.md")


def _load_file_text(path):
    """Return file contents as string, or '' if missing."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def send_text_to_facebook(sender_id: str, text: str, page_id: str = "", platform: str = "facebook") -> bool:
    """Compatibility wrapper: all customer sending goes through ManyChat."""
    return send_reply_via_manychat(sender_id, text, platform)


def send_webhook_result_to_facebook(result, fallback_sender_id: str = "") -> bool:
    """Send the generated webhook reply directly to Facebook, not back through n8n."""
    result = result or {}
    reply = (result.get("reply") or "").strip()
    sender_id = result.get("sender_id") or fallback_sender_id
    page_id = result.get("page_id") or ""
    platform = result.get("platform") or "facebook"
    if not sender_id:
        print("[WebhookSend] Missing sender_id; cannot send reply to Facebook.", flush=True)
        return False

    sent = False
    if reply:
        sent = send_text_to_facebook(sender_id, reply, page_id, platform)
        debug = result.get("debug")
        if isinstance(debug, dict):
            debug["fb_text_sent_directly"] = sent
        print(f"[WebhookSend] Text sent directly to FB={sent}", flush=True)

    img_urls = result.get("product_image_urls") or result.get("image_urls") or []
    if not img_urls and result.get("product_image_url"):
        img_urls = [result.get("product_image_url")]
    if result.get("send_image"):
        for img_url in img_urls:
            img_sent = send_image_to_facebook(sender_id, img_url, page_id, platform)
            print(f"[WebhookSend] Image sent directly to customer={img_sent}", flush=True)
            sent = sent or img_sent

    if not reply and not img_urls:
        print(f"[WebhookSend] No text/image to send.", flush=True)

    return sent


def auto_reply_after_product_link(db, sender_id, matched_product, conversation_history=None):
    """Generate and send the first AI reply as soon as a human links a product."""
    if not matched_product:
        return {"sent": False, "reply": "", "reason": "missing_product"}
    complete_customer_product_link(
        db,
        sender_id,
        matched_product,
        matched_product.get("match_method") or "auto_product_link",
        confidence=matched_product.get("confidence") or 100,
        source="auto_reply_after_product_link",
    )
    if not is_ai_enabled(db) or not is_customer_ai_enabled(db, sender_id):
        return {"sent": False, "reply": "", "reason": "ai_disabled"}

    customer = get_or_create_customer(db, sender_id, None)
    latest = latest_incoming_message(db, sender_id)
    customer_products = load_customer_products(db, sender_id)
    history = load_history(db, sender_id, limit=100)
    history_loaded_after_latest = conversation_history is None
    if conversation_history is None:
        conversation_history = get_conversation_history(db, sender_id, limit=10)
    products = load_active_products(db)
    instructions_text, rules_list = load_ai_config(db, sender_id=sender_id)
    link_instruction = (
        "تم ربط الصورة/المحادثة الآن بهذا المنتج من قبل الإدارة. "
        "راجع سجل المحادثة بالكامل واستخرج آخر سؤال أو طلب واضح من الزبون قبل الرد. "
        "يجب أن يكون الرد جواباً مباشراً على سؤال الزبون بالتحديد، وليس وصفاً عاماً للمنتج. "
        "إذا الزبون ذكر العمر أو القياس أو اللون أو أي معلومة سابقاً، استخدمها ولا تسأل عنها مرة ثانية. "
        "إذا كان آخر رد من الزبون مجرد قياس/عمر بعد الصورة، فجاوبه بتأكيد التوفر للقياس إن كان ضمن بيانات المنتج "
        "ثم رغّبه بالحجز بلطف واطلب رقم الموبايل والمحافظة والعنوان عند الرغبة. "
        "ممنوع طلب اسم الزبون للحجز؛ الاسم اختياري ولا نوقف الحجز عليه. "
        "ممنوع تكرار قالب مثل: المنتج متوفر وسعره كذا والقياسات كذا شنو تحبين تعرفين عنه. "
        "لا تختم بسؤال عام مثل: شنو تحبين تعرفين عنه؟ اختم بسؤال بيع واضح مثل: أحجزه إلج؟"
    )
    instructions_text = f"{instructions_text}\n\n{link_instruction}".strip()

    ev = {
        "sender_id": sender_id,
        "text": latest.get("text") or "تم ربط المنتج من الإدارة؛ حضر رداً مناسباً للزبون.",
        "image_url": latest.get("image_url"),
        "attachments": [],
        "ref": latest.get("ref"),
        "ad_id": latest.get("ad_id"),
        "referral_source": None,
        "referral_type": None,
        "postback_payload": None,
        "quick_reply_payload": None,
        "timestamp": latest.get("created_at"),
        "page_id": customer.get("page_id") or "",
        "platform": customer.get("platform") or "facebook",
    }
    message_type = latest.get("message_type") or "text"
    latest_text = (ev.get("text") or "").strip()
    if (
        history_loaded_after_latest
        and latest_text
        and conversation_history
        and conversation_history[-1].get("role") == "user"
        and conversation_history[-1].get("content") == latest_text
    ):
        conversation_history = conversation_history[:-1]
    ai_result = call_main_ai(
        ev, message_type, customer, history, products,
        matched_product, None, instructions_text, rules_list,
        customer_products=customer_products,
        conversation_history=conversation_history,
    )
    if ai_result.get("failed"):
        try:
            set_customer_ai_enabled(db, sender_id, False)
        except Exception as exc:
            print(f"[ProductLink] Could not pause AI after failure: {exc}", flush=True)
        create_human_review(
            db,
            {"sender_id": sender_id, "page_id": customer.get("page_id") or "", "platform": customer.get("platform") or "facebook",
             "text": ev.get("text") or "", "image_url": ev.get("image_url"), "ad_id": ev.get("ad_id"), "ref": ev.get("ref")},
            f"AI could not produce a reply after product link ({ai_result.get('failure_reason') or 'ai_failed'})",
            build_product_vision_candidates(products, limit=10) if products else [],
        )
        return {"sent": False, "reply": "", "reason": "ai_failed_human_required"}
    reply = (ai_result.get("reply") or "").strip()
    if not reply or is_ai_handoff_reply(reply):
        try:
            set_customer_ai_enabled(db, sender_id, False)
        except Exception as exc:
            print(f"[ProductLink] Could not pause AI on empty/handoff reply: {exc}", flush=True)
        return {"sent": False, "reply": "", "reason": "empty_or_handoff_reply"}

    order_created = False
    if ai_result.get("create_order"):
        order_created, order_reply = create_order_if_valid(db, sender_id, ai_result, matched_product)
        if order_reply:
            reply = order_reply

    save_message(
        db, sender_id, "outgoing", "text",
        reply, None, None, None,
        {"auto_after_product_link": True, "product_id": matched_product.get("product_id")},
    )
    save_conversation_message(db, sender_id, "assistant", reply)
    if not order_created:
        try:
            schedule_followup_if_needed(
                db,
                sender_id,
                stage=ai_result.get("intent") or "conversation",
                product=matched_product,
                meta={"source": "auto_reply_after_product_link", "reply": reply[:160]},
            )
        except Exception as exc:
            print(f"[FollowUp] Could not schedule after product link: {exc}", flush=True)
    sent = send_text_to_facebook(sender_id, reply, ev["page_id"], ev["platform"])
    print(f"[ProductLink] Auto reply sent={sent} to {sender_id}", flush=True)
    return {"sent": sent, "reply": reply, "reason": None if sent else "manychat_send_failed"}


def load_ai_config(db, sender_id=None):
    instructions = db.execute(
        "SELECT content FROM ai_instructions WHERE active=1"
    ).fetchall()
    rules = db.execute(
        "SELECT rule FROM forbidden_rules WHERE active=1"
    ).fetchall()
    instructions_text = "\n".join(r["content"] for r in instructions)
    rules_list        = [r["rule"] for r in rules]

    # If DB has no instructions, fall back to instructions.txt then playbook
    if not instructions_text.strip():
        file_instructions = _load_file_text(_INSTRUCTIONS_FILE)
        playbook          = _load_file_text(_PLAYBOOK_FILE)
        if file_instructions:
            instructions_text = file_instructions
            print("[Config] Loaded instructions from instructions.txt (DB was empty)", flush=True)
        if playbook:
            instructions_text = (instructions_text + "\n\n---\n\n" + playbook).strip()
            print("[Config] Appended gemini_sales_playbook.md to instructions", flush=True)

    # Append customer-specific and global supervisor instructions
    try:
        if sender_id:
            custom_rows = db.execute(
                "SELECT instructions FROM customer_instructions "
                "WHERE (sender_id=? OR apply_to_all=1) AND instructions IS NOT NULL AND instructions != ''",
                (sender_id,),
            ).fetchall()
        else:
            custom_rows = db.execute(
                "SELECT instructions FROM customer_instructions "
                "WHERE apply_to_all=1 AND instructions IS NOT NULL AND instructions != ''"
            ).fetchall()
        if custom_rows:
            custom_text = "\n".join(r["instructions"] for r in custom_rows)
            instructions_text += f"\n\nتعليمات المشرف الخاصة:\n{custom_text}"
    except Exception as exc:
        print(f"[Config] customer_instructions fetch error: {exc}", flush=True)

    try:
        active_rules = get_active_ai_rules(db)
        active_rules_text = "\n".join(
            f"- {row['rule_text']}" for row in active_rules if (row.get("rule_text") or "").strip()
        )
        if active_rules_text:
            instructions_text += (
                "\n\nقواعد مبيعات مفعلة من الأدمن:\n"
                f"{active_rules_text}\n"
                "التزم بهذه القواعد، لكن لا تخترع معلومات غير موجودة في بيانات المنتج. "
                "إذا تعارضت القواعد مع بيانات المنتج أو سياسة النظام، اتبع بيانات المنتج وسياسة النظام."
            )
    except Exception as exc:
        print(f"[Config] active_ai_rules fetch error: {exc}", flush=True)

    delivery_rule = (
        f"قاعدة ثابتة لأجور التوصيل: {FIXED_DELIVERY_TEXT}. "
        "لا تستخدم أي أجور توصيل أخرى حتى لو ظهرت داخل بيانات المنتجات."
    )
    instructions_text = (instructions_text + "\n\n" + delivery_rule).strip()

    return instructions_text, rules_list


# ── Local intent router (no AI call) ─────────────────────────────────────────

_CATALOG_KEYWORDS = (
    "كتالوج", "كاتالوج", "كل الموديلات", "كل الموديل", "كل الانواع", "كل الأنواع",
    "كل البضاعة", "شو عندكم", "شو عندج", "شنو الموجود", "شنو موجود",
    "اعرض كل", "ارسلي كل", "أرسلي كل", "وريني كل", "كلش الموديل",
)

_HUMAN_HANDOFF_KEYWORDS = (
    "شكوى", "مشكلة", "مدير", "مديرة", "غش", "احتيال", "كذب",
    "اشتكي", "اتصل بي", "كلمني", "تحدث معي", "نصب",
)


_PROBLEM_DELAY_KEYWORDS = (
    "تاخر الطلب", "تأخر الطلب", "الطلب متاخر", "الطلب متأخر",
    "طلب متاخر", "طلب متأخر", "ما وصل الطلب", "ماوصل الطلب",
    "ما وصلني", "ماوصلني", "ما اجاني", "ماجاني", "لم يصل",
    "وين الطلب", "وين طلبي", "بعده ما واصل", "بعد ما واصل",
    "صارله هواية", "صار له هواية", "تأخير", "تاخير",
)

_PROBLEM_DISSATISFACTION_KEYWORDS = (
    "غير راضي", "مو راضي", "مو راضية", "غير راضية",
    "ما عجبني", "ماعجبني", "مو عاجبني", "مو عاجبني",
    "ما طلع", "ماطلع", "مو نفس", "مختلف", "مو مثل الصورة",
    "مو نفس الصورة", "خربان", "تالف", "متضرر", "مكسور",
    "رديء", "سيء", "مو حلو", "خامة سيئة", "الخامة مو",
    "قياس غلط", "المقاس غلط", "لون غلط", "ناقص", "ارجاع",
    "أرجاع", "اريد ارجع", "أريد أرجع", "استبدال", "بدلولي",
)

_PROBLEM_OBJECTION_KEYWORDS = (
    "اعتراض", "معترض", "معترضة", "اغلى", "أغلى", "غالي",
    "السعر عالي", "سعره عالي", "ليش غالي", "مو مناسب",
    "ما اقبل", "ما أقبل", "ما اثق", "ما أثق", "مو مضمون",
    "اخاف مو نفس", "خاف مو نفس", "ما مطمئن", "مو مطمئن",
)


def _text_contains_any(text: str, keywords) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(k in lowered for k in keywords)


def classify_customer_problem(text: str):
    normalized = re.sub(r"\s+", " ", str(text or "").strip().lower())
    if not normalized:
        return None
    if _text_contains_any(normalized, _PROBLEM_DELAY_KEYWORDS):
        return "تأخر الطلب أو لم يصل للزبون"
    if (
        ("وصل الطلب" in normalized or "استلمت" in normalized or "وصلني" in normalized)
        and _text_contains_any(normalized, _PROBLEM_DISSATISFACTION_KEYWORDS)
    ):
        return "عدم رضا بعد استلام الطلب"
    if _text_contains_any(normalized, _PROBLEM_DISSATISFACTION_KEYWORDS):
        return "مشكلة في المنتج أو رغبة بإرجاع/استبدال"
    if _text_contains_any(normalized, _PROBLEM_OBJECTION_KEYWORDS):
        return "اعتراض أو تردد من الزبون"
    if _text_contains_any(normalized, _HUMAN_HANDOFF_KEYWORDS):
        return "شكوى أو طلب تدخل بشري"
    return None


def determine_intent(ev, customer_products, matched_product):
    """يقرر النية بدون استدعاء AI خارجي. يحاكي المخرج القديم لـ call_router_ai."""
    text = (ev.get("text") or "").strip()
    has_context = bool(matched_product or customer_products)
    wants_catalog = _text_contains_any(text, _CATALOG_KEYWORDS)
    problem_reason = classify_customer_problem(text)
    needs_human = bool(problem_reason and problem_reason != "اعتراض أو تردد من الزبون")
    return {
        "intent": "catalog_request" if wants_catalog else ("complaint" if problem_reason else "other"),
        "has_product_context": has_context,
        "wants_catalog": wants_catalog,
        "wants_image": False,
        "needs_human": needs_human,
        "should_reply": not wants_catalog,
        "reason": problem_reason or "local_keyword_router",
        "problem_reason": problem_reason,
    }


# ── Main AI call ──────────────────────────────────────────────────────────────

def call_main_ai(
    ev, message_type, customer, history, products,
    matched_product, image_result, instructions_text, rules_list,
    fix_instruction=None, customer_products=None, conversation_history=None,
    catalog_search_context=None,
):
    if image_result and image_result.get("unmatched_customer_image"):
        review_id = image_result.get("human_review_id")
        review_text = f" رقم المراجعة: {review_id}" if review_id else ""
        return {
            "reply": (
                "حبيبتي هذا الموديل ما كدرت أتأكد منه بشكل واضح ضمن منتجاتنا الحالية 🌸 "
                f"حولته للإدارة حتى تتأكدلج وترجع بالج جواب أدق.{review_text}"
            ),
            "intent": "image_check",
            "create_order": False,
            "order": {},
            "confidence": 100,
        }

    if image_result and image_result.get("waiting_for_image"):
        return {
            "reply": "حبيبتي دزيلي صورة الموديل حتى أتأكدلج إذا متوفر عدنا 🌸",
            "intent": "image_check",
            "create_order": False,
            "order": {},
            "confidence": 100,
        }

    if (
        ev.get("text")
        and _requires_linked_product_for_details(ev.get("text"))
        and not matched_product
        and not customer_products
    ):
        return {
            "reply": (
                "ما واضح عندي أي موديل تقصدين حالياً 🌸 "
                "دزيلي صورة المنتج أو اسمه حتى أتأكدلج من السعر والتوفر."
            ),
            "intent": "question",
            "create_order": False,
            "order": {},
            "confidence": 100,
        }

    if not OPENROUTER_KEY:
        print("[MainAI] No API key, escalating to human.", flush=True)
        return {
            "reply": "", "intent": "unknown",
            "create_order": False, "order": {}, "confidence": 0,
            "failed": True, "failure_reason": "no_api_key",
        }

    customer_profile = {k: v for k, v in (customer or {}).items() if k != "id"}
    customer_products = customer_products or []

    transcript_lines = []
    last_customer_text = ""
    last_customer_image = ""
    unanswered_entries = []  # كل رسائل الزبون منذ آخر رد للوكيل (نص/صورة + ختم زمني)
    has_any_outgoing = False
    for m in history:
        is_in = m["direction"] == "incoming"
        speaker = "زبون" if is_in else "وكيل"
        msg_kind = m.get("message_type") or "text"
        ts = (m.get("created_at") or "").strip()
        body = (m.get("text") or "").strip()
        if not body and m.get("image_url"):
            body = "[أرسل صورة]"
        elif not body:
            body = "[رسالة فارغة]"
        line_prefix = f"{speaker}"
        if ts:
            line_prefix += f" ({ts})"
        transcript_lines.append(f"{line_prefix}: {body}")
        if is_in:
            unanswered_entries.append({
                "ts": ts,
                "type": msg_kind,
                "text": body,
                "image_url": m.get("image_url") or "",
            })
            if msg_kind == "image" or m.get("image_url"):
                last_customer_image = m.get("image_url") or last_customer_image
            if body:
                last_customer_text = body
        else:
            has_any_outgoing = True
            unanswered_entries = []
    history_text = "\n".join(transcript_lines) or "لا توجد رسائل سابقة."

    if unanswered_entries:
        unanswered_lines = []
        for idx, item in enumerate(unanswered_entries, 1):
            ts_str = f" [{item['ts']}]" if item["ts"] else ""
            extra = f" (نوع: {item['type']})" if item["type"] != "text" else ""
            img = f"  | صورة: {item['image_url']}" if item["image_url"] else ""
            unanswered_lines.append(f"{idx}){ts_str}{extra} {item['text']}{img}")
        unanswered_block = "\n".join(unanswered_lines)
    else:
        unanswered_block = ev.get("text") or "(لا شيء جديد)"

    is_first_reply = not has_any_outgoing

    if matched_product:
        product_status = (
            "نفذ/غير متوفر" if _is_out_of_stock(matched_product)
            else "متوفر"
        )
    else:
        product_status = "لا يوجد منتج محدد"

    def _short_product(prod, full=False):
        if not isinstance(prod, dict):
            return prod
        keys = (
            "product_id", "product_name", "price", "stock",
            "sizes", "colors", "description", "category",
            "image_url" if full else None,
        )
        return {k: prod.get(k) for k in keys if k}

    matched_short = _short_product(matched_product, full=True) if matched_product else None
    customer_products_short = [_short_product(p) for p in customer_products if p]
    products_short = [
        _short_product(p)
        for p in (products or [])
        if _stock_state(p) == "available"
    ][:30]

    rules_text = "\n".join(f"- {r}" for r in rules_list) if rules_list else "- لا توجد قواعد محظورة."

    if is_first_reply:
        greeting_rule = (
            "هذا أول رد ترسله في هذه المحادثة — لك الحرية في اختيار صيغة الترحيب المناسبة "
            "(هلا حبيبتي، يا هلا، أهلين، نورتينا، تأمرين عيني، ...) بناءً على رسالة الزبون."
        )
    else:
        greeting_rule = (
            "هذا ليس أول رد لك في المحادثة — ممنوع أي ترحيب أو تحية في بداية الرد "
            "(لا تستخدم: هلا، يا هلا، أهلا، السلام، مرحبا، نورتينا). "
            "ابدأ مباشرة بالإجابة بنبرة ودودة (مثل: من عيوني / تدللين / تأمرين)."
        )

    system_prompt = (
        "أنت موظفة مبيعات قصيرة الكلام، عملية، باللهجة العراقية الودودة، في متجر أنيقة للموديلات والقطع النسائية المحتشمة.\n"
        "هدفك الوحيد في كل رد: تقريب الزبون خطوة واحدة من الحجز.\n"
        "أسلوبك: قصير + جذاب + محفّز على المتابعة. تجنب الإطالة لأنها تُطفئ الزبون.\n\n"
        "قواعد عامة صارمة:\n"
        "1) لا تخترع أسعاراً أو مقاسات أو ألواناً ليست في بيانات المنتج.\n"
        "2) ⚠️ طول الرد إلزامي: من جملة إلى جملتين قصيرتين فقط (≤ 25 كلمة). ممنوع الإطالة.\n"
        "3) إذا الرسالة الأخيرة من الزبون عبارة عن صورة بدون منتج مرتبط — لا تخترع موديلاً.\n"
        "4) إذا كلمات مثل 'هذا/هاي/الموديل/سعره/متوفر/قياس/ارجعه/احجز' وردت — اعتبرها تخص آخر منتج محفوظ للزبون.\n"
        "5) إذا المنتج stock فارغ أو 'نفذ' — قل صراحة 'خلص حالياً' ولا تقل متوفر.\n"
        "6) عند سؤال عن الجودة/الفحص/الثقة — أكد أن الفحص عند الاستلام، وإذا غير مطابق يرجع مجاناً.\n"
        "7) عند سؤال عن التوصيل — استخدم نص أجور التوصيل الثابت بالضبط.\n"
        "8) لا تذكر product_id أو ref أو ad_id أو sender_id في الرد.\n"
        "9) إذا اكتملت بيانات الحجز (موبايل واضح + عنوان + منتج متوفر) اجعل create_order=true. ممنوع طلب اسم الزبون للحجز.\n"
        "10) لا تختم بسؤال عام مثل 'شنو تحبين تعرفين عنه؟' — اختم بسؤال بيع واضح يجلب الخطوة التالية مثل 'أحجزه إلج؟' أو 'دزّيلي العنوان والموبايل وأحجزه؟'.\n"
        "11) إذا أرسل الزبون أكثر من رسالة متتالية بدون رد منك بينها، اعتبرها كلها سياقاً واحداً وأجب عنها كلها في ردٍ واحد دون تكرار، وراعِ ترتيبها وآخر معلومة قالها.\n"
        "12) جنس الزبون نفسه (الحقل gender في ملف الزبون):\n"
        "   - 'male' → خاطبه بصيغة المذكر بأسلوب محترم ورسمي (أستاذ/تأمر/تحب/أحجزه إلك/أخوي/تفضّل).\n"
        "     ⚠️ ممنوع منعاً باتاً استخدام كلمة 'حبيبي' أو 'عيني' أو أي كلمة عاطفية مماثلة مع الذكور. استبدلها دائماً بـ 'أستاذ'.\n"
        "   - 'female' → خاطبيها بصيغة المؤنث (تأمرين/تحبين/أحجزه إلج/حبيبتي/عيني/تدللين).\n"
        "   - فارغ أو غير محدد → استخدم صياغة محايدة قدر الإمكان ولا تفترض الجنس ولا تسأل عنه.\n"
        "   لا تخلط الصيغ في نفس الرد، والتزم بالجنس المحدد طوال الرد.\n"
        "13) ممنوع منعاً باتاً ذكر اسم الزبون أو أي جزء منه في نص الرد. خاطبه بصيغ عامة فقط (عيني، حبيبتي، يا هلا، تأمرين).\n"
        "14) ممنوع حصر المتجر بالعبايات أو اللون الأسود. استخدم كلمة الموديل/القطعة عند السؤال العام.\n"
        "15) ⚠️ ممنوع تكرار تفاصيل المنتج (السعر، القياسات، الألوان، الوصف، اسم المنتج الكامل) في كل رد. اذكر فقط ما طلبه الزبون في رسالته الحالية:\n"
        "   - سأل عن السعر فقط؟ → رد بالسعر فقط بدون قياسات أو ألوان.\n"
        "   - سأل عن المقاس فقط؟ → رد بالمقاسات فقط بدون السعر أو الألوان.\n"
        "   - سأل عن اللون فقط؟ → رد بالألوان فقط.\n"
        "   - سأل عن التوصيل؟ → رد بأجور التوصيل فقط.\n"
        "   - لم يسأل عن أي تفاصيل؟ → لا تعرض أي تفاصيل، فقط استمر بالحوار وحفّزه للحجز.\n"
        "   اعرض كل التفاصيل دفعة واحدة فقط عند أول طلب صريح من الزبون لها أو عند تأكيد الحجز.\n"
        "16) ⚠️ ممنوع الردود العاطفية الطويلة أو الأدعية أو المجاملات الزائدة (مثل: 'تسلمين يا طيبة'، 'فدوة لعمرج'، 'أجمعين يا رب'، 'يرزقج كل الخير'، 'تدللين بأي وقت'). يُسمح بكلمة ودّ خفيفة واحدة فقط مثل 'تأمرين' أو 'من عيوني' ضمن نفس الجملة.\n"
        "17) ⚠️ ممنوع إعادة الترحيب عند كل معلومة يقدّمها الزبون (مثل لما يذكر المحافظة أو العمر أو الاسم). لا تقل 'يا هلا بأهل الناصرية' أو 'نورتينا'. تعامل مع المعلومة مباشرة بدون احتفال.\n"
        "18) كل رد لازم يدفع المحادثة للأمام نحو الحجز: إما يطلب معلومة ناقصة (موبايل/عنوان) أو يستفز الرغبة (مثل 'الموديل قاعد ينتظرج، أحجزه؟'). تجنب الردود الميتة التي لا تجلب رد من الزبون.\n"
        "19) ⚠️⚠️ ممنوع منعاً باتاً إرسال قائمة منتجات أو موديلات أو صور إلا إذا الزبون طلب ذلك صراحةً. حلّل المحادثة كاملة أولاً:\n"
        "   - إذا الزبونة سألت سؤالاً عاماً بدون تحديد موديل → اسأليها عن صورة الموديل أو القياس المطلوب، لا تعرضي قائمة إلا إذا طلبت ذلك صراحة.\n"
        "   - إذا الزبون طلب صراحةً ('ورّيني الموديلات' / 'شنو عندكم' / 'عرضي') → اقترح 1-2 موديل فقط بأسلوب طبيعي.\n"
        "   - ممنوع إرسال رسائل مثبتة جاهزة مثل 'لقيت لك X موديل'. كل رد يجب أن يكون مخصصاً لسياق المحادثة.\n"
        "   - ممنوع عرض قائمة مرقّمة بالمنتجات (1. اسم - سعر - قياس). اذكر الموديل بشكل طبيعي ضمن الجملة.\n"
        "20) ⚠️ ممنوع إرسال صور المنتجات إلا إذا الزبون طلب الصورة صراحةً أو وافق على الاقتراح. لا ترسل صورة مع أول اقتراح.\n\n"
        f"قاعدة الترحيب: {greeting_rule}\n\n"
        "تعليمات الإدارة (الأولوية الأعلى بعد القواعد):\n"
        f"{instructions_text or 'لا توجد تعليمات إضافية.'}\n\n"
        "القواعد المحظورة:\n"
        f"{rules_text}\n\n"
        "أجب بـ JSON فقط بدون أي نص آخر:\n"
        "{\n"
        '  "reply": "نص الرد للزبون",\n'
        '  "intent": "question|price|availability|order|image_check|unknown",\n'
        '  "create_order": false,\n'
        '  "order": {"customer_name":"","phone":"","province":"","address":"","product_id":"","product_name":"","color":"","size":"","notes":""},\n'
        '  "confidence": 0\n'
        "}"
    )

    sections = []
    sections.append(
        "[حالة المحادثة]\n"
        f"عدد الرسائل غير المجابة من الزبون: {len(unanswered_entries)}\n"
        f"هل هذا أول رد لنا في المحادثة؟ {'نعم' if is_first_reply else 'لا'}"
    )
    sections.append(
        "[سجل المحادثة الكامل — الأقدم أولاً، الأحدث آخراً]\n" + history_text
    )
    sections.append(
        "[كل رسائل الزبون غير المجابة منذ آخر رد للوكيل — مرتبة من الأقدم للأحدث]\n"
        "أجب عنها كلها في ردٍ واحد متماسك بدون تكرار وبدون تجاهل أي رسالة منها:\n"
        f"{unanswered_block}"
    )
    sections.append(
        "[الرسالة الأخيرة من الزبون (الأحدث ضمن القائمة أعلاه)]\n"
        f"النوع: {message_type}\n"
        f"النص: {ev.get('text') or '[لا يوجد نص]'}\n"
        f"رابط الصورة المرسل الآن: {ev.get('image_url') or 'لا يوجد'}"
    )
    sections.append(
        "[المنتج المرتبط حالياً]\n"
        f"الحالة: {product_status}\n"
        + (json.dumps(matched_short, ensure_ascii=False, indent=2) if matched_short else "(لا يوجد منتج مرتبط)")
    )
    if customer_products_short:
        sections.append(
            "[الموديلات المحفوظة لهذا الزبون — الأحدث أولاً]\n"
            + json.dumps(customer_products_short, ensure_ascii=False, indent=2)
        )
    if products_short:
        sections.append(
            "[AVAILABLE_PRODUCT_CATALOG_ONLY]\n"
            "Use this list only when the customer asks for new product suggestions. "
            "Pick products that match age/size/gender/type/color and never suggest unavailable products or products outside this list.\n"
            + json.dumps(products_short, ensure_ascii=False, indent=2)
        )
    binding_source = (matched_product or {}).get("source") or (matched_product or {}).get("match_method")
    if binding_source == "auto_default_product":
        sections.append(
            "[تنبيه داخلي عن الربط التلقائي]\n"
            "هذا المنتج تم ربطه تلقائيا لأنه المنتج الافتراضي الحالي للحملة. "
            "إذا اعترض الزبون أو قال إنه يقصد منتجا آخر، لا تصر على نفس المنتج واطلب منه إرسال صورة المنتج المطلوب."
        )
    elif binding_source == "image_recognition":
        sections.append(
            "[تنبيه داخلي عن التعرف بالصورة]\n"
            "هذا المنتج تم تحديده من صورة أرسلها الزبون. رد بثقة لكن لا تخترع معلومات غير موجودة في بيانات المنتج."
        )
    if not matched_product and customer_products_short:
        sections.append(
            "[تنبيه]\n"
            "لا يوجد منتج مطابق للرسالة الحالية. ممنوع ذكر سعر أو قياس أو لون أي منتج قبل ربط منتج واضح."
        )
    if catalog_search_context and catalog_search_context.get("matches"):
        search_products = []
        for item in catalog_search_context["matches"]:
            p = item["product"]
            search_products.append({
                "product_id": p.get("product_id"),
                "product_name": p.get("product_name"),
                "price": p.get("price"),
                "sizes": p.get("sizes"),
                "colors": p.get("colors"),
            })
        sections.append(
            "[نتائج بحث المنتجات — للاستخدام الذكي فقط]\n"
            "وجدت هذه المنتجات المطابقة لطلب الزبون. قواعد صارمة:\n"
            "- ⚠️ ممنوع إرسال قائمة كاملة بالمنتجات والأسعار دفعة واحدة.\n"
            "- ⚠️ ممنوع إرسال رسالة مثبتة جاهزة مثل 'لقيت لك 5 موديلات'.\n"
            "- بدلاً من ذلك: حلّل المحادثة واسأل الزبون ما يفضّل بالضبط (العمر؟ اللون؟ النوع؟) ثم اقترح 1-2 موديل فقط بشكل طبيعي ومخصص.\n"
            "- إذا الزبون لم يطلب صراحةً عرض موديلات، لا تعرض أي شيء — فقط تابع الحوار.\n"
            "- إذا الزبون طلب صراحةً ('ورّيني'، 'عرضي'، 'شنو عندكم')، اقترح 1-2 موديل بأسلوب طبيعي محادثاتي قصير.\n"
            + json.dumps(search_products, ensure_ascii=False, indent=2)
        )
    sections.append(
        "[ملف الزبون]\n" + json.dumps(customer_profile, ensure_ascii=False, indent=2)
    )
    if image_result:
        sections.append(
            "[نتيجة تحليل الصورة المرسلة من الزبون]\n"
            + json.dumps(image_result, ensure_ascii=False)
        )
    if last_customer_image and not ev.get("image_url"):
        sections.append(
            "[ملاحظة: الزبون أرسل صورة سابقة في المحادثة]\n"
            f"رابط آخر صورة من الزبون: {last_customer_image}"
        )
    sections.append(
        "[المهمة]\n"
        "- اعتمد فقط على البيانات أعلاه.\n"
        "- اقرأ كل رسائل الزبون غير المجابة معاً وأجب عنها كلها في ردٍ واحد متماسك بترتيب منطقي.\n"
        "- لا تتجاهل أي رسالة منها ولا تُكرّر إجابات نفس النقطة مرتين.\n"
        "- إذا الزبون سأل عدة أسئلة (مثلاً: السعر + التوصيل + المقاس)، اجمع الإجابات في رد واحد قصير.\n"
        "- لا تكرر وصف المنتج إذا الزبون سأل سؤالاً محدداً.\n"
        "- إذا كانت الإجابة تحتاج بيانات الزبون الناقصة (هاتف/محافظة/عنوان) اطلب الناقص فقط بأسلوب ودود، ولا تطلب الاسم.\n"
        f"- التزم بقاعدة الترحيب أعلاه: {'لك حرية اختيار صيغة الترحيب المناسبة لرسالة الزبون.' if is_first_reply else 'لا ترحيب في بداية الرد، ابدأ مباشرة بالإجابة.'}"
    )
    if fix_instruction:
        sections.append("[⚠️ تعليمات تصحيح من المدقق]\n" + fix_instruction)

    user_content = "\n\n".join(sections)
    ai_messages = (
        [{"role": "system", "content": system_prompt}]
        + (conversation_history or [])
        + [{"role": "user", "content": user_content}]
    )

    try:
        if ev.get("image_url") or image_result or matched_product:
            image_flow(
                "10_main_ai_request",
                sender_id=ev.get("sender_id"),
                model=MAIN_MODEL,
                image_url=ev.get("image_url") or last_customer_image,
                matched_product_id=(matched_product or {}).get("product_id"),
                image_result=(image_result or {}).get("reason"),
            )
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": MAIN_MODEL,
                "messages": ai_messages,
                "max_tokens": 1500,
                "temperature": 0.7,
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        if ev.get("image_url") or image_result or matched_product:
            image_flow(
                "11_main_ai_response",
                sender_id=ev.get("sender_id"),
                status_code=resp.status_code,
                matched_product_id=(matched_product or {}).get("product_id"),
                raw_preview=raw,
            )
        print(f"[MainAI] {raw}", flush=True)
        parsed = _parse_ai_json(raw) if isinstance(raw, str) else (raw or {})
        if not isinstance(parsed, dict):
            parsed = {}
        if not (parsed.get("reply") or "").strip():
            print("[MainAI] Empty reply from model, escalating to human.", flush=True)
            parsed["failed"] = True
            parsed["failure_reason"] = parsed.get("failure_reason") or "empty_reply"
            parsed["reply"] = ""
        return parsed
    except Exception as exc:
        if ev.get("image_url") or image_result or matched_product:
            image_flow(
                "11_main_ai_error",
                sender_id=ev.get("sender_id"),
                matched_product_id=(matched_product or {}).get("product_id"),
                error=str(exc),
            )
        print(f"[MainAI] Error: {exc} → escalating to human.", flush=True)
        return {
            "reply": "", "intent": "unknown",
            "create_order": False, "order": {}, "confidence": 0,
            "failed": True, "failure_reason": f"exception:{type(exc).__name__}",
        }


# ── Reply checker ─────────────────────────────────────────────────────────────

def check_reply(
    reply, ev, matched_product, image_result,
    instructions_text, rules_list, history, products,
    customer_products=None,
):
    if not OPENROUTER_KEY:
        return {"approved": True, "problem": "", "fix_instruction": ""}

    history_text = "\n".join(
        f"{'زبون' if m['direction'] == 'incoming' else 'وكيل'}: "
        f"{m.get('text') or '[صورة]'}"
        for m in history
    ) or "لا توجد رسائل سابقة."

    customer_products = customer_products or []

    system_prompt = """أنت مدقق جودة ردود المبيعات. مهمتك ليست كتابة رد للزبون، بل اكتشاف الخطأ وصياغة تعليمات تصحيح دقيقة للموديل الرئيسي.

ارفض الرد إذا:

1. يخترع سعراً غير موجود في بيانات المنتج المرفق.
2. يقول المنتج متوفر بينما stock يقول "نفذ" أو غير متوفر.
3. يذكر اسم منتج خاطئ ليس موجوداً في البيانات المرفقة.
4. يحتوي على حقول تقنية مكشوفة: ad_id, ref, product_id, sender_id.
5. يتجاوز 80 كلمة.
6. يتجاهل المنتج المطابق أو آخر موديل محفوظ عندما تكون رسالة الزبون مثل: "هذا موجود؟"، "الموديل متوفر؟"، "أريده".
7. يرد بتوفر عام للمنتجات بدل حالة المنتج المطابق.

ملاحظات مهمة — لا ترفض الرد إذا:
- رسالة الزبون مجرد تحية (سلام، أهلاً، مرحبا) والرد عبارة عن ترحيب وسؤال عن الخدمة.
- الرد يطلب توضيح اسم المنتج لأن الزبون لم يحدده بعد.
- لا يوجد منتج محدد ولا معلومات سابقة كافية للرد.
- الرد ودود ويسأل عن المنتج المطلوب.

إذا رفضت، اكتب fix_instruction كتعليمة مباشرة للموديل الرئيسي، مثلاً:
"المنتج المطابق stock=نفذ، أعد صياغة الرد باللهجة العراقية وقل للزبونة أنه خلص حالياً ولا تقل متوفر."

أجب بـ JSON فقط بدون أي نص آخر:
{"approved":true,"problem":"","fix_instruction":""}"""

    user_content = (
        f"رسالة الزبون: {ev.get('text') or '[صورة]'}\n"
        f"الرد المقترح: {reply}\n"
        f"المنتج: {json.dumps(matched_product, ensure_ascii=False) if matched_product else 'لا يوجد'}\n"
        f"تحليل الصورة: {json.dumps(image_result, ensure_ascii=False) if image_result else 'لا يوجد'}\n"
        f"موديلات محفوظة للزبون: {json.dumps(customer_products, ensure_ascii=False)}\n"
        f"سجل المحادثة:\n{history_text}\n"
        f"تعليمات النظام: {instructions_text or 'لا توجد'}\n"
        f"القواعد المحظورة: {json.dumps(rules_list, ensure_ascii=False)}"
    )

    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": CHECKER_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_content},
                ],
                "max_tokens": 300,
                "temperature": 0.2,
            },
            timeout=20,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        print(f"[Checker] {raw}", flush=True)
        return _parse_ai_json(raw)
    except Exception as exc:
        print(f"[Checker] Error: {exc}", flush=True)
        return {"approved": True, "problem": "", "fix_instruction": ""}


# ── Order creation ────────────────────────────────────────────────────────────

def create_order_if_valid(db, sender_id, ai_result, matched_product):
    order_data = ai_result.get("order") or {}
    phone   = (order_data.get("phone")   or "").strip()
    address = (order_data.get("address") or "").strip()

    if not phone or not address:
        print("[Order] Missing phone/address, not creating order.", flush=True)
        return None, "لو سمحتِ أرسلي رقم هاتفك وعنوانك الكامل لإتمام الطلب 🌸"

    now          = now_baghdad_iso()
    product_id   = order_data.get("product_id")   or (matched_product or {}).get("product_id",   "")
    product_name = order_data.get("product_name") or (matched_product or {}).get("product_name", "")
    if not product_id and not product_name:
        print("[Order] Missing linked product, not creating order.", flush=True)
        return None, "لازم أحدد المنتج أولاً حتى أثبت الطلب. دزيلي صورة/اسم الموديل 🌸"

    duplicate = find_duplicate_order(db, sender_id, phone, product_id, address)
    if duplicate:
        print(f"[Order] Duplicate skipped for {sender_id}: existing #{duplicate.get('id')}", flush=True)
        return True, ORDER_CONFIRMATION_TEXT

    booking_data = {
        "created_at": now,
        "sender_id": sender_id,
        "customer_name": order_data.get("customer_name", ""),
        "phone": phone,
        "province": order_data.get("province", ""),
        "address": address,
        "product_id": product_id,
        "product_name": product_name,
        "color": order_data.get("color", ""),
        "size": order_data.get("size", ""),
        "notes": order_data.get("notes", ""),
        "status": "new",
    }

    db.execute(
        """INSERT INTO orders
           (sender_id, customer_name, phone, province, address,
            product_id, product_name, color, size, notes, status, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            sender_id,
            order_data.get("customer_name", ""),
            phone,
            order_data.get("province", ""),
            address,
            product_id,
            product_name,
            order_data.get("color", ""),
            order_data.get("size",  ""),
            order_data.get("notes", ""),
            "new",
            now,
        ),
    )

    db.execute(
        """UPDATE customers SET
           phone    = CASE WHEN ? != '' THEN ? ELSE phone    END,
           address  = CASE WHEN ? != '' THEN ? ELSE address  END,
           province = CASE WHEN ? != '' THEN ? ELSE province END
           WHERE sender_id=?""",
        (
            phone,   phone,
            address, address,
            order_data.get("province", ""), order_data.get("province", ""),
            sender_id,
        ),
    )
    db.commit()

    try:
        if get_followup_settings(db).get("stop_on_order"):
            cancel_followups_for_sender(db, sender_id, "order_created")
    except Exception as exc:
        print(f"[FollowUp] Could not cancel followups after order: {exc}", flush=True)

    save_booking_to_file(booking_data)
    telegram_sent = send_order_to_telegram(booking_data)

    print(f"[Order] Created for {sender_id}: {product_name} | telegram_sent={telegram_sent}", flush=True)
    return True, ORDER_CONFIRMATION_TEXT


# ── Main webhook processor ────────────────────────────────────────────────────

def process_webhook(db, body, use_debounce: bool = True, send_direct_facebook_images: bool = True):
    t_start = time.time()

    # ── STEP 01: Incoming message ─────────────────────────────────────────────
    log_sep("NEW INCOMING MESSAGE")
    log(1, "RAW PAYLOAD", "Incoming raw payload from n8n / Facebook", body)

    # ── STEP 02: Event extraction ─────────────────────────────────────────────
    log(2, "EXTRACT", "Extracting event data from payload...")
    ev = extract_facebook_event(body)

    # ── Deduplication: avoid processing the same message twice ────────────────
    mid = (body.get("entry", [{}])[0]
               .get("messaging", [{}])[0]
               .get("message", {})
               .get("mid", ""))
    if mid:
        existing = db.execute("SELECT mid FROM processed_messages WHERE mid=?", (mid,)).fetchone()
        if existing:
            log(2, "DEDUP", f"Duplicate message detected (mid={mid[:30]}...). Skipping.")
            return {
                "sender_id": ev["sender_id"],
                "reply": "",
                "debug": {"skipped": True, "reason": "duplicate_mid"},
            }
        db.execute(
            "INSERT INTO processed_messages (mid, processed_at) VALUES (?,?)",
            (mid, now_baghdad_iso())
        )
        db.commit()
    log(2, "EXTRACT", "Event data extracted successfully", {
        "sender_id"      : ev["sender_id"],
        "page_id"        : ev["page_id"],
        "platform"       : ev["platform"],
        "timestamp"      : ev["timestamp"],
        "text"           : ev["text"],
        "image_url"      : ev["image_url"],
        "has_postback"   : bool(ev["postback"]),
        "has_quick_reply": bool(ev["quick_reply"]),
        "ref"            : ev["ref"],
        "ad_id"          : ev["ad_id"],
        "referral_source": ev["referral_source"],
        "referral_type"  : ev["referral_type"],
    })

    # ── STEP 03: Message type ─────────────────────────────────────────────────
    message_type = detect_message_type(ev)
    log(3, "MSG TYPE", f"Message type = {message_type!r}")
    if message_type == "image":
        image_flow(
            "02_message_type_detected",
            sender_id=ev["sender_id"],
            message_type=message_type,
            image_url=ev.get("image_url"),
        )

    # ── STEP 04: Customer data ────────────────────────────────────────────────
    log(4, "CUSTOMER", "Looking up or creating customer in database...")
    customer = get_or_create_customer(db, ev["sender_id"], ev["page_id"], ev["platform"])
    log(4, "CUSTOMER", "Customer data", {
        k: v for k, v in customer.items() if k != "id"
    })

    # ── STEP 05: Save incoming message ────────────────────────────────────────
    log(5, "SAVE MSG", "Saving incoming message to database...")
    conversation_history = get_conversation_history(db, ev["sender_id"], limit=10)
    save_message(
        db, ev["sender_id"], "incoming", message_type,
        ev["text"], ev["image_url"], ev["ad_id"], ev["ref"], body,
    )
    save_conversation_message(db, ev["sender_id"], "user", ev["text"])
    log(5, "SAVE MSG", "Incoming message saved")
    if message_type == "image":
        image_flow(
            "03_saved_incoming_image",
            sender_id=ev["sender_id"],
            image_url=ev.get("image_url"),
            ad_id=ev.get("ad_id"),
            ref=ev.get("ref"),
        )

    # إيقاف AI العام/للمحادثة يجب أن يمنع أي رد تلقائي، بما فيه ترحيب الصور.
    if not is_ai_enabled(db) or not is_customer_ai_enabled(db, ev["sender_id"]):
        reason = "ai_globally_disabled" if not is_ai_enabled(db) else "ai_disabled_for_conversation"
        log(5, "AI DISABLED", f"AI disabled ({reason}). Incoming message saved; no automatic reply.")

        if message_type == "image" and HUMAN_REVIEW_ALL_IMAGES:
            products_for_match = load_active_products(db)
            matched_product, match_method, image_result = match_product(db, ev, products_for_match)
            if matched_product:
                log(5, "CATALOG VISION", "Customer image linked while AI was disabled; no human review created", {
                    "product_id": matched_product.get("product_id"),
                    "match_method": match_method,
                    "ai_disabled_reason": reason,
                })
                auto_reply = auto_reply_after_product_link(
                    db, ev["sender_id"], matched_product,
                    conversation_history=conversation_history,
                )
                return {
                    "sender_id": ev["sender_id"],
                    "page_id": ev["page_id"],
                    "platform": ev["platform"],
                    "reply": "",
                    "send_image": False,
                    "debug": {
                        "skipped": True,
                        "reason": reason,
                        "catalog_match": True,
                        "product_id": matched_product.get("product_id"),
                        "match_method": match_method,
                        "auto_reply": auto_reply,
                    },
                }

        # صمام أمان: لو لا توجد مراجعة بشرية معلقة، أنشئ واحدة لكي لا تختفي المحادثة عن لوحة التدخل
        try:
            existing_review = has_pending_human_review(db, ev["sender_id"])
        except Exception as exc:
            existing_review = None
            print(f"[AI DISABLED] Could not check pending review: {exc}", flush=True)
        review_id = existing_review
        if not existing_review:
            try:
                review_id = create_human_review(
                    db, ev,
                    f"AI متوقف ({reason}); رسالة جديدة من الزبون تحتاج تدخل بشري",
                    build_product_vision_candidates(load_active_products(db), limit=10),
                )
                log(5, "AI DISABLED", f"Created human review #{review_id} so the conversation surfaces for human action")
            except Exception as exc:
                print(f"[AI DISABLED] Could not create fallback review: {exc}", flush=True)
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "platform": ev["platform"],
            "reply": "",
            "send_image": False,
            "debug": {"skipped": True, "reason": reason, "human_review_id": review_id},
        }

    # ── STEP 05.5: Debounce — انتظر حتى ينتهي الزبون من إرسال رسائله ────────
    # نهدف إلى جمع كل ما يرسله الزبون (نص + صورة + رسائل متفرقة) قبل تشغيل الموديل
    # حتى يفهم السياق كاملاً ويعطي رداً واحداً دقيقاً بدلاً من ردود مكررة.
    if use_debounce and DEBOUNCE_DELAY > 0 and message_type != "image":
        saved_at = now_baghdad_iso()
        log(5, "DEBOUNCE", f"Waiting {DEBOUNCE_DELAY}s to collect all customer messages before AI...")
        time.sleep(DEBOUNCE_DELAY)
        # هل وصلت رسائل أحدث خلال فترة الانتظار؟
        newer_rows = db.execute(
            "SELECT id, message_type FROM messages WHERE sender_id=? AND direction='incoming' AND created_at > ?",
            (ev["sender_id"], saved_at),
        ).fetchall()
        newer = newer_rows[0] if newer_rows else None
        if newer_rows:
            log(5, "DEBOUNCE", f"Collected {len(newer_rows)} additional message(s) during wait — will be processed by the latest webhook.")
        if newer:
            if message_type == "image" and HUMAN_REVIEW_ALL_IMAGES:
                products_for_review = load_active_products(db)
                matched_product, match_method, image_result = match_product(db, ev, products_for_review)
                if matched_product:
                    log(5, "DEBOUNCE/IMAGE", "Older image auto-linked before newer event processing", {
                        "product_id": matched_product.get("product_id"),
                        "match_method": match_method,
                    })
                else:
                    review_id = create_human_review(
                        db,
                        ev,
                        "Customer image arrived before newer text; routed to human review without customer reply",
                        (image_result or {}).get("human_review_candidates") or build_product_vision_candidates(products_for_review, limit=20),
                    )
                    log(
                        5,
                        "DEBOUNCE/IMAGE",
                        f"Image sent to Telegram review #{review_id} before skipping older event.",
                    )
            log(5, "DEBOUNCE", "Newer message detected — skipping this one, will process with the latest.")
            return {
                "sender_id": ev["sender_id"],
                "reply": "",
                "debug": {"skipped": True, "reason": "debounced_newer_message"},
            }
        log(5, "DEBOUNCE", "No newer message — proceeding with processing.")

    # ── STEP 06: Load conversation history ───────────────────────────────────
    log(6, "HISTORY", "Loading latest customer messages...")
    history = load_history(db, ev["sender_id"])
    log(6, "HISTORY", f"Loaded {len(history)} previous messages")

    # ── STEP 07: Load products ────────────────────────────────────────────────
    log(7, "PRODUCTS", "Loading active products from products.json...")
    products = load_active_products(db)
    log(7, "PRODUCTS", f"Active products count = {len(products)}",
        [{"product_id": p["product_id"], "product_name": p["product_name"], "stock": p["stock"]}
         for p in products])

    customer_products = load_customer_products(db, ev["sender_id"])
    first_incoming = incoming_message_count(db, ev["sender_id"]) == 1
    active_binding = get_active_product_binding(db, ev["sender_id"])

    if message_type == "image" and active_binding and active_binding.get("source") == "auto_default_product":
        reject_current_binding(db, ev["sender_id"], "customer_sent_image_after_auto_default")
        customer_products = load_customer_products(db, ev["sender_id"])
        active_binding = get_active_product_binding(db, ev["sender_id"])

    if (
        message_type != "image"
        and active_binding
        and active_binding.get("source") == "auto_default_product"
        and is_product_objection(ev.get("text"))
    ):
        reject_current_binding(db, ev["sender_id"], ev.get("text"))
        reply = "تمام حبيبتي، حتى أحددلج نفس الموديل بالضبط أرسلي صورة المنتج اللي تقصدينه."
        save_message(
            db, ev["sender_id"], "outgoing", "text",
            reply, None, None, None,
            {"auto_default_product_rejected": True},
        )
        save_conversation_message(db, ev["sender_id"], "assistant", reply)
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "platform": ev["platform"],
            "reply": reply,
            "send_image": False,
            "debug": {
                "skipped": True,
                "reason": "auto_default_product_rejected",
            },
        }

    # بحث المنتجات المتاحة — النتائج تُحفظ كسياق يُمرر للـ AI لاحقاً
    # لا يتم إرسال أي قائمة أو صور تلقائياً هنا
    _catalog_search_matches = []
    _catalog_search_request_info = {}
    if message_type != "image" and ev.get("text"):
        search_result = search_available_products_for_request(ev.get("text"), products, limit=5)
        request_info = search_result.get("request") or {}
        matches = search_result.get("matches") or []
        skip_search_for_current_context = bool(
            customer_products and _is_contextual_product_question(ev.get("text", ""))
        )
        if request_info.get("is_search") and not skip_search_for_current_context and matches:
            remember_product_search_results(db, ev["sender_id"], matches, request_info)
            customer_products = load_customer_products(db, ev["sender_id"])
            _catalog_search_matches = matches
            _catalog_search_request_info = request_info
            log(8, "PRODUCT SEARCH", "Found matching products — will pass to AI for personalized response", {
                "query": ev.get("text"),
                "request": request_info,
                "product_ids": [item["product"].get("product_id") for item in matches],
            })
        elif request_info.get("is_search") and not matches:
            log(8, "PRODUCT SEARCH", "No available products matched customer request", {
                "query": ev.get("text"),
                "request": request_info,
            })

    if should_use_auto_product(db, ev["sender_id"], ev, message_type, customer_products):
        auto_settings = get_auto_product_settings(db)
        auto_product = auto_settings.get("product")
        active_binding = bind_customer_to_product(
            db,
            ev["sender_id"],
            auto_product,
            source="auto_default_product",
            confidence=100,
            notes="Automatically linked from auto_product_id setting",
        )
        customer_products = load_customer_products(db, ev["sender_id"])
        send_product_image_if_available(
            db,
            ev["sender_id"],
            ev["page_id"],
            ev["platform"],
            auto_product,
            binding=active_binding,
        )

    if first_incoming and not customer_products and message_type != "image":
        # نحمّل التعليمات والقواعد مبكراً للترحيب الديناميكي بأول رسالة
        first_instructions, first_rules = load_ai_config(db, sender_id=ev["sender_id"])
        review_id = create_human_review(
            db,
            ev,
            "First customer message needs human product selection",
            build_product_vision_candidates(products, limit=20),
        )
        first_reply_text, detected_gender = generate_first_message_reply(
            db, ev, products, first_instructions, first_rules,
        )
        save_message(
            db, ev["sender_id"], "outgoing", "text",
            first_reply_text, None, None, None,
            {
                "human_review_id": review_id,
                "first_message_reply": True,
                "detected_gender": detected_gender,
            },
        )
        save_conversation_message(db, ev["sender_id"], "assistant", first_reply_text)
        # نخزّن الجنس المكتشف في profile الزبون لاستخدامه في الردود اللاحقة
        try:
            if detected_gender in ("boy", "girl"):
                db.execute(
                    "UPDATE customers SET notes = COALESCE(notes,'') || ? WHERE sender_id=?",
                    (f" | child_gender={detected_gender}",
                     ev["sender_id"]),
                )
                db.commit()
        except Exception as exc:
            print(f"[FirstMsg] Could not save detected gender: {exc}", flush=True)
        log(8, "FIRST MESSAGE", f"First message routed to review #{review_id}; AI greeting (gender={detected_gender}) sent.")
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "platform": ev["platform"],
            "reply": first_reply_text,
            "send_image": False,
            "debug": {
                "skipped": True,
                "reason": "first_message_waiting_for_human_product",
                "human_review_id": review_id,
                "detected_gender": detected_gender,
            },
        }

    if message_type == "image" and HUMAN_REVIEW_ALL_IMAGES:
        image_flow("04_before_image_matching", sender_id=ev["sender_id"], image_url=ev.get("image_url"))
        matched_product, match_method, image_result = match_product(db, ev, products)
        if matched_product:
            image_flow(
                "09_no_human_review_auto_reply_start",
                sender_id=ev["sender_id"],
                product_id=matched_product.get("product_id"),
                match_method=match_method,
            )
            log(8, "CATALOG VISION", "Customer image linked automatically; no human review created", {
                "product_id": matched_product.get("product_id"),
                "match_method": match_method,
            })
            auto_reply = auto_reply_after_product_link(
                db, ev["sender_id"], matched_product,
                conversation_history=conversation_history,
            )
            image_flow(
                "12_auto_reply_finished",
                sender_id=ev["sender_id"],
                product_id=matched_product.get("product_id"),
                sent=(auto_reply or {}).get("sent"),
                reason=(auto_reply or {}).get("reason"),
                reply_preview=(auto_reply or {}).get("reply", "")[:120],
            )
            return {
                "sender_id": ev["sender_id"],
                "page_id": ev["page_id"],
                "platform": ev["platform"],
                "reply": "",
                "send_image": False,
                "debug": {
                    "message_type": message_type,
                    "catalog_match": True,
                    "product_id": matched_product.get("product_id"),
                    "match_method": match_method,
                    "auto_reply": auto_reply,
                },
            }

        existing_review = has_pending_image_review(db, ev["sender_id"]) or has_pending_human_review(db, ev["sender_id"])
        if existing_review and has_sent_pending_image_reply(db, ev["sender_id"], existing_review):
            log(8, "HUMAN REVIEW", f"Image already has pending review #{existing_review}; customer was asked size/age before.")
            return {
                "sender_id": ev["sender_id"],
                "page_id": ev["page_id"],
                "platform": ev["platform"],
                "reply": "",
                "send_image": False,
                "debug": {
                    "message_type": message_type,
                    "skipped": True,
                    "reason": "pending_image_reply_already_sent",
                    "human_review_id": existing_review,
                },
            }
        candidates = (image_result or {}).get("human_review_candidates") or build_product_vision_candidates(products, limit=20)
        image_flow(
            "09_human_review_required",
            sender_id=ev["sender_id"],
            candidates_count=len(candidates),
            reason=(image_result or {}).get("reason"),
        )
        review_id = create_human_review(
            db,
            ev,
            "All customer images are routed to human Telegram review",
            candidates,
        )
        # عند وصول صورة من الزبون: إيقاف AI لهذه المحادثة + لا يُرسل أي رد للزبون
        try:
            set_customer_ai_enabled(db, ev["sender_id"], False)
            log(8, "AI PAUSE", f"Auto-paused AI for {ev['sender_id']} until human review #{review_id}")
        except Exception as exc:
            log(8, "AI PAUSE", f"Failed to auto-pause AI: {exc}")
        log(8, "HUMAN REVIEW", f"Image routed to review #{review_id}. NO auto reply sent — waiting for human action.")
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "platform": ev["platform"],
            "reply": "",
            "send_image": False,
            "debug": {
                "message_type": message_type,
                "human_review_id": review_id,
                "routed_to_telegram": True,
                "waiting_for_human_action": True,
                "ai_paused": True,
            },
        }

    # إذا توجد صورة تنتظر مراجعة بشرية ولم يتم ربط منتج بعد:
    # نسكت تماماً ونحفظ رسائل الزبون فقط — لا نرسل أي رد تلقائي إطلاقاً
    # حتى يتدخل الإنسان (يربط المنتج أو يرد يدوياً).
    pending_review_id = has_pending_image_review(db, ev["sender_id"])
    if pending_review_id:
        remembered = load_customer_products(db, ev["sender_id"], limit=1)
        if not remembered:
            log(8, "PENDING IMAGE", f"Image-related conversation #{pending_review_id} silent — waiting for human action.")
            return {
                "sender_id": ev["sender_id"],
                "page_id": ev["page_id"],
                "platform": ev["platform"],
                "reply": "",
                "send_image": False,
                "debug": {
                    "skipped": True,
                    "reason": "image_pending_human_review_silent",
                    "human_review_id": pending_review_id,
                },
            }

    # إذا توجد أي صورة سابقة في المحادثة ولم يحدد الموظف المنتج بعد:
    # نسكت تماماً ونضمن وجود مراجعة بشرية معلقة (بدون أي رسالة للزبون).
    if has_any_customer_image(db, ev["sender_id"]) and not load_customer_products(db, ev["sender_id"], limit=1):
        existing_review = has_pending_human_review(db, ev["sender_id"])
        if not existing_review:
            existing_review = create_human_review(
                db,
                ev,
                "Conversation has a customer image but no product is manually linked yet",
                build_product_vision_candidates(products, limit=20),
            )
        log(8, "IMAGE HISTORY", f"Conversation has image; silent until product link / manual reply (review #{existing_review}).")
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "platform": ev["platform"],
            "reply": "",
            "send_image": False,
            "debug": {
                "skipped": True,
                "reason": "conversation_image_silent_until_human",
                "human_review_id": existing_review,
            },
        }

    pending_manual_review = has_pending_human_review(db, ev["sender_id"])
    if pending_manual_review and not customer_products:
        log(8, "PENDING REVIEW", f"Customer has pending review #{pending_manual_review}; waiting silently for human product link.")
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "platform": ev["platform"],
            "reply": "",
            "send_image": False,
            "debug": {
                "skipped": True,
                "reason": "pending_manual_review_waiting_silently",
                "human_review_id": pending_manual_review,
            },
        }

    # ── STEP 08: Manual product context only ──────────────────────────────────
    # لا نربط المنتجات تلقائياً من ref/ad_id/text. الربط يجب أن يكون يدوياً من الداشبورد.
    matched_product = None
    match_method = None
    image_result = None
    if customer_products:
        matched_product = select_customer_context_product(ev.get("text", ""), customer_products) or customer_products[0]
        match_method = matched_product.get("match_method") or "manual"
        log(8, "MANUAL PRODUCT", "Using manually linked product", {
            "product_id"  : matched_product.get("product_id"),
            "product_name": matched_product.get("product_name"),
            "match_method": match_method,
        })
    elif ev.get("ref") or ev.get("ad_id"):
        existing_review = has_pending_human_review(db, ev["sender_id"])
        if not existing_review:
            existing_review = create_human_review(
                db,
                ev,
                "Customer came from ad/ref but no product is manually linked yet",
                build_product_vision_candidates(products, limit=20),
            )
        log(8, "MANUAL PRODUCT", "Ad/ref received; waiting for manual product link", {
            "human_review_id": existing_review,
            "ref": ev.get("ref"),
            "ad_id": ev.get("ad_id"),
        })
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "platform": ev["platform"],
            "reply": "",
            "send_image": False,
            "debug": {
                "skipped": True,
                "reason": "manual_product_link_required",
                "human_review_id": existing_review,
                "ref": ev.get("ref"),
                "ad_id": ev.get("ad_id"),
            },
        }
    else:
        log(8, "MANUAL PRODUCT", "No manually linked product for this customer")
    log(8, "CUSTOMER PRODUCTS", f"Remembered customer products count = {len(customer_products)}", [
        {
            "product_id": p.get("product_id"),
            "product_name": p.get("product_name"),
            "stock": p.get("stock"),
            "last_seen_at": p.get("last_seen_at"),
        }
        for p in customer_products
    ])

    # ── STEP 09: AI instructions and rules ───────────────────────────────────
    log(9, "AI CONFIG", "Loading AI instructions and forbidden rules...")
    instructions_text, rules_list = load_ai_config(db, sender_id=ev["sender_id"])
    log(9, "AI CONFIG", f"Loaded {len(instructions_text.splitlines())} instruction lines | {len(rules_list)} forbidden rules")

    # إذا طلب الزبون صورة/تفاصيل، ربط المنتج من الذاكرة إن لم يكن محدداً
    _image_requested = _is_product_info_request(ev.get("text", ""))
    if not matched_product and _image_requested and customer_products:
        matched_product = select_customer_context_product(ev.get("text", ""), customer_products) or customer_products[0]
        log(10, "PRODUCT INFO", "Using remembered product for image/details request", {
            "product_id": matched_product.get("product_id"),
        })

    # ربط حتمي للسياق: "شكد سعره/هذا متوفر/يلبس 2 سنة/ارجعه" تعني آخر منتج محفوظ.
    if (
        not matched_product
        and customer_products
        and _is_contextual_product_question(ev.get("text", ""))
    ):
        matched_product = select_customer_context_product(ev.get("text", ""), customer_products) or customer_products[0]
        log(10, "PRODUCT CONTEXT", "Using remembered product for contextual question", {
            "product_id": matched_product.get("product_id"),
            "text": ev.get("text"),
        })

    # ── STEP 09.5: Local intent router (no external AI call) ────────────────
    routing = determine_intent(ev, customer_products, matched_product)
    log(9, "INTENT", "Local intent decision", routing)

    if routing.get("intent") == "complaint":
        try:
            problem_id, problem_created = create_problem_report(
                db,
                ev,
                routing.get("problem_reason") or routing.get("reason") or "مشكلة من الزبون",
                matched_product=matched_product,
            )
            problem = db.execute("SELECT * FROM problem_reports WHERE id=?", (problem_id,)).fetchone()
            if problem and problem_created:
                send_problem_to_telegram(dict(problem))
        except Exception as exc:
            print(f"[Problems] Could not create/send problem report: {exc}", flush=True)

    # إذا الزبون لديه سياق منتج → ربط أقوى من الذاكرة
    if routing.get("has_product_context") and not matched_product and customer_products:
        matched_product = select_customer_context_product(ev.get("text", ""), customer_products) or customer_products[0]
        log(9, "ROUTER", f"Router confirmed product context → using {matched_product.get('product_id')}")

    # إذا طلب الكتالوج → سيتولاه الـ Main AI بناءً على routing
    # إذا يحتاج تدخل بشري → لا رد
    if routing.get("needs_human") and not matched_product:
        log(9, "ROUTER", "Router decided: needs human intervention. No auto reply.")
        review_id = create_human_review(
            db,
            ev,
            f"Router requested human intervention: {routing.get('reason') or 'unknown'}",
            build_product_vision_candidates(products, limit=10) if products else [],
        )
        return {
            "sender_id": ev["sender_id"],
            "reply": "",
            "debug": {
                "skipped": True,
                "reason": "router_needs_human",
                "human_review_id": review_id,
                "routing": routing,
            },
        }

    if routing.get("wants_catalog"):
        image_messages = build_catalog_image_messages(products)
        image_urls = [m["url"] for m in image_messages]
        for image_url in image_urls:
            save_message(
                db, ev["sender_id"], "outgoing", "image",
                None, image_url, None, None,
                {"catalog": True, "image_url": image_url},
            )
        log(9, "CATALOG", f"Catalog requested; prepared {len(image_urls)} images.")
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "platform": ev["platform"],
            "reply": "",
            "image_url": image_urls[0] if image_urls else "",
            "product_image_url": image_urls[0] if image_urls else "",
            "image_urls": image_urls,
            "product_image_urls": image_urls,
            "send_image": bool(image_urls),
            "attachments": image_messages,
            "messages": image_messages,
            "debug": {"catalog": True, "image_count": len(image_urls), "routing": routing},
        }

    # ── STEP 10: Main AI call ────────────────────────────────────────────────
    log(10, "MAIN AI", f"Sending request to {MAIN_MODEL}...")
    t_ai = time.time()
    _search_ctx = None
    if _catalog_search_matches:
        _search_ctx = {"matches": _catalog_search_matches, "request": _catalog_search_request_info}
    ai_result = call_main_ai(
        ev, message_type, customer, history, products,
        matched_product, image_result, instructions_text, rules_list,
        customer_products=customer_products,
        conversation_history=conversation_history,
        catalog_search_context=_search_ctx,
    )
    log(10, "MAIN AI", f"Model responded in {time.time()-t_ai:.1f}s", {
        "intent"      : ai_result.get("intent"),
        "create_order": ai_result.get("create_order"),
        "confidence"  : ai_result.get("confidence"),
        "reply"       : ai_result.get("reply"),
        "failed"      : ai_result.get("failed"),
    })

    # If the model couldn't produce a reply (no key / exception / empty),
    # escalate to a human and pause AI for this conversation.
    if ai_result.get("failed"):
        failure_reason = ai_result.get("failure_reason") or "ai_failed"
        try:
            set_customer_ai_enabled(db, ev["sender_id"], False)
        except Exception as exc:
            print(f"[MainAI] Could not pause AI for sender after failure: {exc}", flush=True)
        review_id = create_human_review(
            db, ev,
            f"AI could not produce a reply ({failure_reason}); human action required",
            build_product_vision_candidates(products, limit=10) if products else [],
        )
        log(10, "MAIN AI", "AI failed → routed to human review, AI paused", {
            "human_review_id": review_id,
            "failure_reason": failure_reason,
        })
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "reply": "",
            "send_image": False,
            "debug": {
                "skipped": True,
                "reason": "ai_failed_handoff_to_human",
                "human_review_id": review_id,
                "failure_reason": failure_reason,
            },
        }

    if matched_product:
        link_source = (matched_product or {}).get("source") or ""
        if match_method == "image_recognition":
            link_source = "image_recognition"
        elif match_method in {"ref", "ad_id"}:
            link_source = "ad_ref"
        elif match_method == "manual":
            link_source = "manual_admin"
        complete_customer_product_link(
            db,
            ev["sender_id"],
            matched_product,
            match_method or matched_product.get("match_method") or "matched_product_context",
            confidence=matched_product.get("confidence") or (image_result or {}).get("confidence") or 100,
            source=link_source or "process_webhook_after_main_ai",
        )

    reply = ai_result.get("reply") or FALLBACK_REPLY
    if is_ai_handoff_reply(reply):
        try:
            set_customer_ai_enabled(db, ev["sender_id"], False)
        except Exception as exc:
            print(f"[MainAI] Could not pause AI on handoff reply: {exc}", flush=True)
        review_id = create_human_review(
            db,
            ev,
            "AI produced a handoff/apology reply; suppressed customer message",
            build_product_vision_candidates(products, limit=10) if products else [],
        )
        log(10, "MAIN AI", "Suppressed handoff reply and created human review", {
            "human_review_id": review_id,
            "reply": reply,
        })
        return {
            "sender_id": ev["sender_id"],
            "page_id": ev["page_id"],
            "reply": "",
            "send_image": False,
            "debug": {
                "skipped": True,
                "reason": "ai_handoff_reply_suppressed",
                "human_review_id": review_id,
            },
        }

    # ── STEP 11: Reply checker ───────────────────────────────────────────────
    checker = {"approved": True, "problem": "", "fix_instruction": ""}
    checker_approved = True
    if not CHECKER_ENABLED:
        log(11, "CHECKER", "Checker is disabled. Main model reply will be sent directly.")
    else:
        local_checker = local_reply_validation(reply, matched_product, customer_products)
        if not local_checker.get("approved", True):
            checker = local_checker
            checker_approved = False
            log(11, "LOCAL CHECKER", "Local validation rejected the reply before AI checker", checker)
        else:
            log(11, "CHECKER", f"Sending reply to {CHECKER_MODEL} for validation...")
            t_chk = time.time()
            checker = check_reply(
                reply, ev, matched_product, image_result,
                instructions_text, rules_list, history, products,
                customer_products=customer_products,
            )
            checker_approved = checker.get("approved", True)
            log(11, "CHECKER", f"Checker responded in {time.time()-t_chk:.1f}s | approved={checker_approved}", {
                "approved"       : checker_approved,
                "problem"        : checker.get("problem"),
                "fix_instruction": checker.get("fix_instruction"),
            })

    # ── STEP 12: Retry rejected reply ────────────────────────────────────────
    if not checker_approved:
        fix = checker.get("fix_instruction", "")
        log(12, "RETRY", "Reply rejected. Retrying with correction instructions.")
        log(12, "RETRY", f"Problem: {checker.get('problem')}")

        ai_result = call_main_ai(
            ev, message_type, customer, history, products,
            matched_product, image_result, instructions_text, rules_list,
            fix_instruction=fix, customer_products=customer_products,
            conversation_history=conversation_history,
            catalog_search_context=_search_ctx,
        )
        if ai_result.get("failed"):
            failure_reason = ai_result.get("failure_reason") or "ai_failed_on_retry"
            try:
                set_customer_ai_enabled(db, ev["sender_id"], False)
            except Exception as exc:
                print(f"[MainAI] Could not pause AI after retry failure: {exc}", flush=True)
            review_id = create_human_review(
                db, ev,
                f"AI retry could not produce a reply ({failure_reason}); human action required",
                build_product_vision_candidates(products, limit=10) if products else [],
            )
            log(12, "RETRY", "AI retry failed → human review, AI paused", {
                "human_review_id": review_id,
                "failure_reason": failure_reason,
            })
            return {
                "sender_id": ev["sender_id"],
                "page_id": ev["page_id"],
                "reply": "",
                "send_image": False,
                "debug": {
                    "skipped": True,
                    "reason": "ai_retry_failed_handoff_to_human",
                    "human_review_id": review_id,
                    "failure_reason": failure_reason,
                },
            }
        reply = ai_result.get("reply") or FALLBACK_REPLY
        if is_ai_handoff_reply(reply):
            try:
                set_customer_ai_enabled(db, ev["sender_id"], False)
            except Exception as exc:
                print(f"[MainAI] Could not pause AI on retry handoff: {exc}", flush=True)
            review_id = create_human_review(
                db,
                ev,
                "AI retry produced a handoff/apology reply; suppressed customer message",
                build_product_vision_candidates(products, limit=10) if products else [],
            )
            log(12, "RETRY", "Suppressed handoff retry and created human review", {
                "human_review_id": review_id,
                "reply": reply,
            })
            return {
                "sender_id": ev["sender_id"],
                "page_id": ev["page_id"],
                "reply": "",
                "send_image": False,
                "debug": {
                    "skipped": True,
                    "reason": "ai_handoff_reply_suppressed",
                    "human_review_id": review_id,
                },
            }
        log(12, "RETRY", "New AI reply", {"reply": reply})

        local_checker2 = local_reply_validation(reply, matched_product, customer_products)
        if not local_checker2.get("approved", True):
            checker2 = local_checker2
            log(12, "RETRY/LOCAL CHECKER2", "Local validation rejected the corrected reply", checker2)
        else:
            checker2 = check_reply(
                reply, ev, matched_product, image_result,
                instructions_text, rules_list, history, products,
                customer_products=customer_products,
            )
        checker_approved = checker2.get("approved", True)
        log(12, "RETRY/CHECKER2", f"approved={checker_approved}", {
            "problem": checker2.get("problem"),
            "fix_instruction": checker2.get("fix_instruction"),
        })

        if not checker_approved:
            log(12, "RETRY/CHECKER2", "Reply still rejected. Routing to human review — no auto reply.")
            try:
                set_customer_ai_enabled(db, ev["sender_id"], False)
            except Exception as exc:
                print(f"[MainAI] Could not pause AI after checker rejection: {exc}", flush=True)
            review_id = create_human_review(
                db, ev,
                f"AI reply rejected twice: {checker2.get('problem', 'unknown')}",
                build_product_vision_candidates(products, limit=10) if products else [],
            )
            return {
                "sender_id": ev["sender_id"],
                "reply": "",
                "debug": {
                    "skipped": True,
                    "reason": "ai_reply_rejected_twice",
                    "human_review_id": review_id,
                    "checker_problem": checker2.get("problem"),
                },
            }
    else:
        log(12, "RETRY", "Reply accepted. No retry needed.")

    # ── STEP 13: Create order when requested ─────────────────────────────────
    order_created = False
    if ai_result.get("create_order"):
        log(13, "ORDER", "AI requested creating a new order...")
        order_created, order_reply = create_order_if_valid(
            db, ev["sender_id"], ai_result, matched_product
        )
        if order_reply:
            reply = order_reply
            log(13, "ORDER", "Order created and outgoing reply updated", {"reply": reply})
    else:
        log(13, "ORDER", "No purchase order requested in this message")

    # قرار الصورة قبل حفظ الرد حتى لا تظهر "صورة" في الداشبورد مع كل رد نصي.
    send_img = _should_send_image(db, ev["sender_id"], matched_product, ev)
    outgoing_image_urls = product_image_urls(matched_product) if send_img else []
    outgoing_image_url = outgoing_image_urls[0] if outgoing_image_urls else None

    # ── STEP 14: Save outgoing reply ─────────────────────────────────────────
    log(14, "SAVE REPLY", "Saving outgoing reply to database...")
    save_message(
        db, ev["sender_id"], "outgoing", "text",
        reply, None, None, None,
        {"reply": reply, "product_image_url": outgoing_image_url},
    )
    save_conversation_message(db, ev["sender_id"], "assistant", reply)
    for img_url in outgoing_image_urls:
        save_message(
            db, ev["sender_id"], "outgoing", "image",
            None, img_url, None, None,
            {"reply_image": True, "product_image_url": img_url},
        )
    if not order_created:
        try:
            schedule_followup_if_needed(
                db,
                ev["sender_id"],
                stage=ai_result.get("intent") or routing.get("intent") or "conversation",
                product=matched_product,
                meta={"source": "process_webhook", "reply": reply[:160]},
            )
        except Exception as exc:
            print(f"[FollowUp] Could not schedule followup: {exc}", flush=True)
    log(14, "SAVE REPLY", "Outgoing reply saved", {
        "reply": reply,
        "product_image_urls": outgoing_image_urls,
    })

    # ── STEP 15: Final response ──────────────────────────────────────────────
    final = {
        "sender_id": ev["sender_id"],
        "page_id":   ev["page_id"],
        "platform":  ev["platform"],
        "reply":     reply,
        "debug": {
            "message_type"    : message_type,
            "product_found"   : matched_product is not None,
            "match_method"    : match_method,
            "ad_id"           : ev["ad_id"],
            "ref"             : ev["ref"],
            "checker_approved": checker_approved,
        },
    }
    # إرسال الصورة فقط عند الطلب أو دخول إعلان
    final = attach_product_image_payload(final, matched_product if send_img else None, reply)
    final["debug"]["image_send_decision"] = send_img

    # Send image directly to Facebook (bypasses n8n image handling)
    if send_direct_facebook_images and final.get("send_image"):
        sent_images = []
        for image_url in final.get("product_image_urls") or []:
            img_sent = send_image_to_facebook(ev["sender_id"], image_url, ev["page_id"], ev["platform"])
            sent_images.append({"image_url": image_url, "sent": img_sent})
        final["debug"]["fb_images_sent_directly"] = sent_images
        log(15, "DIRECT IMAGE", f"Sent {len(sent_images)} images directly", sent_images)

    elapsed = time.time() - t_start
    log_sep(f"FINAL RESPONSE - completed in {elapsed:.1f}s")
    log(15, "FINAL REPLY", "Outgoing response prepared for direct Facebook send", final)
    log_sep()

    return final


def process_single_webhook_in_background(body):
    """Process one Facebook messaging event and send any reply directly."""
    sender_id = extract_sender_id_from_body(body)
    with app.app_context():
        db = get_db()
        try:
            result = process_webhook(db, body)
            send_webhook_result_to_facebook(result, sender_id)
        except (KeyError, IndexError) as exc:
            print(f"[AsyncWebhook] Unsupported event: {exc}", flush=True)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            print(f"[AsyncWebhook] Error: {exc}", flush=True)
            # صمام أمان: أي خطأ غير متوقع → مراجعة بشرية + إيقاف AI لهذه المحادثة
            if sender_id:
                try:
                    ev_fallback = extract_facebook_event(body)
                except Exception:
                    ev_fallback = {
                        "sender_id": sender_id,
                        "page_id": "",
                        "platform": "facebook",
                        "text": "",
                        "image_url": None,
                        "ad_id": None,
                        "ref": None,
                    }
                try:
                    set_customer_ai_enabled(db, sender_id, False)
                except Exception as inner:
                    print(f"[AsyncWebhook] Could not pause AI: {inner}", flush=True)
                try:
                    rid = create_human_review(
                        db, ev_fallback,
                        f"خطأ غير متوقع أثناء المعالجة: {type(exc).__name__}: {exc}",
                        [],
                    )
                    print(f"[AsyncWebhook] Fallback human review #{rid} created.", flush=True)
                except Exception as inner:
                    print(f"[AsyncWebhook] Could not create fallback review: {inner}", flush=True)


def process_webhook_in_background(body):
    """Process Facebook/n8n events after returning HTTP 200 quickly.

    Facebook can batch multiple customers/messages in one webhook request.
    Each event gets its own background worker so no event in the batch is
    silently dropped and same-sender debounce can see newer saved messages.
    """
    event_bodies = split_facebook_event_bodies(body)
    print(f"[AsyncWebhook] Accepted batch with {len(event_bodies)} event(s).", flush=True)
    for single_body in event_bodies:
        threading.Thread(
            target=process_single_webhook_in_background,
            args=(single_body,),
            daemon=True,
        ).start()


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def home():
    """نقطة دخول آمنة للـ PWA دون تضمين مفتاح في manifest."""
    return redirect(f"/dashboard?{urlencode({'key': DASHBOARD_PASSWORD})}", code=302)


@app.route("/webhook", methods=["POST"])
@require_api_key
def webhook():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"error": "JSON object body required"}), 400
    _extract_ad_info_from_body(body)
    db   = get_db()
    sender_id = extract_sender_id_from_body(body)
    lock_acquired = False

    try:
        if ASYNC_WEBHOOK:
            event_count = len(split_facebook_event_bodies(body))
            threading.Thread(
                target=process_webhook_in_background,
                args=(body,),
                daemon=True,
            ).start()
            return jsonify({
                "sender_id": sender_id,
                "reply": "",
                "debug": {"accepted": True, "async": True, "event_count": event_count},
            }), 200

        if sender_id:
            lock_acquired = acquire_sender_lock(db, sender_id)
            if not lock_acquired:
                return jsonify({
                    "sender_id": sender_id,
                    "reply": "",
                    "debug": {"skipped": True, "reason": "sender_busy_timeout"},
                }), 200

        result = process_webhook(db, body)
        sent = send_webhook_result_to_facebook(result, sender_id)
        return jsonify({
            "sender_id": (result or {}).get("sender_id") or sender_id,
            "reply": "",
            "debug": {
                "accepted": True,
                "async": False,
                "direct_to_facebook": sent,
                "processing_debug": (result or {}).get("debug", {}),
            },
        }), 200
    except (KeyError, IndexError) as exc:
        print(f"[Webhook] Unsupported event: {exc}", flush=True)
        return jsonify({"skip": True}), 200
    except Exception as exc:
        print(f"[Webhook] Error: {exc}", flush=True)
        return jsonify({"error": "internal_error"}), 500
    finally:
        if lock_acquired:
            release_sender_lock(db, sender_id)


@app.route("/manychat/webhook", methods=["POST"])
def manychat_webhook():
    """
    يستقبل الرسائل من ManyChat External Request ويرجع الرد في نفس response.
    هذا المسار لا يحتاج X-API-Key لأن ManyChat يستدعيه مباشرة.
    """
    data = request.get_json(silent=True) or request.form.to_dict() or request.args.to_dict() or {}

    subscriber_id = str(data.get("id") or data.get("subscriber_id") or data.get("user_id") or "")
    text = (
        data.get("last_input_text")
        or data.get("text")
        or data.get("message")
        or data.get("last_text")
        or ""
    ).strip()
    first_name = data.get("first_name", "") or ""
    last_name = data.get("last_name", "") or ""
    platform = detect_manychat_platform(data)
    page_id = str(data.get("page_id") or "")
    image_url = extract_image_url_from_manychat_data(data)
    ref = data.get("ref") or ""
    ad_id = data.get("ad_id") or ""

    mc_ad_record = {
        "timestamp": now_baghdad_iso(),
        "page_id": page_id,
        "ad_id": ad_id or None,
    }
    with _ad_tracking_lock:
        with open(AD_TRACKING_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(mc_ad_record, ensure_ascii=False, default=str) + "\n")
    print(f"[AdTrack/ManyChat] page_id={page_id} ad_id={ad_id}", flush=True)

    print(
        f"[ManyChat IN] subscriber={subscriber_id} text={text!r} image={bool(image_url)}",
        flush=True,
    )

    if not subscriber_id:
        print(f"[ManyChat IN] Missing subscriber_id; raw data={data}", flush=True)
        return jsonify({
            "version": "v2",
            "content": {
                "type": manychat_content_type(platform),
                "messages": [],
            },
        }), 200

    timestamp_ms = int(datetime.now(BAGHDAD_TZ).timestamp() * 1000)
    fake_body = {
        "object": "instagram" if is_instagram_platform(platform) else "page",
        "entry": [{
            "id": page_id,
            "messaging": [{
                "sender": {"id": subscriber_id},
                "recipient": {"id": page_id},
                "timestamp": timestamp_ms,
                "message": {
                    "mid": f"manychat_{subscriber_id}_{timestamp_ms}",
                    "text": text,
                    "attachments": ([{
                        "type": "image",
                        "payload": {"url": image_url},
                    }] if image_url else []),
                },
                "referral": {
                    "ref": ref,
                    "ad_id": ad_id,
                    "source": "manychat",
                    "type": "OPEN_THREAD",
                } if (ref or ad_id) else {},
            }],
        }],
    }

    db = get_db()

    # نضمن وجود سجل الزبون ونحفظ الاسم قبل المعالجة لتمكين الردود من استخدامه
    if subscriber_id:
        try:
            get_or_create_customer(db, subscriber_id, page_id, platform)
            if first_name or last_name:
                full_name = f"{first_name} {last_name}".strip()
                db.execute(
                    "UPDATE customers SET name=? WHERE sender_id=?",
                    (full_name, subscriber_id),
                )
                db.commit()
                print(f"[ManyChat IN] Saved profile name={full_name}", flush=True)
        except Exception as exc:
            print(f"[ManyChat] Could not save customer profile pre-process: {exc}", flush=True)

    # تشغيل المعالجة في خلفية مع debounce لتجميع رسائل الزبون قبل تشغيل الموديل،
    # ثم إرسال الرد عبر ManyChat API. هكذا نرجع لـ ManyChat فوراً ولا نحتاج رد متزامن.
    threading.Thread(
        target=_process_manychat_webhook_async,
        args=(fake_body, subscriber_id, platform),
        daemon=True,
    ).start()

    print(f"[ManyChat IN] Queued background processing for {subscriber_id} (debounce={DEBOUNCE_DELAY}s)", flush=True)
    return jsonify({
        "version": "v2",
        "content": {
            "type": manychat_content_type(platform),
            "messages": [],
        },
    }), 200


def _process_manychat_webhook_async(fake_body, subscriber_id, platform):
    """يعالج رسائل ManyChat في الخلفية مع debounce ثم يُرسل الرد عبر ManyChat API."""
    with app.app_context():
        db = get_db()
        try:
            result = process_webhook(
                db,
                fake_body,
                use_debounce=True,
                send_direct_facebook_images=False,
            )
        except Exception as exc:
            import traceback
            traceback.print_exc()
            print(f"[ManyChatAsync] process error: {exc}", flush=True)
            try:
                create_human_review(
                    db,
                    {
                        "sender_id": subscriber_id,
                        "text": (fake_body.get("entry", [{}])[0]
                                 .get("messaging", [{}])[0]
                                 .get("message", {})
                                 .get("text", "")),
                    },
                    f"ManyChat async process error: {exc}",
                    [],
                )
            except Exception:
                pass
            return

        reply_text = (result.get("reply") or "").strip()
        image_urls = result.get("product_image_urls") or result.get("image_urls") or []
        if not image_urls and result.get("product_image_url"):
            image_urls = [result.get("product_image_url")]

        # إرسال النص أولاً ثم الصور
        if reply_text:
            sent = send_reply_via_manychat(subscriber_id, reply_text, platform)
            print(f"[ManyChatAsync] reply sent={sent} | {reply_text[:80]}", flush=True)
        else:
            print(f"[ManyChatAsync] No reply to send (debounced/skipped/handoff).", flush=True)

        if image_urls and result.get("send_image"):
            for image_url in image_urls:
                ok = send_image_via_manychat(subscriber_id, image_url, platform=platform)
                print(f"[ManyChatAsync] image sent={ok} url={image_url}", flush=True)


@app.route("/manychat/send", methods=["POST"])
@require_api_key
def manychat_send():
    """
    Route يدوي لإرسال رد من الداشبورد عبر ManyChat.
    body: { "subscriber_id": "...", "text": "...", "image_url": "" }
    """
    data = request.get_json(silent=True) or {}
    subscriber_id = (data.get("subscriber_id") or "").strip()
    text = (data.get("text") or "").strip()
    image_url = (data.get("image_url") or "").strip()
    platform = detect_manychat_platform(data)

    if not subscriber_id or not text:
        return jsonify({"error": "subscriber_id and text required"}), 400

    ok_text = send_reply_via_manychat(subscriber_id, text, platform)
    ok_image = True
    if image_url:
        ok_image = send_image_via_manychat(subscriber_id, image_url, platform=platform)

    db = get_db()
    save_message(
        db,
        subscriber_id,
        "outgoing",
        "text",
        text,
        image_url or None,
        None,
        None,
        {"sent_via": "manychat_api", "ok": ok_text},
    )

    return jsonify({
        "ok": ok_text,
        "image_ok": ok_image,
        "subscriber_id": subscriber_id,
    }), 200


@app.route("/import/products", methods=["POST"])
@require_api_key
def import_products():
    data = request.get_json(silent=True)
    if not isinstance(data, list):
        return jsonify({"error": "Expected a JSON array"}), 400

    products = [
        _normalize_product(item)
        for item in data
        if isinstance(item, dict) and str(item.get("product_id") or "").strip()
    ]
    save_products_to_file(products)
    return jsonify({"imported": len(products), "source": "products.json"}), 200


@app.route("/import/instructions", methods=["POST"])
@require_api_key
def import_instructions():
    db   = get_db()
    data = request.get_json(silent=True)
    if not isinstance(data, list):
        return jsonify({"error": "Expected a JSON array [{title, content}]"}), 400

    db.execute("UPDATE ai_instructions SET active=0")
    for item in data:
        db.execute(
            "INSERT INTO ai_instructions (title, content, active) VALUES (?,?,1)",
            (item.get("title", ""), item.get("content", "")),
        )
    db.commit()
    return jsonify({"status": "ok", "count": len(data)}), 200


@app.route("/import/forbidden_rules", methods=["POST"])
@require_api_key
def import_forbidden_rules():
    db   = get_db()
    data = request.get_json(silent=True)
    if not isinstance(data, list):
        return jsonify({"error": "Expected a JSON array of strings"}), 400

    db.execute("UPDATE forbidden_rules SET active=0")
    for rule in data:
        db.execute(
            "INSERT INTO forbidden_rules (rule, active) VALUES (?,1)", (rule,)
        )
    db.commit()
    return jsonify({"status": "ok", "count": len(data)}), 200


def _orders_payload(db, limit=500):
    rows = db.execute(
        """SELECT
             o.*,
             c.name AS customer_display_name,
             c.page_id,
             COALESCE(c.platform, 'facebook') AS platform
           FROM orders o
           LEFT JOIN customers c ON c.sender_id = o.sender_id
           ORDER BY o.id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    orders = []
    seen_order_keys = set()
    for row in rows:
        order = dict(row)
        dedupe_key = (
            _norm_order_value(order.get("sender_id")),
            _norm_order_value(order.get("phone")),
            _norm_order_value(order.get("product_id")),
            _norm_order_value(order.get("address")),
        )
        if all(dedupe_key) and dedupe_key in seen_order_keys:
            continue
        seen_order_keys.add(dedupe_key)
        orders.append(order)
    return {
        "orders": orders,
        "total": len(orders),
        "new_count": sum(1 for o in orders if (o.get("status") or "new") == "new"),
    }


@app.route("/orders", methods=["GET"])
def orders_page_or_legacy_api():
    db = get_db()
    if _dash_auth():
        return render_template("orders.html")
    if API_SECRET_KEY and request.headers.get("X-API-Key") == API_SECRET_KEY:
        return jsonify(_orders_payload(db)["orders"]), 200
    return (
        "<h1>403 — Unauthorized</h1>"
        "<p>أضف <code>?key=YOUR_PASSWORD</code> للرابط أو استخدم X-API-Key للـ API.</p>",
        403,
    )


@app.route("/problems")
def problems_page():
    return _admin_page("problems.html")


@app.route("/api/problems")
def api_problems():
    if not _dash_auth():
        return jsonify({"error": "Unauthorized"}), 403
    db = get_db()
    limit = request.args.get("limit", "500")
    status = request.args.get("status")
    try:
        limit = max(1, min(int(limit), 2000))
    except ValueError:
        limit = 500
    problems = get_problem_reports(db, status=status, limit=limit)
    return jsonify({"problems": problems, "total": len(problems)}), 200


@app.route("/api/problems/<int:problem_id>/status", methods=["POST"])
def api_problem_update_status(problem_id):
    if not _dash_auth():
        return jsonify({"error": "Unauthorized"}), 403
    data = request.get_json(silent=True) or {}
    status = (data.get("status") or "closed").strip()
    if status not in {"open", "closed", "resolved", "needs_attention"}:
        return jsonify({"error": "invalid status"}), 400
    db = get_db()
    now = now_baghdad_iso()
    db.execute(
        "UPDATE problem_reports SET status=?, updated_at=? WHERE id=?",
        (status, now, problem_id),
    )
    db.commit()
    return jsonify({"ok": True, "problem_id": problem_id, "status": status}), 200


@app.route("/customers", methods=["GET"])
@require_api_key
def list_customers():
    db   = get_db()
    rows = db.execute("SELECT * FROM customers ORDER BY id DESC").fetchall()
    return jsonify([dict(r) for r in rows]), 200


@app.route("/messages/<sender_id>", methods=["GET"])
@require_api_key
def get_messages(sender_id):
    db   = get_db()
    rows = db.execute(
        "SELECT * FROM messages WHERE sender_id=? ORDER BY id DESC LIMIT 50",
        (sender_id,),
    ).fetchall()
    return jsonify([dict(r) for r in rows]), 200


@app.route("/human_reviews", methods=["GET"])
@require_api_key
def list_human_reviews():
    db = get_db()
    rows = db.execute("SELECT * FROM human_reviews ORDER BY id DESC LIMIT 50").fetchall()
    return jsonify([dict(r) for r in rows]), 200


@app.route("/human_reviews/<int:review_id>/reply", methods=["POST"])
@require_api_key
def reply_human_review(review_id):
    data = request.get_json(silent=True) or {}
    reply = (data.get("reply") or "").strip()
    if not reply:
        return jsonify({"error": "reply is required"}), 400
    db = get_db()
    row = db.execute("SELECT * FROM human_reviews WHERE id=?", (review_id,)).fetchone()
    if not row:
        return jsonify({"error": "review not found"}), 404
    now = now_baghdad_iso()
    db.execute(
        "UPDATE human_reviews SET status='replied', admin_reply=?, replied_at=? WHERE id=?",
        (reply, now, review_id),
    )
    db.commit()
    save_message(
        db, row["sender_id"], "outgoing", "text",
        reply, None, None, None, {"human_review_id": review_id, "reply": reply},
    )
    sent = send_human_reply_to_customer(row["sender_id"], reply)
    return jsonify({
        "status": "ok",
        "review_id": review_id,
        "sender_id": row["sender_id"],
        "sent_to_customer": sent,
    }), 200


@app.route("/human_reviews/<int:review_id>/product", methods=["POST"])
@require_api_key
def select_human_review_product(review_id):
    data = request.get_json(silent=True) or {}
    product_id = (data.get("product_id") or "").strip()
    if not product_id:
        return jsonify({"error": "product_id is required"}), 400
    db = get_db()
    result = handle_human_product_selection(db, review_id, product_id)
    if not result.get("ok"):
        return jsonify(result), 400
    return jsonify(result), 200


@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True) or {}
    message = update.get("message") or update.get("edited_message") or {}
    text = (message.get("text") or "").strip()
    chat_id = str((message.get("chat") or {}).get("id", ""))

    if TELEGRAM_CHAT_ID and chat_id != str(TELEGRAM_CHAT_ID):
        return jsonify({"ok": True, "ignored": True}), 200

    product_match = re.match(r"^/product\s+(\d+)\s+(\S+)\s*$", text, re.IGNORECASE)
    reply_match = re.match(r"^/reply\s+(\d+)\s+(.+)$", text, re.DOTALL)

    if product_match:
        review_id = int(product_match.group(1))
        product_id = product_match.group(2).strip()
        with app.app_context():
            db = get_db()
            result = handle_human_product_selection(db, review_id, product_id)
        if result.get("ok"):
            send_telegram_message(
                f"تم اختيار المنتج للمراجعة {review_id}\n"
                f"Product: {result.get('product_id')}\n"
                f"Sent to customer: {result.get('sent_to_customer')}\n\n"
                f"Reply:\n{result.get('reply')}"
            )
        else:
            send_telegram_message(f"خطأ: {result.get('error')}")
        return jsonify({"ok": True}), 200

    if reply_match:
        review_id = int(reply_match.group(1))
        reply = reply_match.group(2).strip()
        sent = False
        row = None
        with app.app_context():
            db = get_db()
            row = db.execute("SELECT * FROM human_reviews WHERE id=?", (review_id,)).fetchone()
            if not row:
                send_telegram_message(f"لم أجد مراجعة برقم {review_id}")
                return jsonify({"ok": True}), 200
            now = now_baghdad_iso()
            db.execute(
                "UPDATE human_reviews SET status='replied', admin_reply=?, replied_at=? WHERE id=?",
                (reply, now, review_id),
            )
            db.commit()
            save_message(
                db, row["sender_id"], "outgoing", "text",
                reply, None, None, None, {"human_review_id": review_id, "reply": reply},
            )
            sent = send_human_reply_to_customer(row["sender_id"], reply)
        send_telegram_message(
            f"تم حفظ الرد للمراجعة {review_id}.\n"
            f"Sender: {row['sender_id']}\n"
            f"Sent to customer: {sent}"
        )
        return jsonify({"ok": True}), 200

    # لا ترسل رسالة المساعدة لكل رسالة واردة (صور/نص عادي) — فقط عند أمر / غير مفهوم
    if text.startswith("/"):
        send_telegram_message(
            "استخدم إحدى الصيغ:\n"
            "/product REVIEW_ID P001\n"
            "/product REVIEW_ID NONE\n"
            "/reply REVIEW_ID نص الرد"
        )
    return jsonify({"ok": True}), 200


@app.route("/product_image/<path:filename>", methods=["GET"])
def serve_product_image(filename):
    """تخدم صور المنتجات من مجلد product_image بدون مصادقة (مطلوبة للـ AI)."""
    return send_from_directory(PRODUCT_IMAGE_DIR, filename)


@app.route("/aud/<path:filename>", methods=["GET"])
def serve_audio(filename):
    """تخدم ملفات التنبيه الصوتي للداشبورد."""
    return send_from_directory(AUD_DIR, filename)


@app.route("/index/product", methods=["POST"])
@require_api_key
def index_single_product():
    """تفهرس صورة منتج واحد وتخزّن الـ embedding في products.json."""
    data       = request.get_json(silent=True) or {}
    product_id = (data.get("product_id") or "").strip()
    image_url  = (data.get("image_url")  or "").strip()
    if not product_id or not image_url:
        return jsonify({"error": "product_id and image_url are required"}), 400
    if not CLIP_AVAILABLE or _clip_model is None:
        return jsonify({"error": "CLIP model not loaded"}), 503

    if not index_product_image(product_id, image_url):
        return jsonify({"error": "failed to index product"}), 500
    return jsonify({"status": "ok", "product_id": product_id}), 200


@app.route("/index/all_products", methods=["POST"])
@require_api_key
def index_all_products():
    """تفهرس جميع المنتجات النشطة في products.json التي لا يوجد لها embedding."""
    if not CLIP_AVAILABLE or _clip_model is None:
        return jsonify({"error": "CLIP model not loaded"}), 503

    products = [
        product
        for product in load_active_products(None)
        if product.get("image_url") and not product.get("image_embedding")
    ]

    indexed, failed = 0, 0
    for product in products:
        if index_product_image(product["product_id"], product["image_url"]):
            indexed += 1
        else:
            failed += 1

    print(f"[CLIP] Bulk index done — indexed={indexed}, failed={failed}", flush=True)
    return jsonify({"indexed": indexed, "failed": failed}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status"        : "ok",
        "db"            : DB_PATH,
        "clip_loaded"   : _clip_model is not None,
        "clip_available": CLIP_AVAILABLE,
        "catalog_match_enabled": CATALOG_MATCH_ENABLED,
        "catalog_match_model": CATALOG_MATCH_MODEL,
        "catalog_image_exists": bool(_resolve_catalog_image_paths()),
        "catalog_images_count": len(_resolve_catalog_image_paths()),
        "openrouter_key_present": bool(OPENROUTER_KEY),
    }), 200


# ── Dashboard ─────────────────────────────────────────────────────────────────

def _dash_auth():
    key = request.args.get("key") or request.headers.get("X-Dashboard-Key", "")
    return key == DASHBOARD_PASSWORD


def _dash_require(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not _dash_auth():
            return jsonify({"error": "Unauthorized"}), 403
        return fn(*args, **kwargs)
    return wrapper


@app.route("/dashboard")
def dashboard():
    if not _dash_auth():
        return (
            "<h1>403 — Unauthorized</h1>"
            "<p>أضف <code>?key=YOUR_PASSWORD</code> للرابط</p>",
            403,
        )
    return render_template("dashboard.html")


def _admin_page(template_name, **context):
    if not _dash_auth():
        return (
            "<h1>403 — Unauthorized</h1>"
            "<p>أضف <code>?key=YOUR_PASSWORD</code> للرابط</p>",
            403,
        )
    return render_template(template_name, **context)


@app.route("/settings")
def settings_index_page():
    return _admin_page("settings/index.html")


@app.route("/settings/ai")
def settings_ai_page():
    return _admin_page("settings/ai.html")


@app.route("/settings/auto-product")
def settings_auto_product_page():
    return _admin_page("settings/auto_product.html")


@app.route("/settings/catalog")
def settings_catalog_page():
    return _admin_page("settings/catalog.html")


@app.route("/settings/followup")
def settings_followup_page():
    return _admin_page("settings/followup.html")


@app.route("/settings/channels")
def settings_channels_page():
    return _admin_page("settings/channels.html")


@app.route("/settings/store")
def settings_store_page():
    return _admin_page("settings/store.html")


@app.route("/settings/maintenance")
def settings_maintenance_page():
    return _admin_page("settings/maintenance.html")


@app.route("/analytics")
def analytics_page():
    return _admin_page("analytics.html")


@app.route("/evaluation")
def evaluation_page():
    return _admin_page("evaluation.html")


@app.route("/manifest.webmanifest")
def pwa_manifest():
    """يجب أن يكون على نفس scope الجذر حتى يتعرف عليه المتصفح."""
    return send_from_directory("static", "manifest.webmanifest", mimetype="application/manifest+json")


@app.route("/sw.js")
def pwa_service_worker():
    """Service worker على scope الجذر."""
    response = send_from_directory("static", "sw.js", mimetype="application/javascript")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Service-Worker-Allowed"] = "/"
    return response


@app.route("/products")
def products_page():
    if not _dash_auth():
        return (
            "<h1>403 — Unauthorized</h1>"
            "<p>أضف <code>?key=YOUR_PASSWORD</code> للرابط</p>",
            403,
        )
    return render_template("products.html")


@app.route("/api/orders")
@_dash_require
def api_orders():
    limit = request.args.get("limit", "500")
    try:
        limit = max(1, min(int(limit), 2000))
    except ValueError:
        limit = 500
    return jsonify(_orders_payload(get_db(), limit=limit))


def _backup_sqlite_file_to_bytes(path):
    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
            tmp_path = tmp.name
        src = sqlite3.connect(path)
        dst = sqlite3.connect(tmp_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        with open(tmp_path, "rb") as f:
            data = f.read()
        bio = io.BytesIO(data)
        bio.seek(0)
        return bio
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


@app.route("/api/export/products")
@_dash_require
def api_export_products():
    if not os.path.exists(PRODUCTS_FILE):
        return jsonify({"error": "products.json not found"}), 404
    return send_file(
        PRODUCTS_FILE,
        mimetype="application/json",
        as_attachment=True,
        download_name=f"products-{datetime.now(BAGHDAD_TZ).strftime('%Y%m%d-%H%M%S')}.json",
    )


@app.route("/api/import/products", methods=["POST"])
@_dash_require
def api_import_products():
    upload = request.files.get("file")
    if not upload or not upload.filename:
        return jsonify({"error": "اختر ملف products.json أولاً"}), 400
    try:
        data = json.load(upload.stream)
    except Exception as exc:
        return jsonify({"error": f"ملف المنتجات ليس JSON صالحاً: {exc}"}), 400
    if not isinstance(data, list):
        return jsonify({"error": "ملف المنتجات يجب أن يحتوي قائمة JSON"}), 400
    normalized = []
    seen = set()
    for idx, item in enumerate(data, 1):
        if not isinstance(item, dict):
            return jsonify({"error": f"المنتج رقم {idx} ليس object"}), 400
        product = _normalize_product(item)
        if not product.get("product_id") or not product.get("product_name"):
            return jsonify({"error": f"المنتج رقم {idx} يحتاج product_id و product_name"}), 400
        if product["product_id"] in seen:
            return jsonify({"error": f"كود المنتج مكرر في الملف: {product['product_id']}"}), 400
        seen.add(product["product_id"])
        normalized.append(product)
    with _products_file_lock:
        save_products_to_file(normalized)
    return jsonify({"ok": True, "count": len(normalized)})


@app.route("/api/export/database")
@_dash_require
def api_export_database():
    if not os.path.exists(DB_PATH):
        return jsonify({"error": "sales.db not found"}), 404
    bio = _backup_sqlite_file_to_bytes(DB_PATH)
    return send_file(
        bio,
        mimetype="application/vnd.sqlite3",
        as_attachment=True,
        download_name=f"sales-{datetime.now(BAGHDAD_TZ).strftime('%Y%m%d-%H%M%S')}.db",
    )


@app.route("/api/import/database", methods=["POST"])
@_dash_require
def api_import_database():
    upload = request.files.get("file")
    if not upload or not upload.filename:
        return jsonify({"error": "اختر ملف قاعدة البيانات أولاً"}), 400

    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
            tmp_path = tmp.name
            upload.save(tmp)

        src = sqlite3.connect(tmp_path)
        try:
            integrity = src.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                return jsonify({"error": f"فحص قاعدة البيانات فشل: {integrity}"}), 400
            tables = {
                row[0]
                for row in src.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            required = {"customers", "messages", "orders", "app_settings"}
            missing = sorted(required - tables)
            if missing:
                return jsonify({"error": "قاعدة البيانات لا تحتوي الجداول المطلوبة: " + ", ".join(missing)}), 400

            backups_dir = os.path.join(os.path.dirname(__file__), "backups")
            os.makedirs(backups_dir, exist_ok=True)
            backup_path = os.path.join(
                backups_dir,
                f"sales-before-import-{datetime.now(BAGHDAD_TZ).strftime('%Y%m%d-%H%M%S')}.db",
            )
            current = sqlite3.connect(DB_PATH)
            backup = sqlite3.connect(backup_path)
            try:
                current.backup(backup)
            finally:
                backup.close()
                current.close()

            dst = get_db()
            src.backup(dst)
            dst.commit()
        finally:
            src.close()
    except Exception as exc:
        return jsonify({"error": f"فشل استيراد قاعدة البيانات: {exc}"}), 500
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    return jsonify({"ok": True, "message": "تم استيراد قاعدة البيانات بنجاح"})


@app.route("/api/conversations")
@_dash_require
def api_conversations():
    db = get_db()
    rows = db.execute("""
        SELECT
            c.sender_id,
            c.name,
            c.phone,
            c.province,
            c.address,
            c.gender,
            m.text       AS last_message,
            m.direction  AS last_direction,
            m.created_at AS last_time,
            COALESCE(c.platform, 'facebook') AS platform,
            CASE
              WHEN (
                SELECT COALESCE(MAX(id), 0) FROM messages
                WHERE sender_id = c.sender_id AND direction = 'incoming'
              ) > (
                SELECT COALESCE(MAX(id), 0) FROM messages
                WHERE sender_id = c.sender_id AND direction = 'outgoing'
              )
              THEN 1 ELSE 0
            END AS unanswered,
            (SELECT COUNT(*) FROM human_reviews hr
             WHERE hr.sender_id = c.sender_id AND hr.status = 'pending') AS pending_reviews_count,
            (SELECT COUNT(*) FROM problem_reports pr
             WHERE pr.sender_id = c.sender_id AND COALESCE(pr.status, 'open') IN ('open', 'needs_attention')) AS problem_count,
            (SELECT reason FROM problem_reports pr
             WHERE pr.sender_id = c.sender_id AND COALESCE(pr.status, 'open') IN ('open', 'needs_attention')
             ORDER BY id DESC LIMIT 1) AS problem_reason,
            (SELECT COALESCE(MAX(id), 0) FROM messages
             WHERE sender_id = c.sender_id
               AND direction = 'incoming'
               AND (message_type = 'image' OR image_url IS NOT NULL)
            ) AS last_image_id,
            cpi.product_id,
            cpi.product_name,
            (SELECT GROUP_CONCAT(product_id, '||') FROM customer_product_interests
             WHERE sender_id = c.sender_id) AS product_ids,
            (SELECT GROUP_CONCAT(product_name, '||') FROM customer_product_interests
             WHERE sender_id = c.sender_id) AS product_names,
            (SELECT ad_id FROM messages
             WHERE sender_id = c.sender_id AND ad_id IS NOT NULL
             ORDER BY id DESC LIMIT 1) AS ad_id,
            (SELECT ref FROM messages
             WHERE sender_id = c.sender_id AND ref IS NOT NULL
             ORDER BY id DESC LIMIT 1) AS ref,
            COALESCE(cais.enabled, 1) AS ai_enabled
        FROM customers c
        LEFT JOIN customer_ai_settings cais ON cais.sender_id = c.sender_id
        LEFT JOIN messages m ON m.id = (
            SELECT id FROM messages WHERE sender_id = c.sender_id ORDER BY id DESC LIMIT 1
        )
        LEFT JOIN customer_product_interests cpi ON cpi.id = (
            SELECT id FROM customer_product_interests
            WHERE sender_id = c.sender_id
              AND COALESCE(status, 'active')='active'
            ORDER BY last_seen_at DESC LIMIT 1
        )
        ORDER BY COALESCE(m.created_at, c.last_seen_at, '') DESC
    """).fetchall()
    return jsonify({"conversations": [dict(r) for r in rows]})


@app.route("/api/conversations/<sender_id>/messages")
@_dash_require
def api_conversation_messages(sender_id):
    db = get_db()
    rows = db.execute(
        "SELECT id, direction, message_type, text, image_url, ad_id, created_at "
        "FROM messages WHERE sender_id=? ORDER BY id DESC LIMIT 50",
        (sender_id,),
    ).fetchall()
    return jsonify({"messages": list(reversed([dict(r) for r in rows]))})


@app.route("/api/conversations/<sender_id>", methods=["DELETE"])
@_dash_require
def api_delete_conversation(sender_id):
    sender_id = str(sender_id or "").strip()
    if not sender_id:
        return jsonify({"ok": False, "error": "sender_id required"}), 400

    db = get_db()
    deleted = {}
    for table in (
        "messages",
        "conversation_memory",
        "human_reviews",
        "customer_product_interests",
        "customer_instructions",
        "customer_ai_settings",
        "sender_processing_locks",
    ):
        cur = db.execute(f"DELETE FROM {table} WHERE sender_id=?", (sender_id,))
        deleted[table] = cur.rowcount
    cur = db.execute("DELETE FROM customers WHERE sender_id=?", (sender_id,))
    deleted["customers"] = cur.rowcount
    db.commit()
    return jsonify({
        "ok": True,
        "sender_id": sender_id,
        "deleted": deleted,
        "orders_preserved": True,
    })


@app.route("/api/improve_message", methods=["POST"])
@_dash_require
def api_improve_message():
    """
    تحسين/إعادة صياغة نص الموظف بدون أي ارتباط بمحادثة.
    يعمل دائماً حتى لو AI متوقف لكل المحادثات.
    يأخذ نص الحقل + التعليمات + القواعد ويرجع نصاً منقحاً جاهزاً للإرسال.
    """
    data = request.get_json(silent=True) or {}
    raw_text = (data.get("text") or "").strip()
    if not raw_text:
        return jsonify({"ok": False, "error": "text required"}), 400
    if not OPENROUTER_KEY:
        return jsonify({"ok": False, "error": "no_openrouter_key", "improved": raw_text}), 503

    db = get_db()
    instructions_text, rules_list = load_ai_config(db, sender_id=None)
    rules_text = "\n".join(f"- {r}" for r in rules_list) if rules_list else "- لا توجد قواعد محظورة."

    system_prompt = (
        "أنت محرر رسائل لمتجر أنيقة للموديلات والقطع النسائية المحتشمة يتحدث باللهجة العراقية الودودة.\n"
        "مهمتك الوحيدة: إعادة صياغة النص الذي يكتبه الموظف ليكون احترافياً ومقنعاً وقصيراً.\n\n"
        "قواعد صارمة:\n"
        "- لا تضف معلومات (سعر/قياس/لون/منتج) لم يذكرها الموظف.\n"
        "- لا تحذف أي معلومة جوهرية ذكرها الموظف.\n"
        "- لا تضف توقيعاً أو تحية إذا لم يطلبها الموظف.\n"
        "- لا تتجاوز 60 كلمة.\n"
        "- التزم بالقواعد المحظورة وتعليمات الإدارة أدناه.\n\n"
        "تعليمات الإدارة:\n"
        f"{instructions_text or 'لا توجد تعليمات إضافية.'}\n\n"
        "القواعد المحظورة:\n"
        f"{rules_text}\n\n"
        "أخرج JSON فقط بهذا الشكل: {\"improved\":\"النص الجديد\"}"
    )

    user_content = f"نص الموظف الأصلي:\n{raw_text}\n\nأعد صياغته فقط، احتفظ بكل معلومة فيه."

    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": IMPROVE_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                "max_tokens": 400,
                "temperature": 0.5,
            },
            timeout=20,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        parsed = _parse_ai_json(raw) if isinstance(raw, str) else {}
        improved = (parsed.get("improved") or "").strip()
        if not improved:
            m = re.search(r'"improved"\s*:\s*"([^"]+)"', raw or "", re.DOTALL)
            improved = m.group(1).strip() if m else (raw or "").strip()
        improved = improved.strip().strip('"').strip()
        if not improved:
            return jsonify({"ok": False, "error": "empty_improvement", "improved": raw_text}), 200
        print(f"[Improve] {raw_text[:60]} → {improved[:60]}", flush=True)
        return jsonify({"ok": True, "improved": improved})
    except Exception as exc:
        print(f"[Improve] Error: {exc}", flush=True)
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}", "improved": raw_text}), 200


@app.route("/api/conversations/<sender_id>/send", methods=["POST"])
@_dash_require
def api_send_message(sender_id):
    data      = request.get_json(silent=True) or {}
    text      = (data.get("text")      or "").strip()
    image_url = (data.get("image_url") or "").strip()
    if not text and not image_url:
        return jsonify({"error": "text or image_url required"}), 400

    db  = get_db()
    customer = db.execute(
        "SELECT page_id, COALESCE(platform, 'facebook') AS platform FROM customers WHERE sender_id=?",
        (sender_id,),
    ).fetchone()
    page_id = customer["page_id"] if customer else ""
    platform = customer["platform"] if customer else "facebook"
    now = now_baghdad_iso()
    db.execute(
        "INSERT INTO messages (sender_id, direction, message_type, text, image_url, created_at) "
        "VALUES (?, 'outgoing', 'text', ?, ?, ?)",
        (sender_id, text or None, image_url or None, now),
    )
    db.commit()

    text_result = None
    image_result = None
    manual_tag = MANYCHAT_DEFAULT_MESSAGE_TAG
    if text:
        text_result = send_text_via_manychat_detailed(sender_id, text, platform, message_tag=manual_tag)
    if image_url:
        public_img = build_public_image_url(image_url)
        print(f"[Dashboard] Sending image: {public_img}", flush=True)
        image_result = send_image_via_manychat_detailed(sender_id, public_img, platform=platform, message_tag=manual_tag)

    sent = bool(
        (text_result and text_result.get("ok"))
        or (image_result and image_result.get("ok"))
    )
    manychat_key = current_manychat_api_key()
    primary = text_result or image_result or {}
    warning = None
    if not manychat_key and not sent:
        warning = "MANYCHAT_API_KEY غير مُهيّأ — تم حفظ الرسالة في القاعدة فقط"
    elif not sent:
        status = primary.get("status") or "unknown"
        message = primary.get("message") or ""
        http_code = primary.get("status_code")
        warning = f"ManyChat رفض الإرسال (status={status}, http={http_code}) لمنصة {platform}"
        if message:
            warning += f"\nالتفاصيل: {message}"

    print(
        f"[Dashboard] Manual send to {sender_id} sent={sent} platform={platform} "
        f"text_status={(text_result or {}).get('status')} image_status={(image_result or {}).get('status')}",
        flush=True,
    )
    return jsonify({
        "ok": sent,
        "fb_sent": sent,
        "warning": warning,
        "platform": platform,
        "subscriber_id": sender_id,
        "text_result": text_result,
        "image_result": image_result,
    })


@app.route("/api/conversations/<sender_id>/ask_ai", methods=["POST"])
@_dash_require
def api_ask_ai(sender_id):
    data               = request.get_json(silent=True) or {}
    text               = (data.get("text")               or "").strip()
    extra_instructions = (data.get("extra_instructions") or "").strip()
    product_id         = (data.get("product_id")         or "").strip()
    allow_empty        = bool(data.get("allow_empty"))

    db = get_db()
    if not is_ai_enabled(db):
        return jsonify({"reply": "", "intent": "disabled", "confidence": 0, "disabled": True}), 200
    customer         = get_or_create_customer(db, sender_id, None)
    history          = load_history(db, sender_id)
    conversation_history = get_conversation_history(db, sender_id, limit=10)
    products         = load_active_products(None)
    customer_prods   = load_customer_products(db, sender_id)
    inst_text, rules = load_ai_config(db, sender_id=sender_id)

    if not text and allow_empty:
        latest_incoming = db.execute(
            """SELECT text, message_type, image_url
               FROM messages
               WHERE sender_id=? AND direction='incoming'
               ORDER BY id DESC LIMIT 1""",
            (sender_id,),
        ).fetchone()
        if latest_incoming:
            latest_text = (latest_incoming["text"] or "").strip()
            latest_image = (latest_incoming["image_url"] or "").strip()
            if latest_text and latest_text != latest_image:
                text = latest_text
            elif latest_image or latest_incoming["message_type"] == "image":
                text = "الزبون أرسل صورة. حلل سياق المحادثة والمنتج المرتبط إن وجد واكتب الرد المناسب بدون اختراع تفاصيل."
        if not text:
            text = "حلل آخر رسائل المحادثة واكتب الرد المناسب للزبون بدون اختراع تفاصيل."
        extra_instructions = (
            (extra_instructions + "\n\n") if extra_instructions else ""
        ) + "الحقل كان فارغاً عند المشرف؛ اعتمد على تاريخ المحادثة وسياق المنتج المرتبط إن وجد، ولا تخترع سعراً أو قياساً أو توفراً غير مؤكد."
    elif not text:
        return jsonify({
            "reply": "",
            "intent": "empty_text",
            "confidence": 0,
            "error": "text required",
        }), 400

    if extra_instructions:
        inst_text += f"\n\nتعليمات إضافية من المشرف:\n{extra_instructions}"

    matched_product = find_product_by_id(product_id) if product_id else None

    ev = {
        "sender_id": sender_id, "text": text,
        "image_url": None, "attachments": [],
        "ref": None, "ad_id": None,
        "referral_source": None, "referral_type": None,
        "postback_payload": None, "quick_reply_payload": None,
        "timestamp": None, "page_id": None,
    }
    ai_result = call_main_ai(
        ev, "text", customer, history, products,
        matched_product, None, inst_text, rules,
        customer_products=customer_prods,
        conversation_history=conversation_history,
    )
    return jsonify({
        "reply":      ai_result.get("reply", ""),
        "intent":     ai_result.get("intent", ""),
        "confidence": ai_result.get("confidence", 0),
    })


@app.route("/api/conversations/<sender_id>/save_instructions", methods=["POST"])
@_dash_require
def api_save_instructions(sender_id):
    data         = request.get_json(silent=True) or {}
    instructions = (data.get("instructions") or "").strip()
    apply_to_all = 1 if data.get("apply_to_all") else 0
    db  = get_db()
    now = now_baghdad_iso()

    existing = db.execute(
        "SELECT id FROM customer_instructions WHERE sender_id=?", (sender_id,)
    ).fetchone()
    if existing:
        db.execute(
            "UPDATE customer_instructions SET instructions=?, apply_to_all=?, updated_at=? WHERE sender_id=?",
            (instructions, apply_to_all, now, sender_id),
        )
    else:
        db.execute(
            "INSERT INTO customer_instructions (sender_id, instructions, apply_to_all, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (sender_id, instructions, apply_to_all, now, now),
        )

    if apply_to_all:
        global_row = db.execute(
            "SELECT id FROM customer_instructions WHERE sender_id IS NULL AND apply_to_all=1"
        ).fetchone()
        if global_row:
            db.execute(
                "UPDATE customer_instructions SET instructions=?, updated_at=? WHERE id=?",
                (instructions, now, global_row["id"]),
            )
        else:
            db.execute(
                "INSERT INTO customer_instructions (sender_id, instructions, apply_to_all, created_at, updated_at) "
                "VALUES (NULL, ?, 1, ?, ?)",
                (instructions, now, now),
            )
    db.commit()
    print(f"[Dashboard] Instructions saved for {sender_id} global={apply_to_all}", flush=True)
    return jsonify({"ok": True})


@app.route("/api/conversations/<sender_id>/instructions")
@_dash_require
def api_get_instructions(sender_id):
    db  = get_db()
    row = db.execute(
        "SELECT instructions, apply_to_all FROM customer_instructions WHERE sender_id=?",
        (sender_id,),
    ).fetchone()
    if row:
        return jsonify({"instructions": row["instructions"], "apply_to_all": bool(row["apply_to_all"])})
    return jsonify({"instructions": "", "apply_to_all": False})


@app.route("/api/products")
@_dash_require
def api_products():
    products = load_active_products(None)
    return jsonify({"products": [
        {
            "product_id":   p.get("product_id"),
            "product_name": p.get("product_name"),
            "price":        p.get("price"),
            "stock":        p.get("stock"),
            "sizes":        p.get("sizes"),
            "colors":       p.get("colors"),
            "delivery":     p.get("delivery"),
            "image_url":    p.get("image_url"),
            "image_urls":   product_image_urls(p),
        }
        for p in products
    ]})


def _product_from_request(data, existing=None):
    data = data or {}
    existing = existing or {}
    product = dict(existing)
    for field in PRODUCT_FIELDS:
        if field in data:
            product[field] = data.get(field)
        elif field not in product:
            product[field] = ""
    product["product_id"] = str(product.get("product_id") or "").strip()
    product["product_name"] = str(product.get("product_name") or "").strip()
    product["status"] = str(product.get("status") or "active").strip() or "active"
    product["image_url"] = _normalize_product_image_value(product.get("image_url"))
    return _normalize_product(product)


@app.route("/api/products/manage")
@_dash_require
def api_manage_products():
    include_inactive = request.args.get("all", "1") != "0"
    products = load_products_from_file() if include_inactive else load_active_products(None)
    return jsonify({"products": [product_payload(p) for p in products]})


@app.route("/api/products/manage", methods=["POST"])
@_dash_require
def api_create_product():
    data = request.get_json(silent=True) or {}
    product = _product_from_request(data)
    if not product.get("product_id") or not product.get("product_name"):
        return jsonify({"error": "product_id and product_name required"}), 400

    with _products_file_lock:
        products = load_products_from_file()
        if any(p.get("product_id") == product["product_id"] for p in products):
            return jsonify({"error": "product_id already exists"}), 409
        products.append(product)
        save_products_to_file(products)
    return jsonify({"ok": True, "product": product_payload(product)}), 201


@app.route("/api/products/manage/<product_id>", methods=["PUT"])
@_dash_require
def api_update_product(product_id):
    data = request.get_json(silent=True) or {}
    product_id = str(product_id or "").strip()
    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    with _products_file_lock:
        products = load_products_from_file()
        for idx, existing in enumerate(products):
            if existing.get("product_id") == product_id:
                updated = _product_from_request(data, existing)
                updated["product_id"] = product_id
                if not updated.get("product_name"):
                    return jsonify({"error": "product_name required"}), 400
                products[idx] = updated
                save_products_to_file(products)
                return jsonify({"ok": True, "product": product_payload(updated)})

    return jsonify({"error": "product not found"}), 404


@app.route("/api/products/manage/<product_id>", methods=["DELETE"])
@_dash_require
def api_delete_product(product_id):
    product_id = str(product_id or "").strip()
    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    with _products_file_lock:
        products = load_products_from_file()
        remaining = [p for p in products if p.get("product_id") != product_id]
        if len(remaining) == len(products):
            return jsonify({"error": "product not found"}), 404
        save_products_to_file(remaining)
    return jsonify({"ok": True, "deleted": product_id})


@app.route("/api/conversations/<sender_id>/link_product", methods=["POST"])
@_dash_require
def api_link_product(sender_id):
    data       = request.get_json(silent=True) or {}
    product_ids = data.get("product_ids")
    if not isinstance(product_ids, list):
        product_ids = [data.get("product_id")]
    product_ids = [str(pid or "").strip() for pid in product_ids if str(pid or "").strip()]
    silent = bool(data.get("silent")) or bool(data.get("skip_auto_reply"))
    resume_ai = bool(data.get("resume_ai", True))
    if not product_ids:
        return jsonify({"error": "product_id required"}), 400

    db  = get_db()
    now = now_baghdad_iso()
    linked_products = []
    for product_id in product_ids:
        product = find_product_by_id(product_id)
        if not product:
            return jsonify({"error": f"product not found: {product_id}"}), 404
        db.execute(
            """INSERT INTO customer_product_interests
               (sender_id, product_id, product_name, match_method, confidence, last_seen_at, source, status, rejected_at)
               VALUES (?, ?, ?, 'manual', 100, ?, 'manual_admin', 'active', NULL)
               ON CONFLICT(sender_id, product_id) DO UPDATE SET
                   product_name=excluded.product_name,
                   match_method='manual', confidence=100,
                   last_seen_at=excluded.last_seen_at,
                   source='manual_admin',
                   status='active',
                   rejected_at=NULL""",
            (sender_id, product_id, product.get("product_name"), now),
        )
        linked_products.append(product)
    db.execute(
        "UPDATE human_reviews SET status='linked', replied_at=? "
        "WHERE sender_id=? AND status='pending'",
        (now, sender_id),
    )
    db.commit()
    print(
        f"[Dashboard] Linked {len(linked_products)} product(s) → {sender_id} "
        f"(silent={silent}, resume_ai={resume_ai})",
        flush=True,
    )
    if resume_ai:
        try:
            set_customer_ai_enabled(db, sender_id, True)
        except Exception as exc:
            print(f"[Dashboard] Could not re-enable AI after link: {exc}", flush=True)
    auto_reply = None
    if not silent:
        auto_reply = auto_reply_after_product_link(db, sender_id, linked_products[-1])
    return jsonify({
        "ok": True,
        "product": linked_products[-1],
        "products": linked_products,
        "auto_reply": auto_reply,
        "ai_resumed": resume_ai,
        "silent": silent,
    })


@app.route("/api/upload_image", methods=["POST"])
@_dash_require
def api_upload_image():
    if "image" not in request.files:
        return jsonify({"error": "No image file"}), 400
    file = request.files["image"]
    if not file.filename:
        return jsonify({"error": "Empty filename"}), 400

    uploads_dir = os.path.join(PRODUCT_IMAGE_DIR, "uploads")
    os.makedirs(uploads_dir, exist_ok=True)

    ext      = os.path.splitext(file.filename)[1].lower() or ".jpg"
    filename = f"upload_{int(time.time())}{ext}"
    file.save(os.path.join(uploads_dir, filename))

    image_url = build_public_image_url(f"/product_image/uploads/{filename}")
    print(f"[Dashboard] Image uploaded: {filename}", flush=True)
    return jsonify({"image_url": image_url, "filename": filename})


@app.route("/api/catalog_image", methods=["GET", "POST"])
@_dash_require
def api_catalog_image():
    """Legacy endpoint — redirects to /api/settings/catalog_images."""
    if request.method == "GET":
        images = [_catalog_image_meta(path) for path in _resolve_catalog_image_paths()]
        return jsonify({
            "ok": True,
            "exists": len(images) > 0,
            "images": images,
            "images_count": len(images),
            "catalog_match_enabled": CATALOG_MATCH_ENABLED,
            "catalog_match_model": CATALOG_MATCH_MODEL,
        })
    # POST: forward single image as multi-image upload
    if "image" not in request.files:
        return jsonify({"ok": False, "error": "No image file", "message": "فشل رفع صورة الكتالوج"}), 400
    file = request.files["image"]
    if not file.filename:
        return jsonify({"ok": False, "error": "Empty filename", "message": "فشل رفع صورة الكتالوج"}), 400
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
        return jsonify({
            "ok": False,
            "error": "Only PNG, JPG, JPEG, or WEBP images are supported",
            "message": "فشل رفع صورة الكتالوج — استخدم صفحة /settings/catalog لرفع أكثر من صورة",
        }), 400
    saved = []
    with _catalog_image_lock:
        os.makedirs(CATALOG_IMAGE_DIR, exist_ok=True)
        filename = _safe_catalog_filename(file.filename)
        if not filename:
            return jsonify({"ok": False, "error": "Unsupported image type"}), 400
        stem, fext = os.path.splitext(filename)
        candidate = filename
        counter = 1
        while os.path.exists(os.path.join(CATALOG_IMAGE_DIR, candidate)):
            candidate = f"{stem}_{counter}{fext}"
            counter += 1
        path = os.path.join(CATALOG_IMAGE_DIR, candidate)
        try:
            file.save(path)
            if os.path.getsize(path) <= 0:
                os.remove(path)
                return jsonify({"ok": False, "error": "Uploaded file is empty"}), 400
            saved.append(_catalog_image_meta(path))
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc), "message": "فشل رفع صورة الكتالوج"}), 500
    image_flow("catalog_image_uploaded", path=candidate, size=os.path.getsize(path), action="uploaded")
    return jsonify({
        "ok": True,
        "action": "uploaded",
        "message": "تم رفع صورة الكتالوج — لإدارة جميع الصور استخدم /settings/catalog",
        "saved": saved,
        "images": [_catalog_image_meta(p) for p in _resolve_catalog_image_paths()],
    })


@app.route("/api/settings/catalog_images", methods=["GET", "POST"])
@_dash_require
def api_settings_catalog_images():
    if request.method == "GET":
        images = [_catalog_image_meta(path) for path in _resolve_catalog_image_paths()]
        return jsonify({
            "ok": True,
            "images": images,
            "count": len(images),
            "catalog_match_enabled": CATALOG_MATCH_ENABLED,
            "catalog_match_model": CATALOG_MATCH_MODEL,
            "upload_dir": os.path.basename(CATALOG_IMAGE_DIR),
        })

    files = request.files.getlist("images") or request.files.getlist("image")
    files = [file for file in files if file and file.filename]
    if not files:
        return jsonify({"ok": False, "error": "No image files"}), 400

    saved = []
    errors = []
    with _catalog_image_lock:
        os.makedirs(CATALOG_IMAGE_DIR, exist_ok=True)
        for file in files:
            filename = _safe_catalog_filename(file.filename)
            if not filename:
                errors.append({"filename": file.filename, "error": "Unsupported image type"})
                continue
            stem, ext = os.path.splitext(filename)
            candidate = filename
            counter = 1
            while os.path.exists(os.path.join(CATALOG_IMAGE_DIR, candidate)):
                candidate = f"{stem}_{counter}{ext}"
                counter += 1
            path = os.path.join(CATALOG_IMAGE_DIR, candidate)
            try:
                file.save(path)
                if os.path.getsize(path) <= 0:
                    os.remove(path)
                    errors.append({"filename": file.filename, "error": "Uploaded file is empty"})
                    continue
                saved.append(_catalog_image_meta(path))
            except Exception as exc:
                errors.append({"filename": file.filename, "error": str(exc)})

    image_flow("catalog_images_uploaded", count=len(saved), errors=len(errors))
    status = 200 if saved else 400
    return jsonify({
        "ok": bool(saved),
        "saved": saved,
        "errors": errors,
        "images": [_catalog_image_meta(path) for path in _resolve_catalog_image_paths()],
    }), status


@app.route("/api/settings/catalog_images/<image_id>", methods=["DELETE"])
@_dash_require
def api_delete_catalog_image(image_id):
    image_id = str(image_id or "").strip()
    if not image_id:
        return jsonify({"ok": False, "error": "image_id required"}), 400

    if image_id == "__legacy__":
        path = _resolve_catalog_image_path()
    else:
        safe_name = os.path.basename(image_id.replace("\\", "/"))
        if safe_name != image_id or not _is_catalog_image_file(safe_name):
            return jsonify({"ok": False, "error": "invalid image_id"}), 400
        path = os.path.join(CATALOG_IMAGE_DIR, safe_name)

    if not path or not os.path.isfile(path):
        return jsonify({"ok": False, "error": "image not found"}), 404

    with _catalog_image_lock:
        try:
            os.remove(path)
        except FileNotFoundError:
            return jsonify({"ok": False, "error": "image not found"}), 404

    image_flow("catalog_image_deleted", image_id=image_id, filename=os.path.basename(path))
    images = [_catalog_image_meta(item) for item in _resolve_catalog_image_paths()]
    return jsonify({"ok": True, "deleted": image_id, "images": images})


@app.route("/api/manychat/diag", methods=["GET"])
@_dash_require
def api_manychat_diag():
    """تشخيص ما هي المتغيرات التي وصلت فعلاً لعملية التطبيق (بدون كشف القيم)."""
    candidates = [
        "MANYCHAT_API_KEY", "MANYCHAT_KEY", "MC_API_KEY",
        "MANYCHAT_MESSAGE_TAG", "OPENROUTER_API_KEY", "PUBLIC_URL",
        "DASHBOARD_PASSWORD", "API_SECRET_KEY", "TELEGRAM_CHAT_ID", "TELEGRAM_ORDERS_CHAT_ID",
        "TELEGRAM_NOTIFICATION_HEADER", "CATALOG_MATCH_MODEL", "CATALOG_MATCH_ENABLED", "CATALOG_IMAGE_PATH",
    ]
    seen = {}
    for name in candidates:
        raw = os.environ.get(name)
        if raw is None:
            seen[name] = {"present": False}
            continue
        norm = (raw or "").strip()
        seen[name] = {
            "present": True,
            "raw_length": len(raw),
            "value_length": len(norm),
            "has_whitespace": raw != norm,
            "preview": (norm[:6] + "..." + norm[-4:]) if len(norm) > 14 else ("***" if norm else "(empty)"),
        }
    env_file_present = os.path.exists(_env_path)
    env_lines = []
    if env_file_present:
        try:
            with open(_env_path, encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#") or "=" not in s:
                        continue
                    name, _, value = s.partition("=")
                    env_lines.append({
                        "name": name.strip(),
                        "value_empty": not value.strip().strip('"\''),
                    })
        except Exception as exc:
            env_lines = [{"error": f"{type(exc).__name__}: {exc}"}]
    return jsonify({
        "env": seen,
        "env_file_present": env_file_present,
        "env_file_lines": env_lines,
        "manychat_api_url": MANYCHAT_API_URL,
        "current_key_loaded": bool(current_manychat_api_key()),
    })


@app.route("/api/manychat/test", methods=["GET", "POST"])
@_dash_require
def api_manychat_test():
    """Quick diagnostic: verify the configured ManyChat key by hitting /fb/page/getInfo."""
    api_key = current_manychat_api_key()
    if not api_key:
        return jsonify({"ok": False, "reason": "missing_key", "message": "MANYCHAT_API_KEY غير مُعرَّف — ضيفه في Railway Variables أو .env"})
    try:
        resp = requests.get(
            f"{MANYCHAT_API_URL}/fb/page/getInfo",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=15,
        )
        try:
            body = resp.json()
        except Exception:
            body = {"status": "http_error", "body": resp.text}
    except Exception as exc:
        return jsonify({"ok": False, "reason": "exception", "message": f"{type(exc).__name__}: {exc}"})

    status = body.get("status") if isinstance(body, dict) else None
    ok = resp.ok and status == "success"
    page = (body or {}).get("data") or {}
    return jsonify({
        "ok": ok,
        "http": resp.status_code,
        "status": status,
        "page_name": page.get("name") if isinstance(page, dict) else None,
        "page_id": page.get("id") if isinstance(page, dict) else None,
        "raw": body,
        "key_preview": (api_key[:10] + "..." + api_key[-4:]) if len(api_key) > 14 else "***",
    })


def _top_ordered_products(db, limit=5):
    rows = db.execute(
        """SELECT product_id, product_name
           FROM orders
           WHERE COALESCE(product_id, '') != '' OR COALESCE(product_name, '') != ''"""
    ).fetchall()
    counts = Counter()
    labels = {}
    for row in rows:
        ids = [x.strip() for x in str(row["product_id"] or "").split(",") if x.strip()]
        names = [x.strip() for x in str(row["product_name"] or "").split(",") if x.strip()]
        if not ids and names:
            ids = names
        for idx, pid in enumerate(ids):
            key = pid or (names[idx] if idx < len(names) else "")
            if not key:
                continue
            name = names[idx] if idx < len(names) and names[idx] else key
            counts[key] += 1
            labels.setdefault(key, name)
    return [
        {"product_id": key, "product_name": labels.get(key, key), "orders": count}
        for key, count in counts.most_common(limit)
    ]


@app.route("/api/dashboard_stats")
@_dash_require
def api_dashboard_stats():
    db    = get_db()
    today = datetime.now(BAGHDAD_TZ).date().isoformat()
    total_messages = db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    total_orders = db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    total_conversations = db.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    incoming_messages = db.execute("SELECT COUNT(*) FROM messages WHERE direction='incoming'").fetchone()[0]
    outgoing_messages = db.execute("SELECT COUNT(*) FROM messages WHERE direction='outgoing'").fetchone()[0]
    conversion_rate = round((total_orders / total_messages) * 100, 2) if total_messages else 0
    conversation_conversion_rate = round((total_orders / total_conversations) * 100, 2) if total_conversations else 0
    return jsonify({
        "total_conversations": total_conversations,
        "pending_reviews":     db.execute("SELECT COUNT(*) FROM human_reviews WHERE status='pending'").fetchone()[0],
        "orders_today":        db.execute("SELECT COUNT(*) FROM orders WHERE created_at >= ?", (today,)).fetchone()[0],
        "messages_today":      db.execute("SELECT COUNT(*) FROM messages WHERE created_at >= ?", (today,)).fetchone()[0],
        "messages_total":      total_messages,
        "incoming_messages":   incoming_messages,
        "outgoing_messages":   outgoing_messages,
        "orders_total":        total_orders,
        "message_to_order_conversion": conversion_rate,
        "conversation_to_order_conversion": conversation_conversion_rate,
        "top_products":        _top_ordered_products(db),
        "ai_enabled":          is_ai_enabled(db),
    })


def _analytics_range():
    period = (request.args.get("period") or "today").strip()
    today = datetime.now(BAGHDAD_TZ).date()
    if period == "yesterday":
        date_from = datetime.fromordinal(today.toordinal() - 1).date()
        date_to = today
    elif period == "7d":
        date_from = datetime.fromordinal(today.toordinal() - 6).date()
        date_to = datetime.fromordinal(today.toordinal() + 1).date()
    elif period == "30d":
        date_from = datetime.fromordinal(today.toordinal() - 29).date()
        date_to = datetime.fromordinal(today.toordinal() + 1).date()
    elif period == "custom":
        raw_from = request.args.get("from") or today.isoformat()
        raw_to = request.args.get("to") or raw_from
        try:
            date_from = datetime.fromisoformat(raw_from).date()
            date_to = datetime.fromordinal(datetime.fromisoformat(raw_to).date().toordinal() + 1).date()
        except ValueError:
            date_from = today
            date_to = datetime.fromordinal(today.toordinal() + 1).date()
    else:
        date_from = today
        date_to = datetime.fromordinal(today.toordinal() + 1).date()
    return period, date_from.isoformat(), date_to.isoformat()


@app.route("/api/analytics")
@_dash_require
def api_analytics():
    db = get_db()
    period, date_from, date_to = _analytics_range()
    params = (date_from, date_to)
    messages = db.execute(
        "SELECT COUNT(*) FROM messages WHERE created_at >= ? AND created_at < ?",
        params,
    ).fetchone()[0]
    incoming = db.execute(
        "SELECT COUNT(*) FROM messages WHERE direction='incoming' AND created_at >= ? AND created_at < ?",
        params,
    ).fetchone()[0]
    outgoing = db.execute(
        "SELECT COUNT(*) FROM messages WHERE direction='outgoing' AND created_at >= ? AND created_at < ?",
        params,
    ).fetchone()[0]
    orders = db.execute(
        "SELECT COUNT(*) FROM orders WHERE created_at >= ? AND created_at < ?",
        params,
    ).fetchone()[0]
    new_customers = db.execute(
        "SELECT COUNT(*) FROM customers WHERE first_seen_at >= ? AND first_seen_at < ?",
        params,
    ).fetchone()[0]
    human_reviews = db.execute(
        "SELECT COUNT(*) FROM human_reviews WHERE created_at >= ? AND created_at < ?",
        params,
    ).fetchone()[0]
    pending_reviews = db.execute(
        "SELECT COUNT(*) FROM human_reviews WHERE status='pending'",
    ).fetchone()[0]
    unanswered = db.execute(
        """SELECT COUNT(*) FROM customers c
           JOIN messages m ON m.id = (
             SELECT id FROM messages WHERE sender_id=c.sender_id ORDER BY id DESC LIMIT 1
           )
           WHERE m.direction='incoming'""",
    ).fetchone()[0]
    interests = db.execute(
        """SELECT product_id, product_name, COUNT(*) AS count
           FROM customer_product_interests
           WHERE COALESCE(status, 'active')='active'
           GROUP BY product_id, product_name
           ORDER BY count DESC
           LIMIT 8""",
    ).fetchall()
    objections = db.execute(
        """SELECT COALESCE(NULLIF(TRIM(notes), ''), 'اعتراض بدون نص') AS text, COUNT(*) AS count
           FROM customer_product_interests
           WHERE status='rejected'
           GROUP BY text
           ORDER BY count DESC
           LIMIT 8""",
    ).fetchall()
    return jsonify({
        "period": period,
        "date_from": date_from,
        "date_to": date_to,
        "cards": {
            "messages": messages,
            "incoming_messages": incoming,
            "outgoing_messages": outgoing,
            "orders": orders,
            "message_to_order_conversion": round((orders / messages) * 100, 2) if messages else 0,
            "new_customers": new_customers,
            "human_reviews": human_reviews,
            "pending_reviews": pending_reviews,
            "unanswered_conversations": unanswered,
        },
        "top_ordered_products": _top_ordered_products(db, limit=8),
        "top_interested_products": [dict(row) for row in interests],
        "top_objections": [dict(row) for row in objections],
        "ai": {
            "enabled": is_ai_enabled(db),
            "auto_product": get_auto_product_settings(db),
        },
    })


def _request_date_range(default_period="today"):
    period = (request.args.get("period") or default_period).strip()
    today = datetime.now(BAGHDAD_TZ).date()
    if period == "yesterday":
        start = today - timedelta(days=1)
        end = today
    elif period == "7d":
        start = today - timedelta(days=6)
        end = today + timedelta(days=1)
    elif period == "30d":
        start = today - timedelta(days=29)
        end = today + timedelta(days=1)
    elif period == "custom":
        try:
            start = datetime.fromisoformat(request.args.get("from") or today.isoformat()).date()
            end = datetime.fromisoformat(request.args.get("to") or today.isoformat()).date() + timedelta(days=1)
        except ValueError:
            start = today
            end = today + timedelta(days=1)
    else:
        period = "today"
        start = today
        end = today + timedelta(days=1)
    return period, start.isoformat(), end.isoformat()


def _percent(part, whole):
    return round((part / whole) * 100, 2) if whole else 0


def _infer_sales_stage(text, has_order=False, rejected=False):
    text = (text or "").strip().lower()
    if has_order:
        return "booked"
    if rejected:
        return "lost"
    if any(word in text for word in ("سعر", "السعر", "بكم", "شكد", "كم")):
        return "asked_price"
    if any(word in text for word in ("قياس", "مقاس", "سايز", "عمر")):
        return "asked_size"
    if any(word in text for word in ("غالي", "تخفيض", "خصم", "ليش")):
        return "price_objection"
    if any(word in text for word in ("عنوان", "موبايل", "هاتف", "رقم")):
        return "waiting_for_customer_info"
    return "conversation"


def calculate_message_to_order_conversion(db, start_date, end_date):
    total_messages = db.execute(
        "SELECT COUNT(*) FROM messages WHERE created_at >= ? AND created_at < ?",
        (start_date, end_date),
    ).fetchone()[0]
    total_conversations = db.execute(
        """SELECT COUNT(DISTINCT sender_id) FROM messages
           WHERE direction='incoming' AND created_at >= ? AND created_at < ?""",
        (start_date, end_date),
    ).fetchone()[0]
    total_orders = db.execute(
        "SELECT COUNT(*) FROM orders WHERE created_at >= ? AND created_at < ?",
        (start_date, end_date),
    ).fetchone()[0]
    return {
        "total_messages": total_messages,
        "total_conversations": total_conversations,
        "total_orders": total_orders,
        "conversation_conversion_rate": _percent(total_orders, total_conversations),
        "message_conversion_rate": _percent(total_orders, total_messages),
    }


def get_stage_breakdown(db, start_date, end_date):
    rows = db.execute(
        """SELECT m.sender_id, m.text,
                  EXISTS(SELECT 1 FROM orders o WHERE o.sender_id=m.sender_id AND o.created_at >= ? AND o.created_at < ?) AS has_order,
                  EXISTS(SELECT 1 FROM customer_product_interests cpi WHERE cpi.sender_id=m.sender_id AND cpi.status='rejected') AS rejected
           FROM messages m
           JOIN (
             SELECT sender_id, MAX(id) AS max_id
             FROM messages
             WHERE direction='incoming' AND created_at >= ? AND created_at < ?
             GROUP BY sender_id
           ) latest ON latest.max_id=m.id""",
        (start_date, end_date, start_date, end_date),
    ).fetchall()
    counts = Counter()
    for row in rows:
        counts[_infer_sales_stage(row["text"], bool(row["has_order"]), bool(row["rejected"]))] += 1
    total = sum(counts.values())
    return [
        {
            "stage": stage,
            "count": count,
            "percentage": _percent(count, total),
            "evaluation": "high_dropoff" if total and count / total >= 0.35 and stage not in {"booked"} else "normal",
            "suggestion": _stage_suggestion(stage),
        }
        for stage, count in counts.most_common()
    ]


def _stage_suggestion(stage):
    return {
        "asked_price": "حسن رد السعر بإضافة فحص عند الاستلام وسؤال حجز واضح.",
        "asked_size": "اختصر إجابة القياس واختم بسؤال يطلب العمر أو تأكيد الحجز.",
        "price_objection": "أضف رد اعتراض يوضح القيمة والتبديل والفحص بدون إطالة.",
        "waiting_for_customer_info": "اجعل طلب الهاتف والعنوان مباشرًا وبخطوة واحدة.",
        "conversation": "أضف CTA أوضح في الردود العامة حتى لا تتوقف المحادثة.",
        "lost": "راجع أسباب الرفض المتكررة قبل تغيير قواعد الرد.",
        "booked": "حافظ على النمط الحالي لأنه وصل إلى طلب.",
    }.get(stage, "راجع الرسائل في هذه المرحلة وأضف خطوة بيع أوضح.")


def get_dropoff_analysis(db, start_date, end_date):
    breakdown = get_stage_breakdown(db, start_date, end_date)
    return [row for row in breakdown if row["stage"] not in {"booked"}]


def get_followup_performance(db, start_date, end_date):
    rows = db.execute(
        """SELECT stage,
                  COUNT(*) AS sent,
                  SUM(CASE WHEN EXISTS(
                    SELECT 1 FROM messages m
                    WHERE m.sender_id=followups.sender_id
                      AND m.direction='incoming'
                      AND m.created_at > followups.sent_at
                  ) THEN 1 ELSE 0 END) AS replies,
                  SUM(CASE WHEN EXISTS(
                    SELECT 1 FROM orders o
                    WHERE o.sender_id=followups.sender_id
                      AND o.created_at > followups.sent_at
                  ) THEN 1 ELSE 0 END) AS orders_after
           FROM followups
           WHERE status='sent' AND sent_at >= ? AND sent_at < ?
           GROUP BY stage
           ORDER BY sent DESC""",
        (start_date, end_date),
    ).fetchall()
    return [
        {
            "stage": row["stage"] or "conversation",
            "sent": row["sent"] or 0,
            "replies": row["replies"] or 0,
            "orders_after": row["orders_after"] or 0,
            "success_rate": _percent(row["orders_after"] or 0, row["sent"] or 0),
        }
        for row in rows
    ]


def get_evaluation_metrics(db, start_date, end_date):
    conversion = calculate_message_to_order_conversion(db, start_date, end_date)
    followups_sent = db.execute(
        "SELECT COUNT(*) FROM followups WHERE status='sent' AND sent_at >= ? AND sent_at < ?",
        (start_date, end_date),
    ).fetchone()[0]
    orders_after_followup = db.execute(
        """SELECT COUNT(DISTINCT o.id)
           FROM orders o
           JOIN followups f ON f.sender_id=o.sender_id
           WHERE f.status='sent' AND o.created_at > f.sent_at
             AND o.created_at >= ? AND o.created_at < ?""",
        (start_date, end_date),
    ).fetchone()[0]
    human_review_count = db.execute(
        "SELECT COUNT(*) FROM human_reviews WHERE created_at >= ? AND created_at < ?",
        (start_date, end_date),
    ).fetchone()[0]
    stages = get_stage_breakdown(db, start_date, end_date)
    objections = db.execute(
        """SELECT COALESCE(NULLIF(TRIM(notes), ''), 'اعتراض بدون نص') AS text, COUNT(*) AS count
           FROM customer_product_interests
           WHERE status='rejected'
           GROUP BY text
           ORDER BY count DESC
           LIMIT 1"""
    ).fetchone()
    top_dropoff = next((row for row in stages if row["stage"] != "booked"), None)
    return {
        **conversion,
        "followups_sent": followups_sent,
        "orders_after_followup": orders_after_followup,
        "human_review_count": human_review_count,
        "top_dropoff_stage": (top_dropoff or {}).get("stage") or "",
        "top_objection": objections["text"] if objections else "",
    }


def _build_suggestions_from_metrics(metrics, stages, followups):
    suggestions = []
    if metrics.get("top_dropoff_stage"):
        stage = metrics["top_dropoff_stage"]
        suggestions.append({
            "suggestion_type": "reply_rule",
            "title": f"تحسين رد مرحلة {stage}",
            "content": _stage_suggestion(stage),
            "metric_name": "top_dropoff_stage",
            "metric_value": stage,
            "conversion_rate": metrics.get("conversation_conversion_rate", 0),
            "reason": f"أعلى نقطة توقف حالية هي {stage}.",
        })
    if metrics.get("conversation_conversion_rate", 0) < 15 and metrics.get("total_conversations", 0) >= 5:
        suggestions.append({
            "suggestion_type": "reply_rule",
            "title": "رفع وضوح دعوة الحجز",
            "content": "اختم الردود المهمة بسؤال حجز مباشر مثل: أحجزه إلج؟ أو ترسلين العنوان والموبايل؟",
            "metric_name": "conversation_conversion_rate",
            "metric_value": str(metrics.get("conversation_conversion_rate", 0)),
            "conversion_rate": metrics.get("conversation_conversion_rate", 0),
            "reason": "نسبة تحويل المحادثات إلى طلبات منخفضة ضمن الفترة المختارة.",
        })
    weak_followup = next((row for row in followups if row.get("sent") and row.get("success_rate", 0) < 10), None)
    if weak_followup:
        suggestions.append({
            "suggestion_type": "followup_rule",
            "title": f"تحسين متابعة {weak_followup['stage']}",
            "content": "اجعل رسالة المتابعة أقصر وفيها سبب واضح للرجوع للحجز بدل التذكير العام فقط.",
            "metric_name": "followup_success_rate",
            "metric_value": str(weak_followup.get("success_rate", 0)),
            "conversion_rate": weak_followup.get("success_rate", 0),
            "reason": "طلبات ما بعد المتابعة قليلة مقارنة بعدد المتابعات المرسلة.",
        })
    if not suggestions:
        suggestions.append({
            "suggestion_type": "review_note",
            "title": "مراجعة دورية للردود",
            "content": "لا توجد مشكلة واضحة في البيانات الحالية. راجع المحادثات يدويًا قبل اعتماد قواعد جديدة.",
            "metric_name": "data_volume",
            "metric_value": str(metrics.get("total_conversations", 0)),
            "conversion_rate": metrics.get("conversation_conversion_rate", 0),
            "reason": "البيانات الحالية لا تكفي لاستخراج توصية قوية أو لا تظهر هبوطًا واضحًا.",
        })
    return suggestions


def save_evaluation_suggestions(db, suggestions, date_range, metrics):
    now = now_baghdad_iso()
    saved = []
    for item in suggestions:
        cur = db.execute(
            """INSERT INTO evaluation_suggestions
               (suggestion_type, title, content, metric_name, metric_value, conversion_rate,
                reason, status, date_range_start, date_range_end, metrics_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, ?, ?)""",
            (
                item.get("suggestion_type") or "reply_rule",
                item.get("title") or "اقتراح تحسين",
                item.get("content") or "",
                item.get("metric_name") or "",
                str(item.get("metric_value") or ""),
                float(item.get("conversion_rate") or 0),
                item.get("reason") or "",
                date_range[0],
                date_range[1],
                json.dumps(metrics, ensure_ascii=False),
                now,
                now,
            ),
        )
        saved.append(cur.lastrowid)
    db.commit()
    return saved


def get_evaluation_suggestions(db, status=None, limit=100):
    params = []
    where = ""
    if status:
        where = "WHERE status=?"
        params.append(status)
    params.append(int(limit))
    return [
        dict(row)
        for row in db.execute(
            f"SELECT * FROM evaluation_suggestions {where} ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
    ]


def update_evaluation_suggestion(db, suggestion_id, title=None, content=None, status=None, admin_notes=None):
    row = db.execute("SELECT * FROM evaluation_suggestions WHERE id=?", (suggestion_id,)).fetchone()
    if not row:
        return None
    db.execute(
        """UPDATE evaluation_suggestions
           SET title=?, content=?, status=?, admin_notes=?, updated_at=?
           WHERE id=?""",
        (
            title if title is not None else row["title"],
            content if content is not None else row["content"],
            status if status is not None else row["status"],
            admin_notes if admin_notes is not None else row["admin_notes"],
            now_baghdad_iso(),
            suggestion_id,
        ),
    )
    db.commit()
    return dict(db.execute("SELECT * FROM evaluation_suggestions WHERE id=?", (suggestion_id,)).fetchone())


def approve_suggestion_as_rule(db, suggestion_id, priority=5):
    suggestion = db.execute(
        "SELECT * FROM evaluation_suggestions WHERE id=?",
        (suggestion_id,),
    ).fetchone()
    if not suggestion:
        return None
    now = now_baghdad_iso()
    cur = db.execute(
        """INSERT INTO active_ai_rules
           (source_suggestion_id, rule_type, rule_text, priority, active, created_at, updated_at)
           VALUES (?, ?, ?, ?, 1, ?, ?)""",
        (
            suggestion_id,
            suggestion["suggestion_type"] or "reply_rule",
            suggestion["content"],
            _setting_int(priority, 5, 1, 10),
            now,
            now,
        ),
    )
    db.execute(
        "UPDATE evaluation_suggestions SET status='active', updated_at=? WHERE id=?",
        (now, suggestion_id),
    )
    db.commit()
    return dict(db.execute("SELECT * FROM active_ai_rules WHERE id=?", (cur.lastrowid,)).fetchone())


def get_active_ai_rules(db):
    return [
        dict(row)
        for row in db.execute(
            "SELECT * FROM active_ai_rules WHERE active=1 ORDER BY priority ASC, id DESC"
        ).fetchall()
    ]


def run_daily_learning(db=None, start_date=None, end_date=None):
    db = db or get_db()
    today = datetime.now(BAGHDAD_TZ).date()
    start_date = start_date or (today - timedelta(days=1)).isoformat()
    end_date = end_date or today.isoformat()
    try:
        metrics = get_evaluation_metrics(db, start_date, end_date)
        stages = get_stage_breakdown(db, start_date, end_date)
        followups = get_followup_performance(db, start_date, end_date)
        suggestions = _build_suggestions_from_metrics(metrics, stages, followups)
        ids = save_evaluation_suggestions(db, suggestions, (start_date, end_date), metrics)
        report = (
            f"Daily learning {start_date} -> {end_date}: "
            f"messages={metrics.get('total_messages', 0)}, "
            f"conversations={metrics.get('total_conversations', 0)}, "
            f"orders={metrics.get('total_orders', 0)}, "
            f"conversion={metrics.get('conversation_conversion_rate', 0)}%"
        )
        db.execute(
            """INSERT INTO daily_learning_runs
               (date_range_start, date_range_end, metrics_json, report_text, suggestions_count, status)
               VALUES (?, ?, ?, ?, ?, 'completed')""",
            (start_date, end_date, json.dumps(metrics, ensure_ascii=False), report, len(ids)),
        )
        db.commit()
        return {"ok": True, "suggestions_count": len(ids), "suggestion_ids": ids, "report": report}
    except Exception as exc:
        db.execute(
            """INSERT INTO daily_learning_runs
               (date_range_start, date_range_end, status, error_text)
               VALUES (?, ?, 'failed', ?)""",
            (start_date, end_date, f"{type(exc).__name__}: {exc}"),
        )
        db.commit()
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


@app.route("/api/evaluation")
@_dash_require
def api_evaluation():
    db = get_db()
    period, start_date, end_date = _request_date_range()
    metrics = get_evaluation_metrics(db, start_date, end_date)
    stages = get_stage_breakdown(db, start_date, end_date)
    followups = get_followup_performance(db, start_date, end_date)
    return jsonify({
        "ok": True,
        "period": period,
        "date_from": start_date,
        "date_to": end_date,
        "metrics": metrics,
        "stage_breakdown": stages,
        "dropoff_analysis": get_dropoff_analysis(db, start_date, end_date),
        "followup_performance": followups,
        "suggestions": get_evaluation_suggestions(db, limit=50),
        "active_rules": get_active_ai_rules(db),
    })


@app.route("/api/evaluation/generate_suggestions", methods=["POST"])
@_dash_require
def api_generate_evaluation_suggestions():
    db = get_db()
    data = request.get_json(silent=True) or {}
    start_date = data.get("date_from")
    end_date = data.get("date_to")
    if not start_date or not end_date:
        _, start_date, end_date = _request_date_range()
    metrics = get_evaluation_metrics(db, start_date, end_date)
    stages = get_stage_breakdown(db, start_date, end_date)
    followups = get_followup_performance(db, start_date, end_date)
    suggestions = _build_suggestions_from_metrics(metrics, stages, followups)
    ids = save_evaluation_suggestions(db, suggestions, (start_date, end_date), metrics)
    return jsonify({"ok": True, "created": len(ids), "ids": ids})


@app.route("/api/evaluation/run_daily_learning", methods=["POST"])
@_dash_require
def api_run_daily_learning():
    data = request.get_json(silent=True) or {}
    return jsonify(run_daily_learning(get_db(), data.get("date_from"), data.get("date_to")))


@app.route("/api/evaluation/suggestions/<int:suggestion_id>", methods=["PUT"])
@_dash_require
def api_update_evaluation_suggestion(suggestion_id):
    data = request.get_json(silent=True) or {}
    updated = update_evaluation_suggestion(
        get_db(),
        suggestion_id,
        title=data.get("title"),
        content=data.get("content"),
        status=data.get("status"),
        admin_notes=data.get("admin_notes"),
    )
    if not updated:
        return jsonify({"ok": False, "error": "suggestion not found"}), 404
    return jsonify({"ok": True, "suggestion": updated})


@app.route("/api/evaluation/suggestions/<int:suggestion_id>/approve", methods=["POST"])
@_dash_require
def api_approve_evaluation_suggestion(suggestion_id):
    data = request.get_json(silent=True) or {}
    rule = approve_suggestion_as_rule(get_db(), suggestion_id, priority=data.get("priority", 5))
    if not rule:
        return jsonify({"ok": False, "error": "suggestion not found"}), 404
    return jsonify({"ok": True, "rule": rule})


@app.route("/api/evaluation/rules/<int:rule_id>", methods=["PUT"])
@_dash_require
def api_update_active_rule(rule_id):
    data = request.get_json(silent=True) or {}
    db = get_db()
    row = db.execute("SELECT * FROM active_ai_rules WHERE id=?", (rule_id,)).fetchone()
    if not row:
        return jsonify({"ok": False, "error": "rule not found"}), 404
    db.execute(
        """UPDATE active_ai_rules
           SET rule_text=?, rule_type=?, priority=?, active=?, updated_at=?
           WHERE id=?""",
        (
            data.get("rule_text", row["rule_text"]),
            data.get("rule_type", row["rule_type"]),
            _setting_int(data.get("priority", row["priority"]), row["priority"] or 5, 1, 10),
            1 if data.get("active", bool(row["active"])) else 0,
            now_baghdad_iso(),
            rule_id,
        ),
    )
    db.commit()
    return jsonify({"ok": True, "rule": dict(db.execute("SELECT * FROM active_ai_rules WHERE id=?", (rule_id,)).fetchone())})


@app.route("/api/settings/overview")
@_dash_require
def api_settings_overview():
    db = get_db()
    return jsonify({
        "ok": True,
        "ai": {
            "enabled": is_ai_enabled(db),
            "main_model": MAIN_MODEL,
            "improve_model": IMPROVE_MODEL,
            "checker_enabled": CHECKER_ENABLED,
            "checker_model": CHECKER_MODEL,
            "openrouter_key_present": bool(OPENROUTER_KEY),
        },
        "channels": {
            "manychat_key_present": bool(current_manychat_api_key()),
            "manychat_api_url": MANYCHAT_API_URL,
            "telegram_bot_present": bool(TELEGRAM_BOT_TOKEN),
            "telegram_chat_present": bool(TELEGRAM_CHAT_ID),
            "telegram_orders_chat_present": bool(TELEGRAM_ORDERS_CHAT_ID),
            "telegram_problems_chat_present": bool(TELEGRAM_PROBLEMS_CHAT_ID),
            "public_url": PUBLIC_URL,
            "human_reply_webhook_url": HUMAN_REPLY_WEBHOOK_URL,
        },
        "store": {
            "name": get_setting(db, "store_name", TELEGRAM_NOTIFICATION_HEADER),
            "phone": get_setting(db, "store_phone", ""),
            "delivery_policy": get_setting(db, "delivery_policy", FIXED_DELIVERY_TEXT),
            "provinces": get_setting(db, "store_provinces", ""),
            "inspection_message": get_setting(db, "inspection_message", ORDER_CONFIRMATION_TEXT),
        },
        "auto_product": get_auto_product_settings(db),
        "maintenance": {
            "database_path": os.path.basename(DB_PATH),
            "database_size": os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0,
            "products_count": len(load_products_from_file()),
        },
    })


@app.route("/api/settings/store", methods=["GET", "POST"])
@_dash_require
def api_store_settings():
    db = get_db()
    if request.method == "GET":
        return jsonify({
            "name": get_setting(db, "store_name", TELEGRAM_NOTIFICATION_HEADER),
            "phone": get_setting(db, "store_phone", ""),
            "delivery_policy": get_setting(db, "delivery_policy", FIXED_DELIVERY_TEXT),
            "provinces": get_setting(db, "store_provinces", ""),
            "inspection_message": get_setting(db, "inspection_message", ORDER_CONFIRMATION_TEXT),
        })
    data = request.get_json(silent=True) or {}
    for key, setting_key in (
        ("name", "store_name"),
        ("phone", "store_phone"),
        ("delivery_policy", "delivery_policy"),
        ("provinces", "store_provinces"),
        ("inspection_message", "inspection_message"),
    ):
        set_setting(db, setting_key, data.get(key, ""))
    return jsonify({"ok": True})


@app.route("/api/settings/ai", methods=["GET", "POST"])
@_dash_require
def api_set_ai_enabled():
    if request.method == "GET":
        db = get_db()
        return jsonify({
            "ok": True,
            "ai_enabled": is_ai_enabled(db),
            "main_model": MAIN_MODEL,
            "improve_model": IMPROVE_MODEL,
            "checker_enabled": CHECKER_ENABLED,
            "checker_model": CHECKER_MODEL,
            "openrouter_key_present": bool(OPENROUTER_KEY),
        })
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))
    db = get_db()
    set_setting(db, "ai_enabled", "1" if enabled else "0")
    print(f"[Settings] AI enabled={enabled}", flush=True)
    return jsonify({"ok": True, "ai_enabled": enabled})


@app.route("/api/settings/auto_product", methods=["GET", "POST"])
@_dash_require
def api_auto_product_settings():
    db = get_db()
    if request.method == "GET":
        settings = get_auto_product_settings(db)
        return jsonify({
            "ok": True,
            "auto_product_enabled": settings["enabled"],
            "auto_product_id": settings["product_id"],
            "auto_product_send_image": settings["send_image"],
            "product": settings["product"],
        })

    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled", data.get("auto_product_enabled", False)))
    product_id = str(data.get("product_id") or data.get("auto_product_id") or "").strip()
    send_image = bool(data.get("send_image", data.get("auto_product_send_image", False)))
    if enabled:
        product = find_product_by_id(product_id)
        if not product:
            return jsonify({
                "ok": False,
                "error": "auto_product_id must be an active product when enabled",
            }), 400
    set_setting(db, "auto_product_enabled", "1" if enabled else "0")
    set_setting(db, "auto_product_id", product_id)
    set_setting(db, "auto_product_send_image", "1" if send_image else "0")
    settings = get_auto_product_settings(db)
    print(
        f"[Settings] Auto product enabled={enabled} product_id={product_id} send_image={send_image}",
        flush=True,
    )
    return jsonify({
        "ok": True,
        "auto_product_enabled": settings["enabled"],
        "auto_product_id": settings["product_id"],
        "auto_product_send_image": settings["send_image"],
        "product": settings["product"],
    })


@app.route("/api/settings/followup", methods=["GET", "POST"])
@_dash_require
def api_followup_settings():
    db = get_db()
    if request.method == "GET":
        pending = db.execute("SELECT COUNT(*) FROM followups WHERE status='pending'").fetchone()[0]
        return jsonify({"ok": True, "settings": get_followup_settings(db), "pending_count": pending})
    settings = save_followup_settings(db, request.get_json(silent=True) or {})
    return jsonify({"ok": True, "settings": settings})


@app.route("/api/followups/customer_messages", methods=["GET", "POST"])
@_dash_require
def api_followup_customer_messages():
    db = get_db()
    if request.method == "GET":
        limit = request.args.get("limit", "500")
        try:
            limit = max(1, min(int(limit), 2000))
        except ValueError:
            limit = 500
        return jsonify({
            "ok": True,
            "customers": list_customer_followup_messages(db, limit=limit),
        })

    data = request.get_json(silent=True) or {}
    result = save_customer_followup_template(
        db,
        data.get("sender_id"),
        data.get("message_template") or "",
        update_pending=bool(data.get("update_pending", True)),
    )
    if not result.get("ok"):
        return jsonify(result), 400
    return jsonify(result)


@app.route("/api/followups/send_due", methods=["POST"])
@_dash_require
def api_send_due_followups():
    return jsonify({"ok": True, **send_due_followups(get_db())})


@app.route("/api/followups/cancel_pending", methods=["POST"])
@_dash_require
def api_cancel_pending_followups():
    db = get_db()
    cur = db.execute("UPDATE followups SET status='cancelled', sent_at=? WHERE status='pending'", (now_baghdad_iso(),))
    db.commit()
    return jsonify({"ok": True, "cancelled": cur.rowcount})


@app.route("/api/conversations/<sender_id>/ai", methods=["POST"])
@_dash_require
def api_set_conversation_ai(sender_id):
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))
    db = get_db()
    set_customer_ai_enabled(db, sender_id, enabled)
    print(f"[Settings] AI for {sender_id} enabled={enabled}", flush=True)
    return jsonify({"ok": True, "ai_enabled": enabled})


@app.route("/api/conversations/<sender_id>/send_catalog", methods=["POST"])
@_dash_require
def api_send_catalog(sender_id):
    db = get_db()
    customer = db.execute(
        "SELECT page_id, COALESCE(platform, 'facebook') AS platform FROM customers WHERE sender_id=?",
        (sender_id,),
    ).fetchone()
    page_id = customer["page_id"] if customer else ""
    platform = customer["platform"] if customer else "facebook"
    image_count = len(build_catalog_image_messages(load_active_products(db)))
    threading.Thread(
        target=send_catalog_to_customer_background,
        args=(sender_id, page_id, platform),
        daemon=True,
    ).start()
    return jsonify({
        "ok": True,
        "sent": True,
        "queued": True,
        "image_count": image_count,
        "reply": "",
    })


@app.route("/api/conversations/<sender_id>/customer", methods=["POST"])
@_dash_require
def api_update_customer(sender_id):
    data = request.get_json(silent=True) or {}
    db   = get_db()
    now  = now_baghdad_iso()
    raw_gender = (data.get("gender") or "").strip().lower()
    gender_value = None
    if raw_gender in {"male", "m", "ذكر"}:
        gender_value = "male"
    elif raw_gender in {"female", "f", "انثى", "أنثى"}:
        gender_value = "female"
    elif raw_gender in {"unknown", "u", "", "غير محدد"}:
        gender_value = None
    db.execute(
        "UPDATE customers SET name=COALESCE(?,name), phone=COALESCE(?,phone), "
        "province=COALESCE(?,province), address=COALESCE(?,address), "
        "gender=COALESCE(?,gender), last_seen_at=? "
        "WHERE sender_id=?",
        (data.get("name") or None, data.get("phone") or None,
         data.get("province") or None, data.get("address") or None,
         gender_value, now, sender_id),
    )
    db.commit()
    return jsonify({"ok": True, "gender": gender_value})


@app.route("/api/conversations/<sender_id>/gender", methods=["POST"])
@_dash_require
def api_set_customer_gender(sender_id):
    data = request.get_json(silent=True) or {}
    raw_gender = (data.get("gender") or "").strip().lower()
    if raw_gender in {"male", "m", "ذكر"}:
        gender_value = "male"
    elif raw_gender in {"female", "f", "انثى", "أنثى"}:
        gender_value = "female"
    else:
        gender_value = None  # غير محدد
    db = get_db()
    db.execute("UPDATE customers SET gender=? WHERE sender_id=?", (gender_value, sender_id))
    db.commit()
    return jsonify({"ok": True, "gender": gender_value})


@app.route("/api/conversations/<sender_id>/create_order", methods=["POST"])
@_dash_require
def api_create_order(sender_id):
    data = request.get_json(silent=True) or {}
    db   = get_db()
    now  = now_baghdad_iso()
    product_ids = data.get("product_ids")
    if not isinstance(product_ids, list):
        product_ids = [data.get("product_id")]
    product_ids = [str(pid or "").strip() for pid in product_ids if str(pid or "").strip()]
    product_names = data.get("product_names")
    if not isinstance(product_names, list):
        product_names = [data.get("product_name")]
    product_names = [str(name or "").strip() for name in product_names if str(name or "").strip()]
    product_id_text = ", ".join(product_ids)
    product_name_text = ", ".join(product_names) or str(data.get("product_name") or "")
    if not product_id_text and not product_name_text.strip():
        return jsonify({"ok": False, "error": "اختر منتجاً قبل تثبيت الطلب"}), 400

    duplicate = find_duplicate_order(
        db,
        sender_id,
        data.get("phone"),
        product_id_text,
        data.get("address"),
    )
    if duplicate:
        print(f"[Dashboard] Duplicate manual order skipped for {sender_id}: existing #{duplicate.get('id')}", flush=True)
        return jsonify({
            "ok": True,
            "duplicate": True,
            "existing_order_id": duplicate.get("id"),
            "telegram_sent": False,
            "message": "هذا الطلب مثبت مسبقاً ولم يتم تكراره",
        })

    order_info = {
        "created_at": now,
        "sender_id": sender_id,
        "customer_name": data.get("customer_name"),
        "phone": data.get("phone"),
        "province": data.get("province"),
        "address": data.get("address"),
        "product_id": product_id_text,
        "product_name": product_name_text,
        "color": data.get("color"),
        "size": data.get("size"),
        "notes": data.get("notes"),
        "status": "new",
    }

    db.execute(
        "INSERT INTO orders (sender_id, customer_name, phone, province, address, "
        "product_id, product_name, color, size, notes, status, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,'new',?)",
        (sender_id, data.get("customer_name"), data.get("phone"),
         data.get("province"), data.get("address"), product_id_text,
         product_name_text, data.get("color"), data.get("size"),
         data.get("notes"), now),
    )
    db.commit()
    save_booking_to_file(order_info)
    telegram_sent = send_order_to_telegram(order_info)
    confirm = ORDER_CONFIRMATION_TEXT
    save_message(db, sender_id, "outgoing", "text", confirm, None, None, None, {"manual_order": True})
    customer = db.execute(
        "SELECT page_id, COALESCE(platform, 'facebook') AS platform FROM customers WHERE sender_id=?",
        (sender_id,),
    ).fetchone()
    send_text_to_facebook(
        sender_id,
        confirm,
        customer["page_id"] if customer else "",
        customer["platform"] if customer else "facebook",
    )
    print(f"[Dashboard] Manual order created for {sender_id}: {product_name_text} | telegram_sent={telegram_sent}", flush=True)
    return jsonify({
        "ok": True,
        "telegram_sent": telegram_sent,
        "telegram_error": "" if telegram_sent else "تعذر إرسال الطلب إلى تلغرام. تأكد من TELEGRAM_BOT_TOKEN و TELEGRAM_ORDERS_CHAT_ID أو ORDER_TELEGRAM_CHAT_ID.",
    })


@app.route("/api/conversations/<sender_id>/mark_reviewed", methods=["POST"])
@_dash_require
def api_mark_reviewed(sender_id):
    db  = get_db()
    now = now_baghdad_iso()
    db.execute(
        "UPDATE human_reviews SET status='reviewed', replied_at=? "
        "WHERE sender_id=? AND status='pending'",
        (now, sender_id),
    )
    db.commit()
    try:
        set_customer_ai_enabled(db, sender_id, True)
    except Exception as exc:
        print(f"[Dashboard] Could not re-enable AI after mark_reviewed: {exc}", flush=True)
    return jsonify({"ok": True, "ai_resumed": True})


# ── Entry point ────────────────────────────────────────────────────────────────

_app_bootstrapped = False


def bootstrap_app(load_clip=False):
    global _app_bootstrapped
    if not _app_bootstrapped:
        init_db()
        _app_bootstrapped = True

    if load_clip and CLIP_AVAILABLE:
        try:
            load_clip_model()
        except Exception as exc:
            print(f"[CLIP] ⚠️  Failed to load at startup: {exc}", flush=True)
    elif load_clip:
        print("[CLIP] Skipping model load — libraries not installed.", flush=True)


bootstrap_app(load_clip=False)


if __name__ == "__main__":
    bootstrap_app(load_clip=True)
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
