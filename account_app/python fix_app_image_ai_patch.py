# -*- coding: utf-8 -*-
"""
fix_app_image_ai_patch.py

طريقة الاستخدام:
1) ضع هذا الملف بجانب app.py
2) شغّل:
   python fix_app_image_ai_patch.py

النتيجة:
- يأخذ نسخة احتياطية من app.py
- يمنع تحويل الصورة للمراجعة البشرية قبل تحليل AI في النسخ القديمة
- يجعل المراجعة البشرية فقط عند فشل التعرف على المنتج
- يضيف دالة سياق المنتجات حتى يفهم AI أكثر من منتج لنفس الزبونة
- يحاول إصلاح زر تحليل المنتجات في قالب الداشبورد إذا وجد الزر
"""

from pathlib import Path
from datetime import datetime
import re
import sys

APP_PATH = Path("app.py")


def fail(msg):
    print("❌ " + msg)
    sys.exit(1)


def backup_file(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.with_name(f"{path.stem}.backup_{stamp}{path.suffix}")
    backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return backup


def replace_old_direct_image_review(text: str) -> tuple[str, bool]:
    """
    يعالج النسخ القديمة التي كانت تحول كل صورة مباشرة للموظف قبل match_product.
    يحذف البلوك الذي يحتوي:
    All customer images are routed to human Telegram review
    قبل خطوة Product matching.
    """
    pattern = re.compile(
        r'''(?P<indent>[ \t]*)if\s+message_type\s*==\s*["']image["']\s+and\s+HUMAN_REVIEW_ALL_IMAGES\s*:\s*\n
(?:(?P=indent)[ \t]+.*\n)*?
(?P=indent)[ \t]+return\s+final\s*\n
\s*\n
(?P<comment>[ \t]*#\s*[-─ ]*STEP\s*0?8:\s*Product matching[^\n]*\n)''',
        re.VERBOSE
    )
    new_text, count = pattern.subn(r"\g<comment>", text, count=1)
    return new_text, bool(count)


def patch_human_review_reason(text: str) -> tuple[str, int]:
    text, count = re.subn(
        r'"All customer images are routed to human Telegram review"',
        '"AI could not confidently match customer image; routed to human review"',
        text,
    )
    return text, count


def patch_clear_memory_on_failed_image(text: str) -> tuple[str, int]:
    """
    في النسخ التي تمسح ذاكرة المنتج عند صورة غير مطابقة، نعلّق المسح حتى لا يضيع سياق المنتجات السابقة.
    """
    pattern = re.compile(
        r'''(?P<indent>[ \t]*)clear_customer_product_memory\(\s*\n
(?P=indent)[ \t]+db,\s*ev\["sender_id"\],\s*"new image did not match any product"\s*\n
(?P=indent)\)\s*''',
        re.VERBOSE
    )
    repl = (
        r'\g<indent># لا نمسح ذاكرة المنتجات عند فشل صورة واحدة؛ قد تكون الزبونة تسأل عن موديلات سابقة\n'
        r'\g<indent># clear_customer_product_memory(db, ev["sender_id"], "new image did not match any product")'
    )
    text, count = pattern.subn(repl, text)
    return text, count


def insert_image_context_instruction(text: str) -> tuple[str, bool]:
    """
    يضيف دالة عامة للموديل حتى يفهم 'هذا/هاي/الموديل' من آخر المنتجات المرتبطة.
    """
    if "def build_customer_product_context_for_ai" in text:
        return text, False

    marker = "def load_customer_products(db, sender_id, limit=5):"
    pos = text.find(marker)
    if pos == -1:
        marker2 = "# ── AI config loader"
        pos = text.find(marker2)
        if pos == -1:
            return text, False

    helper = r'''


def build_customer_product_context_for_ai(db, sender_id, products=None, limit=5):
    """
    يبني سياق مختصر للمنتجات التي ارتبطت بهذه الزبونة من صور/نصوص سابقة.
    استخدم الناتج داخل prompt الموديل الرئيسي حتى يفهم:
    "هذا"، "هاي"، "الموديل"، "متوفر؟"، "السعر؟"، "أريد واحد".
    """
    try:
        remembered = load_customer_products(db, sender_id, limit=limit)
    except Exception:
        remembered = []

    product_map = {}
    if products:
        for p in products:
            product_map[str(p.get("product_id") or "")] = p

    rows = []
    for item in remembered:
        pid = str(item.get("product_id") or "")
        p = product_map.get(pid) or {}
        rows.append({
            "product_id": pid,
            "product_name": item.get("product_name") or p.get("product_name") or "",
            "price": p.get("price"),
            "colors": p.get("colors"),
            "sizes": p.get("sizes"),
            "stock": p.get("stock"),
            "match_method": item.get("match_method"),
            "confidence": item.get("confidence"),
            "last_seen_at": item.get("last_seen_at"),
        })

    if not rows:
        return "لا توجد منتجات مرتبطة بهذه الزبونة بعد."

    return (
        "منتجات مرتبطة بهذه الزبونة من الصور أو المحادثة السابقة:\n"
        + json.dumps(rows, ensure_ascii=False, indent=2)
        + "\n\n"
        "قواعد مهمة:\n"
        "- إذا قالت الزبونة: هذا / هاي / الموديل / السعر / متوفر / أريد واحد، فغالباً تقصد آخر منتج مرتبط.\n"
        "- إذا يوجد أكثر من منتج مرتبط وطلبها غير واضح، اسألها سؤال قصير: تقصدين أي موديل، الأول لو الثاني؟\n"
        "- لا تخترع لون أو قياس أو سعر غير موجود في بيانات المنتج.\n"
    )
'''
    text = text[:pos] + helper + "\n\n" + text[pos:]
    return text, True


def inject_customer_context_near_prompt(text: str) -> tuple[str, bool]:
    """
    يحاول إدخال سياق المنتجات بعد customer_products = load_customer_products(...)
    """
    if "customer_product_context_for_ai" in text:
        return text, False

    pattern = re.compile(
        r'(?P<line>(?P<indent>[ \t]*)customer_products\s*=\s*load_customer_products\(db,\s*ev\["sender_id"\][^\n]*\)\s*\n)'
    )

    def repl(m):
        indent = m.group("indent")
        return (
            m.group("line")
            + f'{indent}customer_product_context_for_ai = build_customer_product_context_for_ai(db, ev["sender_id"], products)\n'
        )

    text2, count = pattern.subn(repl, text, count=1)
    return text2, bool(count)


def patch_prompt_append(text: str) -> tuple[str, bool]:
    """
    إذا كان لديك instructions, rules = load_ai_config(...)
    نضيف سياق المنتجات إلى instructions.
    """
    if 'customer_product_context_for_ai + "\\n\\n" + instructions' in text:
        return text, False

    pattern = re.compile(
        r'(?P<indent>[ \t]*)instructions,\s*rules\s*=\s*load_ai_config\(db,\s*sender_id=ev\["sender_id"\]\)\s*\n'
    )

    def repl(m):
        indent = m.group("indent")
        return (
            m.group(0)
            + f'{indent}try:\n'
            + f'{indent}    instructions = customer_product_context_for_ai + "\\n\\n" + instructions\n'
            + f'{indent}except Exception:\n'
            + f'{indent}    pass\n'
        )

    text2, count = pattern.subn(repl, text, count=1)
    return text2, bool(count)


def patch_dashboard_template() -> tuple[bool, str]:
    candidates = [
        Path("templates/dashboard.html"),
        Path("templates/index.html"),
        Path("templates/dashboard/index.html"),
    ]

    js = r'''
<script>
async function analyzeProducts() {
  const key = new URLSearchParams(window.location.search).get("key") || "admin123";
  const btn = document.getElementById("analyzeProductsBtn");
  const box = document.getElementById("productAnalysisResult");

  try {
    if (btn) {
      btn.disabled = true;
      btn.textContent = "جاري التحليل...";
    }

    const res = await fetch(`/api/products/analyze?key=${encodeURIComponent(key)}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Dashboard-Key": key
      },
      body: JSON.stringify({})
    });

    const data = await res.json();

    if (!res.ok || !data.ok) {
      throw new Error(data.error || "فشل تحليل المنتجات");
    }

    if (box) {
      box.textContent =
        `تم تحليل ${data.count || 0} منتج\n` +
        `المصدر: ${data.source || ""}\n` +
        `آخر تحديث: ${data.updated_at || ""}\n\n` +
        `${data.summary || ""}`;
    }

    alert("تم تحليل المنتجات بنجاح");
  } catch (err) {
    console.error(err);
    alert("خطأ في تحليل المنتجات: " + err.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = "تحليل المنتجات";
    }
  }
}
</script>
'''

    for p in candidates:
        if not p.exists():
            continue
        content = p.read_text(encoding="utf-8")
        if "function analyzeProducts()" in content:
            return False, f"{p} يحتوي سكربت التحليل مسبقاً"
        if "analyzeProductsBtn" not in content and "/api/products/analyze" not in content:
            continue
        backup_file(p)
        if "</body>" in content:
            content = content.replace("</body>", js + "\n</body>")
        else:
            content += "\n" + js + "\n"
        p.write_text(content, encoding="utf-8")
        return True, f"تم تعديل {p}"

    return False, "لم أجد قالب داشبورد يحتوي زر analyzeProductsBtn؛ إذا كان اسم الزر مختلفاً أضف السكربت يدوياً"


def ensure_env_defaults_comment(text: str) -> tuple[str, bool]:
    marker = "# IMAGE_AI_PATCH_NOTES"
    if marker in text:
        return text, False

    insert_after = "HUMAN_REVIEW_ALL_IMAGES"
    idx = text.find(insert_after)
    if idx == -1:
        return text, False

    line_end = text.find("\n", idx)
    note = r'''
# IMAGE_AI_PATCH_NOTES:
# لتفعيل التعرف من الصور ضع في .env:
# VISION_ENABLED=1
# CATALOG_MATCH_ENABLED=1
# VISION_MODEL=google/gemini-3.1-pro-preview
# CATALOG_MATCH_MODEL=google/gemini-3.1-pro-preview
# HUMAN_REVIEW_ALL_IMAGES=1
# ملاحظة: المراجعة البشرية أصبحت بعد فشل AI وليست قبل AI.
'''
    text = text[:line_end + 1] + note + text[line_end + 1:]
    return text, True


def main():
    if not APP_PATH.exists():
        fail("لم أجد app.py. ضع هذا الملف بجانب app.py ثم شغّله.")

    original = APP_PATH.read_text(encoding="utf-8")
    backup = backup_file(APP_PATH)
    text = original
    report = []

    text, changed = replace_old_direct_image_review(text)
    report.append(f"حذف تحويل الصور المباشر قبل AI: {'نعم' if changed else 'غير موجود/معدل مسبقاً'}")

    text, count = patch_human_review_reason(text)
    report.append(f"تعديل سبب المراجعة البشرية للصور: {count} موضع")

    text, count = patch_clear_memory_on_failed_image(text)
    report.append(f"منع مسح ذاكرة المنتجات عند فشل صورة واحدة: {count} موضع")

    text, changed = insert_image_context_instruction(text)
    report.append(f"إضافة دالة سياق المنتجات للـ AI: {'نعم' if changed else 'موجودة مسبقاً/لم أجد مكان مناسب'}")

    text, changed = inject_customer_context_near_prompt(text)
    report.append(f"تحميل سياق المنتجات داخل معالجة الرسالة: {'نعم' if changed else 'موجود مسبقاً/لم أجد النمط'}")

    text, changed = patch_prompt_append(text)
    report.append(f"إضافة سياق المنتجات إلى instructions: {'نعم' if changed else 'لم أجد load_ai_config بالنمط المتوقع'}")

    text, changed = ensure_env_defaults_comment(text)
    report.append(f"إضافة ملاحظات إعدادات الصور: {'نعم' if changed else 'موجودة مسبقاً/لم أجد مكان مناسب'}")

    if text == original:
        print("⚠️ لم يتم تغيير app.py لأن النسخة تبدو معدلة مسبقاً أو مختلفة جداً.")
        print(f"تم إنشاء نسخة احتياطية: {backup}")
    else:
        APP_PATH.write_text(text, encoding="utf-8")
        print("✅ تم تعديل app.py بنجاح")
        print(f"📦 النسخة الاحتياطية: {backup}")

    dash_changed, dash_msg = patch_dashboard_template()
    report.append(f"إصلاح JS زر تحليل المنتجات: {dash_msg}")

    print("\nتقرير التعديلات:")
    for line in report:
        print("- " + line)

    print("\nاختبار سريع:")
    print("1) شغّل: python -m py_compile app.py")
    print("2) أعد تشغيل السيرفر")
    print("3) جرّب إرسال صورة منتج: المفروض AI يحلل أولاً، وإذا فشل فقط تذهب للمراجعة البشرية.")
    print("4) تأكد من .env: VISION_ENABLED=1 و CATALOG_MATCH_ENABLED=1 و OPENROUTER_API_KEY موجود.")


if __name__ == "__main__":
    main()
