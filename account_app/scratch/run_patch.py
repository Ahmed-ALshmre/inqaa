import sys
from pathlib import Path

# Add the workspace root to path to import fix_patch
sys.path.append(str(Path(".").resolve()))

print("Importing fix_patch...")
import fix_patch
print("Imported successfully.")

print("APP_PATH exists?", fix_patch.APP_PATH.exists())
original = fix_patch.APP_PATH.read_text(encoding="utf-8")
print("Read app.py. Length:", len(original))

print("Backing up app.py...")
backup = fix_patch.backup_file(fix_patch.APP_PATH)
print("Backup created:", backup)

text = original
print("Running replace_old_direct_image_review...")
text, changed = fix_patch.replace_old_direct_image_review(text)
print("Finished. Changed?", changed)

print("Running patch_human_review_reason...")
text, count = fix_patch.patch_human_review_reason(text)
print("Finished. Replaced count:", count)

print("Running patch_clear_memory_on_failed_image...")
text, count = fix_patch.patch_clear_memory_on_failed_image(text)
print("Finished. Replaced count:", count)

print("Running insert_image_context_instruction...")
text, changed = fix_patch.insert_image_context_instruction(text)
print("Finished. Changed?", changed)

print("Running inject_customer_context_near_prompt...")
text, changed = fix_patch.inject_customer_context_near_prompt(text)
print("Finished. Changed?", changed)

print("Running patch_prompt_append...")
text, changed = fix_patch.patch_prompt_append(text)
print("Finished. Changed?", changed)

print("Running ensure_env_defaults_comment...")
text, changed = fix_patch.ensure_env_defaults_comment(text)
print("Finished. Changed?", changed)

if text == original:
    print("No changes needed for app.py.")
else:
    print("Writing modified app.py...")
    fix_patch.APP_PATH.write_text(text, encoding="utf-8")
    print("Written app.py successfully.")

print("Running patch_dashboard_template...")
dash_changed, dash_msg = fix_patch.patch_dashboard_template()
print("Finished. Changed?", dash_changed, "Msg:", dash_msg)

print("All patch steps completed successfully!")
