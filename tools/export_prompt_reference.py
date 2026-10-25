"""Export editable prompt text and its exact source map without importing the app.

This is a review artifact, not a runtime prompt loader. It never reads .env,
customer databases, product data, or conversation history.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess
from functools import lru_cache


ROOT = Path(__file__).resolve().parents[1]
ARABIC = re.compile(r'[\u0621-\u064a]')
PROMPT_NAMES = re.compile(r'PROMPT|GUIDE|GUIDANCE|SYSTEM|INSTRUCTION|RULES|STYLE_LOCK|SALES_POLICY|STORE_DESCRIPTION|REVIEWED_LESSONS|SALES_CUES|METHODS|LESSONS|PROFILES')
PROMPT_WORDS = re.compile(r'أنت[َِ]?|انت[َِ]? |مهمتك|أرجع|أعد |أجب|ممنوع|لا تخترع|لا تذكر|لا تطلب|Return JSON|You are|JSON only|Your task', re.I)
PROMPT_VARS = re.compile(r'prompt|instruction|fix_instruction|sections|system|context|user_message|content', re.I)
REPLY_VARS = re.compile(r'reply|answer|opening|boundary|next_step|first|detail|parts|lines|step|close|warm|message|question_help', re.I)
PROMPT_MODULES = {'sales_strategy', 'sales_engagement', 'sales_playbook', 'sales_learning',
                  'sales_context', 'sales_decision', 'conversation_quality', 'catalog_truth', 'baraah'}
REPLY_MODULES = PROMPT_MODULES | {'sales_answers', 'sales_recovery', 'product_albums',
                                'catalog_browsing', 'checkout', 'order_actions', 'conversion_growth'}
PARAMS = {'model', 'temperature', 'max_tokens', 'top_p', 'presence_penalty',
          'frequency_penalty', 'reasoning', 'response_format', 'timeout', 'seed'}
FILES = ('instructions.txt', 'forbidden_rules.txt', 'product_ai_summary.txt', 'gemini_sales_playbook.md')
GROUPS = ('برومبتات وتعليمات مسماة', 'تعليمات وتركيب داخل الدوال',
          'ردود وقوالب جاهزة من الكود', 'إعدادات النموذج وطلب التوليد', 'ملفات التعليمات النصية')


def digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


@lru_cache(maxsize=40)
def source_lines(source):
    return [line.encode('utf-8') for line in source.splitlines(keepends=True)]


def segment(source, node):
    lines = source_lines(source)
    if node.lineno == node.end_lineno:
        return lines[node.lineno - 1][node.col_offset:node.end_col_offset].decode('utf-8')
    return (lines[node.lineno - 1][node.col_offset:] + b''.join(lines[node.lineno:node.end_lineno - 1])
            + lines[node.end_lineno - 1][:node.end_col_offset]).decode('utf-8')


def render(node, source, env=None):
    """Preserve runtime expressions as visible placeholders; never execute code."""
    env = env or {}
    if isinstance(node, ast.Constant):
        return str(node.value) if node.value is not None else 'None'
    if isinstance(node, ast.JoinedStr):
        return ''.join(render(part, source, env) if isinstance(part, ast.Constant)
                       else '{' + ast.unparse(part.value)
                       + (':' + render(part.format_spec, source, env) if part.format_spec else '') + '}'
                       for part in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return render(node.left, source, env) + render(node.right, source, env)
    if isinstance(node, ast.Name) and node.id in env:
        return env[node.id]
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == 'replace' and len(node.args) in (2, 3):
            try:
                base = render(node.func.value, source, env)
                left, right = (render(part, source, env) for part in node.args[:2])
                return base.replace(left, right, int(node.args[2].value) if len(node.args) == 3 else -1)
            except (ValueError, AttributeError):
                pass
    if isinstance(node, (ast.List, ast.Tuple)):
        return '\n'.join(render(part, source, env) for part in node.elts)
    return '⟪تعبير وقت التشغيل: ' + segment(source, node) + '⟫'


def ancestors(node, parents):
    while node in parents:
        node = parents[node]
        yield node


def symbol(node, parents):
    names = [p.name for p in ancestors(node, parents)
             if isinstance(p, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))]
    return '.'.join(reversed(names)) or '<module>'


def value_key(node, parents):
    parent = parents.get(node)
    if isinstance(parent, ast.Dict):
        for key, value in zip(parent.keys, parent.values):
            if value is node and isinstance(key, ast.Constant):
                return str(key.value)
    return ''


def is_docstring(node, parents):
    parent = parents.get(node)
    if not isinstance(parent, ast.Expr):
        return False
    owner = parents.get(parent)
    return bool(getattr(owner, 'body', None) and owner.body[0] is parent)


def non_copy(node, parents):
    for parent in ancestors(node, parents):
        if isinstance(parent, ast.Call):
            name = ast.unparse(parent.func)
            if name.startswith(('re.', 'db.execute', 'db.executemany')) or name in {'log', 'print', 'image_flow'}:
                return True
        if isinstance(parent, ast.stmt):
            break
    return False


def context_target(node, parents):
    for parent in ancestors(node, parents):
        if isinstance(parent, ast.Assign):
            return ', '.join(ast.unparse(target) for target in parent.targets)
        if isinstance(parent, (ast.AnnAssign, ast.AugAssign)):
            return ast.unparse(parent.target)
        if isinstance(parent, ast.Call) and isinstance(parent.func, ast.Attribute) and parent.func.attr in {'append', 'extend'}:
            return ast.unparse(parent.func)
        if isinstance(parent, ast.Return):
            return 'return'
        if isinstance(parent, ast.stmt):
            return type(parent).__name__
    return ''


def natural(text):
    if text.startswith(('SELECT ', 'INSERT ', 'CREATE ', 'UPDATE ', 'DELETE ', 'ALTER ')):
        return False
    return ((len(ARABIC.findall(text)) >= 3 and (' ' in text or len(text) >= 4))
            or bool(PROMPT_WORDS.search(text))
            or (len(text) > 20 and bool(re.search(r'[A-Za-z]{3,}\s+[A-Za-z]{3,}', text))))


def collect(path):
    source = path.read_text(encoding='utf-8-sig')
    tree = ast.parse(source)
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    relative, module = path.relative_to(ROOT).as_posix(), path.stem
    entries, covered, env = [], set(), {}

    def add(node, group, target, body=None, note=''):
        expression = segment(source, node)
        entries.append(dict(source=relative, symbol=symbol(node, parents), target=target,
                            start=node.lineno, end=node.end_lineno, expression=expression,
                            source_sha256=digest(expression), file_sha256=digest(source),
                            group=group, text=body if body is not None else render(node, source), note=note))
        covered.update(id(part) for part in ast.walk(node))

    for statement in tree.body:
        if not isinstance(statement, (ast.Assign, ast.AnnAssign)):
            continue
        targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
        name = ast.unparse(targets[0])
        value = statement.value
        if PROMPT_NAMES.search(name) and not name.startswith('_'):
            if isinstance(value, ast.Dict):
                for key, body in zip(value.keys, value.values):
                    if isinstance(key, ast.Constant):
                        add(body, GROUPS[0], f'{name}[{key.value!r}]', render(body, source, env))
            elif value is not None:
                body = render(value, source, env)
                add(value, GROUPS[0], name, body,
                    'قيمة مشتقة؛ صيغة المصدر محفوظة أسفل القسم.' if not isinstance(value, (ast.Constant, ast.BinOp, ast.JoinedStr)) else '')
                if not body.startswith('⟪'):
                    env[name] = body
        elif (re.search(r'REPLY|CONFIRMATION_TEXT|INSPECTION_MESSAGE', name)
              or (module == 'baraah' and name == 'DESCRIPTION')) and value is not None:
            add(value, GROUPS[0] if name == 'DESCRIPTION' else GROUPS[2], name, render(value, source, env))
        elif re.fullmatch(r'[A-Z_]*MODEL[A-Z_]*', name) and value is not None:
            add(value, GROUPS[3], name)

    # Each provider's system-message slot is indexed, including references to
    # prompt variables. This makes omissions detectable without importing app.py.
    system_slots = []
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg in PARAMS and id(node.value) not in covered:
            owner_call = parents.get(node)
            if isinstance(owner_call, ast.Call) and re.search(r'ai_transport\.post|requests\.post', ast.unparse(owner_call.func)):
                add(node.value, GROUPS[3], node.arg)
        if not isinstance(node, ast.Dict):
            continue
        pairs = {key.value: value for key, value in zip(node.keys, node.values)
                 if isinstance(key, ast.Constant) and isinstance(key.value, str)}
        role = pairs.get('role')
        if isinstance(role, ast.Constant) and role.value == 'system' and 'content' in pairs:
            value = pairs['content']
            system_slots.append((value.lineno, symbol(value, parents)))
            if id(value) not in covered:
                add(value, GROUPS[1], 'messages[role=system].content',
                    note='مدخل system في طلب النموذج؛ المراجع الديناميكية موسومة داخل النص.')
        if set(pairs) & {'messages', 'max_tokens', 'temperature'}:
            for key in sorted(set(pairs) & PARAMS):
                value = pairs[key]
                if id(value) not in covered:
                    add(value, GROUPS[3], key)

    for node in ast.walk(tree):
        if id(node) in covered or not isinstance(node, (ast.Constant, ast.JoinedStr)):
            continue
        if isinstance(node, ast.Constant) and not isinstance(node.value, str):
            continue
        parent = parents.get(node)
        if isinstance(parent, (ast.JoinedStr, ast.FormattedValue)) or is_docstring(node, parents) or non_copy(node, parents):
            continue
        root = node
        while isinstance(parents.get(root), ast.BinOp) and isinstance(parents[root].op, ast.Add):
            root = parents[root]
        if id(root) in covered:
            continue
        body = render(root, source)
        if not natural(body):
            continue
        owner, target, key = symbol(root, parents), context_target(root, parents), value_key(root, parents)
        if owner == '<module>':
            continue  # Operational settings, routes and labels are not prompt copy.
        prompt_context = (PROMPT_VARS.search(target) or PROMPT_WORDS.search(body)
                          or re.search(r'guidance|brief|guide|prompt|reply_error|persuasion_error|upgrade_legacy_rules', owner))
        if prompt_context:
            add(root, GROUPS[1], key or target)
        elif (key == 'reply' or module in REPLY_MODULES
              or (module == 'app' and REPLY_VARS.search(target)
                  and re.search(r'reply|product|catalog|order|checkout|policy|choice|first_message|customer', owner))):
            add(root, GROUPS[2], key or target,
                note='نص جاهز أو جزء قالب؛ قد يرسل مباشرة من الكود دون توليد من النموذج.')

    return entries, dict(source=relative, system_slots=system_slots)


def fence(text, language='text'):
    marker = '`' * max(3, max((len(hit) + 1 for hit in re.findall(r'`+', text)), default=3))
    return marker + language + '\n' + text + '\n' + marker


def ownership(record):
    name = (record['source'] + ' ' + record['symbol'] + ' ' + record['target']).lower()
    store = ('عالم البراءة — ملابس الأطفال' if 'baraah' in name else
             'خيوط الذهب — الجملة' if 'golden' in name else
             'خيوط' if 'khuyoot' in name else
             'الفاتنة' if 'alfatena' in name or 'al-fatena' in name else
             'قالب عام؛ يطبق بحسب المتجر الحالي وإعداداته')
    for tokens, purpose in (
        (('advisor',), 'المستشار الداخلي لمالك النظام'),
        (('smart_reviewer',), 'المراجع الذكي وتحليل المحادثات'),
        (('self_analysis', 'learning'), 'تحليل الأداء والتعلّم من المحادثات'),
        (('post_order', 'order_confirmation'), 'خدمة الطلب المثبت أو تأكيد الحجز'),
        (('first_message',), 'أول تواصل والترحيب'),
        (('followup',), 'المتابعة مع الزبون'),
        (('checker',), 'تدقيق الرد وإرسال تعليمات تصحيح'),
        (('improve', 'rewrite'), 'تحسين مسودة الموظف'),
        (('vision', 'catalog_match', 'image_match', 'screenshot'), 'التعرف على المنتج من الصور وربط الكتالوج'),
        (('selection', 'product_albums', 'product_variants'), 'الاختيارات وربط المنتج واللون والقياس'),
        (('checkout', 'create_order', 'contact'), 'إكمال بيانات الحجز وتثبيت الطلب'),
        (('sales_strategy', 'sales_engagement', 'sales_playbook', 'sales_decision'), 'الأسلوب البيعي والإقناع والاعتراضات وخطوة الحجز'),
        (('sales_context', 'conversation_quality', 'catalog_truth'), 'ذاكرة المحادثة وجودة الرد ودقة الحقائق'),
        (('main', 'system', 'instructions'), 'تعليمات الرد الرئيسي وأسلوب المحادثة'),
    ):
        if any(token in name for token in tokens):
            return purpose, store
    return ('إعداد توليد أو اتصال النموذج' if record['group'] == GROUPS[3] else
            'قالب أو نص مساعد؛ راجع اسم الدالة لتحديد الحالة'), store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=ROOT / 'outputs/prompt-reference-20261025/PROMPTS_MASTER_EDITABLE.md')
    args = parser.parse_args()
    records, inventory = [], []
    for path in sorted((ROOT / 'account_app').glob('*.py')):
        entries, stats = collect(path)
        records.extend(entries)
        inventory.append(stats)
    for name in FILES:
        path = ROOT / 'account_app' / name
        if path.exists():
            text = path.read_text(encoding='utf-8-sig')
            records.append(dict(source=path.relative_to(ROOT).as_posix(), symbol='<text-file>',
                target=name, start=1, end=len(text.splitlines()), expression=text,
                source_sha256=digest(text), file_sha256=digest(text), group=GROUPS[4], text=text,
                note='ملف تعليمات كامل كما هو في المصدر المحلي.'))
    records.sort(key=lambda item: (GROUPS.index(item['group']), item['source'], item['start']))
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    counts = {group: sum(item['group'] == group for item in records) for group in GROUPS}
    lines = ['# ملف البرومبتات الكامل القابل للتعديل', '',
        'نسخة المصدر المحلي بتاريخ 2026-10-25. آخر التزام: `' + commit + '`.',
        'تتضمن هذه النسخة التعديلات المحلية غير المرفوعة وقت التصدير. جُمعت النصوص بتحليل ملفات المصدر دون تشغيل التطبيق.', '',
        '## طريقة التعديل وحفظ مكان كل نص', '',
        'عدّل محتوى كتلة `text` التابعة للقسم المطلوب، واحتفظ بعلامات `PROMPT-BEGIN` و`PROMPT-END` وبيانات المصدر. لكل نص معرّف مستقل وملف ودالة وموضع وبصمة للمصدر الأصلي؛ بهذه العلامات يمكن إرجاع تعديلك إلى مكانه الصحيح حتى لو تغيّرت أرقام الأسطر.',
        'القيم مثل `{store_name}` و`{age_label}` و`⟪تعبير وقت التشغيل: ...⟫` بيانات أو مراجع يملؤها الكود. احتفظ بها إذا أردت بقاء الربط. صيغة Python الأصلية تحت «مرجع التعبير» لتوضيح التركيبات وليست جزءاً يرسل للزبون.',
        'هذا الملف مرجع للتحرير والمراجعة؛ التطبيق لا يقرأه تلقائياً حالياً. تطبيق التعديلات يتم إلى المصادر المشار إليها بعد مراجعتها. النصوص الجاهزة موسومة لأنها قد ترسل مباشرة دون الرجوع إلى برومبت الموديل.', '',
        '## حدود الجمع ومصادر وقت التشغيل', '',
        '- يشمل ملفات Python التشغيلية المباشرة في `account_app` وملفات التعليمات النصية الأربعة أدناه؛ يستبعد نسخ المراجعات القديمة والاختبارات.',
        '- إعدادات الإدارة قد تتجاوز الافتراضيات: `app_settings` بمفاتيح `prompt_*` أو `store:<store_id>:prompt_*`، وفق `get_app_setting` و`render_setting_template` في `account_app/app.py`.',
        '- `load_ai_config` يضيف تعليمات المتجر والقواعد وملخص المنتجات وتعليمات المشرف المحفوظة. `baraah.ai_config` له قواعد متجر الأطفال. `sales_learning.guidance` وذاكرة المستشار مصادر ديناميكية أخرى.',
        '- بيانات المنتجات والمحادثات وذاكرة الزبائن وقواعد البيانات ليست برومبتات ثابتة داخل الكود؛ لم تُنسخ محتوياتها. لم يُقرأ ملف `.env` أو أي مفتاح سري.',
        '- قسم إعدادات النموذج يوضح القيم الافتراضية والتعابير المستخدمة؛ قيمة متغير البيئة أو إعداد الاستضافة الفعلي قد تختلف.', '',
        '## الفهرس', '', '| النوع | عدد الأقسام |', '|---|---:|']
    lines.extend(f'| {group} | {counts[group]} |' for group in GROUPS)
    lines += ['', '### دليل سريع للمسؤوليات', '',
        '| المصدر | المسؤولية |', '|---|---|',
        '| app.py: DEFAULT_MAIN / COMPACT_MAIN / _call_main_ai_once | الموديل الرئيسي وصيغة الرد |',
        '| sales_engagement / sales_strategy / sales_playbook / sales_decision | الأسلوب والإقناع والقياس والحجز والاعتراضات |',
        '| sales_context / conversation_quality / catalog_truth | حفظ الاختيار وفهم السياق ودقة حقائق المنتج |',
        '| baraah / GOLDEN_THREADS / PROFILES | قواعد متجر الأطفال والجملة وتخصص كل متجر |',
        '| FIRST_MESSAGE / FOLLOWUP / CHECKER / IMPROVE | أول رسالة والمتابعة والمدقق وتحسين مسودة الموظف |',
        '| VISION / CATALOG_MATCH / SCREENSHOT / selection | التعرف على المنتج والصور وتبديل الاختيار |',
        '| advisor / self_analysis / learning / evaluation | المستشار وتحليل المحادثات والتعلّم والتقييم |',
        '| الردود الجاهزة | رسائل يصوغها مسار برمجي محدد مباشرة |', '']
    for group in GROUPS:
        lines += ['## ' + group, '']
        for number, record in enumerate(records, 1):
            if record['group'] != group:
                continue
            identifier = f'PRM-{number:04d}'
            metadata = {key: record[key] for key in ('source', 'symbol', 'target', 'start', 'end', 'source_sha256', 'file_sha256')}
            metadata['id'] = identifier
            purpose, store = ownership(record)
            lines += [f'### {identifier} — {record["source"]} / {record["symbol"]} / {record["target"]}', '',
                      '<!-- PROMPT-BEGIN ' + json.dumps(metadata, ensure_ascii=False) + ' -->',
                      f'المصدر: `{record["source"]}` — الدالة/النطاق: `{record["symbol"]}` — الهدف: `{record["target"]}` — السطر: {record["start"]}.',
                      f'يتبع: **{purpose}**. النطاق: **{store}**.', '']
            if record['note']:
                lines += [record['note'], '']
            lines += [fence(record['text']), '', '<details><summary>مرجع التعبير الأصلي للتطبيق على الكود</summary>', '',
                      fence(record['expression'], 'python' if record['source'].endswith('.py') else 'text'), '', '</details>',
                      '<!-- PROMPT-END ' + identifier + ' -->', '']
    lines += ['## تدقيق التغطية', '',
              f'عدد ملفات Python المحللة: {len(inventory)}. عدد مداخل system في طلبات النماذج: {sum(len(i["system_slots"]) for i in inventory)}. جميعها مفهرسة كمراجع مستقلة.', '',
              'تجميع الجمل المتجاورة يحافظ على النص كاملاً، ولا يحذف التكرار بين المصادر؛ وجود تعليمات متعارضة أو قديمة في هذا المرجع يعكس المصدر المحلي ويتيح تعديل موضعها المحدد.', '']
    result = '\n'.join(lines)
    if re.search(r'sk-or-v1-[a-f0-9]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|Bearer\s+[A-Za-z0-9_-]{25,}', result):
        raise RuntimeError('Secret-like content detected; export refused')
    assert len(re.findall(r'<!-- PROMPT-BEGIN ', result)) == len(records)
    assert len(re.findall(r'<!-- PROMPT-END ', result)) == len(records)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(result, encoding='utf-8')
    print(json.dumps(dict(output=str(args.output), sections=len(records), counts=counts,
                          files=len(inventory), bytes=len(result.encode('utf-8'))), ensure_ascii=True))


if __name__ == '__main__':
    main()
