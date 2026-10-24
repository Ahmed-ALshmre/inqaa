"""Create a source-verified, secret-free overlay for the sales conversation update."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import zipfile


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'sales-plan-release-20261024'
LIVE = ROOT / 'outputs' / 'sales-plan-live-final'
OFFLINE = ROOT / 'outputs' / 'sales-plan-final-tests.log'
REVIEW = ROOT / 'docs' / 'updates' / '2026-10-24-persuasion-plan-implementation.md'


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> None:
    report = json.loads((LIVE / 'report.json').read_text(encoding='utf-8'))
    assert report['deterministic_pass'] and not report['source_changed_during_run']
    assert report['checks_passed'] == report['checks_total']
    for name, expected in report['source_file_hashes'].items():
        assert sha256((ROOT / name).read_bytes()) == expected, f'Untested source: {name}'
    tests = OFFLINE.read_text(encoding='utf-8-sig')
    passed = re.search(r'(\d+) passed in [\d.]+s', tests)
    assert passed and ' failed' not in tests

    paths = set((ROOT / 'account_app').glob('*.py'))
    paths.update((ROOT / 'account_app' / 'tests').rglob('*.py'))
    paths.update([
        ROOT / '.gitignore', ROOT / 'account_app' / 'templates' / 'products.html',
        ROOT / 'account_app' / 'static' / 'js' / 'products.js',
        ROOT / 'docs' / 'menger-order-link.md', REVIEW,
        ROOT / 'tools' / 'evaluate_sales_dialogues.py', Path(__file__).resolve(),
    ])
    for name in ('requirements.txt', 'account_app/requirements-clip.txt'):
        path = ROOT / name
        if path.is_file():
            paths.add(path)
    entries = {path.relative_to(ROOT).as_posix(): path.read_bytes() for path in paths}
    entries.update({
        'verification/live-report.json': (LIVE / 'report.json').read_bytes(),
        'verification/live-report.md': (LIVE / 'report.md').read_bytes(),
        'verification/offline-tests.txt': OFFLINE.read_bytes(),
    })
    entries['README-AR.md'] = (
        '# تحديث محادثة المبيعات\n\n'
        'هذه حزمة ملفات مصدرية لتطبيقها بعد المراجعة؛ لا تتضمن قاعدة البيانات أو إعدادات التشغيل أو مفاتيح API.\n'
        'تقرير الاختبار الحي يستخدم زبائن افتراضيين ويعترض إرسال الرسائل، ولا يقيس معدل الحجوزات الفعلي.\n'
        'راجع docs/updates/2026-10-24-persuasion-plan-implementation.md قبل التطبيق.\n'
    ).encode('utf-8')
    for name, data in entries.items():
        assert not re.search(rb'sk-or-v1-[A-Za-z0-9]{20,}', data), f'Secret in {name}'
        assert not name.endswith(('.env', '.db', '.jsonl'))
    manifest = {
        'kind': 'tested-code-overlay',
        'production_deployed': False,
        'offline_tests_passed': int(passed[1]),
        'live_checks': [report['checks_passed'], report['checks_total']],
        'live_scenarios': len(report['scenarios']),
        'source_sha256': report['source_file_hashes'],
        'files': {name: sha256(data) for name, data in sorted(entries.items())},
    }
    entries['manifest.json'] = json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8')
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / 'lamsa-persuasion-update-20261024.zip'
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(entries.items()):
            archive.writestr(name, data)
    with zipfile.ZipFile(target) as archive:
        assert archive.testzip() is None
        assert set(archive.namelist()) == set(entries)
    (OUT / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({
        'zip': str(target), 'files': len(entries), 'bytes': target.stat().st_size,
        'sha256': sha256(target.read_bytes()), 'offline_tests_passed': int(passed[1]),
        'live_checks_passed': report['checks_passed'],
    }, ensure_ascii=False))


if __name__ == '__main__':
    main()
