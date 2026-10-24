"""Package code and sanitized acceptance evidence; refuse untested source."""
import hashlib
import json
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs' / 'sales-deep-review-20261024'


def read(relative):
    return json.loads((ROOT / relative).read_text(encoding='utf-8'))


def main():
    dialogue = read('outputs/dialogues-live-release/report.json')
    vision = read('outputs/vision-live-release/report.json')
    before = read('outputs/dialogues-live-before/report.json')
    review = read('outputs/sales-deep-review-20261024/quality-review.json')
    assert dialogue['deterministic_pass'] and not dialogue['source_changed_during_run']
    assert vision['passed'] == vision['total'] and not vision['source_changed_during_run']
    assert review['reviewer_type'] == 'assistant' and review['all_transcripts_read']
    for report in (dialogue, vision):
        for relative, expected in report['source_file_hashes'].items():
            assert hashlib.sha256((ROOT / relative).read_bytes()).hexdigest() == expected, relative
    tests = (ROOT / 'analysis_20261024' / 'release-tests.log').read_text(encoding='utf-8-sig')
    passed = re.search(r'(\d+) passed in ([\d.]+)s', tests)
    assert passed and not re.search(r'\d+ failed', tests)
    files = set((ROOT / 'account_app').glob('*.py'))
    files.update((ROOT / 'account_app' / 'tests').rglob('*.py'))
    files.update([ROOT / '.gitignore', ROOT / 'docs' / 'menger-order-link.md',
        ROOT / 'docs' / 'updates' / '2026-10-24-deep-sales-review.md',
        ROOT / 'tools' / 'evaluate_sales_dialogues.py', ROOT / 'tools' / 'evaluate_catalog_vision.py',
        Path(__file__).resolve()])
    for name in ('requirements.txt', 'account_app/requirements-clip.txt'):
        if (ROOT / name).is_file():
            files.add(ROOT / name)
    evidence = {
        'verification/dialogues-before.json': ROOT / 'outputs/dialogues-live-before/report.json',
        'verification/dialogues-after.json': ROOT / 'outputs/dialogues-live-release/report.json',
        'verification/dialogues-after.md': ROOT / 'outputs/dialogues-live-release/report.md',
        'verification/vision-after.json': ROOT / 'outputs/vision-live-release/report.json',
        'verification/offline-tests.txt': ROOT / 'analysis_20261024/release-tests.log',
        'verification/quality-review.json': OUT / 'quality-review.json',
    }
    entries = {path.relative_to(ROOT).as_posix(): path.read_bytes() for path in files}
    entries.update({name: path.read_bytes() for name, path in evidence.items()})
    entries['README-AR.md'] = (OUT / 'README-AR.md').read_bytes()
    for name, data in entries.items():
        assert not re.search(rb'sk-or-v1-[0-9a-f]{50,}', data), 'Credential in ' + name
        assert not name.endswith(('.db', '.env', '.jsonl'))
    manifest = dict(kind='Code overlay; original assets/settings/database retained',
        offline_tests=int(passed[1]), dialogue_before=[before['checks_passed'],before['checks_total']],
        dialogue_after=[dialogue['checks_passed'],dialogue['checks_total']], vision=[vision['passed'],vision['total']],
        production_deployed=False, source_sha256=dialogue['source_file_hashes'],
        files={name:hashlib.sha256(data).hexdigest() for name,data in sorted(entries.items())})
    entries['manifest.json'] = json.dumps(manifest, ensure_ascii=False, indent=2).encode('utf-8')
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / 'lamsa-tested-sales-update-20261024.zip'
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(entries.items()):
            archive.writestr(name, data)
    with zipfile.ZipFile(target) as archive:
        assert archive.testzip() is None
        assert set(archive.namelist()) == set(entries)
    (OUT / 'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(dict(files=len(entries),bytes=target.stat().st_size,
        sha256=hashlib.sha256(target.read_bytes()).hexdigest(),offline_tests=int(passed[1])), ensure_ascii=True))


if __name__ == '__main__':
    main()
