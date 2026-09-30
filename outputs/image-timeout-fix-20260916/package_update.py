import hashlib
import json
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
TARGET = ROOT / 'outputs/lamsa-image-timeout-fix-20260916.zip'
tests = (OUT / 'tests-final.txt').read_text(encoding='utf-8-sig')
assert 'Ran 315 tests' in tests and tests.strip().endswith('OK')
browser = (OUT / 'browser-tests.txt').read_text(encoding='utf-8-sig')
assert '# fail 0' in browser and '# pass 2' in browser
replay = json.loads((OUT / 'replay-results.json').read_text(encoding='utf-8'))
assert replay['incoming_events_replayed'] == 2105
assert not any(case['outcome'] == 'simulation_error' for case in replay['cases'])
assert replay['layout_content_changes'] == 0
archive = Path.home() / 'Downloads/lamsa-store-backup-20260916-125306.zip'
assert hashlib.sha256(archive.read_bytes()).hexdigest() == replay['archive_sha256']

files = list((ROOT / 'account_app').glob('*.py'))
for folder in ('templates', 'static', 'tests'):
    files += [p for p in (ROOT / 'account_app' / folder).rglob('*')
              if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc']
files += [ROOT / name for name in ('Dockerfile', 'requirements.txt', '.dockerignore',
                                  'README.md', 'account_app/.env.example')]
manifest = {'release': '2026.09.16-image-timeout', 'files': {},
            'verification': {'backend_tests': 315, 'frontend_suites': 2, 'replayed_incoming_events': 2105,
                             'live_provider_tested': False, 'deployed': False,
                             'source_backup_unchanged': True}}
with zipfile.ZipFile(TARGET, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
    for file in sorted(set(files)):
        name = file.relative_to(ROOT).as_posix()
        assert file.name != '.env' and file.suffix not in {'.db', '.sqlite', '.jsonl'}
        data = file.read_bytes()
        manifest['files'][name] = hashlib.sha256(data).hexdigest()
        bundle.writestr(name, data)
    for name in ('release-notes.md', 'tests-final.txt', 'browser-tests.txt', 'replay-results.json', 'replay_24h.py'):
        bundle.write(OUT / name, 'verification/' + name)
    bundle.writestr('update-manifest.json', json.dumps(manifest, ensure_ascii=False, indent=2))
with zipfile.ZipFile(TARGET) as bundle:
    assert bundle.testzip() is None
    for name, digest in manifest['files'].items():
        assert hashlib.sha256(bundle.read(name)).hexdigest() == digest
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest
verification = dict(manifest['verification'], archive=TARGET.name,
                    archive_sha256=hashlib.sha256(TARGET.read_bytes()).hexdigest(),
                    code_files=len(manifest['files']), archive_bytes=TARGET.stat().st_size)
(OUT / 'verification.json').write_text(json.dumps(verification, indent=2), encoding='utf-8')
print(json.dumps(verification, indent=2))
