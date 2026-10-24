"""Package the tested cumulative code and deidentified image-audit evidence."""
import hashlib
import json
from pathlib import Path
import re
import zipfile


def main():
    root=Path(__file__).resolve().parents[1]
    out=root/'outputs/today-image-fix-20261024'
    def read(path):
        return json.loads((root/path).read_text(encoding='utf-8'))
    initial=read('outputs/today-image-replay-170919/report.json')
    retry=read('outputs/today-image-replay-170919-retry9/report.json')
    findings=read('analysis_20261024/image-audit-170919/findings.json')
    cases={case['case_id']:case for case in initial['cases']}
    for case in retry['cases']:
        assert case['input_sha256']==cases[case['case_id']]['input_sha256']
        cases[case['case_id']]=case
    for result in (initial,retry):
        assert not result['source_changed_during_run']
        for relative,digest in result['source_file_hashes'].items():
            assert hashlib.sha256((root/relative).read_bytes()).hexdigest()==digest,relative
    assert len(cases)==9 and all(all(c['checks'].values()) for c in cases.values())
    known=[case for case in cases.values() if case['expected_product_ids'] is not None]
    assert len(known)==8 and all(case['checks']['visual_identity_matches_review'] for case in known)
    log=(root/'analysis_20261024/image-audit-170919/final-tests.log').read_text(encoding='utf-8-sig')
    tests=re.search(r'(\d+) passed in ([\d.]+)s',log)
    assert tests and not re.search(r'\d+ failed',log)
    costs=[c.get('reported_cost_usd') for report in (initial,retry) for case in report['cases'] for c in case['requests']]
    acceptance=dict(cases=list(cases.values()),checks_passed=sum(sum(c['checks'].values()) for c in cases.values()),
        checks_total=sum(len(c['checks']) for c in cases.values()),independent_visual_review='assistant_before_provider_replay',
        visually_supported_inputs=8,visually_supported_matches=8,uncertain_inputs_not_scored=1,
        initial_checks=[initial['checks_passed'],initial['checks_total']],
        bounded_retry='Only photo-9 repeated once after provider HTTP520; original failure retained.',
        provider_requests=initial['provider_requests']+retry['provider_requests'],
        known_provider_reported_cost_usd=sum(c for c in costs if isinstance(c,(int,float))),
        requests_without_reported_cost=sum(c is None for c in costs),
        customer_messages_sent=0,external_orders_created=0,production_modified=False,
        source_file_hashes=initial['source_file_hashes'])
    out.mkdir(parents=True,exist_ok=True)
    (out/'acceptance.json').write_text(json.dumps(acceptance,ensure_ascii=False,indent=2),encoding='utf-8')
    summary=dict(summary=findings['summary'],method=findings['method'])
    (out/'today-summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    files=set((root/'account_app').glob('*.py'))
    files.update((root/'account_app/tests').rglob('*.py'))
    files.update([root/'.gitignore',root/'docs/menger-order-link.md',
        root/'docs/updates/2026-10-24-today-image-recognition.md',
        root/'tools/replay_today_images.py',root/'tools/evaluate_sales_dialogues.py',Path(__file__).resolve()])
    if (root/'requirements.txt').is_file(): files.add(root/'requirements.txt')
    entries={p.relative_to(root).as_posix():p.read_bytes() for p in files}
    for name,path in {
        'verification/initial-replay.json':'outputs/today-image-replay-170919/report.json',
        'verification/photo9-retry.json':'outputs/today-image-replay-170919-retry9/report.json',
        'verification/acceptance.json':'outputs/today-image-fix-20261024/acceptance.json',
        'verification/today-summary.json':'outputs/today-image-fix-20261024/today-summary.json',
        'verification/recognized-case-timeline.json':'analysis_20261024/image-audit-170919/recognized-case-142475-timeline.json',
        'verification/offline-tests.txt':'analysis_20261024/image-audit-170919/final-tests.log',
        'README-AR.md':'outputs/today-image-fix-20261024/README-AR.md',
    }.items(): entries[name]=(root/path).read_bytes()
    for name,data in entries.items():
        assert not name.endswith(('.env','.db','.jsonl','.jpg','.png'))
        assert not re.search(rb'sk-or-v1-[0-9a-f]{50,}',data),name
    manifest=dict(offline_tests=int(tests[1]),source_file_hashes=initial['source_file_hashes'],
        checks=[acceptance['checks_passed'],acceptance['checks_total']],
        visually_supported_matches=[8,8],uncertain_input_excluded=True,production_deployed=False,
        contents={name:hashlib.sha256(data).hexdigest() for name,data in sorted(entries.items())})
    entries['manifest.json']=json.dumps(manifest,ensure_ascii=False,indent=2).encode('utf-8')
    target=out/'lamsa-image-continuity-tested-20261024.zip'
    with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED) as archive:
        for name,data in sorted(entries.items()): archive.writestr(name,data)
    with zipfile.ZipFile(target) as archive:
        assert archive.testzip() is None
        assert all(hashlib.sha256(archive.read(name)).hexdigest()==digest for name,digest in manifest['contents'].items())
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(dict(files=len(entries),bytes=target.stat().st_size,tests=int(tests[1]),
        checks=manifest['checks'],matches=manifest['visually_supported_matches'],
        sha256=hashlib.sha256(target.read_bytes()).hexdigest())))


if __name__=='__main__':
    main()
