"""Check cloud prerequisites; optional live probe prints metadata only, never credentials."""
import argparse
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.live_system import Client


def check(live=False):
    for name in ('AGENTS.md','docs/CLOUD_HANDOFF_AR.md','docs/live-system-access.md','tools/live_system.py'):
        if not (ROOT/name).is_file():
            raise RuntimeError('Missing handoff file: '+name)
    result=dict(handoff_files=True,python=sys.version.split()[0],live_checked=False)
    if live:
        client=Client()
        manifest=client.get('manifest')
        if manifest.get('version')!=1 or manifest.get('scope')!='read:operations:all-stores':
            raise RuntimeError('Unexpected live API scope/version')
        stores=client.get('stores')['items']
        products=client.get('products')['items']
        inbox=client.get('inbox',dict(limit=1))
        if 'conversations' not in inbox:
            raise RuntimeError('Inbox read failed')
        result.update(live_checked=True,code_version=manifest['code_version'],stores=len(stores),
                      products=len(products),resources=sum(bool(v['available']) for v in manifest['resources'].values()))
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live',action='store_true')
    args=parser.parse_args()
    try:
        print(json.dumps(check(args.live)))
    except Exception:
        print('Cloud check failed. Verify repository files, the live secret and allowed HTTPS host. No credential was printed.',file=sys.stderr)
        sys.exit(1)
