"""Read the live operational API using a locally protected, revocable credential."""
import argparse
import base64
import ctypes
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

BASE = 'https://inqaa-production.up.railway.app'


def profile_path():
    if os.environ.get('INQAA_LIVE_PROFILE'):
        return Path(os.environ['INQAA_LIVE_PROFILE']).expanduser()
    return Path(os.environ.get('LOCALAPPDATA', str(Path.home()/'.config')))/'Inqaa'/'live-access.json'


def dpapi(data, decrypt=False):
    from ctypes import wintypes
    class Blob(ctypes.Structure):
        _fields_ = [('size', wintypes.DWORD), ('data', ctypes.POINTER(ctypes.c_ubyte))]
    buf = ctypes.create_string_buffer(data)
    source = Blob(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)))
    target = Blob()
    function = ctypes.windll.crypt32.CryptUnprotectData if decrypt else ctypes.windll.crypt32.CryptProtectData
    if not function(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(target)):
        raise OSError('Unable to open the Windows-protected live access credential')
    try:
        return ctypes.string_at(target.data, target.size)
    finally:
        ctypes.windll.kernel32.LocalFree(ctypes.cast(target.data, ctypes.c_void_p))


def save_profile(token, token_id, expires_at, base=BASE, path=None):
    path = Path(path or profile_path())
    if not base.startswith('https://') or urlparse(base).username or urlparse(base).query:
        raise ValueError('A clean HTTPS API origin is required')
    document = dict(base=base.rstrip('/'), token_id=token_id, expires_at=expires_at,
                    scope='read:operations:all-stores', protection='windows-dpapi' if os.name=='nt' else 'private-file')
    if os.name=='nt':
        document['protected_token'] = base64.b64encode(dpapi(token.encode())).decode()
    else:
        document['token'] = token
    path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic replacement; the Windows file contains only a current-user DPAPI blob.
    temporary=path.with_suffix('.tmp')
    descriptor=os.open(temporary, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, 'w', encoding='utf8') as output:
        json.dump(document, output, ensure_ascii=False, indent=2)
    os.replace(temporary, path)
    if os.name!='nt':
        path.chmod(0o600)
    return path


class Client:
    def __init__(self, path=None):
        # Cloud network secrets can be proxy placeholders: pass them through
        # unchanged and never write them into a profile or validation report.
        # The environment credential is pinned to this application's HTTPS host.
        if path is None and os.environ.get('INQAA_LIVE_TOKEN'):
            self.base=BASE
            self.session=requests.Session()
            self.session.headers.update(Authorization='Bearer '+os.environ['INQAA_LIVE_TOKEN'].strip())
            return
        location=Path(path or profile_path())
        if not location.exists():
            raise RuntimeError('Live connection is not configured. See docs/live-system-access.md')
        profile=json.loads(location.read_text(encoding='utf8'))
        self.base=profile['base']
        parsed=urlparse(self.base)
        if parsed.scheme!='https' or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in {'','/'}:
            raise ValueError('Invalid live API origin')
        if profile['protection']=='windows-dpapi':
            if os.name!='nt':
                raise RuntimeError('This connection is protected for its Windows user. Create another connection on this device.')
            token=dpapi(base64.b64decode(profile['protected_token']), decrypt=True).decode()
        else:
            if os.name!='nt' and location.stat().st_mode & 0o077:
                raise RuntimeError('Credential file must be private (chmod 600)')
            token=profile['token']
        self.session=requests.Session()
        self.session.headers.update(Authorization='Bearer '+token)

    def request(self, path, params=None):
        if not path.startswith('/api/live/v1/') or '?' in path or '#' in path:
            raise ValueError('Only live read API paths are allowed')
        for attempt in range(4):
            response=self.session.get(self.base+path, params=params, timeout=(10,60), allow_redirects=False)
            if response.status_code!=429 or attempt==3:
                break
            time.sleep(60)
        if response.status_code!=200:
            # Do not print request headers or response bodies that could contain credentials.
            raise RuntimeError('Live API returned HTTP '+str(response.status_code))
        return response

    def get(self, path, params=None):
        return self.request('/api/live/v1/'+path, params).json()

    def pages(self, resource, params=None):
        params=dict(params or {}, limit=200)
        seen=set()
        for _ in range(10000):
            page=self.get('records/'+resource, params)
            yield page
            if not page['has_more']:
                return
            cursor=page['next_cursor']
            if cursor is None or cursor in seen:
                raise RuntimeError('Invalid pagination cursor; collection is incomplete')
            seen.add(cursor)
            params.update(cursor=cursor, upper_rowid=page['upper_rowid'])
        raise RuntimeError('Collection page limit reached; use narrower filters')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', type=Path)
    sub=parser.add_subparsers(dest='command', required=True)
    sub.add_parser('manifest')
    inbox=sub.add_parser('inbox')
    for option in ('store-id','q','status','date-from','date-to','cursor-time','cursor-sender'):
        inbox.add_argument('--'+option)
    inbox.add_argument('--limit',type=int,default=100)
    for name in ('stores','products','settings','catalog'):
        command=sub.add_parser(name); command.add_argument('--store-id')
    read=sub.add_parser('read')
    read.add_argument('resource')
    for option in ('store-id','sender-id','status','q','cursor','upper-rowid'):
        read.add_argument('--'+option)
    read.add_argument('--order', choices=['asc','desc'], default='desc')
    read.add_argument('--limit', type=int, default=100)
    read.add_argument('--all', action='store_true', help='Follow all bounded pages')
    conversation=sub.add_parser('conversation'); conversation.add_argument('sender_id')
    media=sub.add_parser('media'); media.add_argument('path', help='/api/live/v1/media/ID/INDEX or a returned catalogue media_path'); media.add_argument('--output', required=True, type=Path)
    parser.add_argument('--output', type=Path, help='Save private JSON instead of printing it')
    args=parser.parse_args()
    client=Client(args.profile)
    params={k:getattr(args,k,None) for k in ('store_id','sender_id','status','q','cursor','upper_rowid','order','limit','date_from','date_to','cursor_time','cursor_sender') if getattr(args,k,None) is not None}
    if args.command=='media':
        response=client.request(args.path)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(response.content)
        print(json.dumps(dict(saved=str(args.output.resolve()), bytes=len(response.content))))
        return
    if args.command=='read':
        if args.all:
            pages=list(client.pages(args.resource,params))
            result=dict(resource=args.resource,items=[r for p in pages for r in p['items']], pages=len(pages),
                upper_rowid=pages[0]['upper_rowid'], complete=True, consistency='Bounded row range; existing rows may change while reading.')
        else:
            result=client.get('records/'+args.resource,params)
    elif args.command=='conversation':
        resources=('conversations','messages','product_interests','context','ad_context','reviews','problems','orders','pauses','conversation_ai','choices','customer_instructions','followups','order_actions','jobs')
        available=client.get('manifest')['resources']
        result={name:[r for p in client.pages(name,dict(sender_id=args.sender_id)) for r in p['items']]
                for name in resources if available[name]['available']}
    else:
        result=client.get(args.command,params)
    text=json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True); args.output.write_text(text,encoding='utf8')
        print(json.dumps(dict(saved=str(args.output.resolve()))))
    else:
        print(text)


if __name__=='__main__':
    try:
        main()
    except requests.RequestException:
        print('Live API connection failed; check the allowed HTTPS host and connection configuration.',file=sys.stderr)
        sys.exit(1)
    except (OSError,ValueError,RuntimeError,KeyError) as exc:
        print(str(exc),file=sys.stderr)
        sys.exit(1)
