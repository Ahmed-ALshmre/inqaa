"""Portable Cloud authentication must not depend on Windows files or expose secrets."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock,patch

from tools import live_system,check_cloud_handoff


class CloudClientTests(unittest.TestCase):
    def test_network_secret_placeholder_is_sent_unchanged_to_fixed_origin_without_profile_access(self):
        with patch.dict(os.environ,{'INQAA_LIVE_TOKEN':'cloud-proxy-placeholder','INQAA_LIVE_BASE_URL':'https://untrusted.test'}), \
             patch.object(live_system,'profile_path',side_effect=AssertionError('Cloud cannot open a device profile')):
            client=live_system.Client()
        self.assertEqual(client.base,live_system.BASE)
        self.assertEqual(client.session.headers['Authorization'],'Bearer cloud-proxy-placeholder')
        response=Mock(status_code=200)
        with patch.object(client.session,'get',return_value=response) as get:
            client.get('manifest')
        self.assertEqual(get.call_args.args[0],live_system.BASE+'/api/live/v1/manifest')
        self.assertFalse(get.call_args.kwargs['allow_redirects'])

    def test_explicit_profile_takes_precedence_over_cloud_environment(self):
        with tempfile.TemporaryDirectory() as folder,patch.dict(os.environ,{'INQAA_LIVE_TOKEN':'environment-placeholder'}):
            path=Path(folder)/'profile.json'
            live_system.save_profile('profile-token','test-id',9999999999,path=path)
            client=live_system.Client(path)
        self.assertEqual(client.session.headers['Authorization'],'Bearer profile-token')

    def test_missing_cloud_secret_does_not_silently_use_other_privileged_credentials(self):
        with patch.dict(os.environ,{'INQAA_LIVE_TOKEN':'','DASHBOARD_PASSWORD':'never-used'}), \
             patch.object(live_system,'profile_path',return_value=Path('definitely-missing-cloud-profile.json')):
            with self.assertRaisesRegex(RuntimeError,'not configured'):
                live_system.Client()

    def test_live_probe_reports_only_counts_and_source_version(self):
        responses={
            'manifest':dict(version=1,scope='read:operations:all-stores',code_version='test-code',resources={'messages':dict(available=True)}),
            'stores':dict(items=[dict(name='private store details')]),
            'products':dict(items=[dict(notes='private product details')]),
            'inbox':dict(conversations=[dict(name='private customer name',phone='private customer phone')])}
        client=Mock();client.get.side_effect=lambda path,*a:responses[path]
        with patch.object(check_cloud_handoff,'Client',return_value=client):
            result=check_cloud_handoff.check(True)
        self.assertTrue(result['live_checked'])
        self.assertEqual((result['stores'],result['products'],result['resources']),(1,1,1))
        self.assertNotIn('private',json.dumps(result))
