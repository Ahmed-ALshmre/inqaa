import io
import json
import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from account_app import app as m
from account_app import storage_maintenance as storage
from account_app.staff_access import required_permission
from account_app.tests import test_close_human_attention as fixtures


class StorageMaintenanceTests(unittest.TestCase):
    def setUp(self):
        fixtures.CloseHumanAttentionTests.setUp(self)
        self.root=Path(self.temp.name)/'uploads'
        self.root.mkdir()
        self.catalog=Path(self.temp.name)/'products.json'
        self.catalog.write_text('[]',encoding='utf-8')
        self.paths=patch.multiple(m,UPLOADS_DIR=str(self.root),PRODUCTS_FILE=str(self.catalog))
        self.paths.start()
        storage.init(self.db)

    def tearDown(self):
        self.paths.stop()
        fixtures.CloseHumanAttentionTests.tearDown(self)

    def upload(self,content=b'image',purpose='library',store='default'):
        response=self.client.post('/api/upload_image?store_id='+store,data={'image':(io.BytesIO(content),'photo.jpg'),'purpose':purpose})
        self.assertEqual(response.status_code,200,response.json)
        return response.json['filename'],response.json['image_url']

    def age(self,filename):
        old=time.time()-40*storage.DAY
        os.utime(self.root/filename,(old,old))
        self.db.execute('UPDATE upload_assets SET updated_at=?,created_at=? WHERE filename=?',(old,old,filename));self.db.commit()

    def test_identical_upload_reuses_file_and_library_survives_cleanup(self):
        first,_=self.upload()
        second,_=self.upload()
        self.assertEqual(first,second)
        self.assertEqual(len(list(self.root.iterdir())),1)
        self.age(first)
        result=storage.cleanup(self.db,self.root,[],force=True)
        self.assertEqual(result['images'],0)
        self.assertTrue((self.root/first).exists())
        self.assertEqual(self.client.get('/api/image_library').json['images'][0]['filename'],first)

    def test_unused_product_file_removed_but_messages_orders_and_catalog_kept(self):
        orphan,_=self.upload(b'orphan','product')
        current,current_url=self.upload(b'current','product')
        used,used_url=self.upload(b'used','product')
        for filename in [orphan,current,used]:self.age(filename)
        self.db.execute("INSERT INTO messages(sender_id,direction,image_url,created_at) VALUES('old','outgoing',?,'2000-01-01')",(used_url,))
        self.db.execute("INSERT INTO orders(sender_id,product_name) VALUES('old','test')")
        self.db.commit()
        report=storage.cleanup(self.db,self.root,[{'image_url':current_url}],force=True)
        self.assertEqual(report['images'],1)
        self.assertFalse((self.root/orphan).exists())
        self.assertTrue((self.root/current).exists())
        self.assertTrue((self.root/used).exists())
        self.assertEqual(self.db.execute('SELECT count(*) FROM orders').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM messages').fetchone()[0],1)

    def test_preview_does_not_delete_rows_or_files(self):
        name,_=self.upload(b'orphan','product');self.age(name)
        job=m.ai_jobs.enqueue(self.db,'default','old','preview',{},'old-done')
        self.db.execute("UPDATE ai_jobs SET status='done',finished_at=? WHERE id=?",(time.time()-40*storage.DAY,job['id']));self.db.commit()
        report=storage.cleanup(self.db,self.root,[],preview=True,force=True)
        self.assertEqual(report['images'],1)
        self.assertEqual(report['deleted_rows']['ai_jobs'],1)
        self.assertTrue((self.root/name).exists())
        self.assertIsNotNone(self.db.execute('SELECT id FROM ai_jobs WHERE id=?',(job['id'],)).fetchone())

    def test_completed_only_cleanup_preserves_active_failed_and_deduplication(self):
        old=time.time()-40*storage.DAY
        for status in ['done','dismissed','running','queued','failed']:
            job=m.ai_jobs.enqueue(self.db,'default','old','preview',{},status)
            self.db.execute('UPDATE ai_jobs SET status=?,created_at=?,finished_at=? WHERE id=?',(status,old,old,job['id']))
        self.db.execute("INSERT INTO processed_messages VALUES('old-mid','2000-01-01')")
        self.db.execute("INSERT INTO ai_deliveries VALUES('key','job','sent','{}',?)",(old,))
        self.db.commit()
        result=storage.cleanup(self.db,self.root,[],force=True)
        self.assertEqual(result['deleted_rows']['ai_jobs'],2)
        self.assertEqual({row[0] for row in self.db.execute('SELECT status FROM ai_jobs')},{'running','queued','failed'})
        self.assertEqual(self.db.execute('SELECT count(*) FROM processed_messages').fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM ai_deliveries').fetchone()[0],1)
        self.assertFalse(storage.compact_database(self.db))
        self.assertTrue(storage.cleanup(self.db,self.root,[])['skipped'])

    def test_old_uncategorized_images_are_reusable_not_deleted(self):
        name='product_100_0.jpg'
        (self.root/name).write_bytes(b'old device upload')
        os.utime(self.root/name,(1,1))
        storage.register_legacy(self.db,self.root,[])
        self.assertEqual(storage.cleanup(self.db,self.root,[],force=True)['images'],0)
        self.assertTrue((self.root/name).exists())
        self.assertEqual(self.client.get('/api/image_library').json['images'][0]['filename'],name)

    def test_library_is_scoped_to_store_and_product_reuse_can_pin(self):
        first,_=self.upload(b'same','product','default')
        pinned,_=self.upload(b'same','library','default')
        self.assertEqual(first,pinned)
        self.upload(b'other','library','al-fatena')
        self.assertEqual(len(self.client.get('/api/image_library?store_id=default').json['images']),1)
        self.assertEqual(len(self.client.get('/api/image_library?store_id=al-fatena').json['images']),1)

    def test_product_deletion_registers_legacy_image_before_it_disappears(self):
        name='product_123_0.jpg'
        (self.root/name).write_bytes(b'legacy product');os.utime(self.root/name,(1,1))
        self.catalog.write_text(json.dumps([{'product_id':'P1','product_name':'test','store_id':'default','image_url':'/product_image/uploads/'+name}]),encoding='utf-8')
        result=self.client.delete('/api/products/manage/P1')
        self.assertEqual(result.status_code,200,result.json)
        self.assertEqual(storage.cleanup(self.db,self.root,[],force=True)['images'],0)
        self.db.execute('UPDATE retired_product_images SET retired_at=?',(time.time()-8*storage.DAY,));self.db.commit()
        self.assertEqual(storage.cleanup(self.db,self.root,[],force=True)['images'],1)
        self.assertFalse((self.root/name).exists())

    def test_preview_owner_only_and_paths_are_bounded(self):
        self.assertEqual(required_permission('/api/maintenance/storage/cleanup','POST'),'owner')
        self.assertEqual(required_permission('/api/maintenance/storage/preview','POST'),'owner')
        self.assertEqual(required_permission('/api/image_library','GET'),'reply|products')
        self.assertIsNone(storage.safe_file(self.root,'../product_123_0.jpg'))
        self.assertIn(m.app.test_client().post('/api/maintenance/storage/cleanup').status_code,[302,401,403])

    def test_bad_catalog_stops_cleanup_without_touching_files(self):
        name,_=self.upload(b'old','product');self.age(name)
        self.catalog.write_text('{broken json',encoding='utf-8')
        response=self.client.post('/api/maintenance/storage/cleanup')
        self.assertEqual(response.status_code,409)
        self.assertTrue((self.root/name).exists())

    def test_same_clock_tick_does_not_overwrite_different_images(self):
        with patch.object(storage.time,'time_ns',return_value=100):
            first,_=self.upload(b'first')
            second,_=self.upload(b'second')
        self.assertNotEqual(first,second)
        self.assertEqual((self.root/first).read_bytes(),b'first')
        self.assertEqual((self.root/second).read_bytes(),b'second')

    def test_original_product_folder_retirement_keeps_shared_and_recent_photos(self):
        product_root=Path(self.temp.name)/'product_image';product_root.mkdir()
        retired=product_root/'old-photo.jpg';retired.write_bytes(b'old');os.utime(retired,(1,1))
        shared=product_root/'shared photo.jpg';shared.write_bytes(b'shared');os.utime(shared,(1,1))
        outside=Path(self.temp.name)/'outside.jpg';outside.write_bytes(b'keep');os.utime(outside,(1,1))
        old=time.time()-8*storage.DAY
        for path in [retired,shared,outside]:
            self.db.execute('INSERT INTO retired_product_images VALUES(?,?)',(str(path.resolve()),old))
        self.db.execute("INSERT INTO messages(sender_id,direction,image_url) VALUES('old','outgoing','/product_image/shared%20photo.jpg')")
        self.db.commit()
        report=storage.cleanup(self.db,self.root,[],force=True,product_root=product_root)
        self.assertEqual(report['images'],1)
        self.assertFalse(retired.exists())
        self.assertTrue(shared.exists())
        self.assertTrue(outside.exists())

    def test_shared_product_photo_is_not_retired_when_still_in_catalog(self):
        old=[{'image_url':'/product_image/shared.jpg','product_id':'one'}, {'image_url':'/product_image/shared.jpg','product_id':'two'}]
        storage.retire_product_images(m,self.db,old,[old[1]])
        self.assertEqual(self.db.execute('SELECT count(*) FROM retired_product_images').fetchone()[0],0)
