"""Verify both checkouts together, using disposable data and no real network.

Usage: python tools/verify_menger_roundtrip.py --menger-repo PATH
The receiver's Python dependencies must already be installed.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import Mock, patch
from urllib.parse import urlparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--menger-repo', required=True)
    args = parser.parse_args()
    receiver_root = Path(args.menger_repo).resolve()
    if not (receiver_root / 'app/routes/integrations.py').is_file():
        parser.error('Select the Menger repository containing app/routes/integrations.py')
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    sys.path.insert(0, str(receiver_root))
    with tempfile.TemporaryDirectory(prefix='soof-menger-contract-') as folder:
        os.environ.update(DATA_DIR=folder, DB_PATH=str(Path(folder) / 'sales.db'),
            BOOKINGS_FILE=str(Path(folder) / 'bookings.jsonl'), ENABLE_BACKGROUND_JOBS='0',
            DISABLE_CLIP='1', ZERNIO_AL_FATENA_ENABLED='0',
            MENGER_ORDERS_API_URL='https://receiver.test/api/v1/orders',
            MENGER_ORDERS_API_KEY='synthetic-cross-project-secret', MENGER_STORE_ID='')
        with patch('requests.sessions.Session.request', side_effect=AssertionError('Real network is forbidden')):
            import account_app.app as source
            from app import create_app
            from app.database import db as receiver_db
            from app.models import AISetting, ExternalOrderAttribution, ExternalOrderImport, Order, Product, Store
            receiver = create_app('testing')
            client = receiver.test_client()
            with receiver.app_context():
                store = Store(name='لمسة ستور', slug='synthetic-lamsa')
                receiver_db.session.add(store)
                receiver_db.session.flush()
                receiver_db.session.add(Product(store_id=store.id, name='موديل تجريبي', sku='SYNTHETIC',
                    purchase_price=9000, selling_price=15000))
                receiver_db.session.add_all([
                    AISetting(key='TELEGRAM_BOT_TOKEN', value='synthetic-token'),
                    AISetting(key='TELEGRAM_CHAT_IDS_STORE_' + store.id, value='-10012345')])
                receiver_db.session.commit()
                remote_store = store.id

            def request_receiver(method, url, **kwargs):
                assert urlparse(url).hostname == 'receiver.test'
                with receiver.app_context():
                    response = client.open(urlparse(url).path, method=method,
                        json=kwargs.get('json'), query_string=kwargs.get('params'), headers=kwargs.get('headers'))
                    body = response.get_json()
                return Mock(status_code=response.status_code, json=Mock(return_value=body))

            post = lambda url, **kwargs: request_receiver('POST', url, **kwargs)
            get = lambda url, **kwargs: request_receiver('GET', url, **kwargs)
            source.init_db()
            product = dict(product_id='P1', product_name='موديل تجريبي', price='15000',
                colors='اسود', sizes='38 إلى 52', stock='متوفر', status='active')
            with source.app.app_context():
                source._current_store_id.set('default')
                db = source.get_db()
                source.menger.save_settings(db, 'default', dict(store_id=remote_store,
                    store_name='لمسة ستور', telegram_chat_id='-10012345'))
                source.get_or_create_customer(db, 'synthetic-person', 'synthetic-page', 'facebook')
                source.save_message(db, 'synthetic-person', 'incoming', 'text',
                    'ثبتي الاسود قياس 44', None, None, None, {})
                data = dict(phone='07700000000', province='بغداد', address='حي تجريبي',
                    items=[dict(product_id='P1', product_name='موديل تجريبي', color='اسود', size='44', quantity=1)])
                with patch.object(source, 'load_products_from_file', return_value=[product]), \
                        patch.object(source, 'delivery_fee_for_province', return_value=5000), \
                        patch.object(source, 'send_order_to_telegram', return_value=True), \
                        patch.object(source, 'save_booking_to_file'):
                    created, _ = source.create_order_if_valid(db, 'synthetic-person', {'order': data}, product, cart_confirmed=True)
                assert created
                source.menger.deliver_due(db, post, 100)
                delivery = dict(db.execute('SELECT * FROM menger_deliveries').fetchone())
                assert delivery['status'] == 'sent'
                remote_id = delivery['remote_order_id']
                local_order = dict(db.execute('SELECT * FROM orders').fetchone())
                start = local_order['created_at'][:10]
                end = (source.datetime.fromisoformat(local_order['created_at']).date() + source.timedelta(days=1)).isoformat()
                assert source.conversion_growth.cohort_metrics(db, start, end, booking_source='menger')['buyers'] == 1

                with receiver.app_context():
                    remote = receiver_db.session.get(Order, remote_id)
                    assert remote.price == 20000 and remote.items[0].price == 15000
                    assert (remote.items[0].color, remote.items[0].size, remote.items[0].quantity) == ('اسود', '44', 1)
                    link = receiver_db.session.get(ExternalOrderAttribution, remote_id)
                    assert link.conversation_id == 'synthetic-person' and link.booking_actor == 'ai'
                    remote.status = 'cancelled'
                    receiver_db.session.commit()

                assert source.menger.sync_statuses(db, get, 200) == 1
                assert source.conversion_growth.cohort_metrics(db, start, end, booking_source='menger')['buyers'] == 0
                assert source.conversion_growth.cohort_metrics(db, start, end, booking_source='local')['buyers'] == 1
                # Simulate a real legacy order which already exists remotely.
                with receiver.app_context():
                    receiver_db.session.get(Order, remote_id).status = 'confirmed'
                    ExternalOrderAttribution.query.filter_by(order_id=remote_id).delete()
                    ExternalOrderImport.query.filter_by(order_id=remote_id).delete()
                    receiver_db.session.commit()
                db.execute("UPDATE menger_deliveries SET status='review_required',remote_order_id=NULL,next_attempt=0")
                db.commit()
                assert source.menger.reconcile_legacy(db, post, 500) == 1
                delivery = dict(db.execute('SELECT * FROM menger_deliveries').fetchone())
                assert delivery['remote_order_id'] == remote_id and delivery['booking_actor'] == 'unknown'
                # Reconciliation must not claim autonomous attribution for old data.
                with receiver.app_context():
                    assert Order.query.count() == 1
                    assert receiver_db.session.get(ExternalOrderAttribution, remote_id).booking_actor == 'unknown'
                unknown = json.loads(delivery['payload'])
                unknown['external_order_id'] = 'unmatched-legacy'
                unknown['customer']['phone'] = '07800000000'
                assert post('https://receiver.test/api/v1/orders/reconcile', json=unknown,
                    headers={'X-API-Key': 'synthetic-cross-project-secret'}).status_code == 404
                with receiver.app_context():
                    assert Order.query.count() == 1
                    from app.services.telegram_auto_import import import_telegram_updates
                    update = {'update_id': 991, 'message': {'message_id': 50,
                        'chat': {'id': -10012345, 'type': 'supergroup'},
                        'from': {'id': 123, 'first_name': 'تجريبي'},
                        'text': '#حجز_Menger ' + remote_id}}
                    result = import_telegram_updates([update])
                    assert result['created'] == 0 and Order.query.count() == 1
            with receiver.app_context():
                receiver_db.session.remove()
                receiver_db.engine.dispose()
    print(json.dumps(dict(ok=True, real_network=False, disposable_databases=True,
        checks=['source_to_receiver', 'items_and_total', 'exact_conversation', 'remote_cancellation',
                'separate_local_metrics', 'legacy_link_without_new_order', 'unmatched_never_creates',
                'unknown_legacy_actor', 'telegram_copy_never_duplicates'])))


if __name__ == '__main__':
    main()
