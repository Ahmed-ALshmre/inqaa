# Account App

Flask dashboard for Messenger/ManyChat sales conversations, product catalog management, AI replies, human review, and order notifications.

## Local Run

```bash
pip install -r requirements.txt
python app.py
```

Open:

```text
http://127.0.0.1:5000/dashboard?key=YOUR_DASHBOARD_PASSWORD
http://127.0.0.1:5000/products?key=YOUR_DASHBOARD_PASSWORD
```

## Railway Deploy

This project is ready for Railway using `railway.json` and `Procfile`. Railway provides the `PORT` environment variable automatically, and the app uses `/health` as the health check.

Required environment variables:

```text
API_SECRET_KEY
DASHBOARD_PASSWORD
PUBLIC_URL
OPENROUTER_API_KEY
MANYCHAT_API_KEY
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
DISABLE_CLIP=1
```

Optional:

```text
HUMAN_REPLY_WEBHOOK_URL
ASYNC_WEBHOOK=1
DEBOUNCE_DELAY=35
```

After deploying, set `PUBLIC_URL` to the Railway public URL and configure ManyChat/Facebook webhooks to use:

```text
https://your-railway-domain.up.railway.app/webhook
https://your-railway-domain.up.railway.app/manychat/webhook
```

## Product Images

Products are stored in `products.json`. Product images can be local paths under `product_image/` or external URLs. Multiple images are supported by using a JSON array.

## Notes

Runtime logs and local secrets are intentionally ignored by Git:

- `.env`
- `sales.db`
- `incoming_requests.jsonl`
- `ad_tracking.jsonl`
- `bookings.jsonl`
