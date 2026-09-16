"""Bounded concurrent OpenRouter requests with finite transient recovery."""
import os
import threading
import time
from contextvars import ContextVar
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

import requests

LIMIT = max(1, min(32, int(os.environ.get('AI_MAX_CONCURRENT', '8'))))
READ_TIMEOUT = max(30, min(60, float(os.environ.get('AI_READ_TIMEOUT', '60'))))
_slots = threading.BoundedSemaphore(LIMIT)
_metrics = ContextVar('ai_transport_metrics', default=None)
TRANSIENT = {408, 429, 500, 502, 503, 504}
observer = None


def retry_delay(response):
    value = (response.headers or {}).get('Retry-After', '1')
    try:
        return max(0, float(value))
    except (ValueError, TypeError):
        try:
            return max(0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 1


def post(url, **kwargs):
    started = time.monotonic()
    _metrics.set({})
    status, error = None, None
    try:
        result = _post(url, **kwargs)
        status = result.status_code
        return result
    except requests.RequestException as exc:
        error = type(exc).__name__
        status = getattr(getattr(exc, 'response', None), 'status_code', None)
        raise
    finally:
        values = metrics()
        values.update(elapsed_ms=round((time.monotonic() - started) * 1000), status=status, error=error)
        _metrics.set(values)
        if observer:
            try:
                observer((kwargs.get('json') or {}).get('model'), values)
            except Exception:
                pass  # Diagnostics cannot turn a valid AI response into a failure.


def _post(url, **kwargs):
    """Never retry billing/auth errors; at most two calls per logical request.

    Timeout measures idle socket time (requests semantics). Workers, rather than
    HTTP dashboard requests, own the potentially long provider wait.
    """
    started = time.monotonic()
    timeout = kwargs.pop('timeout', 30)
    read_timeout = max(READ_TIMEOUT, min(60, float(timeout if isinstance(timeout, (int, float)) else timeout[-1])))
    deadline = started + 130
    metrics = {'queue_ms': 0, 'attempts': 0, 'elapsed_ms': 0}
    _metrics.set(metrics)
    last_error = None
    for attempt in range(2):
        waiting = time.monotonic()
        if not _slots.acquire(timeout=max(0, min(15, deadline - waiting))):
            raise requests.Timeout('AI capacity busy; retryable queue wait')
        metrics['queue_ms'] += round((time.monotonic() - waiting) * 1000)
        response = None
        try:
            metrics['attempts'] += 1
            remaining = deadline - time.monotonic()
            if remaining <= 5:
                raise requests.Timeout('AI request deadline exceeded')
            response = requests.post(url, timeout=(5, min(read_timeout, remaining - 5)), **kwargs)
            if response.status_code not in TRANSIENT or attempt == 1:
                return response
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt == 1:
                raise
        finally:
            _slots.release()
            metrics['elapsed_ms'] = round((time.monotonic() - started) * 1000)
        delay = retry_delay(response) if response is not None else 1
        if delay + 5 >= deadline - time.monotonic():
            if response is not None:
                return response
            raise last_error
        if response is not None:
            _ = response.content
            response.close()
        time.sleep(delay)


def metrics():
    return dict(_metrics.get() or {})
