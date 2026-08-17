# LinkPlease Resilient Backend

LinkPlease automates Instagram direct messaging (DMs) for creators when users comment on their posts matching specific keywords.

This repository implements a production-ready, fault-tolerant, rate-limited backend using **Python FastAPI** and **SQLAlchemy** with a resilient local **SQLite** database configured for high concurrency.

---

## Architecture & Mitigations

* **FastAPI Web Framework:** Fully asynchronous endpoints ensuring fast response times (< 500ms) for incoming webhooks.
* **SQLite with WAL Mode:** Highly concurrent database config using Write-Ahead Logging (WAL) and `busy_timeout` to prevent locking under high-concurrency spikes.
* **Rate-Limited Send Worker:** Implements a sliding token-bucket rate limiter that strictly paces outgoing DM dispatches to `<= 9 requests per 60s` to guarantee zero `429` rate limit breaches.
* **Reconciliation Poll Worker:** A background worker that reconciles asynchronous "ghost" platform failures via exponential backoff with jitter, up to 5 retries.
* **Out-of-Order tombstoning:** Employs the **Tombstone Pattern** to record `comment.deleted` events before `comment.created` arrives, preventing race conditions.

---

## API Endpoints

### 1. `POST /webhook`
Receives comment creation and deletion events from the platform. Verifies the signature in `X-PseudoGram-Signature` using HMAC-SHA256 and processes the event asynchronously.

### 2. `POST /rules`
Registers a comment-matching rule.
* **Request:**
  ```json
  { "keyword": "PRICE", "dm_message": "Here's the price list: ..." }
  ```
* **Response:**
  ```json
  { "rule_id": "rule_uuid", "keyword": "PRICE", "dm_message": "..." }
  ```

### 3. `GET /stats`
Reports live statistics of unique sent recipients and job statuses:
```json
{
  "sent": 12,
  "failed": 0,
  "queued": 0,
  "duplicates_blocked": 4
}
```

---

## Local Setup & Verification

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables in `.env` (refer to `.env.example`):
   ```env
   MOCK_API_KEY="your_base64_email.secret_key"
   MOCK_API_BASE_URL="https://pseudogram-api.onrender.com"
   ```

3. Run the automated simulation integration test:
   ```bash
   python3 test_simulation.py
   ```
   This will:
   * Spin up the FastAPI app.
   * Expose port 8000 via a secure tunnel.
   * Start a mock simulation on the platform.
   * Track webhook reception, dispatch messages under rate limits, reconcile statuses, and match platform truth statistics 100% successfully.
