# Edge-Case Failure Scenarios & Mitigations (FAILURES.md)

This document describes four technically rigorous edge-case failure scenarios for the LinkPlease resilient integration backend, detailing how our architecture mitigates them to guarantee fault tolerance and high availability.

---

## Scenario 1: Webhook Concurrency Race Conditions and SQLite Lock Contentions under 500-Event Bursts

### The Problem
During peak spikes, the mock API platform can dispatch 500+ webhooks concurrently in under 10 seconds. In a standard SQLite configuration, multiple parallel FastAPI worker threads attempting to write to the database (e.g. inserting into `processed_events` and enqueuing jobs) will cause write lock contentions. This results in the infamous `OperationalError: database is locked` error, causing webhooks to fail with 500 and drop events.

### Architectural Mitigation
We address this at the connection and database settings level:
1. **Write-Ahead Logging (WAL) Mode:** We execute `PRAGMA journal_mode=WAL;` on database connection. WAL mode allows concurrent readers and a single writer to operate simultaneously without locking the database file, greatly increasing write throughput.
2. **Synchronous Mode Offloading:** We set `PRAGMA synchronous=NORMAL;`. In this mode, SQLite commits transactions to WAL before flushing to disk, which significantly speeds up webhook response times (< 5ms database write latency) while maintaining ACID compliance.
3. **High Busy Timeout:** We set `PRAGMA busy_timeout=5000;`. If a write transaction is currently occupied, concurrent requests will block and wait for up to 5 seconds for the lock to clear, rather than failing immediately.
4. **Single-Worker Dispatch Lock:** Our dispatch worker runs sequentially in a single asyncio loop, ensuring that database updates for outgoing DMs are serialized, avoiding write lock conflicts on status updates.

---

## Scenario 2: Memory vs. Disk State Loss across Container Restarts on Ephemeral Hosts

### The Problem
If the backend application queues messages using in-memory components (e.g., `asyncio.Queue` or a local Python list) or stores the rate-limiting tokens/keys in memory, a container restart on platforms like Render or Railway (due to manual deploys, auto-scaling, or hardware migrations) will instantly erase the queue. This causes permanent message loss (ghost messages) and can violate rate limits when the container starts fresh with an empty rate-limiting window.

### Architectural Mitigation
1. **State Persistence:** All jobs, processed events, and user interactions are stored in the database (`dm_jobs`, `processed_events`, `user_rule_interactions`). If a container restarts, the state remains intact on disk (SQLite volume or PostgreSQL).
2. **Crash-Resilient Workers:** Upon boot, the background send worker queries the database for all jobs with `status = 'queued'` and `dm_id IS NULL`, picking up right where it left off.
3. **Idempotency Keys:** For every DM dispatch, we generate a deterministic idempotency key (`hash(comment_id + rule_id)`). If a restart occurs *during* an outbound HTTP request, the retried request to the mock API will use the same key. The mock API will return the original `202 Accepted` response with the same `dm_id`, preventing double delivery.

---

## Scenario 3: Polling Delays vs. Platform Verification Timeouts (Ghost Failures)

### The Problem
When sending a DM, the mock API immediately returns `202 Accepted` and enqueues it. However, the message might fail *asynchronously* on their end later (e.g., due to account restrictions or internal errors), which we call a "ghost failure". If we rely purely on the webhook or a simple one-off check, these failures will go unnoticed. Additionally, if the polling loop is delayed or crashes, status reconciliation will hang, leading to incomplete statistics.

### Architectural Mitigation
1. **Reconciliation Polling Loop:** We run a dedicated background polling loop that queries `GET /v1/dm/{dm_id}` for every job with status `queued` and a valid `dm_id`.
2. **Ghost Failure Recovery:** If the platform status returns `failed` or `404 Not Found` (meaning the platform lost the message), we increment the job's `retry_count`, reset its `dm_id` to `None`, calculate an exponential backoff time, and return the job to the sending queue.
3. **Backoff with Jitter:** Retried jobs are delayed by `2^retry_count + rand(0, 1)` seconds to prevent thundering herd problems on the mock API platform. If a job fails 5 times, it is marked as terminally `failed`.

---

## Scenario 4: Out-Of-Order Webhook Delivery (Comment Created vs. Deleted Race)

### The Problem
Due to network delays, retries, or distributed queue processing on the platform, a `comment.deleted` webhook can arrive *before* the corresponding `comment.created` webhook. If the deletion is processed first, it will find no matching job to cancel. When the creation webhook subsequently arrives, it will matching rules and enqueue a DM job for a comment that is already deleted, violating the platform constraint.

### Architectural Mitigation
We implement a **Tombstone Pattern**:
1. **Tombstone Database Table:** We maintain a `deleted_comments` table that records the `comment_id` of all deleted comments.
2. **Deletes Enforce Tombstones:** When a `comment.deleted` event is received, we insert the `comment_id` into the `deleted_comments` table, and also update any existing jobs for that comment to `is_deleted = True`.
3. **Creation Checks Tombstones:** When a `comment.created` event is received, we check if the `comment_id` already exists in `deleted_comments`. If it does, we immediately discard the event and do not queue any DM jobs, preventing out-of-order race conditions.

---

## 4 Honest Bullets: How the System Can Still Fail

1. **Losing a DM (Disk Corruption or Permanent Loss):** If the SQLite database file on disk experiences catastrophic corruption or is deleted on the container host before WAL logs are synced to a persistent volume, any queued or retrying jobs will be lost.
2. **Losing a DM (Host Crash during Outbound Request):** If the server crashes after a request to `/v1/dm/send` has succeeded on the mock API, but *before* the worker gets the response and records the `dm_id` to the database, the job's `dm_id` remains `None`. In subsequent retries, the worker will send a new idempotency key (since the retry count increments), which the mock server will treat as a new request. If the mock server has internal user DM limits, it may reject it as a duplicate or fail it, causing the message to be lost.
3. **Sending a Duplicate (Platform Idempotency Cache Expiration):** If the platform's idempotency key cache has a short TTL (e.g. 5 minutes) and a job is retried or re-sent after that TTL expires, the platform will treat the request as a new one and send a duplicate DM to the recipient.
4. **Reporting a Wrong Number (State Lag):** If a user queries `/stats` while a worker is mid-transaction updating job statuses in the database, the counts returned may temporarily lag by a few milliseconds before committing.
