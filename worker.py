import os
import asyncio
import logging
import random
import hashlib
from datetime import datetime, timezone, timedelta
import httpx
from sqlalchemy.orm import Session
from dotenv import load_dotenv
load_dotenv()

from database import SessionLocal, DMJob, is_sqlite

# Configuration
MOCK_API_BASE_URL = os.getenv("MOCK_API_BASE_URL", "https://pseudogram-api.onrender.com")
MOCK_API_KEY = os.getenv("MOCK_API_KEY", "")

# Rate limiting settings: strictly <= 9 requests / 60 seconds (1 every 6.8 seconds)
MIN_SEND_INTERVAL = 6.8

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

last_send_time = 0.0


def claim_next_job(db: Session):
    """
    Selects the next job to process and updates its next_retry_at to lock it.
    Uses FOR UPDATE SKIP LOCKED for PostgreSQL or transaction-based locking for SQLite.
    """
    now = datetime.now(timezone.utc)
    if is_sqlite:
        # SQLite: query and update under standard SQLite serialization
        job = db.query(DMJob).filter(
            DMJob.status == "queued",
            DMJob.dm_id.is_(None),
            DMJob.is_deleted == False,
            DMJob.next_retry_at <= now
        ).order_by(DMJob.created_at.asc()).first()
        if job:
            # Temporarily push next_retry_at to lock it
            job.next_retry_at = now + timedelta(minutes=2)
            db.commit()
            return job
    else:
        # PostgreSQL: FOR UPDATE SKIP LOCKED
        job = db.query(DMJob).filter(
            DMJob.status == "queued",
            DMJob.dm_id.is_(None),
            DMJob.is_deleted == False,
            DMJob.next_retry_at <= now
        ).order_by(DMJob.created_at.asc()).with_for_update(skip_locked=True).first()
        if job:
            # Temporarily push next_retry_at to lock it
            job.next_retry_at = now + timedelta(minutes=2)
            db.commit()
            return job
    return None


def handle_transient_failure(db: Session, job: DMJob, error_msg: str):
    """
    Handles transient failures by incrementing retry count and calculating exponential backoff.
    """
    job.retry_count += 1
    if job.retry_count >= 5:
        job.status = "failed"
        logger.error(f"Job {job.id} reached max retries (5). Setting status to failed. Error: {error_msg}")
    else:
        # Exponential backoff: min(60, 2^retry_count) + jitter (0 to 1s)
        backoff = min(60.0, 2.0 ** job.retry_count) + random.uniform(0.0, 1.0)
        job.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=backoff)
        logger.warning(f"Job {job.id} transient failure ({job.retry_count}/5). Retrying in {backoff:.2f}s. Error: {error_msg}")
    job.updated_at = datetime.now(timezone.utc)
    db.commit()


async def send_dm_via_api(job_id: str):
    """
    Hits the POST /v1/dm/send endpoint for a specific DM job.
    """
    db = SessionLocal()
    try:
        job = db.query(DMJob).filter(DMJob.id == job_id).first()
        if not job:
            return
        
        if job.is_deleted:
            job.status = "failed"
            db.commit()
            logger.info(f"Skipping deleted job {job.id}")
            return

        # Double check user_rule_interactions duplicate prior to send (defense-in-depth)
        existing_sent_job = db.query(DMJob).filter(
            DMJob.recipient_user_id == job.recipient_user_id,
            DMJob.rule_id == job.rule_id,
            DMJob.id != job.id,
            DMJob.status.in_(["sent", "queued"]),
            DMJob.dm_id.is_not(None)
        ).first()
        
        if existing_sent_job:
            job.status = "duplicate_blocked"
            db.commit()
            logger.warning(f"Blocking duplicate user-rule interaction in worker for job {job.id}")
            return
        
        # Prepare request
        url = f"{MOCK_API_BASE_URL}/v1/dm/send"
        headers = {
            "X-API-Key": MOCK_API_KEY,
            "Idempotency-Key": hashlib.sha256(f"{job.comment_id}:{job.rule_id}:{job.retry_count}".encode()).hexdigest(),
            "Content-Type": "application/json"
        }
        payload = {
            "recipient_user_id": job.recipient_user_id,
            "message": job.message,
            "comment_id": job.comment_id
        }
        
        logger.info(f"Dispatching DM for job {job.id} (comment: {job.comment_id}, recipient: {job.recipient_user_id})")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            
        if response.status_code in [200, 202]:
            data = response.json()
            job.dm_id = data.get("dm_id")
            api_status = data.get("status")
            
            if api_status == "delivered":
                job.status = "sent"
                logger.info(f"DM successfully delivered immediately for job {job.id}, dm_id: {job.dm_id}")
            elif api_status == "failed":
                logger.warning(f"DM delivery reported failed immediately for job {job.id}, dm_id: {job.dm_id}. Re-enqueueing.")
                job.dm_id = None
                handle_transient_failure(db, job, "Immediate delivery status failed")
            else:
                job.status = "queued"  # stays queued for polling
                logger.info(f"DM accepted (queued) for job {job.id}, dm_id: {job.dm_id}")
                
            job.updated_at = datetime.now(timezone.utc)
            db.commit()
            
        elif response.status_code == 429:
            retry_after_val = response.headers.get("Retry-After")
            try:
                retry_after = float(retry_after_val) if retry_after_val else 10.0
            except ValueError:
                retry_after = 10.0
            
            # Backoff without incrementing retry count for 429 rate limit
            now = datetime.now(timezone.utc)
            job.next_retry_at = now + timedelta(seconds=retry_after)
            db.commit()
            logger.warning(f"Mock API returned 429 rate limit. Retrying job {job.id} in {retry_after}s")
            
        elif response.status_code == 400:
            job.status = "failed"
            job.updated_at = datetime.now(timezone.utc)
            db.commit()
            logger.error(f"Job {job.id} failed immediately (400 Bad Request): {response.text}")
            
        else:
            # 500 or other transient errors
            handle_transient_failure(db, job, f"HTTP status {response.status_code}: {response.text}")
            
    except Exception as e:
        logger.exception(f"Unexpected error in sending DM for job {job_id}")
        db.rollback()
        # Attempt to handle error as transient
        try:
            job = db.query(DMJob).filter(DMJob.id == job_id).first()
            if job:
                handle_transient_failure(db, job, str(e))
        except Exception:
            logger.exception("Failed to update job status after exception")
    finally:
        db.close()


async def process_next_dm_job():
    """
    Main step to process a single queued DM job, enforcing rate-limit sleep.
    """
    global last_send_time
    db = SessionLocal()
    try:
        job = claim_next_job(db)
        if not job:
            return False
        
        # Enforce rate limiter sleep BEFORE calling the API
        now = asyncio.get_event_loop().time()
        elapsed = now - last_send_time
        if elapsed < MIN_SEND_INTERVAL:
            sleep_time = MIN_SEND_INTERVAL - elapsed
            logger.info(f"Rate limiter pacing: sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)
            
        # Update last send time
        last_send_time = asyncio.get_event_loop().time()
        
        # Dispatch the job
        await send_dm_via_api(job.id)
        return True
    finally:
        db.close()


async def run_send_worker():
    """
    Background loop running the DM sending worker.
    """
    logger.info("Starting send worker loop...")
    while True:
        try:
            processed = await process_next_dm_job()
            if not processed:
                # No jobs to process, sleep briefly
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            logger.info("Send worker loop cancelled.")
            break
        except Exception as e:
            logger.exception(f"Exception in run_send_worker: {e}")
            await asyncio.sleep(1.0)


async def poll_job(job_id: str):
    """
    Polls the GET /v1/dm/{dm_id} status for a specific DM job.
    """
    db = SessionLocal()
    try:
        job = db.query(DMJob).filter(DMJob.id == job_id).first()
        if not job or job.status != "queued" or not job.dm_id:
            return
        
        if job.is_deleted:
            job.status = "failed"
            db.commit()
            logger.info(f"Skipping deleted job {job.id} during poll")
            return
        
        url = f"{MOCK_API_BASE_URL}/v1/dm/{job.dm_id}"
        headers = {"X-API-Key": MOCK_API_KEY}
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
            
        if response.status_code == 200:
            data = response.json()
            status = data.get("status")
            
            if status == "delivered":
                job.status = "sent"
                job.updated_at = datetime.now(timezone.utc)
                db.commit()
                logger.info(f"Job {job.id} (dm_id: {job.dm_id}) delivered successfully.")
                
            elif status == "failed":
                # Ghost failure: re-enqueue for retry
                logger.warning(f"Job {job.id} (dm_id: {job.dm_id}) delivery failed on platform. Re-enqueueing.")
                job.dm_id = None
                handle_transient_failure(db, job, "Platform delivery status failed")
                
            elif status == "queued":
                # Still queued on platform, wait for next poll
                pass
                
        elif response.status_code == 404:
            # Ghost failure: Platform lost the DM ID
            logger.warning(f"Job {job.id} (dm_id: {job.dm_id}) not found on platform (404). Re-enqueueing.")
            job.dm_id = None
            handle_transient_failure(db, job, "Platform returned 404 for dm_id")
            
        else:
            logger.warning(f"Polling job {job.id} received unexpected status: {response.status_code}")
            
    except Exception as e:
        logger.exception(f"Unexpected error polling job {job_id}: {e}")
    finally:
        db.close()


async def run_poll_worker():
    """
    Background loop running the status reconciliation polling worker.
    """
    logger.info("Starting polling worker loop...")
    while True:
        try:
            db = SessionLocal()
            # Fetch all queued jobs that have a dm_id
            jobs_to_poll = db.query(DMJob).filter(
                DMJob.status == "queued",
                DMJob.dm_id.is_not(None),
                DMJob.is_deleted == False
            ).all()
            db.close()
            
            if jobs_to_poll:
                logger.info(f"Polling status for {len(jobs_to_poll)} jobs...")
                # Poll sequentially or concurrently depending on scale. Bounded concurrency is safest.
                tasks = [poll_job(job.id) for job in jobs_to_poll]
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except asyncio.CancelledError:
            logger.info("Polling worker loop cancelled.")
            break
        except Exception as e:
            logger.exception(f"Exception in run_poll_worker: {e}")
            
        # Poll every 5 seconds
        await asyncio.sleep(5.0)
