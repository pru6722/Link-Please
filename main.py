import os
import hmac
import hashlib
import logging
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from database import init_db, get_db, Rule, ProcessedEvent, UserRuleInteraction, DMJob, DeletedComment
from worker import run_send_worker, run_poll_worker

# Environment configuration
MOCK_API_KEY = os.getenv("MOCK_API_KEY", "")
SKIP_SIGNATURE_VERIFICATION = os.getenv("SKIP_SIGNATURE_VERIFICATION", "false").lower() == "true"

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database tables
    logger.info("Initializing database...")
    init_db()
    
    # Start background task workers
    logger.info("Starting background workers...")
    send_worker_task = asyncio.create_task(run_send_worker())
    poll_worker_task = asyncio.create_task(run_poll_worker())
    
    yield
    
    # Clean up and shutdown workers
    logger.info("Stopping background workers...")
    send_worker_task.cancel()
    poll_worker_task.cancel()
    await asyncio.gather(send_worker_task, poll_worker_task, return_exceptions=True)
    logger.info("Shutdown complete.")

app = FastAPI(lifespan=lifespan)


# Signature verification dependency
async def verify_signature(request: Request):
    if SKIP_SIGNATURE_VERIFICATION:
        return
    
    signature_header = request.headers.get("X-PseudoGram-Signature")
    if not signature_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-PseudoGram-Signature header"
        )
    
    if not signature_header.startswith("sha256="):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Signature must be prefixed with sha256="
        )
    
    expected_signature = signature_header.split("=")[1]
    body = await request.body()
    
    import base64
    candidates = [MOCK_API_KEY]
    if "." in MOCK_API_KEY:
        try:
            prefix = MOCK_API_KEY.split(".")[0]
            missing_padding = len(prefix) % 4
            if missing_padding:
                prefix += "=" * (4 - missing_padding)
            decoded_email = base64.b64decode(prefix).decode("utf-8")
            candidates.append(decoded_email)
        except Exception as e:
            logger.warning(f"Failed to decode key prefix: {e}")
        
        candidates.append(MOCK_API_KEY.split(".")[1])
        
    matched = False
    for candidate in candidates:
        computed_sig = hmac.new(
            candidate.encode("utf-8"),
            body,
            hashlib.sha256
        ).hexdigest()
        if hmac.compare_digest(computed_sig, expected_signature):
            matched = True
            break
            
    if not matched:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid request signature"
        )


class RuleCreate(BaseModel):
    keyword: str
    dm_message: str


@app.post("/rules", status_code=status.HTTP_201_CREATED)
def create_rule(body: RuleCreate, db: Session = Depends(get_db)):
    normalized_keyword = body.keyword.strip().lower()
    
    # Create and save rule
    new_rule = Rule(
        keyword=normalized_keyword,
        dm_message=body.dm_message
    )
    db.add(new_rule)
    db.commit()
    db.refresh(new_rule)
    
    return {
        "rule_id": new_rule.id,
        "keyword": new_rule.keyword,
        "dm_message": new_rule.dm_message
    }


@app.post("/webhook", dependencies=[Depends(verify_signature)])
async def webhook_endpoint(request: Request, db: Session = Depends(get_db)):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON body")
        
    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    data = payload.get("data", {})
    
    if not event_id or not event_type:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing event_id or event_type")
        
    # 1. Deduplicate incoming event immediately at the DB layer
    processed_event = ProcessedEvent(event_id=event_id)
    db.add(processed_event)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # Already processed this event, respond 200 OK immediately
        logger.info(f"Deduplicated event {event_id} at DB layer. Skipping.")
        return {"status": "ok", "detail": "duplicate event"}
        
    # 2. Process event_type == "comment.deleted"
    if event_type == "comment.deleted":
        comment_id = data.get("comment_id")
        if comment_id:
            # 2a. Insert Tombstone to prevent out-of-order creation processing later
            tombstone = DeletedComment(comment_id=comment_id)
            db.add(tombstone)
            try:
                db.flush()
            except IntegrityError:
                db.rollback()  # Already has a tombstone, ignore
            
            # Mark pending queued jobs for this comment as deleted
            pending_jobs = db.query(DMJob).filter(
                DMJob.comment_id == comment_id,
                DMJob.status == "queued"
            ).all()
            for job in pending_jobs:
                job.is_deleted = True
                job.status = "failed"  # Cancel/fail them directly
            db.commit()
            logger.info(f"Marked pending jobs for comment {comment_id} as deleted/cancelled and saved tombstone")
        return {"status": "ok"}
        
    # 3. Process event_type == "comment.created"
    elif event_type == "comment.created":
        comment_id = data.get("comment_id")
        user_id = data.get("user_id")
        if not user_id and "from" in data and isinstance(data["from"], dict):
            user_id = data["from"].get("user_id")
        text = data.get("text", "")
        
        if not comment_id or not user_id:
            logger.warning(f"Incomplete event payload for event {event_id}. comment_id: {comment_id}, user_id: {user_id}")
            return {"status": "ok", "detail": "incomplete payload"}
            
        # Check tombstone
        tombstone_exists = db.query(DeletedComment).filter(DeletedComment.comment_id == comment_id).first()
        if tombstone_exists:
            logger.info(f"Discarding event {event_id} because comment {comment_id} was already deleted (tombstone exists)")
            return {"status": "ok", "detail": "comment_already_deleted"}
            
        # Fetch all rules to evaluate keyword matching case-insensitively
        rules = db.query(Rule).all()
        normalized_text = text.strip().lower()
        
        matched_any = False
        for rule in rules:
            if rule.keyword in normalized_text:
                matched_any = True
                # Check for duplicate interaction: user_id & rule_id must be unique
                interaction = UserRuleInteraction(user_id=user_id, rule_id=rule.id)
                db.add(interaction)
                
                try:
                    db.flush()  # Test if unique constraint is violated
                    
                    # Create normal queued job
                    job = DMJob(
                        comment_id=comment_id,
                        recipient_user_id=user_id,
                        rule_id=rule.id,
                        message=rule.dm_message,
                        status="queued"
                    )
                    db.add(job)
                    logger.info(f"Enqueued DM job for user {user_id}, rule {rule.id} (comment: {comment_id})")
                except IntegrityError:
                    db.rollback()  # Rollback constraint failure
                    
                    # Record as duplicate_blocked job
                    job = DMJob(
                        comment_id=comment_id,
                        recipient_user_id=user_id,
                        rule_id=rule.id,
                        message=rule.dm_message,
                        status="duplicate_blocked"
                    )
                    db.add(job)
                    logger.info(f"Interaction for user {user_id} and rule {rule.id} is duplicate. Recording blocked job.")
                    
        db.commit()
        return {"status": "ok", "detail": "matched" if matched_any else "no_match"}
        
    return {"status": "ok", "detail": "unsupported event type"}


@app.get("/stats")
def get_stats_endpoint(db: Session = Depends(get_db)):
    sent = db.query(DMJob.recipient_user_id).filter(DMJob.status == "sent").distinct().count()
    failed = db.query(DMJob.id).filter(DMJob.status == "failed").count()
    queued = db.query(DMJob.id).filter(DMJob.status == "queued").count()
    duplicates_blocked = db.query(DMJob.id).filter(DMJob.status == "duplicate_blocked").count()
    
    return {
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates_blocked
    }
