
import os
import json
import boto3
import logging
from datetime import datetime, timedelta, timezone

# -----------------------------
# Config / Environment Variables
# -----------------------------
# Delay for the one-time schedule. Default = 5 minutes to allow late CTR updates to land.
DELAY_MINUTES   = int(os.environ.get("DELAY_MINUTES", "5"))

# EventBridge Scheduler group where one-time schedules are created
SCHEDULE_GROUP  = os.environ.get("SCHEDULE_GROUP", "calltimeline-delayed")

# Target Lambda ARN to invoke after the delay (now your process-ctr)
LAMBDA_B_ARN    = os.environ.get("LAMBDA_B_ARN")              

# EventBridge Scheduler's target role that can invoke the target Lambda
SCHEDULER_ROLE  = os.environ.get("SCHEDULER_ROLE_ARN")         # IMPORTANT: _ARN

# -----------------------------
# AWS Clients
# -----------------------------
scheduler = boto3.client("scheduler")

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -----------------------------
# Helper Functions
# -----------------------------
def build_schedule_name(root_contact_id: str, suffix: str = "timeline") -> str:
    """
    Build a schedule name ≤ 64 chars (Scheduler constraint).
    For first-event-only logic, a single deterministic name per RootContactId is sufficient.
    """
    name = f"{root_contact_id}-{suffix}"
    return name[:64] if len(name) > 64 else name

def build_fire_time_str() -> str:
    """
    Return UTC time string for EventBridge Scheduler 'at(YYYY-MM-DDTHH:MM:SS)'.
    DO NOT append 'Z' — Scheduler expects no 'Z' in the expression.
    """
    fire_time = datetime.now(timezone.utc) + timedelta(minutes=DELAY_MINUTES)
    # Small buffer so the scheduled time isn’t too close to "now"
    fire_time += timedelta(seconds=5)
    fire_time = fire_time.replace(microsecond=0)
    return fire_time.strftime("%Y-%m-%dT%H:%M:%S")

def create_schedule(root_contact_id: str, payload: dict):
    """
    Creates a one-time EventBridge Scheduler schedule to invoke LAMBDA_B_ARN.
    """
    schedule_name = build_schedule_name(root_contact_id)
    fire_time_str = build_fire_time_str()

    req = {
        "Name": schedule_name,
        "GroupName": SCHEDULE_GROUP,
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "ScheduleExpression": f"at({fire_time_str})",
        "ScheduleExpressionTimezone": "UTC",   # Expression has no 'Z'; timezone is specified here
        "ActionAfterCompletion": "DELETE",
        "Target": {
            "Arn": LAMBDA_B_ARN,
            "RoleArn": SCHEDULER_ROLE,
            "Input": json.dumps(payload)
        },
        "State": "ENABLED"
    }

    logger.info(f"[CREATE] Request: {json.dumps(req)}")
    resp = scheduler.create_schedule(**req)
    logger.info(f"[CREATE] Response: {json.dumps(resp, default=str)}")
    logger.info(f"[OK] Created schedule '{schedule_name}' for rootContactId={root_contact_id} at {fire_time_str} UTC")

# -----------------------------
# Lambda Handler
# -----------------------------
def lambda_handler(event, context):
    """
    New core logic:
      - Listen to DynamoDB Streams on ConnectCallTimeline
      - On FIRST event (INSERT) of an item, create a one-time schedule to invoke process-ctr
      - Delay = DELAY_MINUTES (default 5)
    """
    # Boot checks
    if not LAMBDA_B_ARN or not SCHEDULER_ROLE:
        logger.error(f"[ENV ERROR] Missing env vars. LAMBDA_B_ARN={LAMBDA_B_ARN}, SCHEDULER_ROLE_ARN={SCHEDULER_ROLE}")
        return {"status": "env-error"}

    records = event.get("Records", [])
    logger.info(f"Records received: {len(records)}")

    processed = 0
    skipped = 0
    errors = 0

    for idx, record in enumerate(records, start=1):
        try:
            event_name = record.get("eventName")
            ddb = record.get("dynamodb") or {}
            new_image = ddb.get("NewImage") or {}

            # We only care about the FIRST event for an item: INSERT
            if event_name != "INSERT":
                skipped += 1
                logger.info(f"[SKIP] Not an INSERT (eventName={event_name}) for record #{idx}")
                continue

            root_contact_id = (new_image.get("RootContactId") or {}).get("S")
            if not root_contact_id:
                skipped += 1
                logger.warning(f"[SKIP] Missing RootContactId in NewImage for record #{idx}")
                continue

            logger.info(f"[REC {idx}] INSERT detected for rootContactId={root_contact_id}")

            # Build payload for downstream Lambda (process-ctr)
            payload = {
                "rootContactId": root_contact_id,
                "triggeredBy": "conversation-timeline-scheduler",
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            logger.info(
                f"[PLAN] Creating one-time schedule (+{DELAY_MINUTES}m) for rootContactId={root_contact_id} "
                f"with payload={json.dumps(payload)}"
            )

            create_schedule(root_contact_id, payload)
            processed += 1

        except scheduler.exceptions.ConflictException:
            # If the same INSERT is re-delivered or function retried, schedule may already exist
            processed += 1
            logger.info(f"[OK] Schedule already exists (idempotent). rootContactId={root_contact_id}")
        except Exception as e:
            errors += 1
            logger.exception(f"[ERROR] Failed for record #{idx}: {e}")

    logger.info(f"[SUMMARY] processed={processed} skipped={skipped} errors={errors} total={len(records)}")
    return {"status": "ok" if errors == 0 else "partial-error", "processed": processed, "skipped": skipped, "errors": errors}
