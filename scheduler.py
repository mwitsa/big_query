from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import logging
import sys
import threading
from flask import Flask, jsonify
import os
from main import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

app = Flask(__name__)
_job_lock = threading.Lock()
_job_running = False


def run_job():
    global _job_running
    with _job_lock:
        if _job_running:
            return False
        _job_running = True
    try:
        run()
    finally:
        _job_running = False
    return True


@app.route("/")
def index():
    return jsonify({"status": "running", "schedule": "daily at 09:00, 12:00, and 23:00 Bangkok time"})


@app.route("/trigger", methods=["POST"])
def trigger():
    if _job_running:
        return jsonify({"status": "skipped", "reason": "job already running"}), 409
    thread = threading.Thread(target=run_job, daemon=True)
    thread.start()
    return jsonify({"status": "started"}), 202


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    bangkok = pytz.timezone("Asia/Bangkok")
    scheduler = BackgroundScheduler(timezone=bangkok)
    scheduler.add_job(
        run_job,
        CronTrigger(hour=9, minute=0, timezone=bangkok),
        id="daily_fetch_0900",
        name="Daily BigQuery fetch 09:00",
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        run_job,
        CronTrigger(hour=12, minute=0, timezone=bangkok),
        id="daily_fetch_1200",
        name="Daily BigQuery fetch 12:00",
        misfire_grace_time=3600,
    )
    scheduler.add_job(
        run_job,
        CronTrigger(hour=23, minute=0, timezone=bangkok),
        id="daily_fetch_2300",
        name="Daily BigQuery fetch 23:00",
        misfire_grace_time=3600,
    )
    scheduler.start()
    logging.info("Scheduler started — daily fetch at 09:00, 12:00, and 23:00 Bangkok time")

    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
