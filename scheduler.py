from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import logging
from main import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

bangkok = pytz.timezone("Asia/Bangkok")
scheduler = BlockingScheduler(timezone=bangkok)

scheduler.add_job(
    run,
    CronTrigger(hour=9, minute=0, timezone=bangkok),
    id="daily_fetch",
    name="Daily BigQuery fetch",
    misfire_grace_time=3600,
)

if __name__ == "__main__":
    logging.info("Scheduler started — daily fetch at 09:00 Bangkok time")
    scheduler.start()
