from apscheduler.schedulers.blocking import BlockingScheduler
from daily_update import fetch_and_update

scheduler = BlockingScheduler()

# Jalanin setiap hari jam 18:00 (setelah market close)
scheduler.add_job(fetch_and_update, 'cron', hour=18, minute=0)

print("Scheduler started...")
scheduler.start()
