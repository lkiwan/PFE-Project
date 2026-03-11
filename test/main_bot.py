import schedule
import time
from datetime import datetime
from scraper_level1 import scrape_level_1
from scraper_level2 import scrape_level_2, get_driver

def job():
    now = datetime.now()
    # Market Hours Check (Monday-Friday, 09:30-15:30)
    if 0 <= now.weekday() <= 4 and 9 <= now.hour <= 15:
        print(f"\n🔔 [BOT] Starting cycle at {now}")
        driver = get_driver()
        try:
            scrape_level_1()   # Complete OHLCV
            scrape_level_2(driver) # Perfect Bid/Ask
        finally:
            driver.quit()
    else:
        print(f"😴 [BOT] Market is closed at {now}.")

# Schedule: Run every 1 hour (as you requested)
schedule.every(1).hours.do(job)

if __name__ == "__main__":
    print("🤖 AI Trading Scraper is ACTIVE.")
    job() # First run
    while True:
        schedule.run_pending()
        time.sleep(1)