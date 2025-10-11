import schedule
import time
from datetime import datetime
import logging
from scraper import CaragaPriceScraper
from config import SCHEDULE_TIME, LOG_FILE, MARKETS
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_latest_pdf_url(market_url):
    """Scrape the CARAGA website to get the latest PDF URL"""
    try:
        logging.info(f"Fetching latest PDF from: {market_url}")
        
        # Setup headless Chrome
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(market_url)
        time.sleep(3)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find the first PDF link
        links = soup.find_all('a', href=True)
        
        for link in links:
            href = link['href']
            if 'PriceMonitoring' in href and '.pdf' in href:
                pdf_url = href if href.startswith('http') else f"https://caraga.da.gov.ph{href}"
                driver.quit()
                logging.info(f"Found latest PDF: {pdf_url}")
                return pdf_url
        
        driver.quit()
        logging.warning(f"No PDF found on {market_url}")
        return None
        
    except Exception as e:
        logging.error(f"Error getting latest PDF from {market_url}: {e}")
        return None

def daily_update_job():
    """Job that runs daily to update prices from ALL markets"""
    try:
        logging.info("="*60)
        logging.info("Starting scheduled price update for ALL markets")
        
        print(f"\n{'='*60}")
        print(f"⏰ Scheduled Update - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        total_processed = 0
        scraper = CaragaPriceScraper()
        
        # Loop through each market
        for market_name, market_url in MARKETS.items():
            print(f"\n📍 Processing: {market_name}")
            print(f"   URL: {market_url}")
            
            # Get latest PDF for this market
            pdf_url = get_latest_pdf_url(market_url)
            
            if not pdf_url:
                print(f"   ⚠️  No PDF found for {market_name}, skipping...")
                logging.warning(f"No PDF found for {market_name}")
                continue
            
            print(f"   📄 Found PDF: {pdf_url}")
            
            # Run scraper for this market
            success = scraper.run(pdf_url)
            
            if success:
                print(f"   ✅ {market_name} completed!")
                total_processed += 1
            else:
                print(f"   ❌ {market_name} failed")
                logging.error(f"Failed to process {market_name}")
        
        print(f"\n{'='*60}")
        print(f"📊 Summary: Processed {total_processed}/{len(MARKETS)} markets")
        print(f"{'='*60}\n")
        
        if total_processed > 0:
            logging.info(f"Scheduled update completed - {total_processed} markets processed")
            print("✅ Daily update completed!")
        else:
            logging.error("Scheduled update failed - no markets processed")
            print("❌ Daily update failed - no markets processed")
            
    except Exception as e:
        logging.error(f"Error in scheduled job: {e}")
        print(f"❌ Error: {e}")

def run_scheduler():
    """Start the scheduler"""
    print("="*60)
    print("🤖 CARAGA Multi-Market Price Scraper")
    print("="*60)
    print(f"⏰ Scheduled to run daily at {SCHEDULE_TIME}")
    print(f"📋 Logs: {LOG_FILE}")
    print(f"🏪 Markets:")
    for market_name in MARKETS.keys():
        print(f"   - {market_name}")
    print("="*60)
    print("\nPress Ctrl+C to stop\n")
    
    # Schedule the job to run daily
    schedule.every().day.at(SCHEDULE_TIME).do(daily_update_job)
    
    # Optional: Run immediately on start
    print("🚀 Running initial update now...\n")
    daily_update_job()
    
    # Keep the script running
    print(f"\n⏳ Waiting for next scheduled run at {SCHEDULE_TIME}...")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    try:
        run_scheduler()
    except KeyboardInterrupt:
        print("\n\n👋 Scheduler stopped by user")
        logging.info("Scheduler stopped by user")