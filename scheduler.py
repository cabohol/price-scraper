import schedule
import time
from datetime import datetime
import logging
from scraper import CaragaPriceScraper
from config import SCHEDULE_TIME, LOG_FILE, MARKETS
import requests
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
    """Get latest PDF URL using HTTP requests only (no Selenium)"""
    try:
        logging.info(f"Fetching latest PDF from: {market_url}")
        
        # Make simple HTTP request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(market_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all PDF links
        pdf_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Check if it's a price monitoring PDF
            if 'PriceMonitoring' in href and '.pdf' in href:
                # Make full URL
                if href.startswith('http'):
                    pdf_url = href
                else:
                    pdf_url = f"https://caraga.da.gov.ph{href}"
                
                pdf_links.append(pdf_url)
        
        if pdf_links:
            # Return the FIRST PDF found (usually the latest)
            latest_pdf = pdf_links[0]
            logging.info(f"Found latest PDF: {latest_pdf}")
            return latest_pdf
        else:
            logging.warning(f"No PDF found on {market_url}")
            return None
        
    except Exception as e:
        logging.error(f"Error fetching PDF from {market_url}: {e}")
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
            print("❌ Daily update failed")
            
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
    
    # Run immediately on start
    print("🚀 Running initial update now...\n")
    daily_update_job()
    
    # Keep the script running
    print(f"\n⏳ Waiting for next scheduled run at {SCHEDULE_TIME}...")
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    try:
        run_scheduler()
    except KeyboardInterrupt:
        print("\n\n👋 Scheduler stopped by user")
        logging.info("Scheduler stopped by user")