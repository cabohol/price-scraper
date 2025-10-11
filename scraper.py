import pdfplumber
import requests
from io import BytesIO
from datetime import datetime
import logging
import os
from config import SUPABASE_URL, SUPABASE_ANON_KEY, TABLE_NAME, LOG_FILE, LOG_LEVEL

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

class CaragaPriceScraper:
    def __init__(self):
        """Initialize with direct HTTP requests - NO SUPABASE LIBRARY"""
        self.supabase_url = SUPABASE_URL
        self.supabase_key = SUPABASE_ANON_KEY
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        logging.info("Scraper initialized (direct HTTP)")
        
    def download_pdf(self, pdf_url):
        """Download PDF from URL"""
        try:
            logging.info(f"Downloading PDF from: {pdf_url}")
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
            logging.info("PDF downloaded successfully")
            return BytesIO(response.content)
        except Exception as e:
            logging.error(f"Error downloading PDF: {e}")
            return None
    
    def extract_commodity_data(self, pdf_file):
        """Extract commodity data from PDF"""
        commodities = []
        
        EXCLUDED_CATEGORIES = [
            'FERTILIZER', 
            'INSECTICIDE', 
            'HERBICIDE', 
            'MOLLUSCIDE', 
            'RODENTICIDE',
            'PESTICIDE',
            'FUNGICIDE'
        ]
        
        try:
            logging.info("Extracting data from PDF...")
            with pdfplumber.open(pdf_file) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    logging.info(f"Processing page {page_num}...")
                    tables = page.extract_tables()
                    
                    for table in tables:
                        current_category = ""
                        
                        for i, row in enumerate(table):
                            if i == 0:
                                continue
                                
                            if not row or len(row) < 5:
                                continue
                            
                            commodity_group_raw = (row[0] or "").strip()
                            
                            if commodity_group_raw:
                                current_category = commodity_group_raw.upper()
                            
                            commodity_group = current_category
                            commodity_name = (row[1] or "").strip()
                            specification = (row[2] or "").strip()
                            unit = (row[3] or "").strip()
                            average_price_str = (row[-1] or "").strip()
                            
                            if commodity_group in EXCLUDED_CATEGORIES:
                                continue
                            
                            if not commodity_name or not average_price_str:
                                continue
                            
                            try:
                                avg_price = float(average_price_str.replace(',', ''))
                                
                                commodities.append({
                                    'name': commodity_name,
                                    'category': commodity_group,
                                    'unit': unit or 'kg',
                                    'average_price': avg_price
                                })
                                
                            except (ValueError, AttributeError):
                                continue
            
            logging.info(f"Extracted {len(commodities)} commodities")
            
        except Exception as e:
            logging.error(f"Error extracting: {e}")
        
        return commodities
    
    def insert_to_supabase(self, commodities):
        """Insert using direct HTTP requests"""
        if not commodities:
            return 0
        
        inserted = 0
        updated = 0
        
        for item in commodities:
            try:
                price_range = f"₱{item['average_price']:.2f} per {item['unit']}"
                
                # URL encode the name for query
                from urllib.parse import quote
                encoded_name = quote(item['name'])
                
                # Check if exists via HTTP
                check_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}?name=eq.{encoded_name}&select=id"
                check_response = requests.get(check_url, headers=self.headers)
                
                data = {
                    'name': item['name'],
                    'category': item['category'],
                    'typical_serving_size': f"100 {item['unit']}",
                    'price_range': price_range,
                    'cost_per_serving': item['average_price'],
                    'availability': 'available',
                    'updated_at': datetime.now().isoformat()
                }
                
                if check_response.json():
                    # Update via HTTP PATCH
                    record_id = check_response.json()[0]['id']
                    update_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}?id=eq.{record_id}"
                    requests.patch(update_url, headers=self.headers, json=data)
                    updated += 1
                    logging.info(f"Updated: {item['name']}")
                else:
                    # Insert via HTTP POST
                    data['created_at'] = datetime.now().isoformat()
                    insert_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}"
                    requests.post(insert_url, headers=self.headers, json=data)
                    inserted += 1
                    logging.info(f"Inserted: {item['name']}")
                
            except Exception as e:
                logging.error(f"Error: {item['name']} - {e}")
                continue
        
        logging.info(f"Inserted: {inserted}, Updated: {updated}")
        return inserted + updated
    
    def run(self, pdf_url):
        """Main function"""
        logging.info("="*60)
        logging.info(f"Processing PDF: {pdf_url}")
        
        print("\nDownloading PDF...")
        pdf_file = self.download_pdf(pdf_url)
        
        if not pdf_file:
            return False
        
        print("Extracting...")
        commodities = self.extract_commodity_data(pdf_file)
        
        if not commodities:
            return False
        
        print(f"Saving {len(commodities)} items...")
        count = self.insert_to_supabase(commodities)
        
        print(f"✅ Done! Processed {count} items")
        return True


def main(pdf_url=None):
    """Main entry point"""
    scraper = CaragaPriceScraper()
    return scraper.run(pdf_url)


if __name__ == "__main__":
    import sys
    pdf_url = sys.argv[1] if len(sys.argv) > 1 else None
    main(pdf_url)