import pdfplumber
import requests
from io import BytesIO
from datetime import datetime
from supabase import create_client
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
        """Initialize Supabase client"""
        self.supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        logging.info("Scraper initialized")
        
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
                    
                    for table_num, table in enumerate(tables, 1):
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
                                logging.info(f"Skipping non-food item: {commodity_name} ({commodity_group})")
                                continue
                            
                            if not commodity_name or not average_price_str:
                                continue
                            
                            try:
                                avg_price = float(average_price_str.replace(',', ''))
                                
                                commodities.append({
                                    'name': commodity_name,
                                    'category': commodity_group,
                                    'specification': specification,
                                    'unit': unit or 'kg',
                                    'average_price': avg_price
                                })
                                
                            except (ValueError, AttributeError) as e:
                                logging.warning(f"Skipping invalid price for {commodity_name}: {e}")
                                continue
            
            logging.info(f"Extracted {len(commodities)} food commodities")
            
        except Exception as e:
            logging.error(f"Error extracting data: {e}")
        
        return commodities
    
    def insert_to_supabase(self, commodities):
        """Insert commodities into Supabase ingredients table"""
        if not commodities:
            logging.warning("No commodities to insert")
            return 0
        
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        for item in commodities:
            try:
                price_range = f"₱{item['average_price']:.2f} per {item['unit']}"
                
                existing = self.supabase.table(TABLE_NAME)\
                    .select('id')\
                    .eq('name', item['name'])\
                    .execute()
                
                ingredient_data = {
                    'name': item['name'],
                    'category': item['category'],
                    'typical_serving_size': f"100 {item['unit']}",
                    'price_range': price_range,
                    'cost_per_serving': item['average_price'],
                    'availability': 'available',
                    'updated_at': datetime.now().isoformat()
                }
                
                if existing.data and len(existing.data) > 0:
                    result = self.supabase.table(TABLE_NAME)\
                        .update(ingredient_data)\
                        .eq('id', existing.data[0]['id'])\
                        .execute()
                    updated_count += 1
                    logging.info(f"Updated: {item['name']} - ₱{item['average_price']:.2f}/{item['unit']}")
                else:
                    ingredient_data['created_at'] = datetime.now().isoformat()
                    
                    result = self.supabase.table(TABLE_NAME)\
                        .insert(ingredient_data)\
                        .execute()
                    inserted_count += 1
                    logging.info(f"Inserted: {item['name']} - ₱{item['average_price']:.2f}/{item['unit']}")
                
            except Exception as e:
                error_count += 1
                logging.error(f"Error processing {item['name']}: {e}")
                continue
        
        logging.info(f"Summary - Inserted: {inserted_count}, Updated: {updated_count}, Errors: {error_count}")
        
        return inserted_count + updated_count
    
    def run(self, pdf_url):
        """Main function: Download PDF, extract data, save to Supabase"""
        logging.info("="*60)
        logging.info("Starting scraper job")
        logging.info(f"PDF URL: {pdf_url}")
        
        print(f"\n{'='*60}")
        print("CARAGA Price Scraper")
        print(f"{'='*60}")
        print(f"PDF: {pdf_url}")
        
        print("\nDownloading PDF...")
        pdf_file = self.download_pdf(pdf_url)
        
        if not pdf_file:
            print("Failed to download PDF")
            logging.error("Failed to download PDF")
            return False
        
        print("Extracting commodity data...")
        commodities = self.extract_commodity_data(pdf_file)
        
        print(f"Found {len(commodities)} commodities")
        
        if not commodities:
            print("No valid data extracted")
            logging.warning("No valid data extracted")
            return False
        
        print("\nSaving to Supabase...")
        count = self.insert_to_supabase(commodities)
        
        print(f"\nSuccess! Processed {count} ingredients")
        print(f"{'='*60}\n")
        logging.info(f"Scraper job completed - {count} ingredients processed")
        
        return True


def main(pdf_url=None):
    """Main entry point"""
    scraper = CaragaPriceScraper()
    return scraper.run(pdf_url)


if __name__ == "__main__":
    import sys
    pdf_url = sys.argv[1] if len(sys.argv) > 1 else None
    main(pdf_url)