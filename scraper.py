import pdfplumber
import requests
from io import BytesIO
from datetime import datetime
import logging
import os
import json
from groq import Groq
from config import SUPABASE_URL, SUPABASE_ANON_KEY, GROQ_API_KEY, TABLE_NAME, LOG_FILE, LOG_LEVEL

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
        """Initialize scraper with Supabase and Groq AI"""
        self.supabase_url = SUPABASE_URL
        self.supabase_key = SUPABASE_ANON_KEY
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        
        # Initialize Groq AI
        self.groq_client = Groq(api_key=GROQ_API_KEY)
        
        logging.info("Scraper initialized with AI enrichment")
        
    def get_ai_nutrition(self, name, category):
        """Get nutritional data from Llama 3.3 70B via Groq"""
        try:
            prompt = f"""
Provide accurate nutritional information for this ingredient:
Name: {name}
Category: {category}

Return ONLY a valid JSON object (no markdown, no explanation) with these exact fields:
{{
    "carbs_grams": <float - carbohydrates per 100g>,
    "calories_per_serving": <int - calories per 100g serving>,
    "protein_grams": <float - protein per 100g>,
    "fat_grams": <float - fat per 100g>,
    "fiber_grams": <float - fiber per 100g>,
    "glycemic_index": <int - 0-100, estimate if unknown>,
    "is_diabetic_friendly": <bool - low GI and suitable>,
    "is_vegetarian": <bool>,
    "is_vegan": <bool>,
    "is_halal": <bool>,
    "is_kosher": <bool>,
    "is_catholic": <bool - no restrictions>,
    "common_allergens": <string or null - comma separated if multiple>
}}

Use standard nutrition databases. Be accurate and scientific.
"""
            
            response = self.groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a nutrition expert. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                max_tokens=1000
            )
            
            # Parse AI response
            ai_response = response.choices[0].message.content.strip()
            
            # Remove markdown code blocks if present
            if ai_response.startswith('```'):
                ai_response = ai_response.split('```')[1]
                if ai_response.startswith('json'):
                    ai_response = ai_response[4:]
            
            nutrition_data = json.loads(ai_response)
            logging.info(f"AI nutrition data obtained for {name}")
            return nutrition_data
            
        except Exception as e:
            logging.error(f"AI nutrition error for {name}: {e}")
            # Return default values if AI fails
            return {
                "carbs_grams": None,
                "calories_per_serving": None,
                "protein_grams": None,
                "fat_grams": None,
                "fiber_grams": None,
                "glycemic_index": None,
                "is_diabetic_friendly": False,
                "is_vegetarian": True,
                "is_vegan": False,
                "is_halal": True,
                "is_kosher": False,
                "is_catholic": True,
                "common_allergens": None
            }
    
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
        """Insert with AI-generated nutritional data"""
        if not commodities:
            return 0
        
        inserted = 0
        updated = 0
        
        for item in commodities:
            try:
                price_range = f"₱{item['average_price']:.2f} per {item['unit']}"
                
                from urllib.parse import quote
                encoded_name = quote(item['name'])
                
                # Check if exists
                check_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}?name=eq.{encoded_name}&select=id"
                check_response = requests.get(check_url, headers=self.headers)
                
                # Get AI nutrition data
                print(f"🤖 Getting AI nutrition for: {item['name']}")
                nutrition = self.get_ai_nutrition(item['name'], item['category'])
                
                # Combine price + nutrition data
                data = {
                    'name': item['name'],
                    'category': item['category'],
                    'typical_serving_size': f"100 {item['unit']}",
                    'price_range': price_range,
                    'cost_per_serving': item['average_price'],
                    'availability': 'available',
                    'updated_at': datetime.now().isoformat(),
                    # AI-generated nutrition
                    **nutrition
                }
                
                if check_response.json():
                    # Update
                    record_id = check_response.json()[0]['id']
                    update_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}?id=eq.{record_id}"
                    requests.patch(update_url, headers=self.headers, json=data)
                    updated += 1
                    logging.info(f"Updated with AI: {item['name']}")
                else:
                    # Insert
                    data['created_at'] = datetime.now().isoformat()
                    insert_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}"
                    requests.post(insert_url, headers=self.headers, json=data)
                    inserted += 1
                    logging.info(f"Inserted with AI: {item['name']}")
                
            except Exception as e:
                logging.error(f"Error: {item['name']} - {e}")
                continue
        
        logging.info(f"Inserted: {inserted}, Updated: {updated}")
        return inserted + updated
    
    def run(self, pdf_url):
        """Main function"""
        logging.info("="*60)
        logging.info(f"Processing PDF: {pdf_url}")
        
        print("\n📥 Downloading PDF...")
        pdf_file = self.download_pdf(pdf_url)
        
        if not pdf_file:
            return False
        
        print("📊 Extracting commodity data...")
        commodities = self.extract_commodity_data(pdf_file)
        
        if not commodities:
            return False
        
        print(f"💾 Saving {len(commodities)} items with AI nutrition...")
        count = self.insert_to_supabase(commodities)
        
        print(f"✅ Done! Processed {count} items")
        return True


def main(pdf_url=None):
    scraper = CaragaPriceScraper()
    return scraper.run(pdf_url)


if __name__ == "__main__":
    import sys
    pdf_url = sys.argv[1] if len(sys.argv) > 1 else None
    main(pdf_url)