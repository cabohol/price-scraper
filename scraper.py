import pdfplumber
import requests
from io import BytesIO
from datetime import datetime
import logging
import os
import json
import time
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
        
        # Groq API settings
        self.groq_api_key = GROQ_API_KEY
        self.groq_url = "https://api.groq.com/openai/v1/chat/completions"
        
        logging.info("Scraper initialized with AI enrichment")
        
    def get_ai_nutrition(self, name, category, retry_count=0):
        """Get nutritional data from Llama 3.3 70B with retry logic"""
        max_retries = 3
        
        try:
            prompt = f"""
Provide accurate nutritional information for this ingredient:
Name: {name}
Category: {category}

Return ONLY a valid JSON object (no markdown, no explanation) with these exact fields:
{{
    "carbs_grams": <float - carbohydrates per 100g, MUST be a number not null>,
    "calories_per_serving": <int - calories per 100g serving, MUST be a number not null>,
    "protein_grams": <float - protein per 100g, MUST be a number not null>,
    "fat_grams": <float - total fat per 100g, MUST be a number not null>,
    "fiber_grams": <float - dietary fiber per 100g, MUST be a number not null>,
    "glycemic_index": <int - 0-100, estimate if unknown, MUST be a number not null>,
    "is_diabetic_friendly": <bool - low GI and suitable for diabetics>,
    "is_vegetarian": "<bool - true if contains no meat, poultry, or fish; may include dairy or eggs>",
    "is_vegan": "<bool - true if contains no animal-derived ingredients, including dairy, eggs, honey, or gelatin>",
    "is_halal": "<bool - true if all ingredients are halal-certified or contain no pork/alcohol; meat must be slaughtered per Islamic law>",
    "is_kosher": "<bool - true if ingredients and preparation comply with Jewish dietary laws (no pork, shellfish, or mixing meat and dairy)>",
    "is_catholic": "<bool - true if compliant with Catholic fasting rules: no meat on Fridays or during Lent>"
    "common_allergens": <string - comma separated allergens like "gluten, dairy, nuts, eggs" or "none" if no allergens, NEVER null>
}}

IMPORTANT: 
- ALL numeric fields MUST have actual numbers, never null
- common_allergens MUST be a string, use "none" if no allergens, NEVER null
- Use standard USDA nutrition database values
- Be accurate and scientific
"""
            
            headers = {
                "Authorization": f"Bearer {self.groq_api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": "You are a nutrition expert. Return only valid JSON with NO null values."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 10000
            }
            
            response = requests.post(self.groq_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            ai_response = result['choices'][0]['message']['content'].strip()
            
            # Remove markdown code blocks
            if ai_response.startswith('```'):
                ai_response = ai_response.split('```')[1]
                if ai_response.startswith('json'):
                    ai_response = ai_response[4:]
                ai_response = ai_response.rsplit('```', 1)[0]
            
            nutrition_data = json.loads(ai_response)
            
            # Ensure no null values
            if nutrition_data.get('common_allergens') is None:
                nutrition_data['common_allergens'] = "none"
            
            # Ensure all required fields exist with non-null values
            required_fields = {
                "carbs_grams": 0.0,
                "calories_per_serving": 0,
                "protein_grams": 0.0,
                "fat_grams": 0.0,
                "fiber_grams": 0.0,
                "glycemic_index": 50,
                "common_allergens": "none"
            }
            
            for field, default in required_fields.items():
                if nutrition_data.get(field) is None:
                    nutrition_data[field] = default
            
            logging.info(f"AI nutrition obtained for {name}")
            return nutrition_data
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and retry_count < max_retries:
                # Rate limit - wait and retry
                wait_time = 60 * (retry_count + 1)
                logging.warning(f"Rate limited, waiting {wait_time}s... (retry {retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self.get_ai_nutrition(name, category, retry_count + 1)
            else:
                logging.error(f"AI error for {name}: {e}")
                return self.get_default_nutrition()
                
        except Exception as e:
            logging.error(f"AI nutrition error for {name}: {e}")
            return self.get_default_nutrition()
    
    def get_default_nutrition(self):
        """Return default nutrition values (NO nulls)"""
        return {
            "carbs_grams": 0.0,
            "calories_per_serving": 0,
            "protein_grams": 0.0,
            "fat_grams": 0.0,
            "fiber_grams": 0.0,
            "glycemic_index": 50,
            "is_diabetic_friendly": True,
            "is_vegetarian": True,
            "is_vegan": True,
            "is_halal": True,
            "is_kosher": True,
            "is_catholic": True,
            "common_allergens": "none"
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
    
    def needs_nutrition_update(self, item_name):
        """Check if ingredient needs nutrition data"""
        from urllib.parse import quote
        encoded_name = quote(item_name)
        
        # Check if has nutrition data
        check_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}?name=eq.{encoded_name}&select=id,carbs_grams,calories_per_serving"
        response = requests.get(check_url, headers=self.headers)
        
        if response.json():
            data = response.json()[0]
            # Needs update if carbs or calories are null
            return data.get('carbs_grams') is None or data.get('calories_per_serving') is None
        
        return True  # New item, needs nutrition
    
    def insert_to_supabase(self, commodities):
        """Insert/update with AI nutrition (only for items without nutrition data)"""
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
                check_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}?name=eq.{encoded_name}&select=id,carbs_grams"
                check_response = requests.get(check_url, headers=self.headers)
                
                if check_response.json():
                    # EXISTING ingredient
                    record_id = check_response.json()[0]['id']
                    has_nutrition = check_response.json()[0].get('carbs_grams') is not None
                    
                    if has_nutrition:
                        # ✅ HAS NUTRITION - Update price only
                        data = {
                            'price_range': price_range,
                            'cost_per_serving': item['average_price'],
                            'availability': 'available',
                            'updated_at': datetime.now().isoformat()
                        }
                        logging.info(f"Updating price only: {item['name']}")
                    else:
                        # ⚠️ MISSING NUTRITION - Get AI data
                        print(f"🤖 Getting missing nutrition for: {item['name']}")
                        nutrition = self.get_ai_nutrition(item['name'], item['category'])
                        time.sleep(2)  # Rate limit delay
                        
                        data = {
                            'price_range': price_range,
                            'cost_per_serving': item['average_price'],
                            'availability': 'available',
                            'updated_at': datetime.now().isoformat(),
                            **nutrition
                        }
                        logging.info(f"Updating with AI nutrition: {item['name']}")
                    
                    update_url = f"{self.supabase_url}/rest/v1/{TABLE_NAME}?id=eq.{record_id}"
                    requests.patch(update_url, headers=self.headers, json=data)
                    updated += 1
                    
                else:
                    # ➕ NEW ingredient - Get AI nutrition
                    print(f"🤖 Getting AI nutrition for NEW: {item['name']}")
                    nutrition = self.get_ai_nutrition(item['name'], item['category'])
                    time.sleep(2)  # Rate limit delay
                    
                    data = {
                        'name': item['name'],
                        'category': item['category'],
                        'typical_serving_size': f"100 {item['unit']}",
                        'price_range': price_range,
                        'cost_per_serving': item['average_price'],
                        'availability': 'available',
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat(),
                        **nutrition
                    }
                    
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
        
        print(f"💾 Processing {len(commodities)} items...")
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