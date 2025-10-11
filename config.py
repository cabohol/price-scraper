import os
from dotenv import load_dotenv

load_dotenv()

# Supabase Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')

# Markets to scrape
MARKETS = {
    'Libertad Public Market': 'https://caraga.da.gov.ph/libertad-price-update/',
    'Mayor Salvador Calo Public Market (Butuan City)': 'https://caraga.da.gov.ph/weekly-price-update/'
}

# Scheduler Settings
SCHEDULE_TIME = "08:00"
SCHEDULE_ENABLED = True

# Logging
LOG_FILE = "logs/scraper.log"
LOG_LEVEL = "INFO"

# Database table
TABLE_NAME = "ingredients"

# Validation
if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in .env file")