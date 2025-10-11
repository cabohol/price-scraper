"""
Quick runner script for testing the scraper
"""
from scraper import main

# Test with default PDF
if __name__ == "__main__":
    print("🚀 Running price scraper with default PDF...")
    print("="*60)
    
    # Run scraper
    success = main()
    
    if success:
        print("\n✅ Test run completed successfully!")
    else:
        print("\n❌ Test run failed. Check logs/scraper.log for details")