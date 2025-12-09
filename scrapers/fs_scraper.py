# scrapers/test_session_manager.py

import sys
import json
import logging
from datetime import date
from pathlib import Path

# Get the path to the project's root directory
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from utils.fs_helpers import convert_to_raw_json, scrape_daily_basket_results, convert_to_nice_json

# --- SETUP LOGGING ---
from utils.logger import setup_logging
setup_logging(Path(__file__))

# Get a logger instance for this module
logger = logging.getLogger(__name__)

from utils.session_manager import SessionManager

CURR_RESPONSE_FILE_PATH = project_root / "data/current_response.txt"
URL = "https://2.flashscore.ninja/2/x/feed/f_3_-1_2_en_1"
headers = {"X-Fsign": "SW9D1eZo"}
DELAY_MIN = 2
DELAY_MAX = 4

def main():
    
    logger.info("--- Testing FlashScore scraper - BEGIN ---")
    
    # 1. SessionManager initialization (this will load and check proxies)
    logger.info("Creating and initializing SessionManager...")
    try:
        session_manager = SessionManager()
    except RuntimeError as e:
        logger.error(f"CRITICAL: {e}")
        return

    date_to_scrape = date(2025, 12, 6)
    scrape_daily_basket_results(
        session_manager=session_manager,
        date=date_to_scrape
    )
    convert_to_raw_json(date_to_scrape)
    convert_to_nice_json(date_to_scrape)

if __name__ == "__main__":
    main()