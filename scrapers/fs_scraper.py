# scrapers/test_session_manager.py

import sys
import logging
from datetime import date, timedelta
from pathlib import Path

# Get the path to the project's root directory
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

BY_DATE_RAW_FOLDER = project_root / "data/by_date/raw"
BY_DATE_TO_UPLOAD_FOLDER = project_root / "data/by_date/to_upload"
BY_DATE_UPLOADED_FOLDER = project_root / "data/by_date/uploaded"

BY_TEAM_RAW_FOLDER = project_root / "data/by_team/raw"
BY_TEAM_TO_UPLOAD_FOLDER = project_root / "data/by_team/to_upload"
BY_TEAM_UPLOADED_FOLDER = project_root / "data/by_team/uploaded"

from utils.fs_helpers import (
    get_date_as_str,
    get_url_by_date,
    get_url_by_team,
    scrape_basket_results,
    convert_to_raw_json,
    convert_to_nice_json
)

# --- SETUP LOGGING ---
from utils.logger import setup_logging
setup_logging(Path(__file__))

# Get a logger instance for this module
logger = logging.getLogger(__name__)

from utils.session_manager import SessionManager

def scrape_data_for_date_range():
    logger.info("--- Testing FlashScore scraper - BEGIN ---")
    
    # 1. SessionManager initialization (this will load and check proxies)
    logger.info("Creating and initializing SessionManager...")
    try:
        session_manager = SessionManager()
    except RuntimeError as e:
        logger.error(f"CRITICAL: {e}")
        return

    for d in range(-3, 0, 1):
        
        dt = date.today() + timedelta(days=d)
        prefix = get_date_as_str(dt)
        url = get_url_by_date(dt)

        scrape_basket_results(
            session_manager=session_manager,
            url=url,
            prefix=prefix,
            out_folder=BY_DATE_RAW_FOLDER
        )
        
        convert_to_raw_json(
            prefix=prefix,
            folder_in=BY_DATE_RAW_FOLDER,
            folder_out=BY_DATE_RAW_FOLDER
        )

        convert_to_nice_json(
            prefix=prefix,
            folder_in=BY_DATE_RAW_FOLDER,
            folder_out=BY_DATE_TO_UPLOAD_FOLDER
        )

def scrape_data_for_team():
    logger.info("--- Testing FlashScore scraper - BEGIN ---")
    
    # 1. SessionManager initialization (this will load and check proxies)
    logger.info("Creating and initializing SessionManager...")
    try:
        session_manager = SessionManager()
    except RuntimeError as e:
        logger.error(f"CRITICAL: {e}")
        return

    for step in range(0, 3):
        
        area_src_id = '98'
        team_src_id = 'tUT82gR9'

        url = get_url_by_team(
            area_src_id=area_src_id,
            team_src_id=team_src_id,
            step=step
        )
        prefix = f"{area_src_id}_{team_src_id}_{step}"

        scrape_basket_results(
            session_manager=session_manager,
            url=url,
            prefix=prefix,
            out_folder=BY_TEAM_RAW_FOLDER
        )
        
        convert_to_raw_json(
            prefix=prefix,
            folder_in=BY_TEAM_RAW_FOLDER,
            folder_out=BY_TEAM_RAW_FOLDER
        )

        convert_to_nice_json(
            prefix=prefix,
            folder_in=BY_TEAM_RAW_FOLDER,
            folder_out=BY_TEAM_TO_UPLOAD_FOLDER
        )

def main():
    # scrape_data_for_date_range()
    scrape_data_for_team()

if __name__ == "__main__":
    main()