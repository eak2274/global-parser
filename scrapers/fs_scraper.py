# scrapers\fs_scraper.py

import sys
import logging
from datetime import date, timedelta
from pathlib import Path
import shutil

# Get the path to the project's root directory
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

BY_DATE_RAW_FOLDER = project_root / "data/by_date/raw"
BY_DATE_TO_UPLOAD_FOLDER = project_root / "data/by_date/to_upload"
BY_DATE_UPLOADED_FOLDER = project_root / "data/by_date/uploaded"

BY_TEAM_RAW_FOLDER = project_root / "data/by_team/raw"
BY_TEAM_TO_UPLOAD_FOLDER = project_root / "data/by_team/to_upload"
BY_TEAM_UPLOADED_FOLDER = project_root / "data/by_team/uploaded"

TOURNAMENT_ID=60

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
from database.connection import transaction, check_connection, get_pool
from database.loader import load_to_db, load_to_db_insert_only
from database.queries import get_tournament_teams


def load_scraped_data_to_db(input_folder: Path, output_folder: Path, insert_only: bool = False):
    """
    Iterates through JSON files in an input folder, loads them to the database,
    and moves successfully processed files to an output folder.

    Args:
        input_folder: Path to the directory containing JSON files to process.
        output_folder: Path to the directory where processed files will be moved.
        insert_only: If True, only INSERT new records (skip existing).
                     If False, UPDATE existing records.
    """
    # 1. Check that the input directory exists and is a directory
    if not input_folder.is_dir():
        logger.error(f"Input directory not found or is not a directory: {input_folder}")
        return

    # 2. Ensure the output directory exists. If not, create it.
    # parents=True will create all parent directories if needed.
    # exist_ok=True will not raise an error if the directory already exists.
    output_folder.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting batch processing. Input: {input_folder}, Output: {output_folder}")

    # Initialize overall statistics for the entire batch of files
    total_stats = {
        'files_processed': 0,
        'files_failed': 0,
        'total_inserted': 0,
        'total_updated': 0
    }

    # 3. Iterate through all .json files in the input directory
    # Path.glob() is convenient for finding files by a pattern
    json_files = list(input_folder.glob("*.json"))
    if not json_files:
        logger.info(f"No .json files found in {input_folder}. Nothing to do.")
        return total_stats

    for file_path in json_files:
        try:
            logger.info(f"Processing file: {file_path.name}")
            
            # 4. For EACH file, open its own transaction.
            # This is critically important: if one file fails, the transactions for
            # other files will not be affected.
            with transaction() as cur:
                if insert_only:
                    stats = load_to_db_insert_only(cur, str(file_path))
                else:
                    stats = load_to_db(cur, str(file_path))

                # Update the overall statistics
                if stats:
                    total_stats['total_inserted'] += stats.get('inserted', 0)
                    total_stats['total_updated'] += stats.get('updated', 0)

            # 5. If the transaction was successful (without exceptions), move the file.
            # shutil.move is more reliable than Path.rename, especially if folders are on different drives.
            destination_path = output_folder / file_path.name
            shutil.move(str(file_path), str(destination_path))
            
            total_stats['files_processed'] += 1
            logger.info(f"Successfully loaded and moved file: {file_path.name} -> {destination_path}")

        except Exception as e:
            # 6. If an error occurs while processing the file, log it,
            # but DO NOT interrupt the entire loop. The file remains in input_folder.
            total_stats['files_failed'] += 1
            logger.error(f"Failed to process file {file_path.name}: {e}", exc_info=True)
            # Continue processing the next file

    # 7. At the end, log the final statistics for the entire batch
    logger.info(
        f"Batch processing finished. "
        f"Processed: {total_stats['files_processed']}, "
        f"Failed: {total_stats['files_failed']}. "
        f"Total inserted: {total_stats['total_inserted']}, "
        f"Total updated: {total_stats['total_updated']}."
    )
            
    return total_stats


def scrape_data_for_date_range():
    logger.info("--- Starting FlashScore scraper for date range ---")
    
    # Check database connection first
    if not check_connection():
        logger.error("Could not connect to the database. Aborting scraping.")
        return
    
    # SessionManager initialization (this will load and check proxies)
    logger.info("Creating and initializing SessionManager...")
    try:
        session_manager = SessionManager()
    except RuntimeError as e:
        logger.error(f"CRITICAL: {e}")
        return

    for d in range(-7, 0, 1):
        
        dt = date.today() + timedelta(days=d)
        prefix = get_date_as_str(dt)
        url = get_url_by_date(dt)

        # Scrape data
        scrape_basket_results(
            session_manager=session_manager,
            url=url,
            prefix=prefix,
            out_folder=BY_DATE_RAW_FOLDER
        )
        
        # Convert to raw JSON
        convert_to_raw_json(
            prefix=prefix,
            folder_in=BY_DATE_RAW_FOLDER,
            folder_out=BY_DATE_RAW_FOLDER
        )

        # Convert to nice JSON
        convert_to_nice_json(
            prefix=prefix,
            folder_in=BY_DATE_RAW_FOLDER,
            folder_out=BY_DATE_TO_UPLOAD_FOLDER
        )
        
    # Load to database
    load_scraped_data_to_db(
        input_folder=BY_DATE_TO_UPLOAD_FOLDER, 
        output_folder=BY_DATE_UPLOADED_FOLDER, 
        insert_only=True
    )


def scrape_data_for_teams():
    logger.info("--- Starting FlashScore scraper for team data ---")
    
    # Check database connection first
    if not check_connection():
        logger.error("Could not connect to the database. Aborting scraping.")
        return
    
    logger.info("--- Getting team list for a given tournament ---")
    teams = get_tournament_teams(TOURNAMENT_ID)
    logger.info("--- Team list for a given tournament extracted ---")

    # SessionManager initialization (this will load and check proxies)
    logger.info("Creating and initializing SessionManager...")
    try:
        session_manager = SessionManager()
    except RuntimeError as e:
        logger.error(f"CRITICAL: {e}")
        return

    for team in teams:
        for step in range(0, 3):
            
            area_src_id = team['area_src_id']
            team_src_id = team['team_src_id']
            # team_src_id = 'tUT82gR9'

            url = get_url_by_team(
                area_src_id=area_src_id,
                team_src_id=team_src_id,
                step=step
            )
            prefix = f"{area_src_id}_{team_src_id}_{step}"

            # Scrape data
            scrape_basket_results(
                session_manager=session_manager,
                url=url,
                prefix=prefix,
                out_folder=BY_TEAM_RAW_FOLDER
            )
            
            # Convert to raw JSON
            convert_to_raw_json(
                prefix=prefix,
                folder_in=BY_TEAM_RAW_FOLDER,
                folder_out=BY_TEAM_RAW_FOLDER
            )

            # Convert to nice JSON
            convert_to_nice_json(
                prefix=prefix,
                folder_in=BY_TEAM_RAW_FOLDER,
                folder_out=BY_TEAM_TO_UPLOAD_FOLDER
            )
        
    # Load to database
    load_scraped_data_to_db(
        input_folder=BY_TEAM_TO_UPLOAD_FOLDER, 
        output_folder=BY_TEAM_UPLOADED_FOLDER, 
        insert_only=False
    )


def main():
    """
    Main function to orchestrate the scraping and loading process.
    Manages the database connection pool lifecycle.
    """
    try:
        # Use get_pool() as a context manager.
        # Upon entering the 'with' block, get_pool() will be called, which will create (or return) the pool.
        # Upon exiting the block (even due to an error), the pool's close() method will be called automatically.
        with get_pool():
            logger.info("Database connection pool opened. Starting script execution.")
            
            # All main logic using the DB is executed here
            # scrape_data_for_date_range()
            scrape_data_for_teams()
            
            logger.info("All scraping and loading tasks finished.")
        
        # This line will be executed after the pool closes correctly
        logger.info("Database connection pool closed successfully. Script finished.")

    except Exception as e:
        # This block will catch critical errors that were not handled
        # within the called functions (e.g., if the pool itself failed to open).
        logger.critical(f"A critical error occurred at the script level: {e}", exc_info=True)


if __name__ == "__main__":
    main()