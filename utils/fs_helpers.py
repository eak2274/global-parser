import sys
import os
import json
import logging
from datetime import date
from pathlib import Path

# Get the path to the project's root directory
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from utils.session_manager import SessionManager
from utils.helpers import get_offset_by_date

# --- SETUP LOGGING ---
from utils.logger import setup_logging
setup_logging(Path(__file__))

# Get a logger instance for this module
logger = logging.getLogger(__name__)

def get_url(date: date):
    offset = get_offset_by_date(date)
    return f"https://2.flashscore.ninja/2/x/feed/f_3_{offset}_2_en_1"

from datetime import date, datetime

def get_date_prefix(d: date) -> str:
    """
    Преобразует дату в строку формата YYYY-MM-DD.
    """
    return d.strftime("%Y-%m-%d")


def scrape_daily_basket_results(
        session_manager: SessionManager,
        date: date
):
    X_FSIGN_HEADER = {"X-Fsign": "SW9D1eZo"}
    url = get_url(date)
    date_prefix = get_date_prefix(date)
    curr_response_file_path = project_root / f"data/{date_prefix}_scraped_data.txt"

    try:
        logger.info(f"--- Starting to extract basketball data for {date_prefix}... ---")
        
        response = session_manager.fetch_with_retry(
            'GET', 
            url, 
            headers=dict(X_FSIGN_HEADER)
        )
        # print(response.text[:1000])  # Первые 1000 символов

        lst = response.text.split('¬')

        with open(curr_response_file_path, 'w', encoding='utf-8') as f:
            for el in lst:
                f.write(el + '\n')

    except RuntimeError as e:
        # Эта ошибка возникнет, только если все прокси закончились
        print(f"Critical error: {e}. Execution aborted.")

    logger.info(f"--- Basketball data for {date_prefix} successfully extracted. ---")
    logger.info(f"--- All the data for {date_prefix} saved to ${curr_response_file_path}. ---")

def convert_to_raw_json(date: date)->None:
    date_prefix = get_date_prefix(date)
    scraped_data_file_path = project_root / f"data/{date_prefix}_scraped_data.txt"
    raw_json_file_path = project_root / f"data/{date_prefix}_raw_data.json"


    if not os.path.exists(scraped_data_file_path):
        raise FileNotFoundError(f"File not found: {scraped_data_file_path}")
    
    tournaments = []
    current_tournament = {}
    current_result = {}

    logger.info(f"--- Starting to convert scraped basketball data for {date_prefix} to raw json... ---")

    with open(scraped_data_file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if '÷' not in line:
                continue
            # --- Ключ нового турнира (например, ~ZA÷AFRICA) ---
            if line.startswith('~ZA'):
                # Сохраняем предыдущий турнир, если он был
                if current_tournament:
                    if current_result:
                        current_tournament.setdefault("results", []).append(current_result)
                        current_result = {}
                    tournaments.append(current_tournament)
                    current_tournament = {}
            key = line.split('÷')[0].lstrip('~')
            if key in (
                'ZA', 'ZEE', 'ZB', 'ZY', 'ZC', 'ZD', 'ZE', 'ZF',
                'ZO', 'ZG', 'ZH', 'ZJ', 'ZL', 'OAJ', 'ZX', 'ZCC',
                'TSS', 'ZAF'
            ):
                value = line.split('÷')[1]
                current_tournament[key] = value
            # --- Ключ нового результата (например, ~ZA÷AFRICA) ---
            if line.startswith('~AA'):
                # Сохраняем предыдущий результат, если он был
                if current_result:
                    current_tournament.setdefault("results", []).append(current_result)
                    current_result = {}
            key = line.split('÷')[0].lstrip('~')
            if key not in (
                'ZA', 'ZEE', 'ZB', 'ZY', 'ZC', 'ZD', 'ZE', 'ZF',
                'ZO', 'ZG', 'ZH', 'ZJ', 'ZL', 'OAJ', 'ZX', 'ZCC',
                'TSS', 'ZAF', 'SA'
            ):
                value = line.split('÷')[1]
                current_result[key] = value

    # Сохраняем предыдущий турнир, если он был
    if current_tournament:
        if current_result:
            current_tournament.setdefault("results", []).append(current_result)
        tournaments.append(current_tournament)

    with open(raw_json_file_path, "w", encoding="utf-8") as f:
        json.dump(tournaments, f, ensure_ascii=False, indent=4)

    logger.info(f"--- Scraped basketball data for {date_prefix} successfully converted to raw json... ---")
    logger.info(f"--- Raw json data saved to {raw_json_file_path}. ---")