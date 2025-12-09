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
from utils.helpers import get_offset_by_date, to_int_or_none

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
        raise FileNotFoundError(f"Scraped data file not found: {scraped_data_file_path}")
    
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

def convert_to_nice_json(date: date)->None:
    date_prefix = get_date_prefix(date)
    raw_json_file_path = project_root / f"data/{date_prefix}_raw_data.json"
    nice_json_file_path = project_root / f"data/{date_prefix}_nice_data.json"


    if not os.path.exists(raw_json_file_path):
        raise FileNotFoundError(f"Raw json data file not found: {raw_json_file_path}")
    
    logger.info(f"--- Starting to convert raw json data for {date_prefix} to nice json... ---")

    nice_list = []

    # Открываем файл и загружаем JSON-данные
    with open(raw_json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        for raw in data:
            nice = {}
            nice['area_src_id'] = raw.get('ZB')
            nice['area_name'] = raw.get('ZY')
            nice['tourney_src_id'] = raw.get('ZEE')
            nice['tourney_name'] = raw.get('ZA')
            nice['tourney_url'] = raw.get('ZL')
            nice['tourney_logo'] = raw.get('OAJ')
            nice['tourney_status'] = raw.get('ZCC')
            nice['results'] = []
            for raw_res in raw['results']:
                nice_res = {}
                nice_res['game_src_id'] = raw_res.get('AA')
                nice_res['game_ts'] = to_int_or_none(raw_res.get('AD'))
                if raw_res.get('AC') == '10':
                    nice_res['game_end'] = 'overtime'
                else:
                    nice_res['game_end'] = 'standard'
                nice_res['home_src_id'] = raw_res.get('PX')
                nice_res['away_src_id'] = raw_res.get('PY')
                nice_res['home_name'] = raw_res.get('AE')
                nice_res['away_name'] = raw_res.get('AF')
                nice_res['home_slug'] = raw_res.get('WU')
                nice_res['away_slug'] = raw_res.get('WV')
                nice_res['home_abbr'] = raw_res.get('WM')
                nice_res['away_abbr'] = raw_res.get('WN')
                nice_res['home_score'] = to_int_or_none(raw_res.get('AG'))
                nice_res['away_score'] = to_int_or_none(raw_res.get('AH'))
                nice_res['home_q1'] = to_int_or_none(raw_res.get('BA'))
                nice_res['home_q2'] = to_int_or_none(raw_res.get('BC'))
                nice_res['home_q3'] = to_int_or_none(raw_res.get('BE'))
                nice_res['home_q4'] = to_int_or_none(raw_res.get('BG'))
                nice_res['home_ot1'] = to_int_or_none(raw_res.get('BI'))
                nice_res['home_ot2'] = to_int_or_none(raw_res.get('BK'))
                nice_res['away_q1'] = to_int_or_none(raw_res.get('BB'))
                nice_res['away_q2'] = to_int_or_none(raw_res.get('BD'))
                nice_res['away_q3'] = to_int_or_none(raw_res.get('BF'))
                nice_res['away_q4'] = to_int_or_none(raw_res.get('BH'))
                nice_res['away_ot1'] = to_int_or_none(raw_res.get('BJ'))
                nice_res['away_ot2'] = to_int_or_none(raw_res.get('BL'))
                nice_res['home_logo'] = raw_res.get('OA')
                nice_res['away_logo'] = raw_res.get('OB')
                nice['results'].append(nice_res)
            nice_list.append(nice)

    with open(nice_json_file_path, "w", encoding="utf-8") as f:
        json.dump(nice_list, f, ensure_ascii=False, indent=4)

    logger.info(f"--- Raw json data for {date_prefix} successfully converted to nice json... ---")
    logger.info(f"--- Nice json data saved to {nice_json_file_path}. ---")