# scrapers/fs_parser.py

import sys
import json
import logging
from pathlib import Path

# Get the path to the project's root directory
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# --- SETUP LOGGING ---
from utils.logger import setup_logging
setup_logging(Path(__file__))

# Get a logger instance for this module
logger = logging.getLogger(__name__)

CURR_RESPONSE_FILE_PATH = project_root / "data/current_response.txt"
CURR_PARSED_RESPONSE_FILE_PATH = project_root / "data/parsed_response.json"

# data_list = [1, 2, 3, {"a": 10, "b": 20}]
# data_dict = {"id": 1, "name": "Alice", "active": True}

# data =  dict(data_dict)
# data["results"] = [
#     {"key": "1", "value": 1},
#     {"key": "2", "value": 3}
# ]

tournaments = []
current_tournament = {}
current_result = {}

with open(CURR_RESPONSE_FILE_PATH, "r", encoding="utf-8") as f:
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

with open(CURR_PARSED_RESPONSE_FILE_PATH, "w", encoding="utf-8") as f:
    json.dump(tournaments, f, ensure_ascii=False, indent=4)

