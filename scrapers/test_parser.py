import sys
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

# Get the path to the project's root directory
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# from utils.fs_helpers import scrape_team_basket_results

# convert_to_nice_json(date(2025,12,6))

for d in range(-7, 0, 1):
    dt = datetime.today() + timedelta(days=d)
    print(dt.date())