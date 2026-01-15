import sys
import json
import logging
from pprint import pprint
from datetime import date, datetime, timedelta
from pathlib import Path

# Get the path to the project's root directory
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from database.queries import get_tournament_teams

ls = get_tournament_teams(60)
pprint(ls)