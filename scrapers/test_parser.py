import sys
import json
import logging
from datetime import date
from pathlib import Path

# Get the path to the project's root directory
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from utils.fs_helpers import convert_to_nice_json

convert_to_nice_json(date(2025,12,6))