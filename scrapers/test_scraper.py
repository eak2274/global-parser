# scrapers/test_scraper.py

import sys
import logging
from pathlib import Path

# Get the path to the project's root directory (two levels up from this file)
project_root = Path(__file__).resolve().parent.parent
# Add this path to the list of paths where Python looks for modules
sys.path.append(str(project_root))

# --- SETUP LOGGING ---
from utils.logger import setup_logging
setup_logging(Path(__file__))

# --- CORRECTED IMPORT ---
from utils.proxy_provider import ProxyProvider

# --- TEST CODE ---
logger = logging.getLogger(__name__)
logger.info("Creating and initializing ProxyProvider...")
proxy_provider_instance = ProxyProvider()
proxy_provider_instance.initialize()

# --- DISPLAY RESULT ---
logger.info(f"Result: Found {len(proxy_provider_instance.valid_proxies)} valid proxies.")
if proxy_provider_instance.valid_proxies:
    logger.info("Example of a valid proxy: %s", proxy_provider_instance.get_random_proxy())