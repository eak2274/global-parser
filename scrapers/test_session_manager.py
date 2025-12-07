# scrapers/test_session_manager.py

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

from utils.session_manager import SessionManager

def main():
    logger.info("--- Testing SessionManager in action ---")
    
    # 1. SessionManager initialization (this will load and check proxies)
    logger.info("1. Creating and initializing SessionManager...")
    try:
        session_manager = SessionManager()
    except RuntimeError as e:
        logger.error(f"CRITICAL: {e}")
        return

    # 2. Test headers using the robust retry mechanism
    logger.info("2. Sending a request to https://httpbin.org/headers...")
    try:
        response_headers = session_manager.fetch_with_retry('GET', "https://httpbin.org/headers")
        logger.info("✅ Request successful!")
        logger.info("\n--- Headers seen by the server ---")
        logger.info(json.dumps(response_headers.json(), indent=2))
        logger.info("------------------------------------\n")
    except RuntimeError as e:
        logger.error(f"❌ Final error: {e}")

    # 3. Test the proxy (check the IP address) ALSO using the robust retry mechanism
    logger.info("3. Sending a request to https://httpbin.org/ip to see our IP...")
    try:
        response_ip = session_manager.fetch_with_retry('GET', "https://httpbin.org/ip")
        logger.info("✅ Request successful!")
        logger.info("\n--- Your current IP address (via proxy) ---")
        logger.info(json.dumps(response_ip.json(), indent=2))
        logger.info("-----------------------------------------\n")
    except RuntimeError as e:
        logger.error(f"❌ Error during IP request: {e}")

    # 4. Demonstrate delay
    logger.info("4. Demonstrating delay functionality...")
    session_manager.apply_delay(2, 4) # Delay from 2 to 4 seconds
    logger.info("✅ Delay applied.")

    logger.info("--- SessionManager testing complete ---")


if __name__ == "__main__":
    main()