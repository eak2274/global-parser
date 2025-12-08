# utils/session_manager.py

import logging
import random
import time
from typing import Callable

import requests

from config import settings
from .header_provider import HeaderProvider
from .proxy_provider import ProxyProvider

# Get a logger instance for this module
logger = logging.getLogger(__name__)

class SessionManager:
    """
    Creates and manages a requests.Session with rotating headers and a smart retry mechanism.
    """
    def __init__(self, max_retries: int = 3):
        self.header_provider = HeaderProvider()
        self.proxy_provider = ProxyProvider()
        self.max_retries = max_retries
        
        logger.info("SessionManager: Initializing proxy provider...")
        self.proxy_provider.initialize()
        
        if not self.proxy_provider.valid_proxies:
            logger.critical("SessionManager: CRITICAL ERROR! No valid proxies found.")
            logger.critical("SessionManager: Script execution stopped to prevent real IP address leak.")
            raise RuntimeError("No valid proxies found. Operation cannot continue.")
        
        logger.info(f"SessionManager: Initialization complete. {len(self.proxy_provider.valid_proxies)} proxies in the pool.")

    def get_session(self) -> requests.Session:
        """
        Creates a new session configured with a random header and a proxy.
        """
        session = requests.Session()
        headers = self.header_provider.get_random_header()
        session.headers.update(headers)
        
        proxy = self.proxy_provider.get_random_proxy()
        if proxy:
            session.proxies.update(proxy)
        
        return session

    def apply_delay(self, min_delay: float = 1.0, max_delay: float = 3.0) -> None:
        """Applies a random delay to mimic human behavior."""
        delay = random.uniform(min_delay, max_delay)
        logger.info(f"Applying a delay of {delay:.2f} seconds...")
        time.sleep(delay)

    def fetch_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Makes an HTTP request with a smart retry mechanism for proxy failures.
        
        Args:
            method: HTTP method ('GET', 'POST', etc.).
            url: The URL to fetch.
            **kwargs: Arguments to pass to requests.request (e.g., params, data).
        
        Returns:
            requests.Response object on success.
        
        Raises:
            RuntimeError: If all retries are exhausted.
        """
        last_exception = None
        
        for attempt in range(self.max_retries):
            session = self.get_session()
            current_proxy = session.proxies.get('http')

            try:
                response = session.request(method, url, timeout=30, **kwargs)
                response.raise_for_status()  # Raises an exception for 4xx/5xx status codes
                logger.info(f"✅ Request successful with proxy {current_proxy}.")
                return response

            except (requests.exceptions.ProxyError, requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout, requests.exceptions.SSLError) as e:
                last_exception = e
                logger.warning(f"❌ Proxy error {current_proxy}: {type(e).__name__}. Attempt {attempt + 1}/{self.max_retries}.")
                if current_proxy:
                    self.proxy_provider.mark_proxy_as_bad(session.proxies)
            
            except requests.exceptions.RequestException as e:
                # For other errors (e.g., 404 Not Found), don't change the proxy
                logger.warning(f"⚠️️ Request error (not proxy-related): {e}.")
                raise e

        # If we reach here, all attempts have failed
        raise RuntimeError(f"Failed to execute request to {url} after {self.max_retries} attempts. Last error: {last_exception}")