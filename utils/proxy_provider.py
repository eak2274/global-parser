# utils/proxy_provider.py
# Take proxy list from here and put to proxy/proxies.txt
# https://free-proxy-list.net/ru/ssl-proxy.html

import logging
import queue
import random
import threading
from pathlib import Path

import requests

from config import settings
from .header_provider import HeaderProvider

# Get a logger instance for this module
logger = logging.getLogger(__name__)

class ProxyProvider:
    """
    Manages a pool of valid proxies by accumulating and re-validating them.
    Fetches, validates, and provides a random working proxy.
    """
    def __init__(self):
        # Access to proxy settings is now done through the nested settings.proxy object
        self.config = settings.proxy
        self.valid_proxies: set[str] = set()
        self._proxy_queue = queue.Queue()
        self._validation_threads = 10
        
        # Initialize the header provider
        self.header_provider = HeaderProvider()

    def initialize(self) -> None:
        """
        Orchestrates the full proxy management workflow.
        """
        logger.info("ProxyProvider: Starting smart proxy manager...")
        
        # 1. Load and re-validate previously accumulated valid proxies
        self._revalidate_existing_proxies()
        
        # 2. Load and validate the new proxy list
        self._validate_new_proxies()
        
        # 3. Save the updated list of valid proxies
        self._save_valid_proxies()

        logger.info(f"ProxyProvider: Work complete. {len(self.valid_proxies)} valid proxies in the pool.")

    def _load_proxies_from_file(self, file_path: Path) -> list[str]:
        """Generic method to load proxies from a given file."""
        if not file_path.exists():
            logger.info(f"ProxyProvider: File {file_path.name} not found. Skipping.")
            return []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                proxies = [line.strip() for line in f.readlines() if line.strip()]
            logger.info(f"ProxyProvider: Loaded {len(proxies)} proxies from {file_path.name}.")
            return proxies
        except Exception as e:
            logger.error(f"ProxyProvider: Error reading {file_path.name}: {e}")
            return []

    def _revalidate_existing_proxies(self) -> None:
        """Step 1: Re-validate proxies from valid_proxies.txt."""
        existing_proxies = self._load_proxies_from_file(self.config.valid_proxies_file_path)
        if not existing_proxies:
            logger.info("ProxyProvider: No accumulated proxies to re-validate.")
            return

        logger.info("ProxyProvider: Re-validating accumulated proxies...")
        # --- KEY FIX ---
        # Assign the result of re-validation back to our main set,
        # to avoid losing working proxies from previous runs.
        self.valid_proxies = self._run_validation(existing_proxies)
        logger.info(f"ProxyProvider: Re-validation complete. {len(self.valid_proxies)} working proxies remain.")

    def _validate_new_proxies(self) -> None:
        """Step 2: Validate new proxies from proxies.txt."""
        new_proxies = self._load_proxies_from_file(self.config.proxies_file_path)
        if not new_proxies:
            logger.info("ProxyProvider: No new proxies to check.")
            return

        # Filter: don't check what's already in the valid pool
        proxies_to_check = list(set(new_proxies) - self.valid_proxies)
        if not proxies_to_check:
            logger.info("ProxyProvider: All new proxies are already in the valid pool.")
            return

        logger.info(f"ProxyProvider: Checking {len(proxies_to_check)} new proxies...")
        new_valid_proxies = self._run_validation(proxies_to_check)
        self.valid_proxies.update(new_valid_proxies)
        logger.info(f"ProxyProvider: Found {len(new_valid_proxies)} new working proxies.")

    def _run_validation(self, proxy_list: list[str]) -> set[str]:
        """Core validation logic that runs in threads."""
        found_valid_proxies: set[str] = set()
        
        for proxy in proxy_list:
            self._proxy_queue.put(proxy)
        
        threads = []
        for _ in range(self._validation_threads):
            # Create a thread that will update our shared set
            thread = threading.Thread(target=self._validate_proxy_worker, args=(found_valid_proxies,), daemon=True)
            thread.start()
            threads.append(thread)
        
        self._proxy_queue.join()
        return found_valid_proxies

    def _validate_proxy_worker(self, valid_set: set[str]) -> None:
        """Worker function for threads that adds found proxies to the provided set."""
        while not self._proxy_queue.empty():
            proxy = self._proxy_queue.get()
            try:
                proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
                headers = self.header_provider.get_random_header()
                
                response = requests.get(
                    self.config.validity_check_url, 
                    proxies=proxies, 
                    headers=headers, 
                    timeout=5
                )
                if response.status_code == 200:
                    valid_set.add(proxy)
            except Exception:
                # Proxy failed the check, ignore
                pass
            finally:
                self._proxy_queue.task_done()

    def _save_valid_proxies(self) -> None:
        """Step 3: Save accumulated valid proxies to file."""
        if not self.valid_proxies:
            logger.info("ProxyProvider: No valid proxies to save.")
            return

        try:
            with open(self.config.valid_proxies_file_path, 'w', encoding='utf-8') as f:
                # Save a sorted list for file cleanliness
                for proxy in sorted(list(self.valid_proxies)):
                    f.write(proxy + '\n')
            logger.info(f"ProxyProvider: Saved {len(self.valid_proxies)} proxies to {self.config.valid_proxies_file_path.name}.")
        except Exception as e:
            logger.error(f"ProxyProvider: Error saving valid proxies: {e}")
    

    def get_random_proxy(self) -> dict[str, str] | None:
        """Returns a random valid proxy."""
        if not self.valid_proxies:
            return None
        
        proxy = random.choice(list(self.valid_proxies))
        return {"http": f"http://{proxy}", "https": f"http://{proxy}"}

    def mark_proxy_as_bad(self, proxy_dict: dict[str, str]) -> None:
        """Removes a bad proxy from the valid pool."""
        if not proxy_dict or 'http' not in proxy_dict:
            return
            
        # Extract IP:PORT from a string like "http://ip:port"
        proxy_address = proxy_dict['http'].replace("http://", "")
        
        if proxy_address in self.valid_proxies:
            self.valid_proxies.remove(proxy_address)
            logger.info(f"ProxyProvider: Proxy {proxy_address} marked as bad and removed from the pool.")
            logger.info(f"ProxyProvider: {len(self.valid_proxies)} proxies left in the pool.")