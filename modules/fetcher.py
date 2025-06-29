import random
import time
import logging
from typing import Union
from pathlib import Path
import requests  # type: ignore
from requests.adapters import HTTPAdapter, Retry  # type: ignore
from requests_cache import CachedSession  # type: ignore
from requests.exceptions import RetryError, RequestException  # type: ignore

logger = logging.getLogger(__name__)

MAX_PAGES = 1000


class JobOffersFetcher:
    def __init__(
        self,
        base_url: Union[str, Path],
        cache_name="fetcher_cache",
        cache_expire=60,
        use_cache=False,
        min_delay=3,  # delay between requests
        max_delay=6,  # delay between requests
        retries=5,
        backoff_range=(3, 6),  # range of backoff factor
    ):
        if isinstance(base_url, Path):
            base_url = str(base_url)
        if not base_url:
            raise ValueError("URL is required and cannot be empty")

        self.base_url = base_url
        self.min_delay = min_delay  # delay between requests
        self.max_delay = max_delay  # delay between requests
        self.retries = retries
        self.backoff_range = backoff_range  # range of backoff factor
        self.session = self._init_session(cache_name, cache_expire, use_cache)

    def _init_session(self, cache_name, cache_expire, use_cache):
        backoff_factor = random.uniform(*self.backoff_range)
        retry_strategy = Retry(
            total=self.retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        if use_cache:
            session = CachedSession(
                cache_name=cache_name,
                expire_after=cache_expire,
                use_cache=True,
            )
        else:
            session = requests.Session()

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        def log_response_hook(response, *args, **kwargs):
            logger.info(f"Hook: {response.status_code} {response.url}")

        session.hooks["response"] = [log_response_hook]
        return session

    def fetch_page(self, page: int = 1) -> dict:
        try:
            response = self.session.get(self.base_url, params={"page": page})
            response.raise_for_status()
            return response.json()
        except RetryError as e:
            raise RuntimeError(f"All retries failed: {e}")
        except RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    def get_total_pages(self) -> int:
        logger.info("Checking total number of pages...")
        logger.info("=" * 60)
        logger.info("")
        try:
            response = self.session.get(self.base_url, params={"page": 1})
            response.raise_for_status()
            data = response.json()
            meta = data.get("meta", {})
            total_pages = meta.get("totalPages")

            if total_pages is not None:
                logger.info(f"API reports totalPages: {total_pages}")
                logger.info("=" * 60)
                logger.info("")
                return total_pages

            total_items = meta.get("totalItems")
            items = data.get("data", [])

            if not isinstance(items, list):
                logger.error("Invalid response format: 'data' is not a list")
                return 1

            per_page = len(items)

            if total_items is not None and per_page > 0:
                calculated_pages = (total_items // per_page) + (
                    1 if total_items % per_page else 0
                )
                logger.info(f"Calculated total pages: {calculated_pages}")
                logger.info("=" * 60)
                logger.info("")
                return calculated_pages

            return 1
        except Exception as e:
            logger.error(f"Error fetching total pages: {e}")
            return 1

    def fetch_pages(self, num_pages: int = None):
        if num_pages is None:
            num_pages = self.get_total_pages()
        num_pages = min(num_pages, MAX_PAGES)

        pages = list(range(1, num_pages + 1))
        random.shuffle(pages)
        all_data = []

        for page in pages:
            page_data = self.fetch_page(page)
            all_data.extend(page_data.get("data", []))
            logger.info(f"Saved page: {page}")
            time.sleep(random.randint(self.min_delay, self.max_delay))

        return {"data": all_data}
