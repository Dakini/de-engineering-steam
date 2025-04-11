import requests
import time
import logging


class ApiClient:

    def __init__(self):
        self.session = requests.Session()  # reuse TCP connections
        self.logger = logging.getLogger(__name__)

    def get_request(
        self,
        url: str,
        parameters=None,
        max_retries=5,
        wait_time=5,
        wait_time_multiplier=4,
    ):
        """Send a GET request with retries and exponential backoff."""
        attempts = 0
        headers = {"User-Agent": "YourCustomUserAgent/1.0", "DNT": "1"}
        while attempts < max_retries:
            try:
                response = requests.get(url=url, headers=headers, params=parameters)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # too many requests
                    retry_after = response.headers.get("Retry-After", wait_time)
                    time.sleep(retry_after)
                else:
                    self.logger.error(
                        f"Request failed with status {response.status_code}: {response.text}"
                    )
                    return None

            except Exception as e:
                self.logger.error(f"Request failed with status {e}")
            attempts += 1
            sleep_time = min(
                wait_time * (wait_time_multiplier ** (attempts - 1)), 60
            )  # Cap sleep at 60 sec
            self.logger.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

        return None
