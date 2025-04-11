from src.apis.base_api import ApiClient
from src.models.pydantic_models import GameDetailsList
from datetime import datetime

STEAMSPY_BASE_URL: str = "https://steamspy.com/api.php"
from concurrent.futures import ThreadPoolExecutor


class SteamSpyMetadataFetcher(ApiClient):

    def __init__(self, batch_size=100, num_workers=4):
        super().__init__()
        self.batch_size = batch_size
        self.url = STEAMSPY_BASE_URL
        self.num_workers = num_workers
        self.date_added = datetime.now()

    def fetch_metadata(self, app_id):
        """Fetch metadata for a single appid"""
        parameters = {"request": "appdetails", "appid": app_id}
        data = self.get_request(self.url, parameters)
        data["date_added"] = self.date_added
        if not data:
            self.logger.warning(f"Failed to fetchmetadata for {app_id}")
            return None
        return data

    def process_batch(self, app_ids):
        """Fetch metadata for a batch of appIDS in parallel"""
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            results = list(executor.map(self.fetch_metadata, app_ids))
        return [result for result in results if result]

    def run(self, app_ids):
        all_data = []

        for i in range(0, len(app_ids), self.batch_size):
            batch = app_ids[i : i + self.batch_size]
            self.logger.info(f"Processing batch: {batch}")
            batch_data = self.process_batch(batch)
            if batch_data:
                all_data.extend(batch_data)

        return GameDetailsList(games=all_data)
