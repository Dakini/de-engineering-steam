from src.apis.base_api import ApiClient
from src.models.pydantic_models import SteamGameMetadata, SteamGameMetadataList
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from datetime import datetime

STEAM_BASE_SEARCH_URL: str = "http://store.steampowered.com"


class SteamStoreMetadata(ApiClient):

    def __init__(self, batch_size=100, num_workers=4):
        super().__init__()
        self.batch_size = batch_size
        self.url = STEAM_BASE_SEARCH_URL
        self.num_workers = num_workers
        self.date_added = datetime.now()

    def parser(self, text):
        if text:
            soup = BeautifulSoup(text, "lxml")
            plain_text = soup.get_text("\n", strip=True)
            return plain_text
        return None

    def process_steam_data(self, data: dict):
        try:
            data = {
                "type": data["type"],
                "name": data["name"],
                "appid": data["steam_appid"],
                "required_age": data["required_age"],
                "is_free": data["is_free"],
                "dlc": data.get("dlc", []),
                "controller_support": data.get("controller_support", None),
                "about_the_game": self.parser(data.get("about_the_game", "")),
                "detailed_description": self.parser(
                    data.get("detailed_description", "")
                ),
                "short_description": self.parser(data.get("short_description", "")),
                "supported_languages": self.parser(data.get("supported_languages", "")),
                "reviews": self.parser(data.get("reviews", "")),
                "header_image": data["header_image"],
                "capsule_image": data["capsule_image"],
                "website": data.get("website", ""),
                "requirements": data["pc_requirements"],
                "developers": data.get("developers", None),
                "publishers": data.get("publishers", None),
                "price_overview": data.get("price_overview", None),
                "platform": data["platforms"],
                "metacritic": data.get("metacritic", {}).get("score", 0),
                "categories": data.get("categories", None),
                "genres": data.get("genres", None),
                "recommendations": data.get("recommendations", {}).get("total", 0),
                "achievements_number": data.get("achievements", {}).get("total", 0),
                "release_date": data["release_date"]["date"],
                "coming_soon": data["release_date"]["coming_soon"],
                "date_added": self.date_added,
            }
            return SteamGameMetadata(**data)

        except KeyError as ke:
            self.logger.error(f"The wrong key was not present {ke}")
        return None

    def fetch_metadata(self, app_id: int):
        """Fetch metadata for a single appid"""
        url = f"http://store.steampowered.com/api/appdetails/"
        parameters = {"appids": app_id}
        data = self.get_request(url, parameters)

        if data:
            resp = data[f"{app_id}"]

            if resp["success"] == True:
                data = resp["data"]
                # process data
                data = self.process_steam_data(data)
                if data and data.appid == app_id:
                    return data
                else:
                    self.logger.warning(f"unsuccessful for pulling {app_id} data ")
                    return None

        self.logger.warning(f"Failed to fetch metadata for {app_id}")
        return None

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

        return SteamGameMetadataList(games=all_data)
