from src.apis.base_api import ApiClient
from src.models.pydantic_models import GameTop100Rank

STEAM_TOP_GAMES: str = (
    "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/"
)


class SteamTop100(ApiClient):
    def __init__(self):
        super().__init__()
        self.url = STEAM_TOP_GAMES

    def run(self):

        data = self.get_request(self.url)
        if not data:
            self.logger.error("Failed to fetch data from SteamSpy")

        return GameTop100Rank.from_raw_data(data.values())
