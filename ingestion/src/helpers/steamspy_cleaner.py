import pandas as pd
import logging
from typing import Dict, List, Union
import hashlib

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_tag_unique_ids(df):
    unique_id = [
        hashlib.md5(f"{f.appid}{f.tag}".encode("utf-16")).hexdigest()
        for f in df.itertuples()
    ]
    df["unique_id"] = unique_id
    return df


class GameDetailsProcessor:
    def __init__(self):
        """
        Initialize the GameDetailsProcessor

        Args:
            file_path (str): Path to the CSV file containing game details.
        """

        self.cleaner_dict: Dict[str, Union[str, int, float]] = {
            "developer": "",
            "publisher": "",
            "name": "",
            "price": 0,
            "initialprice": 0,
            "discount": 0.0,
            "languages": "",
            "genre": "",
        }
        logger.info("GameDetailsProcessor initialized and data loaded successfully.")

    def create_tags_dataframe(self):

        appids, tags, user_count = [], [], []

        for row in self.details.itertuples():

            if pd.isna(row.tags):
                appids.append(row.appid)
                tags.append("UNK")
                user_count.append(-1)
            else:
                for k, v in row.tags.items():
                    appids.append(row.appid)
                    tags.append(k)
                    user_count.append(v)

        self.tags = pd.DataFrame(
            {"appid": appids, "tag": tags, "user_count": user_count}
        )
        self.tags = get_tag_unique_ids(self.tags)

    def clean(self, details: pd.DataFrame, drop_cols: List[str] = None) -> pd.DataFrame:
        """
        Clean the DataFrame by performing all essential preprocessing steps.

        Args:
            drop_cols (List[str], optional): List of columns to drop. Defaults to None.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        logger.info("Starting full DataFrame cleaning process...")
        self.details = details
        self.create_tags_dataframe()

        if drop_cols is None:
            drop_cols = [
                "tags" "ccu",
                "score_rank",
                "average_forever",
                "median_forever",
                "userscore",
                "owners",
                "_dlt_load_id",
                "_dlt_id",
            ]
        self.convert_owners_range()
        self.drop_columns(drop_cols)
        self.clean_na()
        self.convert_price_to_dollars()

        logger.info("DataFrame cleaning completed successfully.")
        return self.details, self.tags

    def drop_columns(self, columns: List[str]) -> None:
        """
        Drop unnecessary columns to optimize memory usage.

        Args:
            columns (List[str]): List of column names to be dropped.
        """
        self.details.drop(columns=columns, inplace=True, errors="ignore")
        logger.info(f"Dropped columns: {columns}")

    def clean_na(self) -> None:
        """
        Replace NaN values based on the predefined cleaner dictionary.
        """
        self.details.fillna(self.cleaner_dict, inplace=True)
        logger.info("Missing values cleaned with default values.")

    def convert_owners_range(self) -> None:
        """
        Split the 'owners' column into 'owners_lower_range' and 'owners_upper_range'.
        """
        if "owners" in self.details.columns:
            self.details["owners_lower_range"] = self.details["owners"].apply(
                lambda x: (
                    int(x.split("..")[0].replace(",", "")) if isinstance(x, str) else 0
                )
            )
            self.details["owners_upper_range"] = self.details["owners"].apply(
                lambda x: (
                    int(x.split("..")[1].replace(",", "")) if isinstance(x, str) else 0
                )
            )
            logger.info("Owners range converted successfully.")
        else:
            logger.warning("'owners' column not found in DataFrame.")

    def convert_price_to_dollars(self) -> None:
        """
        Convert 'price' and 'initialprice' from cents to dollars.
        """
        for col in ["price", "initialprice"]:
            if col in self.details.columns:
                self.details[col] = self.details[col].fillna(0) / 100
        logger.info("Prices converted to dollars.")
