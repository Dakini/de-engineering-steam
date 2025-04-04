import pandas as pd
import logging
import time

# Initialize the logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SteamStoreProcessor:

    def __init__(self, df: pd.DataFrame):
        self.steam_store = df

    def clean(self) -> pd.DataFrame:
        """
        Cleans and processes the Steam store DataFrame.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """

        start_time = time.time()
        logger.info("Starting the data cleaning process...")

        self.process_platforms()
        self.process_supported_languages()
        self.process_controller_support()
        self.process_release_dates()
        self.process_descriptions()
        self.return_cols()

        end_time = time.time()
        logger.info(
            f"Data cleaning process completed in {end_time - start_time:.2f} seconds."
        )
        return self.steam_store

    def return_cols(self):

        self.steam_store = self.steam_store.loc[
            :,
            [
                "name",
                "appid",
                "date_added",
                "required_age",
                "is_free",
                "description",
                "platforms",
                "metacritic",
                "recommendations",
                "achievements_number",
                "release_date",
                "_dlt_load_id",
                "_dlt_id",
                "reviews",
                "controller_support",
                "english",
                "release_date_year",
                "release_date_month",
                "release_date_day",
            ],
        ]

    def process_platforms(self) -> None:
        """
        Combine platform columns into a single 'platforms' column using vectorized operations.
        """
        platforms = pd.DataFrame(
            self.steam_store[["platform__windows", "platform__linux", "platform__mac"]]
        )
        platforms = platforms.apply(
            lambda row: " ".join([key for key, value in row.items() if value]), axis=1
        )
        self.steam_store["platforms"] = platforms
        logger.info("Platforms column processed successfully.")

    def process_supported_languages(self) -> None:
        """
        Create a new column 'english' to indicate whether 'english' is supported.
        This operation is vectorized for performance.
        """
        self.steam_store["english"] = self.steam_store[
            "supported_languages"
        ].str.contains("english", case=False, na=False)
        logger.info("Supported languages processed successfully.")

    def process_controller_support(self) -> None:
        """
        Convert 'controller_support' column to binary values (1 for 'full', 0 otherwise).
        Vectorized operations are used for efficiency.
        """
        self.steam_store["controller_support"] = (
            self.steam_store["controller_support"] == "full"
        ).astype(int)
        logger.info("Controller support processed successfully.")

    def process_release_dates(self) -> None:
        """
        Convert 'release_date' to datetime format and extract year, month, and day into separate columns.
        Uses vectorized datetime conversion for better performance.
        """
        self.steam_store["release_date"] = pd.to_datetime(
            self.steam_store["release_date"], errors="coerce"
        )
        self.steam_store["release_date_year"] = (
            self.steam_store["release_date"].dt.year.fillna(0).astype(int)
        )
        self.steam_store["release_date_month"] = (
            self.steam_store["release_date"].dt.month.fillna(0).astype(int)
        )
        self.steam_store["release_date_day"] = (
            self.steam_store["release_date"].dt.day.fillna(0).astype(int)
        )
        logger.info("Release dates processed successfully.")

    def process_descriptions(self) -> None:
        """
        Combine description-related columns into a single 'description' column and remove the original columns.
        Vectorized operations used to combine the columns for efficiency.
        """
        required_columns = [
            "detailed_description",
            "about_the_game",
            "short_description",
            "website",
            "header_image",
        ]

        self.steam_store["description"] = (
            self.steam_store[required_columns]
            .fillna(
                {
                    "detailed_description": "",
                    "about_the_game": "",
                    "short_description": "",
                    "website": "Not available",
                    "header_image": "Not available",
                }
            )
            .agg(
                lambda row: f"{row['detailed_description']} {row['about_the_game']} {row['short_description']} "
                f"Website: {row['website']} Game Image: {row['header_image']}",
                axis=1,
            )
        )

        # Handle any empty descriptions
        self.steam_store["description"].replace("", "Not available", inplace=True)
        self.steam_store.drop(columns=required_columns, inplace=True)
        logger.info("Descriptions processed successfully.")

    def optimize_memory_usage(self) -> None:
        """
        Optimize the memory usage of the DataFrame by downcasting numeric columns and converting object types.
        """

        # Downcast integers and categoricals
        for col in self.steam_store.select_dtypes(include=["int64"]).columns:
            self.steam_store[col] = pd.to_numeric(
                self.steam_store[col], downcast="integer", errors="coerce"
            )

        for col in self.steam_store.select_dtypes(include=["object"]).columns:
            self.steam_store[col] = self.steam_store[col].astype("category")

        logger.info("Optimized memory usage.")
