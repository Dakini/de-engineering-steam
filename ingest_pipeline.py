import os
import logging
from dotenv import load_dotenv

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

from src.apis.steam_top100daily import SteamTop100
from src.apis.steamspy_gamedetails import SteamSpyMetadataFetcher
from src.apis.steam_metadetails import SteamStoreMetadata

from src.helpers.utils import get_appids
from src.helpers.pipeline import create_pipeline
import dlt

# load environment variables
load_dotenv()

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(
    retries=3,
    retry_delay_seconds=10,
    persist_result=True,
    cache_expiration=timedelta(hours=1),
)
def fetch_top_played_100() -> list:
    logger.info("Fetching top played games in the last 24 hours")
    steam_daily_100 = SteamTop100()
    daily_top_100_data = steam_daily_100.run()
    return daily_top_100_data.model_dump()["ranks"]


@task(retries=3, retry_delay_seconds=10)
def fetch_steamspy_game_details(appids) -> list:
    """Function to pull data from steam spy"""
    logger.info("Fetching steam_spy details")
    game_details = SteamSpyMetadataFetcher()
    game_details_data = game_details.run(appids)
    return game_details_data


@task(retries=3, retry_delay_seconds=10)
def fetch_steam_store_data(appids) -> list:
    """Function to pull data from steam store"""
    logger.info("Fetching top played games in the last 24 hours")
    steam_metadata_client = SteamStoreMetadata()
    metadata_data = steam_metadata_client.run(appids)
    return metadata_data


@task
def create_ingestion_pipeline() -> dlt.pipeline:
    """Creating the pipeline for ingesting the data from steam"""
    logger.info("Creating pipeline for ingestion")
    return create_pipeline(
        pipeline_name=os.environ["INGEST_PIPELINE"],
        dataset_name=os.environ["DATASET"],
    )


@task
def get_ingestion_appids(data) -> list:
    """Return a list of the appids of what has been fetched today"""
    return [d["appid"] for d in data]


@task
def ingest_daily_100(pipeline: dlt.pipeline, top_100_data: list):
    """Ingest top 100 games into the pipeline."""
    logger.info("Ingesting Top 100 data into pipeline...")

    pipeline.run(
        top_100_data,
        table_name=os.environ["STEAM_TOP_100_TABLE"],
    )


@task
def determine_appids(pipeline, appids: list) -> list:
    """Determine which app IDs need to be pulled based on existing data in BigQuery."""
    dataset_tables = pipeline.dataset().row_counts().df()["table_name"].values

    if (
        os.environ["STEAM_METADATA_TABLE"] in dataset_tables
        and os.environ["STEAMSPY_GAME_DETAILS_TABLE"] in dataset_tables
    ):
        steamspy_appids = get_appids(
            pipeline, os.environ["STEAMSPY_GAME_DETAILS_TABLE"], appids
        )
        steamstore_appids = get_appids(
            pipeline, os.environ["STEAM_METADATA_TABLE"], appids
        )

        pull_appids = list(set(steamspy_appids + steamstore_appids))
        logger.info(f"Found {len(pull_appids)} app IDs requiring an update.")
    else:
        logger.info("No existing metadata found. Pulling all app IDs.")
        pull_appids = appids
    return pull_appids


@task
def end() -> None:
    """Final Logging message"""
    logger.info("Steam Data ingestion workflow completed successfully")


@task
def ingest_data(ingestion_pipeline, game_details_data, metadata_data):
    """Ingesting data into big query"""
    logger.info("Ingesting SteamSpy data into Big Query...")
    ingestion_pipeline.run(
        game_details_data.games,
        table_name=os.environ["STEAMSPY_GAME_DETAILS_TABLE"],
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="appid",
        columns={"tags": {"data_type": "json"}},
    )

    logger.info("Ingesting SteamStore data into Big Query...")
    ingestion_pipeline.run(
        metadata_data.games,
        table_name=os.environ["STEAM_METADATA_TABLE"],
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="appid",
    )


@flow
def stream_data_workflow():
    """Orchestratees the full steam data ingestion process using Prefect"""
    # Step 1 create the pipeline
    ingestion_pipeline = create_ingestion_pipeline()

    # Step 2: Fetch the top played games in the last 25 hours
    daily_top_played_games = fetch_top_played_100()

    # Step 3: ingest the data to the table
    ingest_daily_100(ingestion_pipeline, daily_top_played_games)

    # Step 4 extract the appids
    appids = get_ingestion_appids(daily_top_played_games)

    # Step 5: Determine which appids to use
    pull_appids = determine_appids(ingestion_pipeline, appids)

    if pull_appids:
        # Step 6: Fetch steam spy
        game_details_data = fetch_steamspy_game_details(pull_appids)
        # Step 7 fetch the steam store details
        metadata_data = fetch_steam_store_data(pull_appids)
        #     # Step 8 ingest the data
        ingest_data(ingestion_pipeline, game_details_data, metadata_data)
        end()

    else:
        end()


if __name__ == "__main__":
    stream_data_workflow()
