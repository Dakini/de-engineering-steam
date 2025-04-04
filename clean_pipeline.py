import os
import logging
from dotenv import load_dotenv

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from src.helpers.steamspy_cleaner import GameDetailsProcessor
from src.helpers.steamstore_cleaner import SteamStoreProcessor
from collections import defaultdict
from src.helpers.pipeline import create_pipeline
import dlt
import pandas as pd
import asyncio

# load environment variables
load_dotenv()

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# grab newly cleaned data
fetch_query = """
SELECT * 
FROM  {table}
WHERE date_added >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
"""


@task(retries=3, retry_delay_seconds=5)
async def create_clean_pipeline() -> dlt.pipeline:
    """Creating the pipeline for cleaning the data from steam"""
    logger.info("Creating pipeline for cleaning")
    try:

        return create_pipeline(
            pipeline_name=os.environ["INGEST_PIPELINE"],
            dataset_name=os.environ["DATASET"],
        )
    except Exception as e:
        logger.error(f"Failed to create pipeline: {e}")
        raise


@task(retries=3, retry_delay_seconds=5)
async def fetch_details(pipeline: dlt.pipeline, table_name: str) -> pd.DataFrame:
    """Fetches data from the pipeline and converts it to a data frame"""
    logger.info(f"Fetching data from {table_name}")
    try:
        with pipeline.sql_client() as client:
            res = client.execute_sql(fetch_query.format(table=table_name))
            if not res:
                logger.info(f"There was no data returned from {table_name}")
                return pd.DataFrame()
            # convert rows to dataframe directly
            df = pd.DataFrame([dict(row) for row in res])
            logger.info(f"Fetching {len(df)} data sources from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error fetching data from {table_name}: {e}")
        raise


@task
async def end() -> None:
    """Final Logging message"""
    logger.info("Steam Data Cleaning workflow completed successfully")


@task
async def ingest_clean_data(pipeline, data, table_name, columns=None, name=""):
    """Ingesting clean data into tables"""

    logger.info(f"Ingesting data rows into {table_name}...")

    try:
        if columns is not None:
            pipeline.run(
                data,
                table_name=table_name,
                write_disposition={"disposition": "merge", "strategy": "upsert"},
                primary_key="appid",
                columns=columns,
            )
        else:
            pipeline.run(
                data,
                table_name=table_name,
                write_disposition={"disposition": "merge", "strategy": "upsert"},
                primary_key="appid",
            )
        logger.info(f"Successfully ingested data into {table_name}")
    except Exception as e:
        logger.error(f"Error ingesting data to {table_name}: {e}")
        raise


@dlt.resource(name="steam_spy")
def yield_steamspy(data):
    for _data in data.to_dict(orient="records"):
        yield _data


@dlt.resource(name="steam_store")
def yield_steamstore(data):
    for _data in data.to_dict(orient="records"):
        yield _data


@dlt.resource(name="steam_user_tags")
def yield_tags(data):
    for _data in data.to_dict(orient="records"):
        yield _data


@flow(task_runner=ConcurrentTaskRunner())
async def clean_data_workflow():
    """Orchestrates the full steam data cleaning process using Prefect"""
    # Step 1 create the pipeline
    clean_pipeline = await create_clean_pipeline()

    # Step 2: Fetch the Data from game details and steam store
    try:
        steam_spy_table = os.getenv("STEAMSPY_GAME_DETAILS_TABLE")
        steam_metadata_table = os.getenv("STEAM_METADATA_TABLE")
        steam_spy_clean_table = os.getenv("STEAMSPY_GAME_DETAILS_TABLE_CLEAN")
        steam_store_clean_table = os.getenv("STEAM_STORE_DETAILS_TABLE_CLEAN")
        steam_user_tag = os.getenv("STEAM_USER_TAG_TABLE")
        steam_spy_data, steam_store_data = await asyncio.gather(
            fetch_details(clean_pipeline, steam_spy_table),
            fetch_details(clean_pipeline, steam_metadata_table),
        )

        if not steam_spy_data.empty:

            game_cleaner = GameDetailsProcessor()
            cleaned_steam_spy_data, cleaned_tags = game_cleaner.clean(steam_spy_data)

            await ingest_clean_data(
                clean_pipeline,
                yield_steamspy(cleaned_steam_spy_data),
                steam_spy_clean_table,
            )

            await ingest_clean_data(
                clean_pipeline,
                yield_tags(cleaned_tags),
                steam_user_tag,
            )

        if not steam_store_data.empty:

            steam_cleaner = SteamStoreProcessor(steam_store_data)
            cleaned_steam_store_data = steam_cleaner.clean()
            await ingest_clean_data(
                clean_pipeline,
                yield_steamstore(cleaned_steam_store_data),
                steam_store_clean_table,
            )

        await end()

    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(clean_data_workflow())
