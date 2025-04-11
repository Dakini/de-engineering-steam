from datetime import datetime, timedelta
import pandas as pd
from typing import List
import dlt
import pandas as pd


def get_date() -> datetime:
    """Returns the current date and time."""
    return datetime.now()


def get_stale_data_ids(df, freshness_days=7):
    seven_days_ago = datetime.now() - timedelta(days=freshness_days)
    df["date_added"] = pd.to_datetime(df["date_added"]).dt.tz_localize(None)
    df_old = df[df["date_added"] < seven_days_ago]
    df_latest_per_appid = (
        df_old.sort_values("date_added").groupby("appid").last().reset_index()
    )
    return df_latest_per_appid["appid"].tolist()


def get_dataset(pipeline: dlt.pipeline, table_name: str):
    return pipeline.dataset()[table_name].df()


def deduplpication(current_data: List, database_data: List) -> List:
    """Returns a list of items in `current_data` that are not present in `database_data`."""
    return list(set(current_data) - set(database_data))


def get_appids(pipeline: dlt.pipeline, table_name: str, appids: list, freshness_days=7):

    dataset = get_dataset(pipeline, table_name)

    missing_ids = deduplpication(appids, dataset["appid"].tolist())
    old_data_ids = get_stale_data_ids(dataset, freshness_days=freshness_days)

    return list(set(missing_ids + old_data_ids))
