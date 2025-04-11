import dlt


def create_pipeline(pipeline_name: str, dataset_name: str, destination="bigquery"):

    return dlt.pipeline(
        pipeline_name=pipeline_name, dataset_name=dataset_name, destination=destination
    )
