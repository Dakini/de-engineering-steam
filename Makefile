
prefect_setup:
	./ingestion/setup.sh


manual_ingest:
	cd ingestion && \
	prefect deployment run 'stream-data-workflow/SteamIngest'
manual_clean:
	cd ingestion && \
	prefect deployment run 'run-clean-dataworkflow/SteamClean'

manual_dbt:
	cd ingestion && \
	prefect deployment run 'dbt-flow/Dbtrun'
prefect_stop:
	prefect server stop

dbt_setup:
	cd dbt && dbt deps

bq_dataset: 
	bq --location=europe-west2  mk --dataset <project id>:dbt_steam

setup:
	pipenv install --dev
