
prefect_setup:
	prefect server start --background 
	prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api 
	prefect work-queue create --pool "steam_de" "default" || true
	nohup prefect worker start --pool "steam_de" --work-queue "default" &> worker.out &
	cd ingestion && \
	prefect deploy -n SteamIngest -n SteamClean --prefect-file prefect.yaml
# prefect_setup:
# 	cd ingestion && \
# 	prefect server start --background && \
# 	prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api && \
# 	prefect work-queue create --pool "steam_de" "default" && \
# 	nohup prefect worker start --pool "steam_de" --work-queue "default" > worker.out 2>&1 & && \
# 	prefect deploy -n SteamIngest -n SteamClean --prefect-file prefect.yaml

manual_ingest:
	cd ingestion && \
	prefect deployment run 'stream-data-workflow/SteamIngest'
manual_clean:
	cd ingestion && \
	prefect deployment run 'run-clean-dataworkflow/SteamClean'
prefect_stop:
	prefect server stop

dbt_setup:
	cd dbt && dbt deps

bq_dataset: 
	bq --location=europe-west2  mk --dataset mythical-legend-450020-c6:dbt_steam_2