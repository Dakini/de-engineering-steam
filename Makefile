
prefect_setup:
	cd ingestion
	prefect server start --background
	prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
	prefect work-queue create --pool "steam_de" "default"
	nohup prefect worker start --pool "steam_de" --work-queue "default" &> worker.out &
	prefect deploy -n SteamIngest -n SteamClean --prefect-file prefect.yaml


manual_ingest:
	cd ingestion
	prefect deployment run 'stream-data-workflow/SteamIngest'
manual_clean:
	cd ingestion
	prefect deployment run 'run-clean-dataworkflow/SteamClean'