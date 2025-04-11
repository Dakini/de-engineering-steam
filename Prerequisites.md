# Prerequisites for Steam Data Engineering Project

This document outlines the steps required to set up the necessary cloud resources and configurations before running the Steam Data Engineering pipeline.

## Initial Setup in Google Cloud Platform (GCP)

### Step 1: Create a New Project in GCP

1.  Go to https://console.cloud.google.com/
2.  Click on the project dropdown at the top of the page
3.  Click on "New Project"
4.  Enter a project name (e.g., "steam-data-engineering")
5.  Click "Create"

### Step 2: Enable Required APIs

In your new project, enable the following APIs:

- BigQuery API
- Cloud Storage API
- Cloud Resource Manager API

To enable these APIs:

1.  Go to "APIs & Services" > "Library"
2.  Search for each API and click "Enable"

### Step 3: Install Google Cloud SDK

1.  Download and install the Google Cloud SDK for your operating system:
    https://cloud.google.com/sdk/docs/install

2.  Initialize the SDK:

```sh
    gcloud init
```

3.  Authenticate with your Google account:

```sh
   gcloud auth login
```

### Step 4: Create a Service Account

1.  Go to "IAM & Admin" > "Service Accounts"
2.  Click "Create Service Account"
3.  Enter a name (e.g., "steam-data-service")
4.  Click "Create and Continue"
5.  Add the following roles:

- BigQuery Admin
- Storage Admin
- Storage Object Admin

6.  Click "Continue" and then "Done"

### Step 5: Generate and Download Service Account Key

1.  In the service accounts list, find your newly created account
2.  Click on the three dots menu and select "Manage Keys"
3.  Click "Add Key" > "Create new key"
4.  Choose JSON format
5.  Click "Create" to download the key file

### Step 6: Create BigQuery Dataset

1.  Go to BigQuery in the GCP console
2.  Click on your project ID in the Explorer panel
3.  Click "Create Dataset"
4.  Enter "dbt_steam" for the Dataset ID
5.  Choose your preferred location (e.g., "europe-west2")
6.  Click "Create Dataset"

## Project Configuration

### Step 1: Clone the Repository

```sh
git clone https://github.com/yourusername/de-engineering-steam.git
cd de-engineering-steam
```

### Step 2: Environment Setup

1.  Install Python 3.13
2.  Install Pipenv:

```sh
   pip install pipenv
```

3.  Install project dependencies:

```sh
   pipenv install
```

4.  Create a .env file in the project root with the following content:

    INGEST_PIPELINE=steam_ingest
    DATASET=dbt_steam
    STEAM_TOP_100_TABLE=steam_top_100_daily
    STEAMSPY_GAME_DETAILS_TABLE=steamspy_game_details_table
    STEAM_METADATA_TABLE=steam_metadata_table
    STEAM_METADATA_TABLE_CLEAN=steam_store
    STEAMSPY_GAME_DETAILS_TABLE_CLEAN=steamspy_game_details_table_clean
    STEAM_USER_TAG_TABLE=steam_user_tag_table

### Step 3: Update Project ID in Configuration Files

#### Prefect Configuration

1.  Open ingestion/prefect.yaml:
    yaml

    # filepath: /your/local/path/to/de-engineering-steam/ingestion/prefect.yaml

2.  Modify the directory path to match your local environment:
    yaml
    pull: - prefect.deployments.steps.set_working_directory:
    directory: /your/local/path/to/de-engineering-steam/ingestion #change this to the

#### DLT Configuration

Open any Python files referencing the DLT pipeline and update the project ID:

1.  In ingestion/.dlt create a secrets.toml file to upload data to big query

```bash
[destination.bigquery]
location = "europe-west2"
[destination.bigquery.credentials]
client_email = "<client_email from services.json>"
private_key = "<private_key from services.json>"
project_id = "<project_id from services.json>"
```

2.  Change the project_id to your GCP project ID.

#### dbt Configuration

1.  Open dbt/profiles.yml:

```yaml
# filepath: /your/local/path/to/de-engineering-steam/dbt/profiles.yml
```

2.  Update the project ID:

```yaml
steam:
outputs:
dev:
type: bigquery
project: YOUR_GCP_PROJECT_ID # Change this
dataset: dbt_steam
threads: 4
keyfile: /path/to/your/service-account-key.json # Change this
location: europe-west2
```

## Verifying Setup

After completing the prerequisites, verify your setup:

1.  Confirm GCP authentication:

```sh
gcloud auth list
```

2.  Check that your service account has the correct permissions:

```sh
   gcloud projects get-iam-policy YOUR_PROJECT_ID
```

3.  Verify you can list BigQuery Datasets

```sh
   bq ls
```

4.  Test Prefect setup:

```sh
    prefect work-pool ls
```

Once all these steps are complete, you can proceed with the main README instructions to run the pipeline.
