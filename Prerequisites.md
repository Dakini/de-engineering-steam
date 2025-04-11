# Prerequisites for Steam Data Engineering Project

This document provides the cloud setup instructions for running the Steam Data Engineering pipeline, including configuration of GCP, service accounts, and environment variables. These steps must be completed before executing the main pipeline workflows.

## Initial Setup in Google Cloud Platform (GCP)

### Step 1: Create a New Project in GCP

1.  Go to [Google Cloud Console](https://console.cloud.google.com/)
2.  Click on the project dropdown at the top of the page
3.  Click on **"New Project"**
4.  Enter a project name (e.g., `steam-data-engineering`)
5.  Click **"Create"**

### Step 2: Enable Required APIs

In your new project, enable the following APIs:

- BigQuery API
- Cloud Storage API
- Cloud Resource Manager API

To enable:

1. Go to **APIs & Services > Library**
2. Search each API name
3. Click **Enable**

### Step 3: Install Google Cloud SDK

1.  Download and install the Google Cloud SDK for your operating system:
    https://cloud.google.com/sdk/docs/install

2.  Initialize the SDK:

```bash
gcloud init
```

3.  Authenticate with your Google account:

```bash
gcloud auth login
```

### Step 4: Create a Service Account

1.  Go to **IAM & Admin** > **Service Accounts**
2.  Click **Create Service Account**
3.  Enter a name (e.g., `steam-data-service`)
4.  Click **Create and Continue**
5.  Add the following roles:

- BigQuery Admin
- Storage Admin
- Storage Object Admin

6.  Click **Continue** and then **Done**

### Step 5: Generate and Download Service Account Key

1.  In the service accounts list, find your newly created account
2.  Click on the three dots menu and select **Manage Keys**
3.  Click **\*Add Key** > **Create new key**
4.  Choose **JSON** format
5.  Click **Create** to download the key file

⚠️ **Important**: Add this key to .gitignore. Never commit it to version control.

## Project Configuration

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/de-engineering-steam.git
cd de-engineering-steam
```

### Step 2: Environment Setup

1.  Install Python 3.13

```bash
python3.12 -m venv myenv
source myenv/bin/activate
```

2.  Install Pipenv:

```bash
pip install pipenv
```

3.  Install project dependencies:

```bash
pipenv install
```

4.  Create a .env file in the project root with the following content:

```bash
touch .env
```

```bash
INGEST_PIPELINE=steam_ingest
DATASET=dbt_steam
STEAM_TOP_100_TABLE=steam_top_100_daily
STEAMSPY_GAME_DETAILS_TABLE=steamspy_game_details_table
STEAM_METADATA_TABLE=steam_metadata_table
STEAM_METADATA_TABLE_CLEAN=steam_store
STEAMSPY_GAME_DETAILS_TABLE_CLEAN=steamspy_game_details_table_clean
STEAM_USER_TAG_TABLE=steam_user_tag_table

```

### Step 3: Update Project ID in Configuration Files

#### Prefect Configuration

1. Open `ingestion/prefect.yaml`:
2. Modify the directory path to match your local environment:

```yaml
pull:
  - prefect.deployments.steps.set_working_directory:
      directory: /your/local/path/to/de-engineering-steam/ingestion
```

#### DLT Configuration

1.  Create `ingestion/.dlt/secrets.toml`:

```toml
[destination.bigquery]
location = "europe-west2"

[destination.bigquery.credentials]
client_email = "<client_email from services.json>"
private_key = "<private_key from services.json>"
project_id = "<project_id from services.json>"
```

Replace the placeholders with values from your service account file.

#### DBT Configuration

1.  Open `dbt/profiles.yml`

```yaml
steam:
  outputs:
    dev:
      type: bigquery
      project: <YOUR_GCP_PROJECT_ID> # Change this
      dataset: dbt_steam
      threads: 4
      keyfile: /path/to/your/service-account-key.json # Change this
      location: europe-west2
  target: dev
```

3. Update the `Makefile`, for creating the dataset to point to the correct project id

```bash
bq_dataset:
    bq --location=europe-west2  mk --dataset <project id>:dbt_steam  # Change this
```

4. Update `dbt/models/staging/schema.yml`

```yaml
sources:
  - name: staging
    database: PROJECT ID #change this to the google project id
    schema: steam_test
```

## Verifying Setup

Make sure everything is working before proceeding.

1. ### Setup Environment and Shell

```bash
make setup
pipenv shell
```

2. ### Verify GCP Auth

```bash
gcloud auth list
```

3. ### Check IAM Policy

```bash
gcloud projects get-iam-policy YOUR_PROJECT_ID
```

4. ### Verify BigQuery Access

```bash
bq ls
```

5. ### Test Prefect Setup

```bash
prefect work-pool ls
```

6. ### Validate .env is Loaded
   (Optional, but helpful for debugging):

```bash
pipenv run python -c "import os; print(os.getenv('DATASET'))"
```

### Next Step

Once all the above steps are complete and verified, return to the main README.md and proceed with running the data pipeline workflows.
