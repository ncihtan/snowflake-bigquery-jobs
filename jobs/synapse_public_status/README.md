# Synapse Public Access Status Function

The `synapse_public_status` script is designed to query data from Snowflake, evaluate file-level access permissions, and load the results into a BigQuery table.  
This script is part of a data pipeline for monitoring data sharing configurations across HTAN projects.

## Features

1. **Snowflake Query Execution**:

    Connects to Snowflake using environment variables (`SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_PAT`).  
    Executes a SQL query to assess file access settings for selected HTAN projects.

    - This query checks whether files are shared with the correct access pattern.
    - It focuses on files only, excluding storage manifests.
    - For each file, it outputs:
        - The Synapse ID of the file
        - A boolean flag indicating whether the access settings meet the desired configuration:
            - Public users can view metadata
            - Registered users can read and download

2. **Access Evaluation Logic**:

    - Evaluates ACLs to determine whether:
        - Public access is limited to `READ` only
        - Registered users have both `READ` and `DOWNLOAD` permissions
    - Flags each file accordingly using a boolean field.

3. **BigQuery Integration**:

    - Loads the evaluated access data into a BigQuery table (`synapse_public_status`) under the specified dataset and project.
    - Uses a predefined schema to ensure proper data structure in BigQuery.

## Workflow

1. **Environment Setup**:

    - Loads environment variables from a `.env` file using `dotenv`.
    - Configures logging for debugging and monitoring.

2. **Data Query**:

    - Retrieves access control data for specific HTAN projects using a SQL query.
    - Joins node metadata with ACL tables to evaluate both public and registered user permissions.

3. **Data Transformation**:

    - Converts raw query results into a structured list of dictionaries for BigQuery loading.

4. **Data Loading**:

    - Writes the results into BigQuery using the `write_to_bigquery` function.

## Key Functions

- `login_to_snowflake()`: Establishes a connection to Snowflake.
- `run_snowflake_query(conn, query)`: Executes the SQL query and retrieves results.
- `write_to_bigquery(results, project_id, dataset_id, table_id)`: Loads data into BigQuery.

## BigQuery Schema

The script defines the following schema for the `synapse_public_status` table:

- `entity_id`: String  
- `has_public_view_registered_user_download_acl`: Boolean  

This output supports auditing and tracking access configurations across HTAN file uploads.

## Deployment

### Build

```bash
gcloud builds submit \
  --substitutions=_JOB_NAME=synapse_public_status \
  --config cloudbuild.yaml
```

### Set secrets

```bash
echo -n "your-snowflake-username" | gcloud secrets create snowflake-user --data-file=-
echo -n "your-snowflake-account"  | gcloud secrets create snowflake-account --data-file=-
echo -n "your-snowflake-pat"      | gcloud secrets create snowflake-pat --data-file=-
```

### Allow Cloud Run access to secrets

```bash
gcloud secrets add-iam-policy-binding snowflake-user \
  --member="serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding snowflake-account \
  --member="serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding snowflake-pat \
  --member="serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Deploy

```bash
gcloud beta run jobs deploy synapse-public-status \
  --image gcr.io/htan-dcc/synapse_public_status \
  --region=us-central1 \
  --set-secrets "SNOWFLAKE_USER=snowflake-user:latest,SNOWFLAKE_ACCOUNT=snowflake-account:latest,SNOWFLAKE_PAT=snowflake-pat:latest" \
  --set-env-vars GOOGLE_CLOUD_PROJECT=htan-dcc
```

### Run

Once deployed, run it:

```bash
gcloud beta run jobs execute synapse-public-status --region=us-central1
```

### Check logs

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=synapse-public-status" \
  --limit 50 --format="value(textPayload)"
```