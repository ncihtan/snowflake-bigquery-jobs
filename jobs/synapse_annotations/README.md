# Synapse Annotations Function

The synapse_annotations script is designed to query data from Snowflake, transform annotations, and load the results into a BigQuery table. 
This script is part of a data pipeline for processing Synapse annotations related to HTAN projects.

## Features

1. **Snowflake Query Execution**:

    Connects to Snowflake using environment variables (SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_PAT).
    Executes a SQL query to retrieve project-specific annotation data from the node_latest table in the Synapse Data Warehouse.

    - This query looks across selected HTAN projects in Synapse and pulls metadata for each file.
    - It skips over any auto-generated storage manifest files (those aren't relevant for most users).
    - For each file, it lists:
    - The Synapse ID of the file
    - The Synapse ID of the project it belongs to
    - A human-readable name for the project (e.g., "HTAN Vanderbilt")
    - The name of the file itself
    - The value of the Component annotation, if it exists
    - The full annotations object
    - A count of how many different annotations the file has

2. **Annotation Transformation**:

    - Processes the annotations field from the query results.
    
    - Converts the annotations into a structured format expected by BigQuery, including keys, types, and values.

3. **BigQuery Integration**:

    - Loads the transformed data into a BigQuery table (synapse_annotations) under the specified dataset and project.
    - Uses a predefined schema to ensure proper data structure in BigQuery.

## Workflow

1. **Environment Setup**:

    - Loads environment variables from a .env file using dotenv.
    - Configures logging for debugging and monitoring.

2. **Data Query**:

    - Retrieves annotation data for specific HTAN projects using a SQL query.
    - Filters out unwanted files (e.g., storage manifests).

3. **Data Transformation**:

    - Converts raw annotations into a structured format using the transform_annotations helper function.

4. **Data Loading**:

    - Writes the transformed data into BigQuery using the write_to_bigquery function.

## Key Functions

- `login_to_snowflake()`: Establishes a connection to Snowflake.
- `run_snowflake_query(conn, query)`: Executes the SQL query and retrieves results.
- `transform_annotations(annotations_obj)`: Converts raw annotations into a structured format.
- `write_to_bigquery(results, project_id, dataset_id, table_id)`: Loads data into BigQuery.

## BigQuery Schema

The script defines the following schema for the synapse_annotations table:

- `project_id`: String
- `project_name`: String
- `entity_id`: String
- `name`: String
- `component`: String
- `annotations`: Repeated record containing:
- `key`: String
- `type`: String
- `value`: Repeated string

## Deployment

Follow the same deployment steps as the synapse_public_status job, but substitute the job name:

### Build

```bash
gcloud builds submit \
  --substitutions=_JOB_NAME=synapse_annotations \
  --config cloudbuild.yaml
```

### Deploy

```bash
gcloud beta run jobs deploy synapse-annotations \
  --image gcr.io/htan-dcc/synapse_annotations \
  --region=us-central1 \
  --set-secrets "SNOWFLAKE_USER=snowflake-user:latest,SNOWFLAKE_ACCOUNT=snowflake-account:latest,SNOWFLAKE_PAT=snowflake-pat:latest" \
  --set-env-vars GOOGLE_CLOUD_PROJECT=htan-dcc
```

### Run

```bash
gcloud beta run jobs execute synapse-annotations --region=us-central1
```

### Check logs

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=synapse-annotations" \
  --limit 50 --format="value(textPayload)"
```