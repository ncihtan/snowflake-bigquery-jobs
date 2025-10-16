# Snowflake BigQuery Jobs

This repository contains Cloud Run jobs for processing HTAN (Human Tumor Atlas Network) data from Snowflake. The jobs are designed to query data from the Synapse Data Warehouse in Snowflake and either load results into BigQuery tables or send notifications to Slack.

## Available Jobs

This repository includes 3 functions:

### 📊 [Synapse Public Access Status](./jobs/synapse_public_status/README.md)
Monitors and evaluates file-level access permissions across HTAN projects, ensuring files have the correct public access configuration. Results are loaded into a BigQuery table for auditing and tracking.

**Key Features:**
- Evaluates ACLs for public and registered user permissions
- Loads results into `synapse_public_status` BigQuery table
- Supports auditing access configurations across HTAN projects

### 🏷️ [Synapse Annotations](./jobs/synapse_annotations/README.md)
Processes and transforms Synapse file annotations from HTAN projects, converting them into a structured format for BigQuery analysis.

**Key Features:**
- Retrieves annotation data from Synapse Data Warehouse
- Transforms annotations into structured BigQuery format
- Loads results into `synapse_annotations` BigQuery table

### 📈 [Synapse Monitor](./jobs/synapse_monitor/README.md)
Tracks real-time file creation and modification activity across HTAN projects and sends formatted notifications to Slack for team awareness.

**Key Features:**
- Monitors file creation and modification events
- Sends structured Slack notifications with activity summaries
- Configurable lookback periods and display formats
- Intelligent formatting for high and low activity periods

## General Requirements

All jobs require the following environment variables for Snowflake authentication:
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_PAT`: Your Snowflake Personal Access Token

Additional requirements vary by job - see individual job documentation for specific needs.

## Common Deployment Setup

### Create Base Secrets

```bash
echo -n "your-snowflake-username" | gcloud secrets create snowflake-user --data-file=-
echo -n "your-snowflake-account"  | gcloud secrets create snowflake-account --data-file=-
echo -n "your-snowflake-pat"      | gcloud secrets create snowflake-pat --data-file=-
```

### Grant Secret Access to Cloud Run

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

## Project Structure

```
├── cloudbuild.yaml          # Cloud Build configuration
├── create_secrets.py        # Script for creating secrets
├── Dockerfile              # Container image definition
├── requirements.txt         # Python dependencies
├── secrets.yaml            # Secret configurations
├── common/                 # Shared utilities
│   └── utils.py
└── jobs/                   # Individual job implementations
    ├── synapse_annotations/
    │   ├── main.py
    │   └── README.md
    ├── synapse_monitor/
    │   ├── main.py
    │   ├── query.sql
    │   └── README.md
    └── synapse_public_status/
        ├── main.py
        └── README.md
```

## Getting Started

1. **Choose a job**: Review the individual job documentation linked above
2. **Set up authentication**: Configure Snowflake credentials and any job-specific secrets
3. **Build and deploy**: Use the Cloud Build configuration to build and deploy the job
4. **Schedule execution**: Set up Cloud Scheduler or run jobs manually as needed

For detailed deployment instructions, see the README file for each specific job.

## Synapse Monitor Function

The `synapse_monitor` script is designed to query data from Snowflake to track file creation and modification activity across HTAN projects and send formatted notifications to Slack.  
This script provides real-time monitoring of data uploads and changes across HTAN projects.

### Features:

1. **Snowflake Query Execution**:

    Connects to Snowflake using environment variables (`SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_PAT`).  
    Executes a SQL query to retrieve recent file activity data from the Synapse Data Warehouse.

    - This query tracks file creation and modification events across selected HTAN2 projects.
    - For each activity, it captures:
        - File ID and name
        - Change type (CREATE or MODIFY)
        - User who made the change (ID and username)
        - Project information (ID and name)
        - Parent folder information
        - Annotation count for the file
    - Configurable lookback period (default: 1 day)

2. **Activity Aggregation and Formatting**:

    - Groups activities by user, project, folder, and change type for organized reporting
    - Provides two display formats:
        - **Standard format**: Detailed view for low activity periods
        - **Condensed format**: Summary view when activity exceeds threshold (20+ combinations)
    - Creates clickable links to Synapse profiles, projects, and folders
    - Includes activity summaries with counts and change type breakdowns

3. **Slack Integration**:

    - Sends formatted messages to Slack via webhook URL
    - Messages include:
        - Activity summary with total files, users, and projects
        - Structured blocks showing user activity by project and folder
        - Appropriate formatting for both high and low activity periods
    - Graceful handling when no Slack webhook is configured (logs message instead)

### Workflow:

1. **Environment Setup**:

    - Loads environment variables from a `.env` file using `dotenv`
    - Configures logging for debugging and monitoring
    - Supports command-line arguments for customization

2. **Data Query**:

    - Retrieves file activity data for specific HTAN2 projects using a parameterized SQL query
    - Joins node metadata with user profiles to get complete activity context

3. **Data Processing**:

    - Aggregates activities by user-project-folder-change type combinations
    - Determines appropriate display format based on activity volume
    - Creates structured Slack message with proper formatting and links

4. **Notification Delivery**:

    - Sends formatted activity report to Slack webhook
    - Provides fallback logging when Slack integration is not configured

### Key Functions:
- `login_to_snowflake()`: Establishes a connection to Snowflake
- `run_snowflake_query(conn, query)`: Executes the SQL query and retrieves results
- `read_sql_query(file_path, days_back)`: Loads and parameterizes SQL query from file
- `format_simple_slack_message(results, days_back)`: Creates structured Slack message
- `send_slack_message(webhook_url, message)`: Delivers message to Slack

### Command Line Options:

- `--query-file, -q`: Path to SQL query file (default: query.sql in script directory)
- `--days-back, -d`: Number of days to look back for activity (default: 1)
- `--verbose, -v`: Enable verbose logging

### Configuration Thresholds:

- **Condensed format threshold**: 20 user-project-folder-change type combinations
- **Max user-project combinations**: 15 in condensed view
- **Max folders displayed**: 5 per user-project summary

## Deployment

In this example we deploy the synapse_public_status job. Similar steps can be followed for synapse_annotations and synapse_monitor jobs.

### Build

```bash
gcloud builds submit \
  --substitutions=_JOB_NAME=synapse_public_status \
  --config cloudbuild.yaml
```

### Set secrets

```
echo -n "your-snowflake-username" | gcloud secrets create snowflake-user --data-file=-
echo -n "your-snowflake-account"  | gcloud secrets create snowflake-account --data-file=-
echo -n "your-snowflake-pat"      | gcloud secrets create snowflake-pat --data-file=-
```

### Allow Cloud Run access to secrets

```
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

## Synapse Monitor Deployment Example

For the synapse_monitor job, you'll need an additional Slack webhook secret:

### Additional Secrets for Synapse Monitor

```bash
echo -n "your-slack-webhook-url" | gcloud secrets create slack-webhook-url --data-file=-
```

### Build

```bash
gcloud builds submit \
  --substitutions=_JOB_NAME=synapse_monitor \
  --config cloudbuild.yaml
```

### Allow Cloud Run access to Slack webhook secret

```bash
gcloud secrets add-iam-policy-binding slack-webhook-url \
  --member="serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Deploy

```bash
gcloud beta run jobs deploy synapse-monitor \
  --image gcr.io/htan-dcc/synapse_monitor \
  --region=us-central1 \
  --set-secrets "SNOWFLAKE_USER=snowflake-user:latest,SNOWFLAKE_ACCOUNT=snowflake-account:latest,SNOWFLAKE_PAT=snowflake-pat:latest,SLACK_WEBHOOK_URL=slack-webhook-url:latest" \
  --set-env-vars GOOGLE_CLOUD_PROJECT=htan-dcc
```

### Run with custom parameters

```bash
# Run with default 1-day lookback
gcloud beta run jobs execute synapse-monitor --region=us-central1

# Run with 7-day lookback
gcloud beta run jobs execute synapse-monitor --region=us-central1 \
  --overrides='{"spec":{"template":{"spec":{"template":{"spec":{"containers":[{"args":["--days-back","7"]}]}}}}}}'
```

### Check logs

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=synapse-monitor" \
  --limit 50 --format="value(textPayload)"
```
