# Synapse Monitor Function

The `synapse_monitor` script is designed to query data from Snowflake to track file creation and modification activity across HTAN projects and send formatted notifications to Slack.  
This script provides real-time monitoring of data uploads and changes across HTAN projects.

## Features

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

## Workflow

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

## Key Functions

- `login_to_snowflake()`: Establishes a connection to Snowflake
- `run_snowflake_query(conn, query)`: Executes the SQL query and retrieves results
- `read_sql_query(file_path, days_back)`: Loads and parameterizes SQL query from file
- `format_simple_slack_message(results, days_back)`: Creates structured Slack message
- `send_slack_message(webhook_url, message)`: Delivers message to Slack

## Command Line Options

- `--query-file, -q`: Path to SQL query file (default: query.sql in script directory)
- `--days-back, -d`: Number of days to look back for activity (default: 1)
- `--verbose, -v`: Enable verbose logging

## Configuration Thresholds

- **Condensed format threshold**: 20 user-project-folder-change type combinations
- **Max user-project combinations**: 15 in condensed view
- **Max folders displayed**: 5 per user-project summary

## Deployment

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