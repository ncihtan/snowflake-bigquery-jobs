# Snowflake Bigquery Jobs

## Deployment

In this example we deploy the synapse_public_status job

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


## 🔁 Scheduled Execution: Daily at 1:00 AM PT

This Cloud Run job (`synapse-public-status`) is configured to run daily at **1:00 AM Pacific Time** via **Cloud Scheduler + Pub/Sub + Eventarc**.

### ✅ One-Time Setup

Run these commands from the root of your repo or Cloud Shell:

Enable required APIs

```bash
gcloud services enable \
  run.googleapis.com \
  scheduler.googleapis.com \
  pubsub.googleapis.com
```

Create a Pub/Sub topic
```
gcloud pubsub topics create trigger-synapse-public-status
```
Grant Cloud Scheduler permission to invoke Cloud Run

```
gcloud run jobs add-invoker-policy-binding synapse-public-status \
  --region=us-central1 \
  --member="serviceAccount:cloud-scheduler@$(gcloud config get-value project).iam.gserviceaccount.com"
```

Create a Cloud Scheduler job to publish to Pub/Sub daily at 1:00 AM PT (9:00 UTC)

```
gcloud scheduler jobs create pubsub trigger-synapse-public-status \
  --schedule="0 9 * * *" \
  --time-zone="America/Los_Angeles" \
  --topic=trigger-synapse-public-status \
  --message-body="{}"
```

Create an Eventarc trigger to launch the Cloud Run job when Pub/Sub is invoked

```
gcloud eventarc triggers create run-synapse-public-status \
  --location=us-central1 \
  --destination-run-job=synapse-public-status \
  --destination-run-region=us-central1 \
  --event-filters="type=google.cloud.pubsub.topic.v1.messagePublished" \
  --transport-topic=trigger-synapse-public-status \
  --service-account="cloud-scheduler@$(gcloud config get-value project).iam.gserviceaccount.com"
```

> 🛠️ These commands assume your job is deployed in `us-central1` and named `synapse-public-status`. Adjust names or regions as needed.
```