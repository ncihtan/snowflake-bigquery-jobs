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
