import json
import logging


# Helper to transform annotations dict to BQ expected format
def transform_annotations(annotations_obj):
    try:
        if isinstance(annotations_obj, str):
            annotations_obj = json.loads(annotations_obj)
        if not isinstance(annotations_obj, dict):
            logging.warning(
                f"Expected dict for annotations_obj but got {type(annotations_obj).__name__}"
            )
            return []
        anns = annotations_obj.get("annotations", {})
        records = []
        for k, v in anns.items():
            val = v.get("value")
            # Ensure value is a list
            if not isinstance(val, list):
                logging.warning(f"Invalid value for key={k}: {val}. Skipping.")
                continue
            records.append({"key": k, "type": v.get("type", "STRING"), "value": val})
        return records
    except Exception as e:
        logging.warning(f"Failed to transform annotations: {e}")
        return []


#!/usr/bin/env python3

# Script that runs a query in snowflake and writes the results to a BigQuery table.

import os
import sys
import logging
import snowflake.connector
from google.cloud import bigquery
import pandas as pd
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()
# Import Synapse client modules

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


# Login to snowflake with PAT
def login_to_snowflake():
    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    pat = os.getenv("SNOWFLAKE_PAT")  # Retrieve PAT from .env file
    logging.info(f"Using user: {user}, account: {account}")
    if not user or not account or not pat:
        logging.error(
            "Missing SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, or SNOWFLAKE_PAT environment variables."
        )
        sys.exit(1)
    try:
        conn = snowflake.connector.connect(
            user=user, account=account, password=pat  # Use PAT for authentication
        )
        logging.info("Successfully connected to Snowflake using PAT.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)


# Run a query in snowflake and return the results
def run_snowflake_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        logging.info("Query executed successfully.")
        return results
    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


# Write results to BigQuery
def write_to_bigquery(results, project_id, dataset_id, table_id):
    try:
        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("project_id", "STRING"),
                bigquery.SchemaField("project_name", "STRING"),
                bigquery.SchemaField("entity_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("component", "STRING"),
                bigquery.SchemaField(
                    "annotations",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("key", "STRING"),
                        bigquery.SchemaField("type", "STRING"),
                        bigquery.SchemaField("value", "STRING", mode="REPEATED"),
                    ],
                ),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = client.load_table_from_json(results, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        logging.info(
            f"Data loaded into BigQuery table {dataset_id}.{table_id} successfully."
        )
    except Exception as e:
        logging.error(f"Failed to write to BigQuery: {e}")
        sys.exit(1)


# Main function to orchestrate the workflow
def main():
    # Define your Snowflake query
    snowflake_query = """
    SELECT
    CONCAT('syn', nl.project_id) AS project_id,
    CASE nl.project_id
        WHEN '21050481' THEN 'HTAN Vanderbilt'
        WHEN '22093319' THEN 'HTAN OHSU'
        WHEN '22123910' THEN 'HTAN HMS'
        WHEN '22124336' THEN 'HTAN BU'
        WHEN '22255320' THEN 'HTAN WUSTL'
        WHEN '22776798' THEN 'HTAN CHOP'
        WHEN '23448901' THEN 'HTAN MSK'
        WHEN '23511954' THEN 'HTAN DFCI'
        WHEN '23511961' THEN 'HTAN Duke'
        WHEN '23511964' THEN 'HTAN Stanford'
        WHEN '23511984' THEN 'HTAN PCAPP'
        WHEN '24984270' THEN 'HTAN TNP SARDANA'
        WHEN '22041595' THEN 'HTAN TNP - TMA'
        WHEN '25555889' THEN 'HTAN SRRS'
        WHEN '20834712' THEN 'HTAN HTAPP'
        WHEN '32596076' THEN 'HTAN Center C'
        WHEN '39058831' THEN 'HTAN Data Flow App'
        WHEN '52861417' THEN 'HTAN TNP CASI'
        ELSE 'Unknown Project'
    END AS project_name,
    CONCAT('syn', nl.ID) AS entity_id,
    nl.NAME,
    nl.ANNOTATIONS:"annotations"."Component"."value"[0]::STRING AS Component,
    nl.ANNOTATIONS,
    ARRAY_SIZE(OBJECT_KEYS(nl.ANNOTATIONS:"annotations")) AS annotation_count
    FROM synapse_data_warehouse.synapse.node_latest nl
    WHERE nl.PROJECT_ID IN (
    '21050481', '22093319', '22123910', '22124336', '22255320',
    '22776798', '23448901', '23511954', '23511961', '23511964',
    '23511984', '24984270', '22041595', '25555889', '20834712',
    '32596076', '39058831', '52861417'
    )
    AND nl.node_type = 'file'
    AND name NOT LIKE 'synapse_storage_manifest_%.csv'
    """

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "htan-dcc")
    dataset_id = "synapse"
    bq_table_id = "synape_annotations"

    # Login to Snowflake
    conn = login_to_snowflake()
    logging.info("Snowflake connection established.")

    # Run the query in Snowflake
    results = run_snowflake_query(conn, snowflake_query)

    # Print the head of thje results for debugging
    logging.info(f"Query sucessfully executed. Number of rows returned: {len(results)}")

    # Convert results to a list of dictionaries for BigQuery
    results_dict = [
        {
            "project_id": row[0],
            "project_name": row[1],
            "entity_id": row[2],
            "name": row[3],
            "component": row[4],
            "annotations": transform_annotations(row[5]),
        }
        for row in results
    ]
    write_to_bigquery(results_dict, project_id, dataset_id, bq_table_id)
    logging.info(
        f"Data successfully written to BigQuery table {project_id}.{dataset_id}.{bq_table_id}."
    )


if __name__ == "__main__":
    main()
