#!/usr/bin/env python3

# Script that runs a query in snowflake and writes the results to a BigQuery table.

import os
import sys
import logging
import snowflake.connector
from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd

# Import Synapse client modules

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


# Login to snowflake with PAT
def login_to_snowflake():
    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    pat = os.getenv("SNOWFLAKE_PAT")  # Retrieve PAT from .env file
    print(f"Using user: {user}, account: {account}")
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
                bigquery.SchemaField("entity_id", "STRING"),
                bigquery.SchemaField(
                    "has_public_view_registered_user_download_acl", "BOOLEAN"
                ),
                # Add more fields as per your query results
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
        CONCAT ('syn', nl.id) as entity_id,
        --nl.node_type, 
        --nl.benefactor_id, 
        -- nl.is_public, 
        
        -- New colum has_public_view_registered_user_download_acl: 
        -- TRUE if public can VIEW and registered users can DOWNLOAD and READ only.
        -- If both are null it returns null (as file probably not released yet)
        CASE 
            WHEN acl1.access_type IS NULL AND acl2.access_type IS NULL THEN FALSE
            WHEN ARRAY_CONTAINS('READ'::variant, acl1.access_type)
                AND ARRAY_SIZE(acl1.access_type) = 1
                AND ARRAY_CONTAINS('DOWNLOAD'::variant, acl2.access_type)
                AND ARRAY_CONTAINS('READ'::variant, acl2.access_type)
                AND ARRAY_SIZE(acl2.access_type) = 2
            THEN TRUE
            ELSE FALSE
        END AS has_public_view_registered_user_download_acl,
        -- acl1.access_type AS public_access, 
        -- acl2.access_type AS registered_user_access
    FROM SYNAPSE_DATA_WAREHOUSE.SYNAPSE.NODE_LATEST nl
    LEFT JOIN SYNAPSE_DATA_WAREHOUSE.SYNAPSE.ACL_LATEST acl1
        ON nl.benefactor_id = acl1.owner_id AND acl1.principal_id = 273949 -- Public
    LEFT JOIN SYNAPSE_DATA_WAREHOUSE.SYNAPSE.ACL_LATEST acl2
        ON nl.benefactor_id = acl2.owner_id AND acl2.principal_id = 273948 -- Registered Users
    WHERE nl.PROJECT_ID IN (
        '68754852', -- HTAN TEST1
        '68754879', -- HTAN TEST2
        '68754939', -- HTAN TEST3
    )
    AND node_type = 'file'
    AND (
        (acl1.access_type IS NULL AND acl2.access_type IS NOT NULL) -- Registered users only
    OR (acl1.access_type IS NOT NULL AND acl2.access_type IS NULL) -- Public only
    OR (acl1.access_type IS NULL AND acl2.access_type IS NULL)    -- Neither has access
    OR (acl1.access_type IS NOT NULL AND acl2.access_type IS NOT NULL) -- Both have access
    );
    """

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "htan2-dcc")
    dataset_id = "synapse"
    bq_table_id = "synapse_public_status"

    # Login to Snowflake
    conn = login_to_snowflake()
    logging.info("Snowflake connection established.")

    # Run the query in Snowflake
    results = run_snowflake_query(conn, snowflake_query)

    # Print the head of thje results for debugging
    logging.info(f"Query sucessfully executed. Number of rows returned: {len(results)}")

    # Convert results to a list of dictionaries for BigQuery
    results_dict = [
        {"entity_id": row[0], "has_public_view_registered_user_download_acl": row[1]}
        for row in results
    ]
    write_to_bigquery(results_dict, project_id, dataset_id, bq_table_id)
    logging.info(
        f"Data successfully written to BigQuery table {project_id}.{dataset_id}.{bq_table_id}."
    )


if __name__ == "__main__":
    main()
