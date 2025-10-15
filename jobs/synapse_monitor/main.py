import json
import logging
import os
import sys
import snowflake.connector
import pandas as pd
import dotenv
import requests

# Load environment variables from .env file
dotenv.load_dotenv()


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


# Format results into a simple Slack message
def format_simple_slack_message(results):
    """
    Create a structured Slack message with summary and blocks for each activity
    """
    if not results:
        return {
            "text": "🔍 HTAN Monitor Bot: No new activity in the last 24 hours",
            "username": "HTAN Monitor Bot",
        }

    # Group by user, project, and folder
    user_project_folder_activity = {}
    user_ids = {}  # Track user IDs for profile links
    project_ids = {}  # Track project IDs for Synapse links
    folder_ids = {}  # Track folder IDs for Synapse links

    for row in results:
        (
            file_id,
            file_name,
            created_on,
            created_by_id,
            username,
            annotation_count,
            project_id,
            project_name,
            benefactor_id,
            parent_id,
            parent_name,
            parent_type,
        ) = row

        # Store user and project IDs for links
        user_ids[username] = created_by_id
        project_ids[project_name] = project_id

        # Create key for grouping by user, project, and folder
        folder = parent_name if parent_type == "folder" else "root"

        # Store folder ID for linking (only if it's actually a folder)
        if parent_type == "folder":
            folder_ids[(project_name, folder)] = parent_id

        key = (username, project_name, folder)

        if key not in user_project_folder_activity:
            user_project_folder_activity[key] = 0

        user_project_folder_activity[key] += 1

    # Calculate summary stats
    total_files = sum(user_project_folder_activity.values())
    unique_users = len(user_ids)
    unique_projects = len(project_ids)
    total_entries = len(user_project_folder_activity)

    # Check if we need to use condensed format
    use_condensed = total_entries > 20  # Threshold for switching to condensed format

    # Build blocks message
    blocks = []

    # Header block with summary
    header_text = f"📊 *HTAN Synapse Activity Report*\nTESTING ONLY\n\n📈 *Summary*: {total_files} items created by {unique_users} users across {unique_projects} projects"
    if use_condensed:
        header_text += f"\n_High activity detected ({total_entries} folder combinations). Using condensed format._"

    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": header_text}})

    blocks.append({"type": "divider"})

    if use_condensed:
        # Condensed format: Group by user-project, list folder counts
        user_project_summary = {}

        for (
            username,
            project_name,
            folder,
        ), count in user_project_folder_activity.items():
            user_project_key = (username, project_name)
            if user_project_key not in user_project_summary:
                user_project_summary[user_project_key] = {"folders": {}, "total": 0}

            user_project_summary[user_project_key]["folders"][folder] = count
            user_project_summary[user_project_key]["total"] += count

        # Sort by total activity (most active first)
        sorted_summary = sorted(
            user_project_summary.items(), key=lambda x: x[1]["total"], reverse=True
        )

        for (username, project_name), data in sorted_summary[
            :15
        ]:  # Limit to top 15 most active
            user_link = (
                f"<https://www.synapse.org/Profile:{user_ids[username]}|{username}>"
            )
            project_link = f"<https://www.synapse.org/Synapse:syn{project_ids[project_name]}|{project_name}>"

            folder_counts = []
            for folder, count in sorted(
                data["folders"].items(), key=lambda x: x[1], reverse=True
            ):
                if folder == "root":
                    folder_counts.append(f"{count} in root")
                else:
                    # Create folder link if we have the folder ID
                    folder_key = (project_name, folder)
                    if folder_key in folder_ids:
                        folder_link = f"<https://www.synapse.org/Synapse:syn{folder_ids[folder_key]}|{folder}>"
                        folder_counts.append(f"{count} in _{folder_link}_")
                    else:
                        folder_counts.append(f"{count} in _{folder}_")

            folder_summary = ", ".join(folder_counts[:5])  # Limit to top 5 folders
            if len(data["folders"]) > 5:
                folder_summary += f" (+{len(data['folders'])-5} more folders)"

            activity_text = f"{user_link} created *{data['total']} items*: {folder_summary} of {project_link}"

            blocks.append(
                {"type": "section", "text": {"type": "mrkdwn", "text": activity_text}}
            )

        if len(sorted_summary) > 15:
            remaining = len(sorted_summary) - 15
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"_{remaining} more user-project combinations..._",
                    },
                }
            )

    else:
        # Standard format: One block per user-project-folder
        sorted_activities = sorted(
            user_project_folder_activity.items(),
            key=lambda x: (x[1], x[0][0], x[0][1], x[0][2]),
            reverse=True,  # Sort by count first (most active folders first)
        )

        for (username, project_name, folder), count in sorted_activities:
            user_link = (
                f"<https://www.synapse.org/Profile:{user_ids[username]}|{username}>"
            )
            project_link = f"<https://www.synapse.org/Synapse:syn{project_ids[project_name]}|{project_name}>"

            if folder == "root":
                location = f"the {project_link} project"
            else:
                # Create folder link if we have the folder ID
                folder_key = (project_name, folder)
                if folder_key in folder_ids:
                    folder_link = f"<https://www.synapse.org/Synapse:syn{folder_ids[folder_key]}|{folder}>"
                    location = f"_{folder_link}_ of the {project_link} project"
                else:
                    location = f"_{folder}_ of the {project_link} project"

            activity_text = f"{user_link} created *{count} item{'s' if count != 1 else ''}* in {location}"

            blocks.append(
                {"type": "section", "text": {"type": "mrkdwn", "text": activity_text}}
            )

    return {
        "blocks": blocks,
        "username": "HTAN Monitor Bot",
        "icon_emoji": ":bar_chart:",
    }


# Send message to Slack webhook
def send_slack_message(webhook_url, message):
    """
    Send a message to Slack via webhook
    """
    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
        logging.info("Slack message sent successfully")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Slack message: {e}")
        return False


# Read SQL query from file
def read_sql_query(file_path):
    """
    Read SQL query from a file
    """
    try:
        with open(file_path, "r") as file:
            query = file.read()
        logging.info(f"SQL query loaded from {file_path}")
        return query
    except FileNotFoundError:
        logging.error(f"SQL file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Failed to read SQL file: {e}")
        sys.exit(1)


# Main function to orchestrate the workflow
def main():
    # Load SQL query from file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    query_file_path = os.path.join(script_dir, "query.sql")
    snowflake_query = read_sql_query(query_file_path)

    # Login to Snowflake
    conn = login_to_snowflake()
    logging.info("Snowflake connection established.")

    # Run the query in Snowflake
    results = run_snowflake_query(conn, snowflake_query)

    # Print the head of thje results for debugging
    logging.info(f"Query sucessfully executed. Number of rows returned: {len(results)}")

    # Print the first 5 rows for debugging
    for row in results[:5]:
        logging.info(row)

    # Format and send Slack message
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook_url:
        slack_message = format_simple_slack_message(results)
        send_slack_message(slack_webhook_url, slack_message)
    else:
        logging.warning(
            "SLACK_WEBHOOK_URL not found in environment variables. Skipping Slack notification."
        )
        # Still show what the message would look like
        slack_message = format_simple_slack_message(results)
        logging.info("Slack message that would be sent:")
        logging.info(slack_message["text"])


if __name__ == "__main__":
    main()
