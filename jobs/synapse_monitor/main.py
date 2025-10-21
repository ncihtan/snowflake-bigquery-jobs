import json
import logging
import os
import sys
import argparse
import snowflake.connector
import pandas as pd
import dotenv
import requests

# Threshold for switching to condensed format in Slack messages
CONDENSED_FORMAT_THRESHOLD = 20

# Maximum number of user-project combinations to display in condensed format
MAX_USER_PROJECT_COMBINATIONS = 15

# Maximum number of folders to display per user-project summary in Slack messages
MAX_FOLDER_DISPLAY = 5

# Load environment variables from .env file
dotenv.load_dotenv()

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
def format_simple_slack_message(results, days_back=1):
    """
    Create a structured Slack message with summary and blocks for each activity
    """
    if not results:
        day_text = "day" if days_back == 1 else "days"
        return {
            "text": f"🔍 No entities were modified in the last {days_back} {day_text}",
            "username": "HTAN Monitor Bot",
        }

    # Group by user, project, folder, and change type
    user_project_folder_activity = {}
    user_ids = {}  # Track user IDs for profile links
    project_ids = {}  # Track project IDs for Synapse links
    folder_ids = {}  # Track folder IDs for Synapse links

    for row in results:
        (
            file_id,
            file_name,
            change_type,
            modified_by_id,
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
        user_ids[username] = modified_by_id
        project_ids[project_name] = project_id

        # Create key for grouping by user, project, folder, and change type
        folder = parent_name if parent_type == "folder" else "root"

        # Store folder ID for linking (only if it's actually a folder)
        if parent_type == "folder":
            folder_ids[(project_name, folder)] = parent_id

        key = (username, project_name, folder, change_type)

        if key not in user_project_folder_activity:
            user_project_folder_activity[key] = 0

        user_project_folder_activity[key] += 1

    # Calculate summary stats
    total_files = sum(user_project_folder_activity.values())
    unique_users = len(user_ids)
    unique_projects = len(project_ids)
    total_entries = len(user_project_folder_activity)

    # Check if we need to use condensed format
    use_condensed = (
        total_entries > CONDENSED_FORMAT_THRESHOLD
    )  # Threshold for switching to condensed format

    # Build blocks message
    blocks = []

    # Header block with summary
    header_text = f"📊 *HTAN Synapse Activity Report*\nTESTING ONLY\n\n📈 *Summary*: {total_files} items modified by {unique_users} users across {unique_projects} projects"
    if use_condensed:
        header_text += f"\n_High activity detected ({total_entries} combinations). Using condensed format._"

    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": header_text}})

    blocks.append({"type": "divider"})

    if use_condensed:
        # Condensed format: Group by user-project, list folder counts
        user_project_summary = {}

        for (
            username,
            project_name,
            folder,
            change_type,
        ), count in user_project_folder_activity.items():
            user_project_key = (username, project_name)
            if user_project_key not in user_project_summary:
                user_project_summary[user_project_key] = {
                    "folders": {},
                    "change_types": {},
                    "total": 0,
                }

            # Track folder activity
            if folder not in user_project_summary[user_project_key]["folders"]:
                user_project_summary[user_project_key]["folders"][folder] = 0
            user_project_summary[user_project_key]["folders"][folder] += count

            # Track change type activity
            if (
                change_type
                not in user_project_summary[user_project_key]["change_types"]
            ):
                user_project_summary[user_project_key]["change_types"][change_type] = 0
            user_project_summary[user_project_key]["change_types"][change_type] += count

            user_project_summary[user_project_key]["total"] += count

        # Sort by total activity (most active first)
        sorted_summary = sorted(
            user_project_summary.items(), key=lambda x: x[1]["total"], reverse=True
        )

        for (username, project_name), data in sorted_summary[
            :MAX_USER_PROJECT_COMBINATIONS
        ]:  # Limit to top N most active
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

            folder_summary = ", ".join(
                folder_counts[:MAX_FOLDER_DISPLAY]
            )  # Limit to top folders
            if len(data["folders"]) > MAX_FOLDER_DISPLAY:
                folder_summary += (
                    f" (+{len(data['folders'])-MAX_FOLDER_DISPLAY} more folders)"
                )

            # Create change type summary
            change_type_summary = []
            for change_type, count in sorted(
                data["change_types"].items(), key=lambda x: x[1], reverse=True
            ):
                verb = "created" if change_type == "CREATE" else "modified"
                change_type_summary.append(f"{verb} {count}")

            change_summary = ", ".join(change_type_summary)
            activity_text = f"{user_link} {change_summary} items: {folder_summary} of {project_link}"

            blocks.append(
                {"type": "section", "text": {"type": "mrkdwn", "text": activity_text}}
            )

        if len(sorted_summary) > MAX_USER_PROJECT_COMBINATIONS:
            remaining = len(sorted_summary) - MAX_USER_PROJECT_COMBINATIONS
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
        # Standard format: One block per user-project-folder-change_type
        sorted_activities = sorted(
            user_project_folder_activity.items(),
            key=lambda x: (x[1], x[0][0], x[0][1], x[0][2], x[0][3]),
            reverse=True,  # Sort by count first (most active combinations first)
        )

        for (username, project_name, folder, change_type), count in sorted_activities:
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

            # Choose verb based on change type
            verb = "created" if change_type == "CREATE" else "modified"
            activity_text = f"{user_link} {verb} *{count} item{'s' if count != 1 else ''}* in {location}"

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


# Read SQL query from file and substitute parameters
def read_sql_query(file_path, days_back=1):
    """
    Read SQL query from a file and substitute the days parameter
    """
    try:
        with open(file_path, "r") as file:
            query = file.read()

        # Replace the days parameter in the query
        query = query.replace("{DAYS_BACK}", str(days_back))

        logging.info(
            f"SQL query loaded from {file_path} with {days_back} days lookback"
        )
        return query
    except FileNotFoundError:
        logging.error(f"SQL file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Failed to read SQL file: {e}")
        sys.exit(1)


# Main function to orchestrate the workflow
def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="HTAN Synapse Monitor - Track file creation activity across HTAN projects"
    )
    parser.add_argument(
        "--query-file",
        "-q",
        default=None,
        help="Path to SQL query file (default: query.sql in script directory)",
    )
    parser.add_argument(
        "--days-back",
        "-d",
        type=int,
        default=1,
        help="Number of days to look back for activity (default: 1)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Determine query file path
    if args.query_file:
        query_file_path = args.query_file
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        query_file_path = os.path.join(script_dir, "query.sql")

    # Load SQL query from file with days parameter
    snowflake_query = read_sql_query(query_file_path, args.days_back)

    # Login to Snowflake
    conn = login_to_snowflake()
    logging.info("Snowflake connection established.")

    # Run the query in Snowflake
    results = run_snowflake_query(conn, snowflake_query)

    # Print the head of the results for debugging
    logging.info(
        f"Query successfully executed. Number of rows returned: {len(results)}"
    )

    # Print the first 5 rows for debugging
    for row in results[:5]:
        logging.info(row)

    # Format and send Slack message
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook_url:
        slack_message = format_simple_slack_message(results, args.days_back)
        send_slack_message(slack_webhook_url, slack_message)
    else:
        logging.warning(
            "SLACK_WEBHOOK_URL not found in environment variables. Skipping Slack notification."
        )
        # Still show what the message would look like
        slack_message = format_simple_slack_message(results, args.days_back)
        logging.info("Slack message that would be sent:")
        logging.info(json.dumps(slack_message))


if __name__ == "__main__":
    main()
