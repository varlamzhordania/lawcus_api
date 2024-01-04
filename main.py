import os
import platform
import oracledb
import requests
from logger import logger
from utils import make_token_request, exchange_authorization_code_for_token, get_authorization_code_url, truncate_table
from contacts import create_contact_table, insert_contacts_into_table
from accounts import create_accounts_table, insert_accounts_into_table
from interactions import create_interactions_table, insert_interactions_into_table
from leads import create_leads_table, insert_leads_into_table
from matters import create_matters_table, insert_matters_into_table, create_matter_assignees_table, \
    create_matter_tags_table, create_matter_custom_field_table
from tasks import create_tasks_table, insert_tasks_into_table
from activities import create_activity_category_table, create_activity_flat_fees_table, \
    insert_activity_flat_fees_into_table, insert_activity_time_entries_into_table, create_activity_time_entry_table, \
    create_activity_expenses_table, insert_activity_category_into_table, insert_activity_expenses_into_table, \
    insert_activity_billing_items_into_table, create_activity_billing_items_table
from users import create_users_me_table, create_users_team_table, create_users_matters_table, \
    insert_users_matters_into_table, insert_users_me_into_table, create_users_contacts_table, \
    create_users_teammates_table, insert_users_teammates_into_table, insert_users_contacts_into_table, \
    insert_users_team_into_table, create_users_matter_tags_table, create_users_matter_custom_field_table, \
    create_users_matter_assignees_table
from reports import create_reports_matters_info_table, insert_reports_matters_info_into_table, \
    insert_reports_accounts_receivable_into_table, create_reports_client_trust_table, create_reports_time_entries_table, \
    insert_reports_client_trust_into_table, insert_reports_matter_balance_into_table, \
    insert_reports_payment_collected_into_table, create_reports_client_ledger_table, \
    insert_reports_invoice_history_into_table, create_reports_accounts_receivable_table, \
    insert_reports_time_entries_into_table, create_reports_trust_ledger_table, insert_reports_client_ledger_into_table, \
    create_reports_payment_collected_table, create_reports_invoice_history_table, create_reports_matter_balance_table, \
    create_reports_revenue_table, insert_reports_revenue_into_table, insert_reports_trust_ledger_into_table


def configure_oracle_client():
    """
    Configure the Oracle client with the appropriate library directory.

    Replace the placeholder values with your actual library directory.
    """
    d = None  # default suitable for Linux
    if platform.system() == "Darwin" and platform.machine() == "x86_64":  # macOS
        d = os.environ.get("HOME") + ("/Downloads/instantclient_21_12")
    elif platform.system() == "Windows":
        d = r"C:\oracle\instantclient_21_12"
    oracledb.init_oracle_client(lib_dir=d)


def connect_to_database(username, password, host, port, service_id):
    """
    Connect to Oracle database and return a cursor.

    :param username: Database username
    :param password: Database password
    :param host: Database host
    :param port: Database port
    :param service_id: Database SID

    :return: Database cursor
    """
    try:
        configure_oracle_client()
        connection = oracledb.connect(user=username, password=password, host=host, port=port, sid=service_id)
        cursor = connection.cursor()
        return cursor
    except oracledb.DatabaseError as e:
        logger.error(f"Error connecting to the database: {e}")
        raise
    except Exception as e:
        logger.error(f"Error occurred during database connection: {e}")
        raise


def make_api_request(endpoint, params=None, base_url=None, headers=None, **kwargs):
    """
    Make a dynamic API request based on the provided endpoint.

    :param endpoint: The API endpoint
    :param params: Parameters to include in the request
    :param base_url: Base URL for the API
    :param headers: Headers for the request
    :param kwargs: Additional arguments for requests.get

    :return: JSON data if the request is successful, None otherwise
    """
    url = f"{base_url}/{endpoint}" if base_url else endpoint
    headers = headers or {}

    try:
        response = requests.get(url, headers=headers, params=params, **kwargs)
        response.raise_for_status()  # Raises HTTPError for bad responses
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request to {endpoint}: {e}")
        return None


if __name__ == "__main__":
    # Placeholder database connection parameters
    DB_USERNAME = "root"  # Replace with your DB username
    DB_PASSWORD = "1234"  # Replace with your DB password
    DB_HOST = "localhost"  # Replace with your DB host
    DB_PORT = 1521
    DB_SID = "xe"  # Replace with your DB SID

    # DB_USERNAME = "Lawcus_Source"
    # DB_PASSWORD = "thidlwk2lerierol33"
    # DB_HOST = " eztestprod.ctv3czkgd3ce.us-west-2.rds.amazonaws.com"
    # DB_PORT = 1526
    # DB_SID = "eztest"

    client_id = '03d45512d01041eea358c505bdbfe4a2'
    redirect_uri = 'https://www.lawkpis.com/oauth'

    state = ''  # Optional
    authorization_code_url = get_authorization_code_url(client_id, redirect_uri, state)
    logger.info(f"Redirect the user to: {authorization_code_url}")
    print(f"Get authorization code from this URL: {authorization_code_url}")

    static_auth_code = "b0417aa0aa1d11eea53a2b13a8761e06"

    authorization_code = input(
        "Enter the authorization code(leave it empty to use default): ", ) or static_auth_code

    client_secret = '81f88e344bad4b68ad0e8d98dafdb0a56bec332fa2cd4172b97391b1f621cb55'

    token_data = exchange_authorization_code_for_token(
        client_id,
        client_secret,
        redirect_uri,
        authorization_code
    )

    access_token = token_data.get("access_token")

    # Placeholder API endpoint and headers
    API_BASE_URL = "https://api.us.lawcus.com"
    API_ACCESS_TOKEN = access_token  # Replace with your actual API access token
    API_HEADERS = {
        "Authorization": f"Oauth Bearer {API_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    # Connect to the Oracle database
    oracle_cursor = connect_to_database(
        username=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        service_id=DB_SID
    )

    # Define the endpoints and parameters you need to call
    endpoints = [
        ("contacts", None),
        ("matters", None),
        ("leadsources", None),
        (
            "interactions",
            {
                "types": '["PHONE","EMAIL","SECURE_MESSAGE"]',
                "sub_types": '["INBOUND","OUTBOUND"]',
                "skip": 0,
                "paginate": 1000
            }
        ),
        ("tasks", None),
        ("accounts", None),
        ("timeentries", None),
        ("expenses", None),
        ("flatfees", None),
        ("activities", None),  # alias Categories
        (
            "reports/payment-collected",
            {
                "user_id": "",
                "practice_id": "",
                "client_id": "",
                "matter_id": "",
                "start": "",
                "end": "",
                "selected_accounts": ""
            }
        ),
        (
            "reports/invoice-history",
            {
                "user_id": "",
                "group_by": "",
                "practice_id": "",
                "client_id": "",
                "start": "",
                "end": ""
            }
        ),
        (
            "reports/matter-balance",
            {
                "user_id": "",
                "group_by": "",
                "practice_id": "",
                "client_id": "",
                "start": "",
                "end": "",
                "trust": ""
            }
        ),
        (
            "reports/client-trust",
            {
                "client_id": "",
                "start": "",
                "end": "",
                "display_zero": "",
                "practice_id": "",
            }
        ),
        (
            "reports/client-ledger",
            {
                "practice_id": "",
                "client_id": "",
                "start": "",
                "end": "",
            }
        ),
        (
            "reports/trust-ledger",
            {
                "account_id": "",
                "practice_id": "",
                "client_id": "",
                "matter_id": "",
                "start": "",
                "end": "",
                "display_zero": "",
            }
        ),
        (
            "reports/time-entries",
            {
                "group_by": "",
                "user_id": "",
                "matter_id": "",
                "start": "",
                "end": "",
                "status": "",
            }
        ),
        (
            "reports/revenue",
            {
                "matter_id": "",
                "client_id": "",
                "user_id": "",
                "start": "",
                "end": "",
            }
        ),
        (
            "reports/accounts-receivable",
            {
                "user_id": "",
                "group_by": "",
                "practice_id": "",
                "client_id": "",
                "matter_id": "",
                "start": "",
                "end": "",
            }
        ),
        ("reports/matters/info", None),
        ("users/me", None),
        ("users/teammates", None),
        ("users/team", None),
        (
            "users/data/contacts",
            {
                "take": "",
                "skip": "",
                "updated_after": "",  # {{YYYY-MM-DD HH:MM:SS}}
            }
        ),
        (
            "users/data/matters",
            {
                "take": "",
                "skip": "",
                "status": "",  # OPEN , LEAD , ARCHIVED , NOT_HIRED
                "updated_after": "",  # YYYY-MM-DD HH:MM:SS
            }
        ),
    ]

    for endpoint, params in endpoints:
        # Make API request for the current endpoint
        endpoint_data = make_api_request(endpoint=endpoint, params=params, base_url=API_BASE_URL, headers=API_HEADERS)
        if endpoint_data:
            # Execute the corresponding function based on the endpoint
            if endpoint == "contacts":
                create_contact_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_CONTACTS")
                insert_contacts_into_table(oracle_cursor, endpoint_data["list"])
            elif endpoint == "matters":
                create_matters_table(oracle_cursor)
                create_matter_assignees_table(oracle_cursor)
                create_matter_custom_field_table(oracle_cursor)
                create_matter_tags_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_MATTERS")
                truncate_table(oracle_cursor, "LAWCUS_MATTER_ASSIGNEES")
                truncate_table(oracle_cursor, "LAWCUS_MATTER_CUSTOM_FIELD")
                truncate_table(oracle_cursor, "LAWCUS_MATTER_TAGS")
                insert_matters_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "leadsources":
                create_leads_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_LEADSOURCES")
                insert_leads_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "interactions":
                create_interactions_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_INTERACTIONS")
                insert_interactions_into_table(oracle_cursor, endpoint_data["list"])
            elif endpoint == "tasks":
                create_tasks_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_TASKS")
                insert_tasks_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "accounts":
                create_accounts_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_ACCOUNTS")
                insert_accounts_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "timeentries":
                create_activity_time_entry_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_ACTIVITY_TIME_ENTRY")
                insert_activity_time_entries_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "expenses":
                create_activity_expenses_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_ACTIVITY_EXPENSES")
                insert_activity_expenses_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "flatfees":
                create_activity_flat_fees_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_ACTIVITY_FLAT_FEE")
                insert_activity_flat_fees_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "activities":
                create_activity_category_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_ACTIVITY_CATEGORY")
                insert_activity_category_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/payment-collected":
                create_reports_payment_collected_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_COLLECTED")
                insert_reports_payment_collected_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/invoice-history":
                create_reports_invoice_history_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_INVOICE_HISTORY")
                insert_reports_invoice_history_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/matter-balance":
                create_reports_matter_balance_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_MATTER_BALANCE")
                # insert_reports_matter_balance_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/client-trust":
                create_reports_client_trust_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_CLIENT_TRUST")
                insert_reports_client_trust_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/client-ledger":
                create_reports_client_ledger_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_CLIENT_LEDGER")
                insert_reports_client_ledger_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/trust-ledger":
                create_reports_trust_ledger_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_TRUST_LEDGER")
                insert_reports_trust_ledger_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/time-entries":
                create_reports_time_entries_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_TIME_ENTRIES")
                insert_reports_time_entries_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/revenue":
                create_reports_revenue_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_REVENUE")
                insert_reports_revenue_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/accounts-receivable":
                create_reports_accounts_receivable_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_RECEIVABLE")
                insert_reports_accounts_receivable_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "reports/matters/info":
                create_reports_matters_info_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_REPORTS_MATTERS_INFO")
                insert_reports_matters_info_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "users/me":
                create_users_me_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_USERS_ME")
                insert_users_me_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "users/teammates":
                create_users_teammates_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_USERS_TEAMMATES")
                insert_users_teammates_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "users/team":
                create_users_team_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_USERS_TEAM")
                insert_users_team_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "users/data/contacts":
                create_users_contacts_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_USERS_CONTACTS")
                insert_users_contacts_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "users/data/matters":
                create_users_matters_table(oracle_cursor)
                create_users_matter_tags_table(oracle_cursor)
                create_users_matter_custom_field_table(oracle_cursor)
                create_users_matter_assignees_table(oracle_cursor)
                truncate_table(oracle_cursor, "LAWCUS_USERS_MATTERS")
                truncate_table(oracle_cursor, "LAWCUS_USERS_MATTER_ASSIGNEES")
                truncate_table(oracle_cursor, "LAWCUS_USERS_MATTER_CF")
                truncate_table(oracle_cursor, "LAWCUS_USERS_MATTER_TAGS")
                insert_users_matters_into_table(oracle_cursor, endpoint_data)

    print("Script successfully completed. Please check the logs for more information.")
    # Close the database cursor when done
    oracle_cursor.close()
