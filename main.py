import os
import platform
import oracledb
import requests
import logging
from contacts import create_contact_table, insert_contacts_into_table
from accounts import create_accounts_table, insert_accounts_into_table
from interactions import create_interactions_table, insert_interactions_into_table
from leads import create_leads_table, insert_leads_into_table
from matters import create_matters_table, insert_matters_into_table
from tasks import create_tasks_table, insert_tasks_into_table

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def get_api_token():
    pass


def create_all_tables():
    create_contact_table(oracle_cursor)
    create_matters_table(oracle_cursor)
    create_leads_table(oracle_cursor)
    create_interactions_table(oracle_cursor)
    create_tasks_table(oracle_cursor)
    create_accounts_table(oracle_cursor)


if __name__ == "__main__":
    # Placeholder database connection parameters
    DB_USERNAME = "root"  # Replace with your DB username
    DB_PASSWORD = "1234"  # Replace with your DB password
    DB_HOST = "localhost"  # Replace with your DB host
    DB_PORT = 1521
    DB_SID = "xe"  # Replace with your DB SID

    # Placeholder API endpoint and headers
    API_BASE_URL = "https://api.us.lawcus.com"
    API_ACCESS_TOKEN = ""  # Replace with your actual API access token
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
        ("interactions",
         {"types": '["PHONE","EMAIL","SECURE_MESSAGE"]', "sub_types": '["INBOUND","OUTBOUND"]', "skip": 0,
          "paginate": 20}),
        ("tasks", None),
        ("accounts", None),
    ]

    # for endpoint, params in endpoints:
    #     # Make API request for the current endpoint
    #     endpoint_data = make_api_request(endpoint=endpoint, params=params, base_url=API_BASE_URL, headers=API_HEADERS)

    # if endpoint_data:
    #     # Execute the corresponding function based on the endpoint
    #     if endpoint == "contacts":
    #         create_contact_table(oracle_cursor)
    #         insert_contacts_into_table(oracle_cursor, endpoint_data["list"])
    #     elif endpoint == "matters":
    #         create_matters_table(oracle_cursor)
    #         insert_matters_into_table(oracle_cursor, endpoint_data["list"])
    #     elif endpoint == "leadsources":
    #         create_leads_table(oracle_cursor)
    #         insert_leads_into_table(oracle_cursor, endpoint_data)
    #     elif endpoint == "interactions":
    #         create_interactions_table(oracle_cursor)
    #         insert_interactions_into_table(oracle_cursor, endpoint_data["list"])
    #     elif endpoint == "tasks":
    #         create_tasks_table(oracle_cursor)
    #         insert_tasks_into_table(oracle_cursor, endpoint_data)
    #     elif endpoint == "accounts":
    #         create_accounts_table(oracle_cursor)
    #         insert_accounts_into_table(oracle_cursor, endpoint_data)

    create_all_tables()

    # Close the database cursor when done
    oracle_cursor.close()
