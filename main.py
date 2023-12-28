import os
import platform
import oracledb
import requests
from contacts import create_contact_table, insert_contacts_into_table
from accounts import create_accounts_table, insert_accounts_into_table
from interactions import create_interactions_table, insert_interactions_into_table
from leads import create_leads_table, insert_leads_into_table
from matters import create_matters_table, insert_matters_into_table
from tasks import create_tasks_table, insert_tasks_into_table

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


def connect_to_database():
    """
    Connect to Oracle database and return a cursor.

    Replace the placeholder values with your actual database connection details.
    """
    d = None  # default suitable for Linux
    if platform.system() == "Darwin" and platform.machine() == "x86_64":  # macOS
        d = os.environ.get("HOME") + ("/Downloads/instantclient_21_12")
    elif platform.system() == "Windows":
        d = r"C:\oracle\instantclient_21_12"
    oracledb.init_oracle_client(lib_dir=d)
    connection = oracledb.connect(user=DB_USERNAME, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, sid=DB_SID)
    cursor = connection.cursor()
    return cursor


def make_api_request(endpoint, params=None):
    """
    Make a dynamic API request based on the provided endpoint.

    Replace the placeholder API_BASE_URL and API_HEADERS with your actual API details.
    """
    url = f"{API_BASE_URL}/{endpoint}"
    response = requests.get(url, headers=API_HEADERS, params=params)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: Unable to fetch data from {endpoint}. Status code: {response.status_code}")
        return None


def get_api_token():
    # Add your code to get the API token here
    pass


if __name__ == "__main__":
    # Connect to the Oracle database
    oracle_cursor = connect_to_database()

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

    for endpoint, params in endpoints:
        # Make API request for the current endpoint
        endpoint_data = make_api_request(endpoint, params)

        if endpoint_data:
            # Execute the corresponding function based on the endpoint
            if endpoint == "contacts":
                create_contact_table(oracle_cursor)
                insert_contacts_into_table(oracle_cursor, endpoint_data["list"])
            elif endpoint == "matters":
                create_matters_table(oracle_cursor)
                insert_matters_into_table(oracle_cursor, endpoint_data["list"])
            elif endpoint == "leadsources":
                create_leads_table(oracle_cursor)
                insert_leads_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "interactions":
                create_interactions_table(oracle_cursor)
                insert_interactions_into_table(oracle_cursor, endpoint_data["list"])
            elif endpoint == "tasks":
                create_tasks_table(oracle_cursor)
                insert_tasks_into_table(oracle_cursor, endpoint_data)
            elif endpoint == "accounts":
                create_accounts_table(oracle_cursor)
                insert_accounts_into_table(oracle_cursor, endpoint_data)

    # Close the database cursor when done
    oracle_cursor.close()
