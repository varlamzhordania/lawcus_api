import cx_Oracle
import requests
from contact import create_contact_table, insert_contacts_into_table

# Placeholder database connection parameters
DB_USERNAME = "your_db_username"
DB_PASSWORD = "your_db_password"
DB_HOST = "your_db_host"
DB_PORT = "your_db_port"
DB_SERVICE_NAME = "your_db_service_name"

# Placeholder API endpoint and headers
API_BASE_URL = "https://api.us.lawcus.com"
API_HEADERS = {
    "Authorization": "Bearer your_api_token",
    "Content-Type": "application/json",
}


def connect_to_database():
    """
    Connect to Oracle database and return a cursor.

    Replace the placeholder values with your actual database connection details.
    """
    dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, service_name=DB_SERVICE_NAME)
    connection = cx_Oracle.connect(DB_USERNAME, DB_PASSWORD, dsn=dsn)
    cursor = connection.cursor()
    return cursor


def make_api_request(endpoint):
    """
    Make a dynamic API request based on the provided endpoint.

    Replace the placeholder API_BASE_URL and API_HEADERS with your actual API details.
    """
    url = f"{API_BASE_URL}/{endpoint}"
    response = requests.get(url, headers=API_HEADERS)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: Unable to fetch data from {endpoint}. Status code: {response.status_code}")
        return None


def table_exists(cursor, table_name):
    """
    Check if a table already exists in the Oracle database.

    :param cursor: Oracle database cursor
    :param table_name: Name of the table to check
    :return: True if the table exists, False otherwise
    """
    check_query = f"SELECT count(*) FROM all_tables WHERE table_name = '{table_name.upper()}'"
    cursor.execute(check_query)
    result = cursor.fetchone()

    return result[0] > 0


if __name__ == "__main__":
    # Connect to the Oracle database
    oracle_cursor = connect_to_database()

    # Make API request for the Contacts endpoint
    contacts_data = make_api_request("contacts")

    if contacts_data:
        # Insert Contacts data into the Oracle database
        insert_contacts_into_table(oracle_cursor, contacts_data["list"])

    # Close the database cursor when done
    oracle_cursor.close()
