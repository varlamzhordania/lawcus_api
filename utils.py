import requests
from logger import app_logger as logger
import smtplib
from email.mime.text import MIMEText
import configparser
from datetime import datetime


config = configparser.ConfigParser()
config.read('lawcus_config.ini')


def get_timestamp():
    """
    Get the current date and time as a formatted timestamp.

    :return: Formatted timestamp string (e.g., "2023-01-20_15-45-30")
    """
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


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

    logger.info(f"Checking if table {table_name} exists.")
    return result[0] > 0


def add_prefix_to_keys(data, prefix='my_'):
    """
    Add a prefix to all keys in the contact dictionary.

    :param data: Dictionary representing contact data
    :param prefix: Prefix to be added to keys
    :return: Dictionary with modified keys
    """

    logger.info(f"Added prefix '{prefix}' to keys in dictionary: {data}")
    return {f"{prefix}{key}": value for key, value in data.items()}


def convert_values_to_str(input_dict):
    """
    Convert all values in the dictionary to strings.

    :param input_dict: Input dictionary
    :return: Dictionary with all values converted to strings
    """

    logger.info(f"Converted dictionary values to strings: {input_dict}")
    return {key: str(value) for key, value in input_dict.items()}


def truncate_table(cursor, table_name):
    """
    Truncate (empty) the specified table in the Oracle database.

    Parameters:
    - cursor: The cursor object for executing SQL queries.
    - table_name: The name of the table to be truncated.

    Returns:
    None
    """
    truncate_query = f"TRUNCATE TABLE {table_name}"

    try:
        cursor.execute(truncate_query)
        cursor.connection.commit()
        logger.info(f"Table {table_name} truncated successfully.")
    except Exception as e:
        logger.error(f"Error truncating table {table_name}: {e}")


def get_authorization_code_url(client_id, redirect_uri, state=''):
    """
    Get the URL to redirect the user to for authorization.

    :param client_id: Lawcus client ID
    :param redirect_uri: Redirect URI for OAuth
    :param state: Optional state parameter
    :return: Authorization URL
    """
    authorization_url = f'https://auth.lawcus.com/auth?response_type=code&state={state}&client_id={client_id}&scope=&redirect_uri={redirect_uri}'
    logger.info(f"Generated authorization URL: {authorization_url}")
    return authorization_url


def make_token_request(token_url, data):
    """
    Make a request to the token endpoint.

    :param token_url: URL of the token endpoint
    :param data: Request data
    :return: Token data if successful, None otherwise
    """
    headers = {'Content-Type': 'application/json'}
    response = requests.post(token_url, json=data, headers=headers)

    if response.status_code == 200:
        token_data = response.json()
        logger.info(f"Token request successful: {token_data}")
        return token_data
    else:
        logger.error(f"Token request failed with status code {response.status_code}")
        raise Exception(f"Token request failed with status code {response.status_code}")


def exchange_authorization_code_for_token(client_id, client_secret, redirect_uri, authorization_code):
    """
    Exchange the authorization code for an access token.

    :param client_id: Lawcus client ID
    :param client_secret: Lawcus client secret
    :param redirect_uri: Redirect URI for OAuth
    :param authorization_code: Authorization code obtained after user approval
    :return: Access token and refresh token if successful, None otherwise
    """
    token_url = 'https://auth.lawcus.com/oauth/token'
    data = {
        'code': authorization_code,
        'grant_type': 'authorization_code',
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri
    }

    return make_token_request(token_url, data)


def refresh_access_token(client_id, client_secret, refresh_token, redirect_uri):
    """
    Refresh the access token using the refresh token.

    :param client_id: Lawcus client ID
    :param client_secret: Lawcus client secret
    :param refresh_token: Refresh token obtained after initial token request
    :param redirect_uri: Redirect URI for OAuth
    :return: New access token and refresh token if successful, None otherwise
    """
    token_url = 'https://auth.lawcus.com/oauth/token'
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri
    }

    return make_token_request(token_url, data)


def send_email(subject, message):
    """
    Send an email using SMTP.

    :param subject: Subject of the email
    :param message: Body of the email
    """
    # Email configuration
    from_email = config['Email']['from_email']
    to_email = config['Email']['to_email']
    email_username = config['Email']['email_username']
    email_password = config['Email']['email_password']

    smtp_server = config['SMTP']['smtp_server']
    port = int(config['SMTP']['port'])

    # Create the email message
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    # Connect to the SMTP server and send the email
    with smtplib.SMTP(smtp_server, port) as server:
        # Start TLS encryption
        server.starttls()
        # Login to the email server
        server.login(email_username, email_password)
        # Send the email
        server.sendmail(from_email, to_email, msg.as_string())
