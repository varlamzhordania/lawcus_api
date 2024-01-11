from utils import table_exists
from logger import accounts_logger as logger

if not logger:
    from logger import app_logger as logger


def create_accounts_table(cursor):
    """
    Create a table for Accounts in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACCOUNTS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_ACCOUNTS (
                ACCOUNT_ID VARCHAR2(4000),
                ACCOUNT_TYPE VARCHAR2(4000),
                NAME VARCHAR2(4000),
                HOLDER VARCHAR2(4000),
                INSTITUTION VARCHAR2(4000),
                DOMICILE_BRANCH VARCHAR2(4000),
                NUMBERS VARCHAR2(4000),
                TRANSIT_NUMBER VARCHAR2(4000),
                SWIFT VARCHAR2(4000),
                CURRENCY VARCHAR2(4000),
                BALANCE VARCHAR2(4000),
                CREATED_BY VARCHAR2(4000),
                TEAM_ID VARCHAR2(4000),
                INTEGRATION_TYPE VARCHAR2(4000),
                CONNECTED_WITH VARCHAR2(4000),
                IS_DEFAULT VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Accounts table created successfully.")
        else:
            logger.info("Accounts table already exists.")
    except Exception as e:
        logger.error(f"Error creating Accounts table: {e}")


def insert_accounts_into_table(cursor, accounts):
    """
    Insert account data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_ACCOUNTS (
        ACCOUNT_ID, ACCOUNT_TYPE, NAME, HOLDER, INSTITUTION, DOMICILE_BRANCH,
        NUMBERS, TRANSIT_NUMBER, SWIFT, CURRENCY, BALANCE, CREATED_BY,
        TEAM_ID, INTEGRATION_TYPE, CONNECTED_WITH, IS_DEFAULT
    ) VALUES (
        :my_id, :my_account_type, :my_name, :my_holder, :my_institution, :my_domicile_branch,
        :my_number, :my_transit_number, :my_swift, :my_currency, :my_balance, :my_created_by,
        :my_team_id, :my_integration_type, :my_connected_with, :my_is_default
    )
    """

    try:
        accounts_len = len(accounts)
        for account in accounts:
            cursor.execute(
                insert_query,
                my_id=account.get("id"),
                my_account_type=account.get("account_type"),
                my_name=account.get("name"),
                my_holder=account.get("holder"),
                my_institution=account.get("institution"),
                my_domicile_branch=account.get("domicile_branch"),
                my_number=account.get("number"),
                my_transit_number=account.get("transit_number"),
                my_swift=account.get("swift"),
                my_currency=account.get("currency"),
                my_balance=account.get("balance"),
                my_created_by=account.get("created_by"),
                my_team_id=account.get("team_id"),
                my_integration_type=account.get("integration_type"),
                my_connected_with=account.get("connected_with"),
                my_is_default=account.get("is_default")
            )

        cursor.connection.commit()
        logger.info(f"{accounts_len} Accounts inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting Accounts into the table: {e}")
