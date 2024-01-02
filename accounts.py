from utils import table_exists


def create_accounts_table(cursor):
    """
    Create a table for Accounts in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACCOUNTS"

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
        print("Accounts table created successfully.")
    else:
        print("Accounts table already exists.")


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
        :id, :account_type, :name, :holder, :institution, :domicile_branch,
        :number, :transit_number, :swift, :currency, :balance, :created_by,
        :team_id, :integration_type, :connected_with, :is_default
    )
    """

    for account in accounts:
        cursor.execute(insert_query, account)
    print("Accounts inserted into the table successfully.")
