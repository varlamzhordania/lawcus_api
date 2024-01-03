from utils import table_exists, add_prefix_to_keys


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
        :my_id, :my_account_type, :my_name, :my_holder, :my_institution, :my_domicile_branch,
        :my_number, :my_transit_number, :my_swift, :my_currency, :my_balance, :my_created_by,
        :my_team_id, :my_integration_type, :my_connected_with, :my_is_default
    )
    """

    for account in accounts:
        prefixed_content = add_prefix_to_keys(account)

        cursor.execute(
            insert_query,
            my_id=prefixed_content.get("my_id"),
            my_account_type=prefixed_content.get("my_account_type"),
            my_name=prefixed_content.get("my_name"),
            my_holder=prefixed_content.get("my_holder"),
            my_institution=prefixed_content.get("my_institution"),
            my_domicile_branch=prefixed_content.get("my_domicile_branch"),
            my_number=prefixed_content.get("my_number"),
            my_transit_number=prefixed_content.get("my_transit_number"),
            my_swift=prefixed_content.get("my_swift"),
            my_currency=prefixed_content.get("my_currency"),
            my_balance=prefixed_content.get("my_balance"),
            my_created_by=prefixed_content.get("my_created_by"),
            my_team_id=prefixed_content.get("my_team_id"),
            my_integration_type=prefixed_content.get("my_integration_type"),
            my_connected_with=prefixed_content.get("my_connected_with"),
            my_is_default=prefixed_content.get("my_is_default")
        )

    cursor.connection.commit()
    print("Accounts inserted into the table successfully.")
