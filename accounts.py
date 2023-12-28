from utils import table_exists


def create_accounts_table(cursor):
    """
    Create a table for Accounts in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "lawkpis_lawcus_accounts"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE lawkpis_lawcus_accounts (
            id VARCHAR2(255),
            account_type VARCHAR2(255),
            name VARCHAR2(255),
            holder VARCHAR2(255),
            institution VARCHAR2(255),
            domicile_branch VARCHAR2(255),
            "number" VARCHAR2(255),
            transit_number VARCHAR2(255),
            swift VARCHAR2(255),
            currency VARCHAR2(255),
            balance NUMBER,
            created_by NUMBER,
            team_id NUMBER,
            integration_type VARCHAR2(255),
            connected_with VARCHAR2(255),
            is_default NUMBER(1,0)
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
    INSERT INTO lawkpis_lawcus_accounts (
        id, account_type, name, holder, institution, domicile_branch, number,
        transit_number, swift, currency, balance, created_by, team_id,
        integration_type, connected_with, is_default
    ) VALUES (
        :id, :account_type, :name, :holder, :institution, :domicile_branch,
        :number, :transit_number, :swift, :currency, :balance, :created_by,
        :team_id, :integration_type, :connected_with, :is_default
    )
    """

    for account in accounts:
        cursor.execute(insert_query, account)
    print("Accounts inserted into the table successfully.")
