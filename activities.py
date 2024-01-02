from utils import table_exists


def create_activity_time_entry_table(cursor):
    """
    Create a table for Time Entries in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_TIME_ENTRY"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_ACTIVITY_TIME_ENTRY (
            TIME_ENTRY_ID VARCHAR2(4000),
            DATES VARCHAR2(4000),
            DESCRIPTION VARCHAR2(4000),
            TIMESTAMPS VARCHAR2(4000),
            RATE VARCHAR2(4000),
            TOTAL VARCHAR2(4000),
            CREATED_AT VARCHAR2(4000),
            UPDATED_AT VARCHAR2(4000),
            USER_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            INVOICE_ID VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            PAYMENT_DATE VARCHAR2(4000),
            ACTIVITY_ID VARCHAR2(4000),
            ACTIVITY_TASK_ID VARCHAR2(4000),
            REAL_TIMESTAMP VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000),
            CLIENT_ID VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("Activity Time Entry table created successfully.")
    else:
        print("Activity Time Entry table already exists.")


def insert_activity_time_entries_into_table(cursor, time_entries):
    """
    Insert time entry data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_ACTIVITY_TIME_ENTRY (
        TIME_ENTRY_ID, DATES, DESCRIPTION, TIMESTAMPS, RATE, TOTAL,
        CREATED_AT, UPDATED_AT, USER_ID, CREATED_BY, TEAM_ID, INVOICE_ID,
        MATTER_ID, PAYMENT_DATE, ACTIVITY_ID, ACTIVITY_TASK_ID, REAL_TIMESTAMP,
        MATTER_NAME, MATTER_UUID, CLIENT_ID
    ) VALUES (
        :id, :date, :description, :timestamp,
        :rate, :total,:created_at,:updated_at, :user_id, :created_by,
        :team_id, :invoice_id, :matter_id, :payment_date, :activity_id,
        :activity_task_id, :real_timestamp, :mattername, :matteruuid, :client_id
    )
    """

    for entry in time_entries:
        cursor.execute(insert_query, entry)
    print("Time Entries inserted into the table successfully.")


def create_activity_expenses_table(cursor):
    """
    Create a table for Expenses in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_EXPENSES"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_ACTIVITY_EXPENSES (
            EXPENSES_ID VARCHAR2(4000),
            DATES VARCHAR2(4000),
            PUBLIC_NOTE VARCHAR2(4000),
            PRIVATE_NOTE VARCHAR2(4000),
            RATE VARCHAR2(4000),
            QUANTITY VARCHAR2(4000),
            TOTAL VARCHAR2(4000),
            CREATED_AT VARCHAR2(4000),
            UPDATED_AT VARCHAR2(4000),
            USER_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            INVOICE_ID VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            PAYMENT_DATE VARCHAR2(4000),
            FILES VARCHAR2(4000),
            ACTIVITY_ID VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("Activity Expenses table created successfully.")
    else:
        print("Activity Expenses table already exists.")


def insert_activity_expenses_into_table(cursor, expenses):
    """
    Insert expense data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_ACTIVITY_EXPENSES (
        EXPENSES_ID, DATES, PUBLIC_NOTE, PRIVATE_NOTE, RATE, QUANTITY, TOTAL,
        CREATED_AT, UPDATED_AT, USER_ID, CREATED_BY, TEAM_ID, INVOICE_ID,
        MATTER_ID, PAYMENT_DATE, FILES, ACTIVITY_ID, MATTER_NAME, MATTER_UUID
    ) VALUES (
        :id, :date, :public_note,
        :private_note, :rate, :quantity, :total,:created_at,
        :updated_at, :user_id, :created_by,
        :team_id, :invoice_id, :matter_id, :payment_date, :files, :activity_id,
        :mattername, :matteruuid
    )
    """

    for expense in expenses:
        cursor.execute(insert_query, expense)
    print("Expenses inserted into the table successfully.")


def create_activity_flat_fees_table(cursor):
    """
    Create a table for Flat Fees in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_FLAT_FEE"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_ACTIVITY_FLAT_FEE (
            FLAT_FEE_ID VARCHAR2(4000),
            DATES VARCHAR2(4000),
            DESCRIPTION VARCHAR2(4000),
            PRIVATE_NOTE VARCHAR2(4000),
            RATE VARCHAR2(4000),
            TOTAL VARCHAR2(4000),
            QUANTITY VARCHAR2(4000),
            USER_ID VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            INVOICE_ID VARCHAR2(4000),
            ACTIVITY_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            CREATED_AT VARCHAR2(4000),
            UPDATED_AT VARCHAR2(4000),
            ACTIVITY_TASK_ID VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("Activity Flat Fees table created successfully.")
    else:
        print("Activity Flat Fees table already exists.")


def insert_activity_flat_fees_into_table(cursor, flat_fees):
    """
    Insert flat fee data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_ACTIVITY_FLAT_FEE (
        FLAT_FEE_ID, DATES, DESCRIPTION, PRIVATE_NOTE, RATE, TOTAL, QUANTITY,
        USER_ID, MATTER_ID, INVOICE_ID, ACTIVITY_ID, CREATED_BY, TEAM_ID,
        CREATED_AT, UPDATED_AT, ACTIVITY_TASK_ID, MATTER_NAME, MATTER_UUID
    ) VALUES (
        :id, :date, :description,
        :private_note, :rate, :total, :quantity, :user_id, :matter_id,
        :invoice_id, :activity_id, :created_by, :team_id, :created_at,
        :updated_at, :activity_task_id,
        :mattername, :matteruuid
    )
    """

    for flat_fee in flat_fees:
        cursor.execute(insert_query, flat_fee)
    print("Flat Fees inserted into the table successfully.")


def create_activity_category_table(cursor):
    """
    Create a table for Activities Category in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_CATEGORY"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_ACTIVITY_CATEGORY (
            ACTIVITY_CATEGORY_ID VARCHAR2(4000),
            NAME VARCHAR2(4000),
            RATE VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            CREATED_AT VARCHAR2(4000),
            UPDATED_AT VARCHAR2(4000),
            DESCRIPTION VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("Activity Category table created successfully.")
    else:
        print("Activity Category table already exists.")


def insert_activity_category_into_table(cursor, activity_categories):
    """
    Insert activity category data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_ACTIVITY_CATEGORY (
        ACTIVITY_CATEGORY_ID, NAME, RATE, TEAM_ID, CREATED_AT, UPDATED_AT, DESCRIPTION
    ) VALUES (
        :id, :name, :rate, :team_id,:created_at,:updated_at, :description
    )
    """

    for activity_category in activity_categories:
        cursor.execute(insert_query, activity_category)
    print("Activity Categories inserted into the table successfully.")
