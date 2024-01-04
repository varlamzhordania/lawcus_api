from utils import table_exists
from logger import logger


def create_activity_time_entry_table(cursor):
    """
    Create a table for Time Entries in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_TIME_ENTRY"

    try:
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
            logger.info("Activity Time Entry table created successfully.")
        else:
            logger.info("Activity Time Entry table already exists.")
    except Exception as e:
        logger.error(f"Error creating Activity Time Entry table: {e}")


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
        :my_id, :my_date, :my_description, :my_timestamp,
        :my_rate, :my_total, :my_created_at, :my_updated_at, :my_user_id, 
        :my_created_by, :my_team_id, :my_invoice_id, :my_matter_id, 
        :my_payment_date, :my_activity_id, :my_activity_task_id, 
        :my_real_timestamp, :my_mattername, :my_matteruuid, :my_client_id
    )
    """

    try:
        for entry in time_entries:
            cursor.execute(
                insert_query,
                my_id=entry.get("id"),
                my_date=entry.get("date"),
                my_description=entry.get("description"),
                my_timestamp=entry.get("timestamp"),
                my_rate=entry.get("rate"),
                my_total=entry.get("total"),
                my_created_at=entry.get("created_at"),
                my_updated_at=entry.get("updated_at"),
                my_user_id=entry.get("user_id"),
                my_created_by=entry.get("created_by"),
                my_team_id=entry.get("team_id"),
                my_invoice_id=entry.get("invoice_id"),
                my_matter_id=entry.get("matter_id"),
                my_payment_date=entry.get("payment_date"),
                my_activity_id=entry.get("activity_id"),
                my_activity_task_id=entry.get("activity_task_id"),
                my_real_timestamp=entry.get("real_timestamp"),
                my_mattername=entry.get("mattername"),
                my_matteruuid=entry.get("matteruuid"),
                my_client_id=entry.get("client_id")
            )
        cursor.connection.commit()
        logger.info("Time Entries inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting Time Entries into the table: {e}")


def create_activity_expenses_table(cursor):
    """
    Create a table for Expenses in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_EXPENSES"

    try:
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
            logger.info("Activity Expenses table created successfully.")
        else:
            logger.info("Activity Expenses table already exists.")
    except Exception as e:
        logger.error(f"Error creating Activity Expenses table: {e}")


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
        :my_id, :my_date, :my_public_note,
        :my_private_note, :my_rate, :my_quantity, :my_total, :my_created_at,
        :my_updated_at, :my_user_id, :my_created_by,
        :my_team_id, :my_invoice_id, :my_matter_id, :my_payment_date, 
        :my_files, :my_activity_id, :my_mattername, :my_matteruuid
    )
    """

    try:
        for expense in expenses:
            # Ensure that parameter names match the bind variables in the query
            cursor.execute(
                insert_query,
                my_id=expense.get("id"),
                my_date=expense.get("date"),
                my_public_note=expense.get("public_note"),
                my_private_note=expense.get("private_note"),
                my_rate=expense.get("rate"),
                my_quantity=expense.get("quantity"),
                my_total=expense.get("total"),
                my_created_at=expense.get("created_at"),
                my_updated_at=expense.get("updated_at"),
                my_user_id=expense.get("user_id"),
                my_created_by=expense.get("created_by"),
                my_team_id=expense.get("team_id"),
                my_invoice_id=expense.get("invoice_id"),
                my_matter_id=expense.get("matter_id"),
                my_payment_date=expense.get("payment_date"),
                my_files=expense.get("files"),
                my_activity_id=expense.get("activity_id"),
                my_mattername=expense.get("mattername"),
                my_matteruuid=expense.get("matteruuid")
            )
        cursor.connection.commit()
        logger.info("Expenses inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting Expenses into the table: {e}")


def create_activity_flat_fees_table(cursor):
    """
    Create a table for Flat Fees in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_FLAT_FEE"

    try:
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
            logger.info("Activity Flat Fees table created successfully.")
        else:
            logger.info("Activity Flat Fees table already exists.")
    except Exception as e:
        logger.error(f"Error creating Activity Flat Fees table: {e}")


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
        :my_id, :my_date, :my_description,
        :my_private_note, :my_rate, :my_total, :my_quantity, :my_user_id,
        :my_matter_id, :my_invoice_id, :my_activity_id, :my_created_by, 
        :my_team_id, :my_created_at, :my_updated_at, :my_activity_task_id,
        :my_mattername, :my_matteruuid
    )
    """

    try:
        for flat_fee in flat_fees:
            # Ensure that parameter names match the bind variables in the query
            cursor.execute(
                insert_query,
                my_id=flat_fee.get("id"),
                my_date=flat_fee.get("date"),
                my_description=flat_fee.get("description"),
                my_private_note=flat_fee.get("private_note"),
                my_rate=flat_fee.get("rate"),
                my_total=flat_fee.get("total"),
                my_quantity=flat_fee.get("quantity"),
                my_user_id=flat_fee.get("user_id"),
                my_matter_id=flat_fee.get("matter_id"),
                my_invoice_id=flat_fee.get("invoice_id"),
                my_activity_id=flat_fee.get("activity_id"),
                my_created_by=flat_fee.get("created_by"),
                my_team_id=flat_fee.get("team_id"),
                my_created_at=flat_fee.get("created_at"),
                my_updated_at=flat_fee.get("updated_at"),
                my_activity_task_id=flat_fee.get("activity_task_id"),
                my_mattername=flat_fee.get("mattername"),
                my_matteruuid=flat_fee.get("matteruuid")
            )
        cursor.connection.commit()
        logger.info("Flat Fees inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting Flat Fees into the table: {e}")


def create_activity_category_table(cursor):
    """
    Create a table for Activities Category in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_CATEGORY"

    try:
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
            logger.info("Activity Category table created successfully.")
        else:
            logger.info("Activity Category table already exists.")
    except Exception as e:
        logger.error(f"Error creating Activity Category table: {e}")


def insert_activity_category_into_table(cursor, activity_categories):
    """
    Insert activity category data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_ACTIVITY_CATEGORY (
        ACTIVITY_CATEGORY_ID, NAME, RATE, TEAM_ID, CREATED_AT, UPDATED_AT, DESCRIPTION
    ) VALUES (
        :my_id, :my_name, :my_rate, :my_team_id, :my_created_at, :my_updated_at, :my_description
    )
    """

    try:
        for activity_category in activity_categories:
            # Ensure that parameter names match the bind variables in the query
            cursor.execute(
                insert_query,
                my_id=activity_category.get("id"),
                my_name=activity_category.get("name"),
                my_rate=activity_category.get("rate"),
                my_team_id=activity_category.get("team_id"),
                my_created_at=activity_category.get("created_at"),
                my_updated_at=activity_category.get("updated_at"),
                my_description=activity_category.get("description")
            )

        cursor.connection.commit()
        logger.info("Activity Categories inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting Activity Categories into the table: {e}")


def create_activity_billing_items_table(cursor):
    """
    Create a table for LAWOCUS Billing Items in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_ACTIVITY_BILLING_ITEMS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_ACTIVITY_BILLING_ITEMS (
                BILLING_ITEM_ID VARCHAR2(4000),
                START_DATE VARCHAR2(4000),
                END_DATE VARCHAR2(4000),
                ITEMS VARCHAR2(4000),
                USER_ID VARCHAR2(4000),
                PRACTICE_ID VARCHAR2(4000),
                IS_INVOICED VARCHAR2(4000),
                MATTER_ID VARCHAR2(4000),
                CLIENT_ID VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("LAWCUS Billing Items table created successfully.")
        else:
            logger.info("LAWCUS Billing Items table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Billing Items table: {e}")


def insert_activity_billing_items_into_table(cursor, billing_items_data):
    """
    Insert LAWOCUS Billing Items data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_ACTIVITY_BILLING_ITEMS (
        BILLING_ITEM_ID, START_DATE, END_DATE, ITEMS, USER_ID, PRACTICE_ID, IS_INVOICED,
        MATTER_ID, CLIENT_ID
    ) VALUES (
        :id, :start_date, :end_date, :items, :user_id, :practice_id, :is_invoiced,
        :matter_id, :client_id
    )
    """

    try:
        for billing_item in billing_items_data.get("list", []):
            cursor.execute(
                insert_query,
                {
                    "id": billing_item.get("id"),
                    "start_date": billing_item.get("start_date"),
                    "end_date": billing_item.get("end_date"),
                    "items": billing_item.get("items"),
                    "user_id": billing_item.get("user_id"),
                    "practice_id": billing_item.get("practice_id"),
                    "is_invoiced": billing_item.get("is_invoiced"),
                    "matter_id": billing_item.get("matter_id"),
                    "client_id": billing_item.get("client_id"),
                },
            )

        cursor.connection.commit()
        logger.info("LAWCUS Billing Items data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Billing Items data into the table: {e}")
