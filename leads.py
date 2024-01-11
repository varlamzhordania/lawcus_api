from utils import table_exists
from logger import leads_logger as logger

if not logger:
    from logger import app_logger as logger


def create_leads_table(cursor):
    """
    Create a table for Leads in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_LEADSOURCES"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_LEADSOURCES (
                LEADSOURCE_ID VARCHAR2(4000),
                NAME VARCHAR2(4000),
                TEAM_ID VARCHAR2(4000),
                CREATED_AT VARCHAR2(4000),
                UPDATED_AT VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Leads table created successfully.")
        else:
            logger.info("Leads table already exists.")
    except Exception as e:
        logger.error(f"Error creating Leads table: {e}")


def insert_leads_into_table(cursor, leads):
    """
    Insert lead data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_LEADSOURCES (
        LEADSOURCE_ID, NAME, TEAM_ID, CREATED_AT, UPDATED_AT
    ) VALUES (
        :id, :name, :team_id, :created_at, :updated_at
    )
    """

    try:
        leads_len = len(leads)
        for lead in leads:
            cursor.execute(insert_query, lead)

        cursor.connection.commit()
        logger.info(f"{leads_len} Leads inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting leads into the table: {e}")
