from utils import table_exists
from logger import interactions_logger as logger

if not logger:
    from logger import app_logger as logger


def create_interactions_table(cursor):
    """
    Create a table for Interactions in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_INTERACTIONS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_INTERACTIONS (
                INTERACTION_ID VARCHAR2(4000),
                INTERACTION_DATE VARCHAR2(4000),
                SUBJECT VARCHAR2(4000),
                BODY CLOB,
                INTERACTION_TYPE VARCHAR2(4000),
                INTERACTION_TYPE_IN VARCHAR2(4000),
                MATTER_ID VARCHAR2(4000),
                CONTACT_ID VARCHAR2(4000),
                INVOICE_TIMEENTRIES_ID VARCHAR2(4000),
                CREATED_BY VARCHAR2(4000),
                CREATED_AT VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Interactions table created successfully.")
        else:
            logger.info("Interactions table already exists.")
    except Exception as e:
        logger.error(f"Error creating Interactions table: {e}")


def insert_interactions_into_table(cursor, interactions):
    """
    Insert interaction data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_INTERACTIONS (
        INTERACTION_ID, INTERACTION_DATE, SUBJECT, BODY, INTERACTION_TYPE, INTERACTION_TYPE_IN,
        MATTER_ID, CONTACT_ID, INVOICE_TIMEENTRIES_ID, CREATED_BY, CREATED_AT
    ) VALUES (
        :my_id, :my_date, :my_subject, :my_body,
        :my_type, :my_type_in, :my_matter_id, :my_contact_id, :my_invoice_timeentries_id,
        :my_created_by, :my_created_at
    )
    """

    try:
        interactions_len = len(interactions)
        for interaction in interactions:
            # Ensure that parameter names match the bind variables in the query
            cursor.execute(
                insert_query,
                my_id=interaction.get("id"),
                my_date=interaction.get("date"),
                my_subject=interaction.get("subject"),
                my_body=interaction.get("body"),
                my_type=interaction.get("type"),
                my_type_in=interaction.get("type_in"),
                my_matter_id=interaction.get("matter_id"),
                my_contact_id=interaction.get("contact_id"),
                my_invoice_timeentries_id=interaction.get("invoice_timeentries_id"),
                my_created_by=interaction.get("created_by"),
                my_created_at=interaction.get("created_at")
            )

        cursor.connection.commit()
        logger.info(f"{interactions_len} Interactions inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting Interactions into the table: {e}")
