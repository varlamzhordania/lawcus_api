from utils import table_exists


def create_interactions_table(cursor):
    """
    Create a table for Interactions in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_INTERACTIONS"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_INTERACTIONS (
            INTERACTION_ID VARCHAR2(4000),
            INTERACTION_DATE VARCHAR2(4000),
            SUBJECT VARCHAR2(4000),
            BODY VARCHAR2(4000),
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
        print("Interactions table created successfully.")
    else:
        print("Interactions table already exists.")


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
        :id,:date, :subject, :body,
        :type, :type_in, :matter_id, :contact_id, :invoice_timeentries_id,
        :created_by,:created_at
    )
    """

    for interaction in interactions:
        cursor.execute(insert_query, interaction)
    print("Interactions inserted into the table successfully.")
