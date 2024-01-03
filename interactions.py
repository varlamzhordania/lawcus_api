from utils import table_exists, add_prefix_to_keys


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
        :my_id, :my_date, :my_subject, :my_body,
        :my_type, :my_type_in, :my_matter_id, :my_contact_id, :my_invoice_timeentries_id,
        :my_created_by, :my_created_at
    )
    """

    for interaction in interactions:
        prefixed_content = add_prefix_to_keys(interaction)
        # Ensure that parameter names match the bind variables in the query
        cursor.execute(
            insert_query,
            my_id=prefixed_content.get("my_id"),
            my_date=prefixed_content.get("my_date"),
            my_subject=prefixed_content.get("my_subject"),
            my_body=prefixed_content.get("my_body"),
            my_type=prefixed_content.get("my_type"),
            my_type_in=prefixed_content.get("my_type_in"),
            my_matter_id=prefixed_content.get("my_matter_id"),
            my_contact_id=prefixed_content.get("my_contact_id"),
            my_invoice_timeentries_id=prefixed_content.get("my_invoice_timeentries_id"),
            my_created_by=prefixed_content.get("my_created_by"),
            my_created_at=prefixed_content.get("my_created_at")
        )

    cursor.connection.commit()
    print("Interactions inserted into the table successfully.")
