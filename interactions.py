def create_interactions_table(cursor):
    """
    Create a table for Interactions in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "lawkpis_lawcus_interactions"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE lawkpis_lawcus_interactions (
            id VARCHAR2(255),
            interaction_date TIMESTAMP,
            subject VARCHAR2(255),
            body CLOB,
            interaction_type VARCHAR2(255),
            interaction_type_in VARCHAR2(255),
            matter_id NUMBER,
            contact_id NUMBER,
            invoice_timeentries_id VARCHAR2(255),
            created_by NUMBER,
            created_at TIMESTAMP
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
    INSERT INTO lawkpis_lawcus_interactions (
        id, interaction_date, subject, body, interaction_type, interaction_type_in,
        matter_id, contact_id, invoice_timeentries_id, created_by, created_at
    ) VALUES (
        :id, TO_TIMESTAMP(:date, 'YYYY-MM-DD HH24:MI:SS'), :subject, :body,
        :type, :type_in, :matter_id, :contact_id, :invoice_timeentries_id,
        :created_by, TO_TIMESTAMP(:created_at, 'YYYY-MM-DD HH24:MI:SS')
    )
    """

    for interaction in interactions:
        cursor.execute(insert_query, interaction)
    print("Interactions inserted into the table successfully.")
