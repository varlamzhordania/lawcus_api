def create_leads_table(cursor):
    """
    Create a table for Leads in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "lawkpis_lawcus_leadsources"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE lawkpis_lawcus_leadsources (
            id NUMBER,
            name VARCHAR2(255),
            team_id NUMBER,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
        cursor.execute(table_creation_query)
        print("Leads table created successfully.")
    else:
        print("Leads table already exists.")

def insert_leads_into_table(cursor, leads):
    """
    Insert lead data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO lawkpis_lawcus_leadsources (
        id, name, team_id, created_at, updated_at
    ) VALUES (
        :id, :name, :team_id, TO_TIMESTAMP(:created_at, 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP(:updated_at, 'YYYY-MM-DD HH24:MI:SS')
    )
    """

    for lead in leads:
        cursor.execute(insert_query, lead)
    print("Leads inserted into the table successfully.")
