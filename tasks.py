from utils import table_exists


def create_tasks_table(cursor):
    """
    Create a table for Tasks in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "lawkpis_lawcus_tasks"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE lawkpis_lawcus_tasks (
            id VARCHAR2(255),
            item_id NUMBER,
            name VARCHAR2(255),
            due_date TIMESTAMP,
            start_date TIMESTAMP,
            is_completed NUMBER(1,0),
            is_custom_time NUMBER(1,0),
            created_by NUMBER,
            matter_id NUMBER,
            due_type VARCHAR2(255),
            due_settings VARCHAR2(255),
            is_private NUMBER(1,0),
            repeat VARCHAR2(255),
            tags VARCHAR2(255),
            sort_field TIMESTAMP,
            sub_tasks_count NUMBER,
            assign_id VARCHAR2(255)
        )
        """
        cursor.execute(table_creation_query)
        print("Tasks table created successfully.")
    else:
        print("Tasks table already exists.")


def insert_tasks_into_table(cursor, tasks):
    """
    Insert task data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO lawkpis_lawcus_tasks (
        id, item_id, name, due_date, start_date, is_completed, is_custom_time,
        created_by, matter_id, due_type, due_settings, is_private, repeat, tags,
        sort_field, sub_tasks_count, assign_id
    ) VALUES (
        :id, :item_id, :name, TO_TIMESTAMP(:due_date, 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP(:start_date, 'YYYY-MM-DD HH24:MI:SS'), :is_completed,
        :is_custom_time, :created_by, :matter_id, :due_type, :due_settings,
        :is_private, :repeat, :tags, TO_TIMESTAMP(:sort_field, 'YYYY-MM-DD HH24:MI:SS'),
        :sub_tasks_count, :assign_id
    )
    """

    for task in tasks:
        cursor.execute(insert_query, task)
    print("Tasks inserted into the table successfully.")
