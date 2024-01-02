from utils import table_exists


def create_tasks_table(cursor):
    """
    Create a table for Tasks in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_TASKS"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_TASKS (
            TASK_ID VARCHAR2(4000),
            ITEM_ID VARCHAR2(4000),
            NAME VARCHAR2(4000),
            DUE_DATE VARCHAR2(4000),
            START_DATE VARCHAR2(4000),
            IS_COMPLETED VARCHAR2(4000),
            IS_CUSTOM_TIME VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            DUE_TYPE VARCHAR2(4000),
            DUE_SETTINGS VARCHAR2(4000),
            IS_PRIVATE VARCHAR2(4000),
            REPEAT VARCHAR2(4000),
            TAGS VARCHAR2(4000),
            SORT_FIELD VARCHAR2(4000),
            SUB_TASKS_COUNT VARCHAR2(4000),
            ASSIGN_ID VARCHAR2(4000)
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
    INSERT INTO LAWCUS_TASKS (
        TASK_ID, ITEM_ID, NAME, DUE_DATE, START_DATE, IS_COMPLETED, IS_CUSTOM_TIME,
        CREATED_BY, MATTER_ID, DUE_TYPE, DUE_SETTINGS, IS_PRIVATE, REPEAT, TAGS,
        SORT_FIELD, SUB_TASKS_COUNT, ASSIGN_ID
    ) VALUES (
        :id, :item_id, :name, :due_date,:start_date, :is_completed,
        :is_custom_time, :created_by, :matter_id, :due_type, :due_settings,
        :is_private, :repeat, :tags,:sort_field,
        :sub_tasks_count, :assign_id
    )
    """

    for task in tasks:
        cursor.execute(insert_query, task)
    print("Tasks inserted into the table successfully.")
