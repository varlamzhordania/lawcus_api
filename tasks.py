from utils import table_exists, add_prefix_to_keys
import json


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
        :my_id, :my_item_id, :my_name, :my_due_date, :my_start_date, :my_is_completed,
        :my_is_custom_time, :my_created_by, :my_matter_id, :my_due_type, :my_due_settings,
        :my_is_private, :my_repeat, :my_tags, :my_sort_field,
        :my_sub_tasks_count, :my_assign_id
    )
    """

    for task in tasks:
        prefixed_content = add_prefix_to_keys(task)

        # Ensure that parameter names match the bind variables in the query

        # Convert lists to a string joined by ';;'
        for key, value in prefixed_content.items():
            if isinstance(value, list):
                prefixed_content[key] = ';;'.join(map(str, value))

        cursor.execute(
            insert_query,
            my_id=prefixed_content.get("my_id"),
            my_item_id=prefixed_content.get("my_item_id"),
            my_name=prefixed_content.get("my_name"),
            my_due_date=prefixed_content.get("my_due_date"),
            my_start_date=prefixed_content.get("my_start_date"),
            my_is_completed=prefixed_content.get("my_is_completed"),
            my_is_custom_time=prefixed_content.get("my_is_custom_time"),
            my_created_by=prefixed_content.get("my_created_by"),
            my_matter_id=prefixed_content.get("my_matter_id"),
            my_due_type=prefixed_content.get("my_due_type"),
            my_due_settings=json.dumps(prefixed_content.get("my_due_settings")),
            my_is_private=prefixed_content.get("my_is_private"),
            my_repeat=prefixed_content.get("my_repeat"),
            my_tags=prefixed_content.get("my_tags"),
            my_sort_field=prefixed_content.get("my_sort_field"),
            my_sub_tasks_count=prefixed_content.get("my_sub_tasks_count"),
            my_assign_id=prefixed_content.get("my_assign_id")
        )

    cursor.connection.commit()
    print("Tasks inserted into the table successfully.")
