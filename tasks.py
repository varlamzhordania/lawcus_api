from utils import table_exists
from logger import logger
import json


def create_tasks_table(cursor):
    """
    Create a table for Tasks in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_TASKS"

    try:
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
                SORT_FIELD VARCHAR2(4000),
                SUB_TASKS_COUNT VARCHAR2(4000),
                ASSIGN_ID VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Tasks table created successfully.")
        else:
            logger.info("Tasks table already exists.")
    except Exception as e:
        logger.error(f"Error creating Tasks table: {e}")


def create_task_tags_table(cursor):
    """
    Create a table for Task Tags in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_TASK_TAGS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_TASK_TAGS (
                TASK_ID VARCHAR2(4000),
                NAME VARCHAR2(4000),
                COLOR VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Task  tags table created successfully.")
        else:
            logger.info("Task tags table already exists.")
    except Exception as e:
        logger.error(f"Error creating task tags table: {e}")


def insert_task_tags_into_table(cursor, task_id, tags):
    """
    Insert Task tag data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    # Define the parameterized query
    insert_query = """
        INSERT INTO LAWCUS_TASK_TAGS (
            TASK_ID, NAME, COLOR
        ) VALUES (
            :my_task_id, :my_name, :my_color
        )
        """

    try:
        for tag in tags:
            # Execute the query with parameters
            cursor.execute(
                insert_query,
                my_task_id=task_id,
                my_name=tag.get("name"),
                my_color=tag.get("color")
            )

        cursor.connection.commit()
        logger.info("Task tags inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting task tags into the table: {e}")


def insert_tasks_into_table(cursor, tasks):
    """
    Insert task data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_TASKS (
        TASK_ID, ITEM_ID, NAME, DUE_DATE, START_DATE, IS_COMPLETED, IS_CUSTOM_TIME,
        CREATED_BY, MATTER_ID, DUE_TYPE, DUE_SETTINGS, IS_PRIVATE, REPEAT,
        SORT_FIELD, SUB_TASKS_COUNT, ASSIGN_ID
    ) VALUES (
        :my_id, :my_item_id, :my_name, :my_due_date, :my_start_date, :my_is_completed,
        :my_is_custom_time, :my_created_by, :my_matter_id, :my_due_type, :my_due_settings,
        :my_is_private, :my_repeat, :my_sort_field,
        :my_sub_tasks_count, :my_assign_id
    )
    """

    try:
        for task in tasks:
            # Ensure that parameter names match the bind variables in the query
            task_id = task.get("id")
            tags_json = task.get("tags") or '[]'

            if tags_json is not None:
                tags = json.loads(tags_json)
                insert_task_tags_into_table(cursor, task_id, tags)

            # Convert lists to a string joined by ';;'
            for key, value in task.items():
                if isinstance(value, list):
                    task[key] = ';;'.join(map(str, value))

            cursor.execute(
                insert_query,
                my_id=task_id,
                my_item_id=task.get("item_id"),
                my_name=task.get("name"),
                my_due_date=task.get("due_date"),
                my_start_date=task.get("start_date"),
                my_is_completed=task.get("is_completed"),
                my_is_custom_time=task.get("is_custom_time"),
                my_created_by=task.get("created_by"),
                my_matter_id=task.get("matter_id"),
                my_due_type=task.get("due_type"),
                my_due_settings=json.dumps(task.get("due_settings")),
                my_is_private=task.get("is_private"),
                my_repeat=task.get("repeat"),
                my_sort_field=task.get("sort_field"),
                my_sub_tasks_count=task.get("sub_tasks_count"),
                my_assign_id=task.get("assign_id")
            )

        cursor.connection.commit()
        logger.info("Tasks inserted into the table successfully.")
    except Exception as e:

        logger.error(f"Error inserting Tasks into the table: {e}")
