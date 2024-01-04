from utils import table_exists
import json
from logger import logger


def create_matters_table(cursor):
    """
    Create a table for Matters in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_MATTERS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_MATTERS (
                MATTER_ID VARCHAR2(4000),
                UUID VARCHAR2(4000),
                COLOR_CODE VARCHAR2(4000),
                CREATED_BY VARCHAR2(4000),
                CLOSE_DATE VARCHAR2(4000),
                NAME VARCHAR2(4000),
                POSITION VARCHAR2(4000),
                STAGE_ID VARCHAR2(4000),
                IS_PRIVATE VARCHAR2(4000),
                PRACTICE VARCHAR2(4000),
                DISPLAY_NUMBER VARCHAR2(4000),
                RATES VARCHAR2(4000),
                NUMBERS VARCHAR2(4000),
                DESCRIPTION VARCHAR2(4000),
                STATUS VARCHAR2(4000),
                CLOSED_AT VARCHAR2(4000),
                LEAD_CREATED_AT VARCHAR2(4000),
                NOT_HIRE_AT VARCHAR2(4000),
                BILLING_TYPE VARCHAR2(4000),
                OPEN_DATE VARCHAR2(4000),
                DUE_DATE VARCHAR2(4000),
                STAGE_POSITION VARCHAR2(4000),
                ARCHIVED VARCHAR2(4000),
                STAGENAME VARCHAR2(4000),
                LOCATION VARCHAR2(4000),
                LOCATION_ID VARCHAR2(4000),
                RATE VARCHAR2(4000),
                ESTIMATED_COST VARCHAR2(4000),
                SETTLEMENT_AMOUNT VARCHAR2(4000),
                BILLING_ATTORNEY_ID VARCHAR2(4000),
                NOT_HIRE_REASON VARCHAR2(4000),
                NOT_HIRE_NOTE VARCHAR2(4000),
                PRACTICE_NAME VARCHAR2(4000),
                CLIENT_REFERENCE_NUMBER VARCHAR2(4000),
                CONVERTED_AT VARCHAR2(4000),
                INTEGRATION VARCHAR2(4000),
                BOX_FOLDER_ID VARCHAR2(4000),
                BOX_SHARED_LINK VARCHAR2(4000),
                ORIGINATING_TIMEKEEPER_ID VARCHAR2(4000),
                RESPONSIBLE_ATTORNEY_ID VARCHAR2(4000),
                GOOGLE_DRIVE_FOLDER_ID VARCHAR2(4000),
                ONE_DRIVE_FOLDER_ID VARCHAR2(4000),
                STARRED VARCHAR2(4000),
                DOCUMENT_COUNT VARCHAR2(4000),
                COMMENTS_COUNT VARCHAR2(4000),
                COMPLETED_TASKS_COUNT VARCHAR2(4000),
                UNCOMPLETED_TASKS_COUNT VARCHAR2(4000),
                RELATIONS VARCHAR2(4000),
                TEAM_ID VARCHAR2(4000),
                WORKFLOW_ID VARCHAR2(4000),
                CLIENT_ID VARCHAR2(4000),
                EVERGREEN_RETAINER_AMOUNT VARCHAR2(4000),
                IS_USE_EVERGREEN_RETAINER VARCHAR2(4000),
                LAST_CONTACTED_AT VARCHAR2(4000),
                WORKFLOWNAME VARCHAR2(4000),
                ASSIGNID VARCHAR2(4000),
                LAWCUS_URL VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Matters table created successfully.")
        else:
            logger.info("Matters table already exists.")
    except Exception as e:
        logger.error(f"Error creating matters table: {e}")


def create_matter_assignees_table(cursor):
    """
    Create a table for Matter Assignees in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_MATTER_ASSIGNEES"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_MATTER_ASSIGNEES (
                MATTER_ID VARCHAR2(4000),
                ASSIGNEES VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Matter Assignees table created successfully.")
        else:
            logger.info("Matter Assignees table already exists.")
    except Exception as e:
        logger.error(f"Error creating Matter Assignees table: {e}")


def create_matter_custom_field_table(cursor):
    """
    Create a table for Matter Custom Field in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_MATTER_CUSTOM_FIELD"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_MATTER_CUSTOM_FIELD (
                MATTER_ID VARCHAR2(4000),
                TYPE VARCHAR2(4000),
                NAME VARCHAR2(4000),
                IFDEFAULT VARCHAR2(4000),
                ISREQUIRED VARCHAR2(4000),
                PRACTICE_IDS VARCHAR2(4000),
                VALUE VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Matter Custom Field table created successfully.")
        else:
            logger.info("Matter Custom Field table already exists.")
    except Exception as e:
        logger.error(f"Error creating Matter Custom Field table: {e}")


def create_matter_tags_table(cursor):
    """
    Create a table for Matter Tags in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_MATTER_TAGS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_MATTER_TAGS (
                MATTER_ID VARCHAR2(4000),
                NAME VARCHAR2(4000),
                COLOR VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Matter Tags table created successfully.")
        else:
            logger.info("Matter Tags table already exists.")
    except Exception as e:
        logger.error(f"Error creating Matter Tags table: {e}")


def insert_matter_tags_into_table(cursor, matter_id, tags):
    """
    Insert Matter Tag data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    try:
        tags_list = json.loads(tags)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON for matter_id {matter_id}: {e}")
        return

    try:
        for tag in tags_list:
            cursor.execute(
                """
                INSERT INTO LAWCUS_MATTER_TAGS (MATTER_ID, NAME, COLOR)
                VALUES (:matter_id, :name, :color)
                """,
                matter_id=matter_id,
                name=tag.get("name"),
                color=tag.get("color")
            )
        cursor.connection.commit()
        logger.info("Matter Tags inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting Matter Tags into the table: {e}")


def insert_matters_into_table(cursor, matters):
    """
    Insert matter data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_MATTERS (
        MATTER_ID, UUID, COLOR_CODE, CREATED_BY, CLOSE_DATE, NAME, POSITION,
        STAGE_ID, IS_PRIVATE, PRACTICE, DISPLAY_NUMBER, RATES, NUMBERS, DESCRIPTION,
        STATUS, CLOSED_AT, LEAD_CREATED_AT, NOT_HIRE_AT, BILLING_TYPE, OPEN_DATE,
        DUE_DATE, STAGE_POSITION, ARCHIVED, STAGENAME, LOCATION, LOCATION_ID,
        RATE, ESTIMATED_COST, SETTLEMENT_AMOUNT, BILLING_ATTORNEY_ID,
        NOT_HIRE_REASON, NOT_HIRE_NOTE, PRACTICE_NAME, CLIENT_REFERENCE_NUMBER,
        CONVERTED_AT, INTEGRATION, BOX_FOLDER_ID, BOX_SHARED_LINK,
        ORIGINATING_TIMEKEEPER_ID, RESPONSIBLE_ATTORNEY_ID, GOOGLE_DRIVE_FOLDER_ID,
        ONE_DRIVE_FOLDER_ID, STARRED, DOCUMENT_COUNT, COMMENTS_COUNT,
        COMPLETED_TASKS_COUNT, UNCOMPLETED_TASKS_COUNT, RELATIONS, TEAM_ID,
        WORKFLOW_ID, CLIENT_ID, EVERGREEN_RETAINER_AMOUNT, IS_USE_EVERGREEN_RETAINER,
        LAST_CONTACTED_AT, WORKFLOWNAME, ASSIGNID, LAWCUS_URL
    ) VALUES (
        :my_id, :my_uuid, :my_color_code, :my_created_by, :my_close_date, :my_name, :my_position,
        :my_stage_id, :my_is_private, :my_practice, :my_display_number, :my_rates, :my_number, :my_description,
        :my_status, :my_closed_at, :my_lead_created_at, :my_not_hire_at, :my_billing_type, :my_open_date,
        :my_due_date, :my_stage_position, :my_archived, :my_stagename, :my_location, :my_location_id,
        :my_rate, :my_estimated_cost, :my_settlement_amount, :my_billing_attorney_id,
        :my_not_hire_reason, :my_not_hire_note, :my_practice_name, :my_client_reference_number,
        :my_converted_at, :my_integration, :my_box_folder_id, :my_box_shared_link,
        :my_originating_timekeeper_id, :my_responsible_attorney_id, :my_google_drive_folder_id,
        :my_one_drive_folder_id, :my_starred, :my_document_count, :my_comments_count,
        :my_completed_tasks_count, :my_uncompleted_tasks_count, :my_relations, :my_team_id,
        :my_workflow_id, :my_client_id, :my_evergreen_retainer_amount, :my_is_use_evergreen_retainer,
        :my_last_contacted_at, :my_workflowname, :my_assignId, :my_lawcus_url
    )
    """

    try:
        for matter in matters:
            # Extract assignees and custom_fields arrays
            assignees = matter.get("assignees", [])
            custom_fields = matter.get("custom_fields")

            # Insert assignees into LAWCUS_MATTER_ASSIGNEES
            for assignee in assignees:
                cursor.execute(
                    """
                    INSERT INTO LAWCUS_MATTER_ASSIGNEES (MATTER_ID, ASSIGNEES)
                    VALUES (:my_matter_id, :my_assignee)
                    """,
                    my_matter_id=matter.get("id"),
                    my_assignee=assignee
                )

            # Insert custom_fields into LAWCUS_MATTER_CUSTOM_FIELD
            if custom_fields is not None:
                for custom_field in custom_fields:
                    cursor.execute(
                        """
                        INSERT INTO LAWCUS_MATTER_CUSTOM_FIELD (MATTER_ID, TYPE, NAME, IFDEFAULT, ISREQUIRED, PRACTICE_IDS, VALUE)
                        VALUES (:my_matter_id, :my_type,:my_name, :my_ifdefault, :my_isrequired, :my_practice_ids, :my_value)
                        """,
                        my_matter_id=matter.get("id"),
                        my_type=custom_field.get("type"),
                        my_name=custom_field.get("name"),
                        my_ifdefault=custom_field.get("ifdefault"),
                        my_isrequired=custom_field.get("isrequired"),
                        my_practice_ids=custom_field.get("practice_ids"),
                        my_value=custom_field.get("value")
                    )

            # Insert Matter Tags into LAWCUS_MATTER_TAGS
            tags = matter.get("tags")
            if tags is not None:
                insert_matter_tags_into_table(cursor, matter.get("id"), tags)

            # Convert lists to a string joined by ';;'
            for key, value in matter.items():
                if isinstance(value, list):
                    matter[key] = ';;'.join(map(str, value))

            cursor.execute(
                insert_query,
                my_id=matter.get("id"),
                my_uuid=matter.get("uuid"),
                my_color_code=matter.get("color_code"),
                my_created_by=matter.get("created_by"),
                my_close_date=matter.get("close_date"),
                my_name=matter.get("name"),
                my_position=matter.get("position"),
                my_stage_id=matter.get("stage_id"),
                my_is_private=matter.get("is_private"),
                my_practice=matter.get("practice"),
                my_display_number=matter.get("display_number"),
                my_rates=matter.get("rates"),
                my_number=matter.get("number"),
                my_description=matter.get("description"),
                my_status=matter.get("status"),
                my_closed_at=matter.get("closed_at"),
                my_lead_created_at=matter.get("lead_created_at"),
                my_not_hire_at=matter.get("not_hire_at"),
                my_billing_type=matter.get("billing_type"),
                my_open_date=matter.get("open_date"),
                my_due_date=matter.get("due_date"),
                my_stage_position=matter.get("stage_position"),
                my_archived=matter.get("archived"),
                my_stagename=matter.get("stagename"),
                my_location=matter.get("location"),
                my_location_id=matter.get("location_id"),
                my_rate=matter.get("rate"),
                my_estimated_cost=matter.get("estimated_cost"),
                my_settlement_amount=matter.get("settlement_amount"),
                my_billing_attorney_id=matter.get("billing_attorney_id"),
                my_not_hire_reason=matter.get("not_hire_reason"),
                my_not_hire_note=matter.get("not_hire_note"),
                my_practice_name=matter.get("practice_name"),
                my_client_reference_number=matter.get("client_reference_number"),
                my_converted_at=matter.get("converted_at"),
                my_integration=matter.get("integration"),
                my_box_folder_id=matter.get("box_folder_id"),
                my_box_shared_link=matter.get("box_shared_link"),
                my_originating_timekeeper_id=matter.get("originating_timekeeper_id"),
                my_responsible_attorney_id=matter.get("responsible_attorney_id"),
                my_google_drive_folder_id=matter.get("google_drive_folder_id"),
                my_one_drive_folder_id=matter.get("one_drive_folder_id"),
                my_starred=matter.get("starred"),
                my_document_count=matter.get("document_count"),
                my_comments_count=matter.get("comments_count"),
                my_completed_tasks_count=matter.get("completed_tasks_count"),
                my_uncompleted_tasks_count=matter.get("uncompleted_tasks_count"),
                my_relations=matter.get("relations"),
                my_team_id=matter.get("team_id"),
                my_workflow_id=matter.get("workflow_id"),
                my_client_id=matter.get("client_id"),
                my_evergreen_retainer_amount=matter.get("evergreen_retainer_amount"),
                my_is_use_evergreen_retainer=matter.get("is_use_evergreen_retainer"),
                my_last_contacted_at=matter.get("last_contacted_at"),
                my_workflowname=matter.get("workflowname"),
                my_assignId=matter.get("assignId"),
                my_lawcus_url=matter.get("lawcus_url"),
            )
        cursor.connection.commit()
        logger.info("Matters inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting matters into the table: {e}")

