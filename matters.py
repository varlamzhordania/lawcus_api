from utils import table_exists, add_prefix_to_keys


def create_matters_table(cursor):
    """
    Create a table for Matters in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_MATTERS"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_MATTERS (
            MATTER_ID VARCHAR2(4000),
            UUID VARCHAR2(4000),
            COLOR_CODE VARCHAR2(4000),
            TAGS VARCHAR2(4000),
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
        print("Matters table created successfully.")
    else:
        print("Matters table already exists.")


def insert_matters_into_table(cursor, matters):
    """
    Insert matter data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_MATTERS (
        MATTER_ID, UUID, COLOR_CODE, TAGS, CREATED_BY, CLOSE_DATE, NAME, POSITION,
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
        :my_id, :my_uuid, :my_color_code, :my_tags, :my_created_by, :my_close_date, :my_name, :my_position,
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

    for matter in matters:
        prefixed_content = add_prefix_to_keys(matter)

        # Convert lists to a string joined by ';;'
        for key, value in prefixed_content.items():
            if isinstance(value, list):
                prefixed_content[key] = ';;'.join(map(str, value))

        cursor.execute(
            insert_query,
            my_id=prefixed_content.get("my_id"),
            my_uuid=prefixed_content.get("my_uuid"),
            my_color_code=prefixed_content.get("my_color_code"),
            my_tags=prefixed_content.get("my_tags"),
            my_created_by=prefixed_content.get("my_created_by"),
            my_close_date=prefixed_content.get("my_close_date"),
            my_name=prefixed_content.get("my_name"),
            my_position=prefixed_content.get("my_position"),
            my_stage_id=prefixed_content.get("my_stage_id"),
            my_is_private=prefixed_content.get("my_is_private"),
            my_practice=prefixed_content.get("my_practice"),
            my_display_number=prefixed_content.get("my_display_number"),
            my_rates=prefixed_content.get("my_rates"),
            my_number=prefixed_content.get("my_number"),
            my_description=prefixed_content.get("my_description"),
            my_status=prefixed_content.get("my_status"),
            my_closed_at=prefixed_content.get("my_closed_at"),
            my_lead_created_at=prefixed_content.get("my_lead_created_at"),
            my_not_hire_at=prefixed_content.get("my_not_hire_at"),
            my_billing_type=prefixed_content.get("my_billing_type"),
            my_open_date=prefixed_content.get("my_open_date"),
            my_due_date=prefixed_content.get("my_due_date"),
            my_stage_position=prefixed_content.get("my_stage_position"),
            my_archived=prefixed_content.get("my_archived"),
            my_stagename=prefixed_content.get("my_stagename"),
            my_location=prefixed_content.get("my_location"),
            my_location_id=prefixed_content.get("my_location_id"),
            my_rate=prefixed_content.get("my_rate"),
            my_estimated_cost=prefixed_content.get("my_estimated_cost"),
            my_settlement_amount=prefixed_content.get("my_settlement_amount"),
            my_billing_attorney_id=prefixed_content.get("my_billing_attorney_id"),
            my_not_hire_reason=prefixed_content.get("my_not_hire_reason"),
            my_not_hire_note=prefixed_content.get("my_not_hire_note"),
            my_practice_name=prefixed_content.get("my_practice_name"),
            my_client_reference_number=prefixed_content.get("my_client_reference_number"),
            my_converted_at=prefixed_content.get("my_converted_at"),
            my_integration=prefixed_content.get("my_integration"),
            my_box_folder_id=prefixed_content.get("my_box_folder_id"),
            my_box_shared_link=prefixed_content.get("my_box_shared_link"),
            my_originating_timekeeper_id=prefixed_content.get("my_originating_timekeeper_id"),
            my_responsible_attorney_id=prefixed_content.get("my_responsible_attorney_id"),
            my_google_drive_folder_id=prefixed_content.get("my_google_drive_folder_id"),
            my_one_drive_folder_id=prefixed_content.get("my_one_drive_folder_id"),
            my_starred=prefixed_content.get("my_starred"),
            my_document_count=prefixed_content.get("my_document_count"),
            my_comments_count=prefixed_content.get("my_comments_count"),
            my_completed_tasks_count=prefixed_content.get("my_completed_tasks_count"),
            my_uncompleted_tasks_count=prefixed_content.get("my_uncompleted_tasks_count"),
            my_relations=prefixed_content.get("my_relations"),
            my_team_id=prefixed_content.get("my_team_id"),
            my_workflow_id=prefixed_content.get("my_workflow_id"),
            my_client_id=prefixed_content.get("my_client_id"),
            my_evergreen_retainer_amount=prefixed_content.get("my_evergreen_retainer_amount"),
            my_is_use_evergreen_retainer=prefixed_content.get("my_is_use_evergreen_retainer"),
            my_last_contacted_at=prefixed_content.get("my_last_contacted_at"),
            my_workflowname=prefixed_content.get("my_workflowname"),
            my_assignId=prefixed_content.get("my_assignId"),
            my_lawcus_url=prefixed_content.get("my_lawcus_url"),
        )
    cursor.connection.commit()
    print("Matters inserted into the table successfully.")

