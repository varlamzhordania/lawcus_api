from utils import table_exists


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
        STAGE_ID, IS_PRIVATE, PRACTICE, DISPLAY_NUMBER, RATES, NUMBER, DESCRIPTION,
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
        :id, :uuid, :color_code, :tags, :created_by, :close_date, :name, :position,
        :stage_id, :is_private, :practice, :display_number, :rates, :number, :description,
        :status, :closed_at, :lead_created_at, :not_hire_at, :billing_type, :open_date,
        :due_date, :stage_position, :archived, :stagename, :location, :location_id,
        :rate, :estimated_cost, :settlement_amount, :billing_attorney_id,
        :not_hire_reason, :not_hire_note, :practice_name, :client_reference_number,
        :converted_at, :integration, :box_folder_id, :box_shared_link,
        :originating_timekeeper_id, :responsible_attorney_id, :google_drive_folder_id,
        :one_drive_folder_id, :starred, :document_count, :comments_count,
        :completed_tasks_count, :uncompleted_tasks_count, :relations, :team_id,
        :workflow_id, :client_id, :evergreen_retainer_amount, :is_use_evergreen_retainer,
        :last_contacted_at, :workflowname, :assignId, :lawcus_url
    )
    """

    for matter in matters:
        cursor.execute(insert_query, matter)
    print("Matters inserted into the table successfully.")
