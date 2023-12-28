from utils import table_exists


def create_matters_table(cursor):
    """
    Create a table for Matters in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "lawkpis_lawcus_matters"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE lawkpis_lawcus_matters (
            id NUMBER,
            uuid VARCHAR2(255),
            color_code VARCHAR2(255),
            tags VARCHAR2(255),
            created_by NUMBER,
            close_date VARCHAR2(255),
            name VARCHAR2(255),
            position NUMBER,
            stage_id NUMBER,
            is_private VARCHAR2(255),
            practice NUMBER,
            display_number VARCHAR2(255),
            rates VARCHAR2(255),
            "number" VARCHAR2(255),
            description VARCHAR2(4000),
            status VARCHAR2(255),
            closed_at VARCHAR2(255),
            lead_created_at VARCHAR2(255),
            not_hire_at VARCHAR2(255),
            billing_type VARCHAR2(255),
            open_date VARCHAR2(255),
            due_date VARCHAR2(255),
            stage_position NUMBER,
            archived NUMBER,
            stagename VARCHAR2(255),
            location VARCHAR2(255),
            location_id NUMBER,
            rate NUMBER,
            estimated_cost NUMBER,
            settlement_amount NUMBER,
            billing_attorney_id NUMBER,
            not_hire_reason VARCHAR2(255),
            not_hire_note VARCHAR2(4000),
            practice_name VARCHAR2(255),
            client_reference_number VARCHAR2(255),
            converted_at VARCHAR2(255),
            integration VARCHAR2(255),
            box_folder_id VARCHAR2(255),
            box_shared_link VARCHAR2(255),
            originating_timekeeper_id NUMBER,
            responsible_attorney_id NUMBER,
            google_drive_folder_id VARCHAR2(255),
            one_drive_folder_id VARCHAR2(255),
            starred NUMBER,
            document_count NUMBER,
            comments_count NUMBER,
            completed_tasks_count NUMBER,
            uncompleted_tasks_count NUMBER,
            relations VARCHAR2(4000),
            team_id NUMBER,
            workflow_id NUMBER,
            client_id NUMBER,
            evergreen_retainer_amount NUMBER,
            is_use_evergreen_retainer NUMBER,
            last_contacted_at VARCHAR2(255),
            workflowname VARCHAR2(255),
            assignId VARCHAR2(255),
            lawcus_url VARCHAR2(4000)
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
    INSERT INTO lawkpis_lawcus_matters (
        id, uuid, color_code, tags, created_by, close_date, name, position,
        stage_id, is_private, practice, display_number, rates, number, description,
        status, closed_at, lead_created_at, not_hire_at, billing_type, open_date,
        due_date, stage_position, archived, stagename, location, location_id,
        rate, estimated_cost, settlement_amount, billing_attorney_id,
        not_hire_reason, not_hire_note, practice_name, client_reference_number,
        converted_at, integration, box_folder_id, box_shared_link,
        originating_timekeeper_id, responsible_attorney_id, google_drive_folder_id,
        one_drive_folder_id, starred, document_count, comments_count,
        completed_tasks_count, uncompleted_tasks_count, relations, team_id,
        workflow_id, client_id, evergreen_retainer_amount, is_use_evergreen_retainer,
        last_contacted_at, workflowname, assignId, lawcus_url
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
