from utils import table_exists


def create_users_me_table(cursor):
    """
    Create a table for LAWOCUS Users Me in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_ME"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_USERS_ME (
            USER_ID VARCHAR2(4000),
            UUID VARCHAR2(4000),
            NAME VARCHAR2(4000),
            FIRST_NAME VARCHAR2(4000),
            LAST_NAME VARCHAR2(4000),
            PHONE VARCHAR2(4000),
            TIMEZONE_NAME VARCHAR2(4000),
            TIMEZONE_OFFSET VARCHAR2(4000),
            RATE VARCHAR2(4000),
            ROLE_NAME VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Users Me table created successfully.")
    else:
        print("LAWCUS Users Me table already exists.")


def insert_users_me_into_table(cursor, users_me_data):
    """
    Insert LAWOCUS Users Me data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_USERS_ME (
        USER_ID, UUID, NAME, FIRST_NAME, LAST_NAME, PHONE, TIMEZONE_NAME, TIMEZONE_OFFSET, RATE, ROLE_NAME
    ) VALUES (
        :id, :uuid, :name, :first_name, :last_name, :phone, :timezone_name, :timezone_offset, :rate, :role_name
    )
    """

    cursor.execute(
        insert_query,
        {
            "id": users_me_data.get("id"),
            "uuid": users_me_data.get("uuid"),
            "name": users_me_data.get("name"),
            "first_name": users_me_data.get("first_name"),
            "last_name": users_me_data.get("last_name"),
            "phone": users_me_data.get("phone"),
            "timezone_name": users_me_data.get("timezone_name"),
            "timezone_offset": users_me_data.get("timezone_offset"),
            "rate": users_me_data.get("rate"),
            "role_name": users_me_data.get("role_name"),
        },
    )

    print("LAWCUS Users Me data inserted into the table successfully.")


def create_users_teammates_table(cursor):
    """
    Create a table for LAWOCUS Users Teammates in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_TEAMMATES"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_USERS_TEAMMATES (
            USER_ID VARCHAR2(4000),
            UUID VARCHAR2(4000),
            NAME VARCHAR2(4000),
            FIRST_NAME VARCHAR2(4000),
            LAST_NAME VARCHAR2(4000),
            PHONE VARCHAR2(4000),
            TIMEZONE_NAME VARCHAR2(4000),
            TIMEZONE_OFFSET VARCHAR2(4000),
            RATE VARCHAR2(4000),
            ROLE_NAME VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Users Teammates table created successfully.")
    else:
        print("LAWCUS Users Teammates table already exists.")


def insert_users_teammates_into_table(cursor, teammates_data):
    """
    Insert LAWOCUS Users Teammates data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_USERS_TEAMMATES (
        USER_ID, UUID, NAME, FIRST_NAME, LAST_NAME, PHONE, TIMEZONE_NAME, TIMEZONE_OFFSET, RATE, ROLE_NAME
    ) VALUES (
        :id, :uuid, :name, :first_name, :last_name, :phone, :timezone_name, :timezone_offset, :rate, :role_name
    )
    """

    for teammate in teammates_data:
        cursor.execute(
            insert_query,
            {
                "id": teammate.get("id"),
                "uuid": teammate.get("uuid"),
                "name": teammate.get("name"),
                "first_name": teammate.get("first_name"),
                "last_name": teammate.get("last_name"),
                "phone": teammate.get("phone"),
                "timezone_name": teammate.get("timezone_name"),
                "timezone_offset": teammate.get("timezone_offset"),
                "rate": teammate.get("rate"),
                "role_name": teammate.get("role_name"),
            },
        )

    print("LAWCUS Users Teammates data inserted into the table successfully.")


def create_users_team_table(cursor):
    """
    Create a table for LAWOCUS Users Team in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_TEAM"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_USERS_TEAM (
            NAME VARCHAR2(4000),
            ADDRESS VARCHAR2(4000),
            CITY VARCHAR2(4000),
            STATE VARCHAR2(4000),
            ZIP VARCHAR2(4000),
            COUNTRY VARCHAR2(4000),
            BUSINESS_PHONE VARCHAR2(4000),
            MOBILE_PHONE VARCHAR2(4000),
            DATE_FORMAT VARCHAR2(4000),
            LOCALE VARCHAR2(4000),
            CURRENCY VARCHAR2(4000),
            UTBMS_SETTINGS VARCHAR2(4000),
            SUPPORT_EMAIL VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Users Team table created successfully.")
    else:
        print("LAWCUS Users Team table already exists.")


def insert_users_team_into_table(cursor, team_data):
    """
    Insert LAWOCUS Users Team data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_USERS_TEAM (
        NAME, ADDRESS, CITY, STATE, ZIP, COUNTRY, BUSINESS_PHONE, MOBILE_PHONE,
        DATE_FORMAT, LOCALE, CURRENCY, UTBMS_SETTINGS, SUPPORT_EMAIL
    ) VALUES (
        :name, :address, :city, :state, :zip, :country, :business_phone, :mobile_phone,
        :date_format, :locale, :currency, :utbms_settings, :support_email
    )
    """

    cursor.execute(
        insert_query,
        {
            "name": team_data.get("name"),
            "address": team_data.get("address"),
            "city": team_data.get("city"),
            "state": team_data.get("state"),
            "zip": team_data.get("zip"),
            "country": team_data.get("country"),
            "business_phone": team_data.get("business_phone"),
            "mobile_phone": team_data.get("mobile_phone"),
            "date_format": team_data.get("date_format"),
            "locale": team_data.get("locale"),
            "currency": team_data.get("currency"),
            "utbms_settings": team_data.get("utbms_settings"),
            "support_email": team_data.get("support_email"),
        },
    )

    print("LAWCUS Users Team data inserted into the table successfully.")


def create_users_contacts_table(cursor):
    """
    Create a table for LAWOCUS Users Contacts in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_CONTACTS"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_USERS_CONTACTS (
            USER_ID VARCHAR2(4000),
            UUID VARCHAR2(4000),
            NAME VARCHAR2(4000),
            FIRST_NAME VARCHAR2(4000),
            MIDDLE_NAME VARCHAR2(4000),
            LAST_NAME VARCHAR2(4000),
            SOURCE VARCHAR2(4000),
            TYPE VARCHAR2(4000),
            EMAIL VARCHAR2(4000),
            EMAILS VARCHAR2(4000),
            AVATAR VARCHAR2(4000),
            STREET VARCHAR2(4000),
            CITY VARCHAR2(4000),
            STATE VARCHAR2(4000),
            ZIP VARCHAR2(4000),
            COUNTRY VARCHAR2(4000),
            NUMBERS VARCHAR2(4000),
            TAGS VARCHAR2(4000),
            SOURCE_ID VARCHAR2(4000),
            REFERRED_BY VARCHAR2(4000),
            PREFIX VARCHAR2(4000),
            GENDER VARCHAR2(4000),
            DATE_OF_BIRTHDAY VARCHAR2(4000),
            NOTE VARCHAR2(4000),
            ADDRESSES VARCHAR2(4000),
            PHONE VARCHAR2(4000),
            PHONES VARCHAR2(4000),
            IS_LEAD VARCHAR2(4000),
            CUSTOM_FIELDS VARCHAR2(4000),
            COMPANY_ID VARCHAR2(4000),
            ASSOCIATED_USER_ID VARCHAR2(4000),
            LAST_CONTACTED_AT VARCHAR2(4000),
            IS_CLIENT VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Users Contacts table created successfully.")
    else:
        print("LAWCUS Users Contacts table already exists.")


def insert_users_contacts_into_table(cursor, contacts_data):
    """
    Insert LAWOCUS Users Contacts data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_USERS_CONTACTS (
        USER_ID, UUID, NAME, FIRST_NAME, MIDDLE_NAME, LAST_NAME, SOURCE, TYPE, EMAIL, EMAILS,
        AVATAR, STREET, CITY, STATE, ZIP, COUNTRY, NUMBERS, TAGS, SOURCE_ID, REFERRED_BY,
        PREFIX, GENDER, DATE_OF_BIRTHDAY, NOTE, ADDRESSES, PHONE, PHONES, IS_LEAD,
        CUSTOM_FIELDS, COMPANY_ID, ASSOCIATED_USER_ID, LAST_CONTACTED_AT, IS_CLIENT
    ) VALUES (
        :id, :uuid, :name, :first_name, :middle_name, :last_name, :source, :type,
        :email, :emails, :avatar, :street, :city, :state, :zip, :country, :number,
        :tags, :source_id, :referred_by, :prefix, :gender, :date_of_birthday, :note,
        :addresses, :phone, :phones, :is_lead, :custom_fields, :company_id,
        :associated_user_id, :last_contacted_at, :is_client
    )
    """

    for contact in contacts_data:
        cursor.execute(
            insert_query,
            {
                "id": contact.get("id"),
                "uuid": contact.get("uuid"),
                "name": contact.get("name"),
                "first_name": contact.get("first_name"),
                "middle_name": contact.get("middle_name"),
                "last_name": contact.get("last_name"),
                "source": contact.get("source"),
                "type": contact.get("type"),
                "email": contact.get("email"),
                "emails": contact.get("emails"),
                "avatar": contact.get("avatar"),
                "street": contact.get("street"),
                "city": contact.get("city"),
                "state": contact.get("state"),
                "zip": contact.get("zip"),
                "country": contact.get("country"),
                "number": contact.get("number"),
                "tags": contact.get("tags"),
                "source_id": contact.get("source_id"),
                "referred_by": contact.get("referred_by"),
                "prefix": contact.get("prefix"),
                "gender": contact.get("gender"),
                "date_of_birthday": contact.get("date_of_birthday"),
                "note": contact.get("note"),
                "addresses": contact.get("addresses"),
                "phone": contact.get("phone"),
                "phones": contact.get("phones"),
                "is_lead": contact.get("is_lead"),
                "custom_fields": contact.get("custom_fields"),
                "company_id": contact.get("company_id"),
                "associated_user_id": contact.get("associated_user_id"),
                "last_contacted_at": contact.get("last_contacted_at"),
                "is_client": contact.get("is_client"),
            },
        )

    print("LAWCUS Users Contacts data inserted into the table successfully.")


def create_users_matters_table(cursor):
    """
    Create a table for LAWOCUS Users Matters in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_MATTERS"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_USERS_MATTERS (
            MATTER_ID VARCHAR2(4000),
            UUID VARCHAR2(4000),
            TAGS VARCHAR2(4000),
            COLOR_CODE VARCHAR2(4000),
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
            BILLING_TYPE VARCHAR2(4000),
            OPEN_DATE VARCHAR2(4000),
            DUE_DATE VARCHAR2(4000),
            STAGE_POSITION VARCHAR2(4000),
            STAGENAME VARCHAR2(4000),
            RATE VARCHAR2(4000),
            ORIGINATING_TIMEKEEPER_ID VARCHAR2(4000),
            RESPONSIBLE_ATTORNEY_ID VARCHAR2(4000),
            SETTLEMENT_AMOUNT VARCHAR2(4000),
            BILLING_ATTORNEY_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            CUSTOM_FIELDS VARCHAR2(4000),
            LEAD_CREATED_AT VARCHAR2(4000),
            LOCATION VARCHAR2(4000),
            LOCATION_ID VARCHAR2(4000),
            ARCHIVED VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            WORKFLOW_ID VARCHAR2(4000),
            CLIENT_ID VARCHAR2(4000),
            CLIENT_REFERRED_BY VARCHAR2(4000),
            CLIENT_SOURCE_ID VARCHAR2(4000),
            ESTIMATED_COST VARCHAR2(4000),
            TASK_DUE_DATE VARCHAR2(4000),
            EVERGREEN_RETAINER_AMOUNT VARCHAR2(4000),
            IS_USE_EVERGREEN_RETAINER VARCHAR2(4000),
            WORKFLOWNAME VARCHAR2(4000),
            RELATIONS VARCHAR2(4000),
            ASSIGNEES VARCHAR2(4000),
            ASSIGN_ID VARCHAR2(4000),
            STARRED VARCHAR2(4000),
            COMPLETED_TASKS_COUNT VARCHAR2(4000),
            UNCOMPLETED_TASKS_COUNT VARCHAR2(4000),
            LAST_CONTACTED_AT VARCHAR2(4000),
            DOCUMENT_COUNT VARCHAR2(4000),
            LAWCUS_URL VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Users Matters table created successfully.")
    else:
        print("LAWCUS Users Matters table already exists.")


def insert_users_matters_into_table(cursor, matters_data):
    """
    Insert LAWOCUS Users Matters data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_USERS_MATTERS (
        MATTER_ID, UUID, TAGS, COLOR_CODE, NAME, POSITION, STAGE_ID, IS_PRIVATE, PRACTICE,
        DISPLAY_NUMBER, RATES, NUMBERS, DESCRIPTION, STATUS, CLOSED_AT, BILLING_TYPE,
        OPEN_DATE, DUE_DATE, STAGE_POSITION, STAGENAME, RATE, ORIGINATING_TIMEKEEPER_ID,
        RESPONSIBLE_ATTORNEY_ID, SETTLEMENT_AMOUNT, BILLING_ATTORNEY_ID, CREATED_BY,
        CUSTOM_FIELDS, LEAD_CREATED_AT, LOCATION, LOCATION_ID, ARCHIVED, TEAM_ID,
        WORKFLOW_ID, CLIENT_ID, CLIENT_REFERRED_BY, CLIENT_SOURCE_ID, ESTIMATED_COST,
        TASK_DUE_DATE, EVERGREEN_RETAINER_AMOUNT, IS_USE_EVERGREEN_RETAINER,
        WORKFLOWNAME, RELATIONS, ASSIGNEES, ASSIGN_ID, STARRED,
        COMPLETED_TASKS_COUNT, UNCOMPLETED_TASKS_COUNT, LAST_CONTACTED_AT,
        DOCUMENT_COUNT, LAWCUS_URL
    ) VALUES (
        :id, :uuid, :tags, :color_code, :name, :position, :stage_id, :is_private,
        :practice, :display_number, :rates, :number, :description, :status,
        :closed_at, :billing_type, :open_date, :due_date, :stage_position, :stagename,
        :rate, :originating_timekeeper_id, :responsible_attorney_id, :settlement_amount,
        :billing_attorney_id, :created_by, :custom_fields, :lead_created_at, :location,
        :location_id, :archived, :team_id, :workflow_id, :client_id, :client_referred_by,
        :client_source_id, :estimated_cost, :task_due_date, :evergreen_retainer_amount,
        :is_use_evergreen_retainer, :workflowname, :relations, :assignees, :assign_id,
        :starred, :completed_tasks_count, :uncompleted_tasks_count, :last_contacted_at,
        :document_count, :lawcus_url
    )
    """

    for matter in matters_data:
        cursor.execute(
            insert_query,
            {
                "id": matter.get("id"),
                "uuid": matter.get("uuid"),
                "tags": matter.get("tags"),
                "color_code": matter.get("color_code"),
                "name": matter.get("name"),
                "position": matter.get("position"),
                "stage_id": matter.get("stage_id"),
                "is_private": matter.get("is_private"),
                "practice": matter.get("practice"),
                "display_number": matter.get("display_number"),
                "rates": matter.get("rates"),
                "number": matter.get("number"),
                "description": matter.get("description"),
                "status": matter.get("status"),
                "closed_at": matter.get("closed_at"),
                "billing_type": matter.get("billing_type"),
                "open_date": matter.get("open_date"),
                "due_date": matter.get("due_date"),
                "stage_position": matter.get("stage_position"),
                "stagename": matter.get("stagename"),
                "rate": matter.get("rate"),
                "originating_timekeeper_id": matter.get("originating_timekeeper_id"),
                "responsible_attorney_id": matter.get("responsible_attorney_id"),
                "settlement_amount": matter.get("settlement_amount"),
                "billing_attorney_id": matter.get("billing_attorney_id"),
                "created_by": matter.get("created_by"),
                "custom_fields": matter.get("custom_fields"),
                "lead_created_at": matter.get("lead_created_at"),
                "location": matter.get("location"),
                "location_id": matter.get("location_id"),
                "archived": matter.get("archived"),
                "team_id": matter.get("team_id"),
                "workflow_id": matter.get("workflow_id"),
                "client_id": matter.get("client_id"),
                "client_referred_by": matter.get("client_referred_by"),
                "client_source_id": matter.get("client_source_id"),
                "estimated_cost": matter.get("estimated_cost"),
                "task_due_date": matter.get("task_due_date"),
                "evergreen_retainer_amount": matter.get("evergreen_retainer_amount"),
                "is_use_evergreen_retainer": matter.get("is_use_evergreen_retainer"),
                "workflowname": matter.get("workflowname"),
                "relations": matter.get("relations"),
                "assignees": matter.get("assignees"),
                "assign_id": matter.get("assignId"),
                "starred": matter.get("starred"),
                "completed_tasks_count": matter.get("completed_tasks_count"),
                "uncompleted_tasks_count": matter.get("uncompleted_tasks_count"),
                "last_contacted_at": matter.get("last_contacted_at"),
                "document_count": matter.get("document_count"),
                "lawcus_url": matter.get("lawcus_url"),
            },
        )

    print("LAWCUS Users Matters data inserted into the table successfully.")
