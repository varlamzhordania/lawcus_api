from utils import table_exists
from logger import logger
import json


def create_users_me_table(cursor):
    """
    Create a table for LAWOCUS Users Me in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_ME"

    try:
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
            logger.info("LAWCUS Users Me table created successfully.")
        else:
            logger.info("LAWCUS Users Me table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Users Me table: {e}")


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

    try:
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
        cursor.connection.commit()
        logger.info("LAWCUS Users Me data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Users Me data into the table: {e}")


def create_users_teammates_table(cursor):
    """
    Create a table for LAWOCUS Users Teammates in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_TEAMMATES"

    try:
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
            logger.info("LAWCUS Users Teammates table created successfully.")
        else:
            logger.info("LAWCUS Users Teammates table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Users Teammates table: {e}")


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

    try:
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
        cursor.connection.commit()
        logger.info("LAWCUS Users Teammates data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Users Teammates data into the table: {e}")


def create_users_team_table(cursor):
    """
    Create a table for LAWOCUS Users Team in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_TEAM"

    try:
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
            logger.info("LAWCUS Users Team table created successfully.")
        else:
            logger.info("LAWCUS Users Team table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Users Team table: {e}")


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

    try:
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
        cursor.connection.commit()
        logger.info("LAWCUS Users Team data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Users Team data into the table: {e}")


def create_users_contacts_table(cursor):
    """
    Create a table for LAWOCUS Users Contacts in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_CONTACTS"

    try:
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
            logger.info("LAWCUS Users Contacts table created successfully.")
        else:
            logger.info("LAWCUS Users Contacts table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Users Contacts table: {e}")


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
        :my_id, :my_uuid, :my_name, :my_first_name, :my_middle_name, :my_last_name,
        :my_source, :my_type, :my_email, :my_emails, :my_avatar, :my_street, :my_city,
        :my_state, :my_zip, :my_country, :my_number, :my_tags, :my_source_id, :my_referred_by,
        :my_prefix, :my_gender, :my_date_of_birthday, :my_note, :my_addresses, :my_phone,
        :my_phones, :my_is_lead, :my_custom_fields, :my_company_id, :my_associated_user_id,
        :my_last_contacted_at, :my_is_client
    )
    """

    try:
        for contact in contacts_data:

            # Convert lists to a string joined by ';;'
            for key, value in contact.items():
                if isinstance(value, list):
                    contact[key] = ';;'.join(map(str, value))

            cursor.execute(
                insert_query,
                my_id=contact.get("id"),
                my_uuid=contact.get("uuid"),
                my_name=contact.get("name"),
                my_first_name=contact.get("first_name"),
                my_middle_name=contact.get("middle_name"),
                my_last_name=contact.get("last_name"),
                my_source=contact.get("source"),
                my_type=contact.get("type"),
                my_email=contact.get("email"),
                my_emails=contact.get("emails"),
                my_avatar=contact.get("avatar"),
                my_street=contact.get("street"),
                my_city=contact.get("city"),
                my_state=contact.get("state"),
                my_zip=contact.get("zip"),
                my_country=contact.get("country"),
                my_number=contact.get("number"),
                my_tags=contact.get("tags"),
                my_source_id=contact.get("source_id"),
                my_referred_by=contact.get("referred_by"),
                my_prefix=contact.get("prefix"),
                my_gender=contact.get("gender"),
                my_date_of_birthday=contact.get("date_of_birthday"),
                my_note=contact.get("note"),
                my_addresses=contact.get("addresses"),
                my_phone=contact.get("phone"),
                my_phones=contact.get("phones"),
                my_is_lead=contact.get("is_lead"),
                my_custom_fields=contact.get("custom_fields"),
                my_company_id=contact.get("company_id"),
                my_associated_user_id=contact.get("associated_user_id"),
                my_last_contacted_at=contact.get("last_contacted_at"),
                my_is_client=contact.get("is_client"),
            )

        cursor.connection.commit()
        logger.info("LAWCUS Users Contacts data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Users Contacts data into the table: {e}")


def create_users_matters_table(cursor):
    """
    Create a table for LAWOCUS Users Matters in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_MATTERS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_USERS_MATTERS (
                MATTER_ID VARCHAR2(4000),
                UUID VARCHAR2(4000),
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
            logger.info("LAWCUS Users Matters table created successfully.")
        else:
            logger.info("LAWCUS Users Matters table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Users Matters table: {e}")


def create_users_matter_assignees_table(cursor):
    """
    Create a table for Users Matter Assignees in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_MATTER_ASSIGNEES"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_USERS_MATTER_ASSIGNEES (
                MATTER_ID VARCHAR2(4000),
                ASSIGNEES VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Users Matter Assignees table created successfully.")
        else:
            logger.info("Users Matter Assignees table already exists.")
    except Exception as e:
        logger.error(f"Error creating Users Matter Assignees table: {e}")


def create_users_matter_custom_field_table(cursor):
    """
    Create a table for Users Matter Custom Field in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_MATTER_CF"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_USERS_MATTER_CF (
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
            logger.info("Users Matter Custom Field table created successfully.")
        else:
            logger.info("Users Matter Custom Field table already exists.")
    except Exception as e:
        logger.error(f"Error creating Users Matter Custom Field table: {e}")


def create_users_matter_tags_table(cursor):
    """
    Create a table for Users Matter Tags in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_USERS_MATTER_TAGS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_USERS_MATTER_TAGS (
                MATTER_ID VARCHAR2(4000),
                NAME VARCHAR2(4000),
                COLOR VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Users Matter Tags table created successfully.")
        else:
            logger.info("Users Matter Tags table already exists.")
    except Exception as e:
        logger.error(f"Error creating Users Matter Tags table: {e}")


def insert_users_matter_tags_into_table(cursor, matter_id, tags):
    """
    Insert Users Matter Tag data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    try:
        tags_list = json.loads(tags)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON for matter_id {matter_id}: {tags}. Error: {e}")
        return

    try:
        for tag in tags_list:
            cursor.execute(
                """
                INSERT INTO LAWCUS_USERS_MATTER_TAGS (MATTER_ID, NAME, COLOR)
                VALUES (:matter_id, :name, :color)
                """,
                matter_id=matter_id,
                name=tag.get("name"),
                color=tag.get("color")
            )

        cursor.connection.commit()
        logger.info("Users Matter Tags inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting Users Matter Tags into the table: {e}")


def insert_users_matters_into_table(cursor, matters_data):
    """
    Insert LAWOCUS Users Matters data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_USERS_MATTERS (
        MATTER_ID, UUID, COLOR_CODE, NAME, POSITION, STAGE_ID, IS_PRIVATE, PRACTICE,
        DISPLAY_NUMBER, RATES, NUMBERS, DESCRIPTION, STATUS, CLOSED_AT, BILLING_TYPE,
        OPEN_DATE, DUE_DATE, STAGE_POSITION, STAGENAME, RATE, ORIGINATING_TIMEKEEPER_ID,
        RESPONSIBLE_ATTORNEY_ID, SETTLEMENT_AMOUNT, BILLING_ATTORNEY_ID, CREATED_BY,
        LEAD_CREATED_AT, LOCATION, LOCATION_ID, ARCHIVED, TEAM_ID,
        WORKFLOW_ID, CLIENT_ID, CLIENT_REFERRED_BY, CLIENT_SOURCE_ID, ESTIMATED_COST,
        TASK_DUE_DATE, EVERGREEN_RETAINER_AMOUNT, IS_USE_EVERGREEN_RETAINER,
        WORKFLOWNAME, RELATIONS, ASSIGN_ID, STARRED,
        COMPLETED_TASKS_COUNT, UNCOMPLETED_TASKS_COUNT, LAST_CONTACTED_AT,
        DOCUMENT_COUNT, LAWCUS_URL
    ) VALUES (
        :my_id, :my_uuid, :my_color_code, :my_name, :my_position, :my_stage_id,
        :my_is_private, :my_practice, :my_display_number, :my_rates, :my_number, :my_description,
        :my_status, :my_closed_at, :my_billing_type, :my_open_date, :my_due_date,
        :my_stage_position, :my_stagename, :my_rate, :my_originating_timekeeper_id,
        :my_responsible_attorney_id, :my_settlement_amount, :my_billing_attorney_id,
        :my_created_by, :my_lead_created_at, :my_location, :my_location_id,
        :my_archived, :my_team_id, :my_workflow_id, :my_client_id, :my_client_referred_by,
        :my_client_source_id, :my_estimated_cost, :my_task_due_date, :my_evergreen_retainer_amount,
        :my_is_use_evergreen_retainer, :my_workflowname, :my_relations,
        :my_assign_id, :my_starred, :my_completed_tasks_count, :my_uncompleted_tasks_count,
        :my_last_contacted_at, :my_document_count, :my_lawcus_url
    )
    """

    try:
        for matter in matters_data:
            # Extract assignees and custom_fields arrays
            assignees = matter.get("assignees", [])
            custom_fields = matter.get("custom_fields")

            # Insert assignees into LAWCUS_USERS_MATTER_ASSIGNEES
            for assignee in assignees:
                cursor.execute(
                    """
                    INSERT INTO LAWCUS_USERS_MATTER_ASSIGNEES (MATTER_ID, ASSIGNEES)
                    VALUES (:my_matter_id, :my_assignee)
                    """,
                    my_matter_id=matter.get("id"),
                    my_assignee=assignee
                )

            # Insert custom_fields into LAWCUS_USERS_MATTER_CUSTOM_FIELD
            if custom_fields is not None:
                for custom_field in custom_fields:
                    cursor.execute(
                        """
                        INSERT INTO LAWCUS_USERS_MATTER_CF (MATTER_ID, TYPE, NAME, IFDEFAULT, ISREQUIRED, PRACTICE_IDS, VALUE)
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

            # Insert Matter Tags into LAWCUS_USERS_MATTER_TAGS
            tags = matter.get("tags")
            if tags is not None:
                insert_users_matter_tags_into_table(cursor, matter.get("id"), tags)

            # Convert lists to a string joined by ';;'
            for key, value in matter.items():
                if isinstance(value, list):
                    matter[key] = ';;'.join(map(str, value))

            cursor.execute(
                insert_query,
                my_id=matter.get("id"),
                my_uuid=matter.get("uuid"),
                my_color_code=matter.get("color_code"),
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
                my_billing_type=matter.get("billing_type"),
                my_open_date=matter.get("open_date"),
                my_due_date=matter.get("due_date"),
                my_stage_position=matter.get("stage_position"),
                my_stagename=matter.get("stagename"),
                my_rate=matter.get("rate"),
                my_originating_timekeeper_id=matter.get("originating_timekeeper_id"),
                my_responsible_attorney_id=matter.get("responsible_attorney_id"),
                my_settlement_amount=matter.get("settlement_amount"),
                my_billing_attorney_id=matter.get("billing_attorney_id"),
                my_created_by=matter.get("created_by"),
                my_lead_created_at=matter.get("lead_created_at"),
                my_location=matter.get("location"),
                my_location_id=matter.get("location_id"),
                my_archived=matter.get("archived"),
                my_team_id=matter.get("team_id"),
                my_workflow_id=matter.get("workflow_id"),
                my_client_id=matter.get("client_id"),
                my_client_referred_by=matter.get("client_referred_by"),
                my_client_source_id=matter.get("client_source_id"),
                my_estimated_cost=matter.get("estimated_cost"),
                my_task_due_date=matter.get("task_due_date"),
                my_evergreen_retainer_amount=matter.get("evergreen_retainer_amount"),
                my_is_use_evergreen_retainer=matter.get("is_use_evergreen_retainer"),
                my_workflowname=matter.get("workflowname"),
                my_relations=matter.get("relations"),
                my_assign_id=matter.get("assign_id"),
                my_starred=matter.get("starred"),
                my_completed_tasks_count=matter.get("completed_tasks_count"),
                my_uncompleted_tasks_count=matter.get("uncompleted_tasks_count"),
                my_last_contacted_at=matter.get("last_contacted_at"),
                my_document_count=matter.get("document_count"),
                my_lawcus_url=matter.get("lawcus_url"),

            )

        cursor.connection.commit()
        logger.info("LAWCUS Users Matters data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Users Matters data into the table: {e}")
