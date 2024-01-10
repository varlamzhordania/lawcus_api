import json
from utils import table_exists
from logger import contacts_logger as logger


def create_contact_table(cursor):
    """
    Create a table for Contacts in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_CONTACTS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_CONTACTS (
                CONTACT_ID VARCHAR2(4000),
                CONTACT_TYPE VARCHAR2(4000),
                NAME VARCHAR2(4000),
                FIRST_NAME VARCHAR2(4000),
                MIDDLE_NAME VARCHAR2(4000),
                LAST_NAME VARCHAR2(4000),
                TITLE VARCHAR2(4000),
                EMAIL VARCHAR2(4000),
                WEBSITE VARCHAR2(4000),
                HOME_PHONE VARCHAR2(4000),
                WORK_PHONE VARCHAR2(4000),
                MOBILE VARCHAR2(4000),
                FAX VARCHAR2(4000),
                HOME_ADDRESS VARCHAR2(4000),
                WORK_ADDRESS VARCHAR2(4000),
                CREATED_BY VARCHAR2(4000),
                CREATED_AT VARCHAR2(4000),
                UPDATED_AT VARCHAR2(4000),
                UUID VARCHAR2(4000),
                AVATAR VARCHAR2(4000),
                TEAM_ID VARCHAR2(4000),
                COMPANY_ID VARCHAR2(4000),
                INTEGRATION VARCHAR2(4000),
                INTEGRATION_ID VARCHAR2(4000),
                BOX_FOLDER_ID VARCHAR2(4000),
                BOX_SHARED_LINK VARCHAR2(4000),
                NUMBERS VARCHAR2(4000),
                QUICKBOOKS_ID VARCHAR2(4000),
                GOOGLE_DRIVE_FOLDER_ID VARCHAR2(4000),
                ONE_DRIVE_FOLDER_ID VARCHAR2(4000),
                PHONE VARCHAR2(4000),
                STREET VARCHAR2(4000),
                CITY VARCHAR2(4000),
                STATE VARCHAR2(4000),
                ZIP VARCHAR2(4000),
                COUNTRY VARCHAR2(4000),
                SOURCE VARCHAR2(4000),
                SOURCE_ID VARCHAR2(4000),
                REFERRED_BY VARCHAR2(4000),
                REFERRED_BY_TYPE VARCHAR2(4000),
                PREFIX VARCHAR2(4000),
                GENDER VARCHAR2(4000),
                DATE_OF_BIRTHDAY VARCHAR2(4000),
                NOTE VARCHAR2(4000),
                STREET2 VARCHAR2(4000),
                IS_LEAD VARCHAR2(4000),
                QUICKBOOKS_ERROR VARCHAR2(4000),
                LEDES_CLIENT_ID VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Contact table created successfully.")
        else:
            logger.info("Contact table already exists.")
    except Exception as e:
        logger.error(f"Error creating contact table: {e}")


def create_contact_address_table(cursor):
    """
    Create a table for Contact Addresses in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_CONTACT_ADDRESS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_CONTACT_ADDRESS (
                CONTACT_ID VARCHAR2(4000),
                IS_PRIMARY VARCHAR2(4000),
                TYPE VARCHAR2(4000),
                STREET VARCHAR2(4000),
                CITY VARCHAR2(4000),
                STATE VARCHAR2(4000),
                ZIP VARCHAR2(4000),
                COUNTRY VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Contact address table created successfully.")
        else:
            logger.info("Contact address table already exists.")
    except Exception as e:
        logger.error(f"Error creating contact address table: {e}")


def create_contact_phones_table(cursor):
    """
    Create a table for Contact Phones in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_CONTACT_PHONES"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_CONTACT_PHONES (
                CONTACT_ID VARCHAR2(4000),
                TYPE VARCHAR2(4000),
                VALUE VARCHAR2(4000),
                IS_PRIMARY VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Contact phones table created successfully.")
        else:
            logger.info("Contact phones table already exists.")
    except Exception as e:
        logger.error(f"Error creating contact phones table: {e}")


def create_contact_tags_table(cursor):
    """
    Create a table for Contact Tags in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_CONTACT_TAGS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_CONTACT_TAGS (
                CONTACT_ID VARCHAR2(4000),
                NAME VARCHAR2(4000),
                COLOR VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Contact  tags table created successfully.")
        else:
            logger.info("Contact tags table already exists.")
    except Exception as e:
        logger.error(f"Error creating contact tags table: {e}")


def create_emails_table(cursor):
    """
    Create a table for Contact Emails in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_CONTACT_EMAILS"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_CONTACT_EMAILS (
                CONTACT_ID VARCHAR2(4000),
                EMAIL_TYPE VARCHAR2(4000),
                EMAIL_VALUE VARCHAR2(4000),
                IS_PRIMARY VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Contact emails table created successfully.")
        else:
            logger.info("Contact Emails table already exists.")
    except Exception as e:
        logger.error(f"Error creating contact emails table: {e}")


def create_contact_custom_field_table(cursor):
    """
    Create a table for Contact Custom Field in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_CONTACT_CUSTOM_FIELD"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_CONTACT_CUSTOM_FIELD (
                CONTACT_ID VARCHAR2(4000),
                TYPE VARCHAR2(4000),
                NAME VARCHAR2(4000),
                IFDEFAULT VARCHAR2(4000),
                ISREQUIRED VARCHAR2(4000),
                PRACTICE_IDS VARCHAR2(4000),
                VALUE VARCHAR2(4000)
            )
            """
            cursor.execute(table_creation_query)
            logger.info("Contact Custom Field table created successfully.")
        else:
            logger.info("Contact Custom Field table already exists.")
    except Exception as e:
        logger.error(f"Error creating contact Custom Field table: {e}")


def insert_contact_addresses_into_table(cursor, contact_id, addresses):
    """
    Insert Contact address data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """

    insert_query = """
        INSERT INTO LAWCUS_CONTACT_ADDRESS (
            CONTACT_ID, IS_PRIMARY, TYPE, STREET, CITY, STATE, ZIP, COUNTRY
        ) VALUES (
            :my_contact_id, :my_is_primary, :my_type, :my_street, :my_city, :my_state, :my_zip, :my_country
        )
        """

    try:
        for address in addresses:
            # Execute the query with parameters
            cursor.execute(
                insert_query,
                my_contact_id=contact_id,
                my_is_primary=address.get("is_primary"),
                my_type=address.get("type"),
                my_street=address.get("street"),
                my_city=address.get("city"),
                my_state=address.get("state"),
                my_zip=address.get("zip"),
                my_country=address.get("country")
            )

        cursor.connection.commit()
        logger.info("Contact addresses inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting contact addresses into the table: {e}")


def insert_contact_phones_into_table(cursor, contact_id, phones):
    """
    Insert Contact phone data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """

    insert_query = """
        INSERT INTO LAWCUS_CONTACT_PHONES (
            CONTACT_ID, TYPE, VALUE, IS_PRIMARY
        ) VALUES (
            :my_contact_id, :my_type, :my_value, :my_is_primary
        )
        """

    try:
        for phone in phones:
            # Execute the query with parameters
            cursor.execute(
                insert_query,
                my_contact_id=contact_id,
                my_type=phone.get("type"),
                my_value=phone.get("value"),
                my_is_primary=phone.get("is_primary")
            )

        cursor.connection.commit()
        logger.info("Contact phones inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting contact phones into the table: {e}")


def insert_contact_tags_into_table(cursor, contact_id, tags):
    """
    Insert Contact tag data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    # Define the parameterized query
    insert_query = """
        INSERT INTO LAWCUS_CONTACT_TAGS (
            CONTACT_ID, NAME, COLOR
        ) VALUES (
            :my_contact_id, :my_name, :my_color
        )
        """

    try:
        for tag in tags:
            # Execute the query with parameters
            cursor.execute(
                insert_query,
                my_contact_id=contact_id,
                my_name=tag.get("name"),
                my_color=tag.get("color")
            )

        cursor.connection.commit()
        logger.info("Contact tags inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting contact tags into the table: {e}")


def insert_emails_into_table(cursor, contact_id, emails):
    """
    Insert contact email data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """

    insert_query = """
        INSERT INTO LAWCUS_CONTACT_EMAILS (
            CONTACT_ID, EMAIL_TYPE, EMAIL_VALUE, IS_PRIMARY
        ) VALUES (
            :contact_id, :email_type, :email_value, :is_primary
        )
        """

    try:
        for email in emails:
            # Execute the query with parameters
            cursor.execute(
                insert_query,
                contact_id=contact_id,
                email_type=email.get("type"),
                email_value=email.get("value"),
                is_primary=str(email.get("is_primary"))
            )

        cursor.connection.commit()
        logger.info("Contact emails inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting contact emails into the table: {e}")


def insert_contacts_into_table(cursor, contacts):
    """
    Insert contact data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    # Define the parameterized query
    insert_query = """
        INSERT INTO LAWCUS_CONTACTS (
            CONTACT_ID, CONTACT_TYPE, NAME, FIRST_NAME, MIDDLE_NAME, LAST_NAME,
            TITLE, EMAIL, WEBSITE, HOME_PHONE, WORK_PHONE, MOBILE, FAX,
            HOME_ADDRESS, WORK_ADDRESS, CREATED_BY, CREATED_AT, UPDATED_AT,
            UUID, AVATAR, TEAM_ID, COMPANY_ID, INTEGRATION,
            INTEGRATION_ID, BOX_FOLDER_ID, BOX_SHARED_LINK, NUMBERS, QUICKBOOKS_ID,
            GOOGLE_DRIVE_FOLDER_ID, ONE_DRIVE_FOLDER_ID,   
            PHONE, STREET, CITY, STATE, ZIP, COUNTRY, SOURCE, SOURCE_ID,
            REFERRED_BY, REFERRED_BY_TYPE, PREFIX, GENDER, DATE_OF_BIRTHDAY, NOTE,
            STREET2, IS_LEAD, QUICKBOOKS_ERROR, LEDES_CLIENT_ID
        ) VALUES (
            :my_id, :my_type, :my_name, :my_first_name, :my_middle_name, :my_last_name,
            :my_title, :my_email, :my_website, :my_home_phone, :my_work_phone, :my_mobile, :my_fax,
            :my_home_address, :my_work_address, :my_created_by, :my_created_at,
            :my_updated_at, :my_uuid, :my_avatar, :my_team_id,
            :my_company_id, :my_integration, :my_integration_id, :my_box_folder_id,
            :my_box_shared_link, :my_number, :my_quickbooks_id, :my_google_drive_folder_id,
            :my_one_drive_folder_id, :my_phone,
            :my_street, :my_city, :my_state, :my_zip, :my_country, :my_source, :my_source_id, :my_referred_by,
            :my_referred_by_type, :my_prefix, :my_gender, :my_date_of_birthday, :my_note, :my_street2,
            :my_is_lead, :my_quickbooks_error, :my_ledes_client_id
        )
        """

    try:
        for contact in contacts:
            contact_id = contact.get("id")
            addresses_json = contact.get("addresses", '[]')
            phones_json = contact.get("phones", '[]')
            custom_fields_json = contact.get("custom_fields", '[]')
            tags_json = contact.get("tags") or '[]'
            emails_json = contact.get("emails", '[]')

            if addresses_json is not None:
                addresses = json.loads(addresses_json)
                insert_contact_addresses_into_table(cursor, contact_id, addresses)

            if phones_json is not None:
                phones = json.loads(phones_json)
                insert_contact_phones_into_table(cursor, contact_id, phones)

            if tags_json is not None:
                tags = json.loads(tags_json)
                insert_contact_tags_into_table(cursor, contact_id, tags)

            if emails_json is not None:
                emails = json.loads(emails_json)
                insert_emails_into_table(cursor, contact_id, emails)

            # Insert custom_fields into LAWCUS_CONTACT_CUSTOM_FIELD
            if custom_fields_json is not None:
                custom_fields = json.loads(custom_fields_json)
                for custom_field in custom_fields:
                    cursor.execute(
                        """
                        INSERT INTO LAWCUS_CONTACT_CUSTOM_FIELD (CONTACT_ID, TYPE, NAME, IFDEFAULT, ISREQUIRED, PRACTICE_IDS, VALUE)
                        VALUES (:my_contact_id, :my_type,:my_name, :my_ifdefault, :my_isrequired, :my_practice_ids, :my_value)
                        """,
                        my_contact_id=contact_id,
                        my_type=custom_field.get("type"),
                        my_name=custom_field.get("name"),
                        my_ifdefault=custom_field.get("ifdefault"),
                        my_isrequired=custom_field.get("isrequired"),
                        my_practice_ids=custom_field.get("practice_ids"),
                        my_value=custom_field.get("value")
                    )

            # Execute the query with parameters

            cursor.execute(
                insert_query,
                my_id=contact_id,
                my_type=contact.get("type"),
                my_name=contact.get("name"),
                my_first_name=contact.get("first_name"),
                my_middle_name=contact.get("middle_name"),
                my_last_name=contact.get("last_name"),
                my_title=contact.get("title"),
                my_email=contact.get("email"),
                my_website=contact.get("website"),
                my_home_phone=contact.get("home_phone"),
                my_work_phone=contact.get("work_phone"),
                my_mobile=contact.get("mobile"),
                my_fax=contact.get("fax"),
                my_home_address=contact.get("home_address"),
                my_work_address=contact.get("work_address"),
                my_created_by=contact.get("created_by"),
                my_created_at=contact.get("created_at"),
                my_updated_at=contact.get("updated_at"),
                my_uuid=contact.get("uuid"),
                my_avatar=contact.get("avatar"),
                my_team_id=contact.get("team_id"),
                my_company_id=contact.get("company_id"),
                my_integration=contact.get("integration"),
                my_integration_id=contact.get("integration_id"),
                my_box_folder_id=contact.get("box_folder_id"),
                my_box_shared_link=contact.get("box_shared_link"),
                my_number=contact.get("number"),
                my_quickbooks_id=contact.get("quickbooks_id"),
                my_google_drive_folder_id=contact.get("google_drive_folder_id"),
                my_one_drive_folder_id=contact.get("one_drive_folder_id"),
                my_phone=contact.get("phone"),
                my_street=contact.get("street"),
                my_city=contact.get("city"),
                my_state=contact.get("state"),
                my_zip=contact.get("zip"),
                my_country=contact.get("country"),
                my_source=contact.get("source"),
                my_source_id=contact.get("source_id"),
                my_referred_by=contact.get("referred_by"),
                my_referred_by_type=contact.get("referred_by_type"),
                my_prefix=contact.get("prefix"),
                my_gender=contact.get("gender"),
                my_date_of_birthday=contact.get("date_of_birthday"),
                my_note=contact.get("note"),
                my_street2=contact.get("street2"),
                my_is_lead=contact.get("is_lead"),
                my_quickbooks_error=contact.get("quickbooks_error"),
                my_ledes_client_id=contact.get("ledes_client_id")
            )

        cursor.connection.commit()
        logger.info("Contacts inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting contacts into the table: {e}")
