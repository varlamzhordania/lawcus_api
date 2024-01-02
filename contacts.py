from utils import table_exists


def create_contact_table(cursor):
    """
    Create a table for Contacts in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_CONTACTS"

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
            CUSTOM_FIELDS VARCHAR2(4000),
            INTEGRATION VARCHAR2(4000),
            INTEGRATION_ID VARCHAR2(4000),
            BOX_FOLDER_ID VARCHAR2(4000),
            BOX_SHARED_LINK VARCHAR2(4000),
            NUMBERS VARCHAR2(4000),
            QUICKBOOKS_ID VARCHAR2(4000),
            GOOGLE_DRIVE_FOLDER_ID VARCHAR2(4000),
            ONE_DRIVE_FOLDER_ID VARCHAR2(4000),
            ADDRESSES VARCHAR2(4000),
            PHONES VARCHAR2(4000),
            TAGS VARCHAR2(4000),
            EMAILS VARCHAR2(4000),
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
        print("Contact table created successfully.")
    else:
        print("Contact table already exists.")


def insert_contacts_into_table(cursor, contacts):
    """
    Insert contact data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_CONTACTS (
        CONTACT_ID, CONTACT_TYPE, NAME, FIRST_NAME, MIDDLE_NAME, LAST_NAME,
        TITLE, EMAIL, WEBSITE, HOME_PHONE, WORK_PHONE, MOBILE, FAX,
        HOME_ADDRESS, WORK_ADDRESS, CREATED_BY, CREATED_AT, UPDATED_AT,
        UUID, AVATAR, TEAM_ID, COMPANY_ID, CUSTOM_FIELDS, INTEGRATION,
        INTEGRATION_ID, BOX_FOLDER_ID, BOX_SHARED_LINK, NUMBERS, QUICKBOOKS_ID,
        GOOGLE_DRIVE_FOLDER_ID, ONE_DRIVE_FOLDER_ID, ADDRESSES, PHONES, TAGS,
        EMAILS, PHONE, STREET, CITY, STATE, ZIP, COUNTRY, SOURCE, SOURCE_ID,
        REFERRED_BY, REFERRED_BY_TYPE, PREFIX, GENDER, DATE_OF_BIRTHDAY, NOTE,
        STREET2, IS_LEAD, QUICKBOOKS_ERROR, LEDES_CLIENT_ID
    ) VALUES (
        :id, :type, :name, :first_name, :middle_name, :last_name,
        :title, :email, :website, :home_phone, :work_phone, :mobile, :fax,
        :home_address, :work_address, :created_by,:created_at,
        :updated_at, :uuid, :avatar, :team_id,
        :company_id, :custom_fields, :integration, :integration_id, :box_folder_id,
        :box_shared_link, :number, :quickbooks_id, :google_drive_folder_id,
        :one_drive_folder_id, :addresses, :phones, :tags, :emails, :phone,
        :street, :city, :state, :zip, :country, :source, :source_id, :referred_by,
        :referred_by_type, :prefix, :gender, :date_of_birthday, :note, :street2,
        :is_lead, :quickbooks_error, :ledes_client_id
    )
    """

    for contact in contacts:
        cursor.execute(insert_query, contact)
    print("Contacts inserted into the table successfully.")
