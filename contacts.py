from utils import table_exists

def create_contact_table(cursor):
    """
    Create a table for Contacts in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "lawkpis_lawcus_contacts"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE lawkpis_lawcus_contacts (
            id NUMBER,
            contact_type VARCHAR2(255),
            name VARCHAR2(255),
            first_name VARCHAR2(255),
            middle_name VARCHAR2(255),
            last_name VARCHAR2(255),
            title VARCHAR2(255),
            email VARCHAR2(255),
            website VARCHAR2(255),
            home_phone NUMBER,
            work_phone NUMBER,
            mobile NUMBER,
            fax NUMBER,
            home_address VARCHAR2(255),
            work_address VARCHAR2(255),
            created_by VARCHAR2(255),
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            uuid VARCHAR2(255),
            avatar VARCHAR2(255),
            team_id NUMBER,
            company_id VARCHAR2(255),
            custom_fields VARCHAR2(255),
            integration VARCHAR2(255),
            integration_id VARCHAR2(255),
            box_folder_id VARCHAR2(255),
            box_shared_link VARCHAR2(255),
            "number" VARCHAR2(255),
            quickbooks_id VARCHAR2(255),
            google_drive_folder_id VARCHAR2(255),
            one_drive_folder_id VARCHAR2(255),
            addresses VARCHAR2(255),
            phones VARCHAR2(255),
            tags VARCHAR2(255),
            emails VARCHAR2(255),
            phone VARCHAR2(255),
            street VARCHAR2(255),
            city VARCHAR2(255),
            state VARCHAR2(255),
            zip VARCHAR2(255),
            country VARCHAR2(255),
            source VARCHAR2(255),
            source_id NUMBER,
            referred_by VARCHAR2(255),
            referred_by_type VARCHAR2(255),
            prefix VARCHAR2(255),
            gender VARCHAR2(255),
            date_of_birthday VARCHAR2(255),
            note VARCHAR2(255),
            street2 VARCHAR2(255),
            is_lead NUMBER(1,0),
            quickbooks_error VARCHAR2(255),
            ledes_client_id VARCHAR2(255)
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
    INSERT INTO lawkpis_lawcus_contacts (
        id, contact_type, name, first_name, middle_name, last_name,
        title, email, website, home_phone, work_phone, mobile, fax,
        home_address, work_address, created_by, created_at, updated_at,
        uuid, avatar, team_id, company_id, custom_fields, integration,
        integration_id, box_folder_id, box_shared_link, number, quickbooks_id,
        google_drive_folder_id, one_drive_folder_id, addresses, phones, tags,
        emails, phone, street, city, state, zip, country, source, source_id,
        referred_by, referred_by_type, prefix, gender, date_of_birthday, note,
        street2, is_lead, quickbooks_error, ledes_client_id
    ) VALUES (
        :id, :type, :name, :first_name, :middle_name, :last_name,
        :title, :email, :website, :home_phone, :work_phone, :mobile, :fax,
        :home_address, :work_address, :created_by, TO_TIMESTAMP(:created_at, 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP(:updated_at, 'YYYY-MM-DD HH24:MI:SS'), :uuid, :avatar, :team_id,
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
