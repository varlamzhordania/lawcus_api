from utils import table_exists


def create_reports_payment_collected_table(cursor):
    """
    Create a table for LAWOCUS Reports Payment Collected in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_COLLECTED"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_COLLECTED (
            KEY VARCHAR2(4000),
            PAYMENT_ID VARCHAR2(4000),
            ACCOUNT_ID VARCHAR2(4000),
            CREDIT VARCHAR2(4000),
            DEBIT VARCHAR2(4000),
            DATES VARCHAR2(4000),
            SOURCE VARCHAR2(4000),
            TYPE VARCHAR2(4000),
            CURRENCY VARCHAR2(4000),
            EXCHANGE_RATE VARCHAR2(4000),
            DESCRIPTION VARCHAR2(4000),
            CHECKS VARCHAR2(4000),
            CLIENT_ID VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            CREATED_AT VARCHAR2(4000),
            UPDATED_AT VARCHAR2(4000),
            SOURCE_ID VARCHAR2(4000),
            DESTINATION_ID VARCHAR2(4000),
            TRANSFER_ID VARCHAR2(4000),
            SOURCE_ITEM_ID VARCHAR2(4000),
            DESTINATION_ITEM_ID VARCHAR2(4000),
            SOURCE_ITEM_TYPE VARCHAR2(4000),
            DESTINATION_ITEM_TYPE VARCHAR2(4000),
            PAYMENT_ID_COL VARCHAR2(4000),
            QUICKBOOKS_ID VARCHAR2(4000),
            INVOICE_ID VARCHAR2(4000),
            TRANSACTION_TYPE VARCHAR2(4000),
            SOURCE_TYPE VARCHAR2(4000),
            NOTE VARCHAR2(4000),
            INTEGRATION_ID VARCHAR2(4000),
            INTEGRATION_STATUS VARCHAR2(4000),
            INTEGRATION_ACCOUNT_ID VARCHAR2(4000),
            QUICKBOOKS_JOURNAL_ID VARCHAR2(4000),
            QUICKBOOKS_ERROR VARCHAR2(4000),
            INVOICE_NUMBER VARCHAR2(4000),
            CLIENT_NAME VARCHAR2(4000),
            AMOUNT VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            RESPONSIBLE_ATTORNEY_ID VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Payment Collected table created successfully.")
    else:
        print("LAWCUS Reports Payment Collected table already exists.")


def insert_reports_payment_collected_into_table(cursor, payment_collected_reports):
    """
    Insert LAWOCUS Reports Payment Collected data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_COLLECTED (
        KEY, PAYMENT_ID, ACCOUNT_ID, CREDIT, DEBIT, DATES, SOURCE, TYPE, CURRENCY,
        EXCHANGE_RATE, DESCRIPTION, CHECKS, CLIENT_ID, MATTER_ID, CREATED_BY, TEAM_ID,
        CREATED_AT, UPDATED_AT, SOURCE_ID, DESTINATION_ID, TRANSFER_ID, SOURCE_ITEM_ID,
        DESTINATION_ITEM_ID, SOURCE_ITEM_TYPE, DESTINATION_ITEM_TYPE, PAYMENT_ID_COL,
        QUICKBOOKS_ID, INVOICE_ID, TRANSACTION_TYPE, SOURCE_TYPE, NOTE, INTEGRATION_ID,
        INTEGRATION_STATUS, INTEGRATION_ACCOUNT_ID, QUICKBOOKS_JOURNAL_ID, QUICKBOOKS_ERROR,
        INVOICE_NUMBER, CLIENT_NAME, AMOUNT, MATTER_NAME, RESPONSIBLE_ATTORNEY_ID
    ) VALUES (
        :key, :payment_id, :account_id, :credit, :debit,:date,
        :source, :type, :currency, :exchange_rate, :description, :check, :client_id, :matter_id,
        :created_by, :team_id, :created_at,:updated_at, :source_id, :destination_id, :transfer_id,
        :source_item_id, :destination_item_id, :source_item_type, :destination_item_type,
        :payment_id_col, :quickbooks_id, :invoice_id, :transaction_type, :source_type,
        :note, :integration_id, :integration_status, :integration_account_id,
        :quickbooks_journal_id, :quickbooks_error, :invoice_number, :client_name,
        :amount, :matter_name, :responsible_attorney_id
    )
    """

    for report in payment_collected_reports:
        key = report.get("key")
        payments = report.get("payments", [])

        for payment in payments:
            cursor.execute(
                insert_query,
                {
                    "key": key,
                    "payment_id": payment.get("id"),
                    "account_id": payment.get("account_id"),
                    "credit": payment.get("credit"),
                    "debit": payment.get("debit"),
                    "date": payment.get("date"),
                    "source": payment.get("source"),
                    "type": payment.get("type"),
                    "currency": payment.get("currency"),
                    "exchange_rate": payment.get("exchange_rate"),
                    "description": payment.get("description"),
                    "check": payment.get("check"),
                    "client_id": payment.get("client_id"),
                    "matter_id": payment.get("matter_id"),
                    "created_by": payment.get("created_by"),
                    "team_id": payment.get("team_id"),
                    "created_at": payment.get("created_at"),
                    "updated_at": payment.get("updated_at"),
                    "source_id": payment.get("source_id"),
                    "destination_id": payment.get("destination_id"),
                    "transfer_id": payment.get("transfer_id"),
                    "source_item_id": payment.get("source_item_id"),
                    "destination_item_id": payment.get("destination_item_id"),
                    "source_item_type": payment.get("source_item_type"),
                    "destination_item_type": payment.get("destination_item_type"),
                    "payment_id_col": payment.get("payment_id"),
                    "quickbooks_id": payment.get("quickbooks_id"),
                    "invoice_id": payment.get("invoice_id"),
                    "transaction_type": payment.get("transaction_type"),
                    "source_type": payment.get("source_type"),
                    "note": payment.get("note"),
                    "integration_id": payment.get("integration_id"),
                    "integration_status": payment.get("integration_status"),
                    "integration_account_id": payment.get("integration_account_id"),
                    "quickbooks_journal_id": payment.get("quickbooks_journal_id"),
                    "quickbooks_error": payment.get("quickbooks_error"),
                    "invoice_number": payment.get("invoice_number"),
                    "client_name": payment.get("client_name"),
                    "amount": payment.get("amount"),
                    "matter_name": payment.get("matter_name"),
                    "responsible_attorney_id": payment.get("responsible_attorney_id"),
                }
            )
    print("LAWCUS Reports Payment Collected data inserted into the table successfully.")


def create_reports_invoice_history_table(cursor):
    """
    Create a table for LAWOCUS Reports Invoice History in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_INVOICE_HISTORY"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_INVOICE_HISTORY (
            INVOICE_HISTORY_ID VARCHAR2(4000),
            AMOUNT_DUE VARCHAR2(4000),
            DISCOUNT VARCHAR2(4000),
            DISCOUNT_TYPE VARCHAR2(4000),
            TYPE VARCHAR2(4000),
            CLIENT_ID VARCHAR2(4000),
            NUMBERS VARCHAR2(4000),
            TOTAL VARCHAR2(4000),
            SUB_TOTAL VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            ISSUE_DATE VARCHAR2(4000),
            DUE_DATE VARCHAR2(4000),
            STATUS VARCHAR2(4000),
            PRACTICE VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000),
            RESPONSIBLE_ATTORNEY_ID VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Invoice History table created successfully.")
    else:
        print("LAWCUS Reports Invoice History table already exists.")


def insert_reports_invoice_history_into_table(cursor, invoice_history_report):
    """
    Insert LAWOCUS Reports Invoice History data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_INVOICE_HISTORY (
        INVOICE_HISTORY_ID, AMOUNT_DUE, DISCOUNT, DISCOUNT_TYPE, TYPE, CLIENT_ID, NUMBERS, TOTAL,
        SUB_TOTAL, MATTER_ID, CREATED_BY, ISSUE_DATE, DUE_DATE, STATUS, PRACTICE,
        MATTER_NAME, MATTER_UUID, RESPONSIBLE_ATTORNEY_ID
    ) VALUES (
        :id, :amount_due, :discount, :discount_type, :type, :client_id, :number, :total,
        :sub_total, :matter_id, :created_by, :issue_date,:due_date, :status, :practice,
        :matter_name, :matter_uuid, :responsible_attorney_id
    )
    """

    for client_id, invoices in invoice_history_report.get("list", {}).items():
        for invoice in invoices:
            cursor.execute(
                insert_query,
                {
                    "id": invoice.get("id"),
                    "amount_due": invoice.get("amount_due"),
                    "discount": invoice.get("discount"),
                    "discount_type": invoice.get("discount_type"),
                    "type": invoice.get("type"),
                    "client_id": invoice.get("client_id"),
                    "number": invoice.get("number"),
                    "total": invoice.get("total"),
                    "sub_total": invoice.get("sub_total"),
                    "matter_id": invoice.get("matter_id"),
                    "created_by": invoice.get("created_by"),
                    "issue_date": invoice.get("issue_date"),
                    "due_date": invoice.get("due_date"),
                    "status": invoice.get("status"),
                    "practice": invoice.get("practice"),
                    "matter_name": invoice.get("matter_name"),
                    "matter_uuid": invoice.get("matter_uuid"),
                    "responsible_attorney_id": invoice.get("responsible_attorney_id"),
                }
            )

    print("LAWCUS Reports Invoice History data inserted into the table successfully.")


def create_reports_matter_balance_table(cursor):
    """
    Create a table for LAWOCUS Reports Matter Balance in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_MATTER_BALANCE"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_MATTER_BALANCE (
            KEY VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            DESCRIPTION VARCHAR2(4000),
            DISPLAY_NUMBER VARCHAR2(4000),
            CLIENT_ID VARCHAR2(4000),
            RESPONSIBLE_ATTORNEY_ID VARCHAR2(4000),
            RECEIVABLE VARCHAR2(4000),
            EXPENSES_IN_WORK VARCHAR2(4000),
            TIME_ENTRY_IN_WORK VARCHAR2(4000),
            FLAT_FEE_IN_WORK VARCHAR2(4000),
            TRUST VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Matter Balance table created successfully.")
    else:
        print("LAWCUS Reports Matter Balance table already exists.")


def insert_reports_matter_balance_into_table(cursor, matter_balance_report):
    """
    Insert LAWOCUS Reports Matter Balance data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_MATTER_BALANCE (
        KEY, MATTER_ID, MATTER_NAME, DESCRIPTION, DISPLAY_NUMBER, CLIENT_ID,
        RESPONSIBLE_ATTORNEY_ID, RECEIVABLE, EXPENSES_IN_WORK, TIME_ENTRY_IN_WORK,
        FLAT_FEE_IN_WORK, TRUST
    ) VALUES (
        :key, :matter_id, :matter_name, :description, :display_number, :client_id,
        :responsible_attorney_id, :receivable, :expenses_in_work, :time_entry_in_work,
        :flat_fee_in_work, :trust
    )
    """

    for client_id, matters_data in matter_balance_report.items():
        for matter_data in matters_data.get("matters", []):
            cursor.execute(
                insert_query,
                {
                    "key": matters_data.get("key"),
                    "matter_id": matter_data.get("id"),
                    "matter_name": matter_data.get("name"),
                    "description": matter_data.get("description"),
                    "display_number": matter_data.get("display_number"),
                    "client_id": matter_data.get("client_id"),
                    "responsible_attorney_id": matter_data.get("responsible_attorney_id"),
                    "receivable": matter_data.get("receivable"),
                    "expenses_in_work": matter_data.get("expenses_in_work"),
                    "time_entry_in_work": matter_data.get("time_entry_in_work"),
                    "flat_fee_in_work": matter_data.get("flat_fee_in_work"),
                    "trust": matters_data.get("trust"),
                }
            )

    print("LAWCUS Reports Matter Balance data inserted into the table successfully.")


def create_reports_client_trust_table(cursor):
    """
    Create a table for LAWOCUS Reports Client Trust in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_CLIENT_TRUST"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_CLIENT_TRUST (
            KEY VARCHAR2(4000),
            ACCOUNT_NAME VARCHAR2(4000),
            ACCOUNT_ID VARCHAR2(4000),
            LAST_DATE VARCHAR2(4000),
            TOTAL VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Client Trust table created successfully.")
    else:
        print("LAWCUS Reports Client Trust table already exists.")


def insert_reports_client_trust_into_table(cursor, client_trust_report):
    """
    Insert LAWOCUS Reports Client Trust data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_CLIENT_TRUST (
        KEY, ACCOUNT_NAME, ACCOUNT_ID, LAST_DATE, TOTAL
    ) VALUES (
        :key, :account_name, :account_id, :last_date, :total
    )
    """

    for client_data in client_trust_report:
        for account_data in client_data.get("accounts", []):
            cursor.execute(
                insert_query,
                {
                    "key": client_data.get("key"),
                    "account_name": account_data.get("name"),
                    "account_id": account_data.get("id"),
                    "last_date": account_data.get("last_date"),
                    "total": account_data.get("total"),
                }
            )

    print("LAWCUS Reports Client Trust data inserted into the table successfully.")


def create_reports_client_ledger_table(cursor):
    """
    Create a table for LAWOCUS Reports Client Ledger in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_CLIENT_LEDGER"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_CLIENT_LEDGER (
            KEY NUMBER,
            TRANSACTION_ID VARCHAR2(4000),
            ACCOUNT_ID VARCHAR2(4000),
            CREDIT VARCHAR2(4000),
            DEBIT VARCHAR2(4000),
            TRANSACTION_DATE VARCHAR2(4000),
            SOURCE VARCHAR2(4000),
            TYPE VARCHAR2(4000),
            CURRENCY VARCHAR2(4000),
            EXCHANGE_RATE VARCHAR2(4000),
            DESCRIPTION VARCHAR2(4000),
            CHECKS VARCHAR2(4000),
            CLIENT_ID VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            CREATED_AT VARCHAR2(4000),
            UPDATED_AT VARCHAR2(4000),
            SOURCE_ID VARCHAR2(4000),
            DESTINATION_ID VARCHAR2(4000),
            TRANSFER_ID VARCHAR2(4000),
            SOURCE_ITEM_ID VARCHAR2(4000),
            DESTINATION_ITEM_ID VARCHAR2(4000),
            SOURCE_ITEM_TYPE VARCHAR2(4000),
            DESTINATION_ITEM_TYPE VARCHAR2(4000),
            PAYMENT_ID VARCHAR2(4000),
            QUICKBOOKS_ID VARCHAR2(4000),
            INVOICE_ID VARCHAR2(4000),
            TRANSACTION_TYPE VARCHAR2(4000),
            SOURCE_TYPE VARCHAR2(4000),
            NOTE VARCHAR2(4000),
            INTEGRATION_ID VARCHAR2(4000),
            INTEGRATION_STATUS VARCHAR2(4000),
            INTEGRATION_ACCOUNT_ID VARCHAR2(4000),
            QUICKBOOKS_JOURNAL_ID VARCHAR2(4000),
            QUICKBOOKS_ERROR VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000),
            ACCOUNT_NAME VARCHAR2(4000),
            CLIENT_NAME VARCHAR2(4000),
            CLIENT_UUID VARCHAR2(4000),
            SOURCE_NAME VARCHAR2(4000),
            DESTINATION_NAME VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Client Ledger table created successfully.")
    else:
        print("LAWCUS Reports Client Ledger table already exists.")


def insert_reports_client_ledger_into_table(cursor, client_ledger_report):
    """
    Insert LAWOCUS Reports Client Ledger data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_CLIENT_LEDGER (
        KEY, TRANSACTION_ID, ACCOUNT_ID, CREDIT, DEBIT, TRANSACTION_DATE, SOURCE,
        TYPE, CURRENCY, EXCHANGE_RATE, DESCRIPTION, CHECKS, CLIENT_ID, MATTER_ID,
        CREATED_BY, TEAM_ID, CREATED_AT, UPDATED_AT, SOURCE_ID, DESTINATION_ID,
        TRANSFER_ID, SOURCE_ITEM_ID, DESTINATION_ITEM_ID, SOURCE_ITEM_TYPE,
        DESTINATION_ITEM_TYPE, PAYMENT_ID, QUICKBOOKS_ID, INVOICE_ID,
        TRANSACTION_TYPE, SOURCE_TYPE, NOTE, INTEGRATION_ID, INTEGRATION_STATUS,
        INTEGRATION_ACCOUNT_ID, QUICKBOOKS_JOURNAL_ID, QUICKBOOKS_ERROR, MATTER_NAME,
        MATTER_UUID, ACCOUNT_NAME, CLIENT_NAME, CLIENT_UUID, SOURCE_NAME,
        DESTINATION_NAME
    ) VALUES (
        :key, :transaction_id, :account_id, :credit, :debit,:transaction_date,
        :source, :type, :currency, :exchange_rate, :description, :check,
        :client_id, :matter_id, :created_by, :team_id,:created_at,:updated_at, :source_id, :destination_id,
        :transfer_id, :source_item_id, :destination_item_id, :source_item_type,
        :destination_item_type, :payment_id, :quickbooks_id, :invoice_id,
        :transaction_type, :source_type, :note, :integration_id, :integration_status,
        :integration_account_id, :quickbooks_journal_id, :quickbooks_error, :matter_name,
        :matter_uuid, :account_name, :client_name, :client_uuid, :source_name,
        :destination_name
    )
    """

    for client_data in client_ledger_report:
        for transaction_data in client_data.get("transactions", []):
            cursor.execute(
                insert_query,
                {
                    "key": client_data.get("key"),
                    "transaction_id": transaction_data.get("id"),
                    "account_id": transaction_data.get("account_id"),
                    "credit": transaction_data.get("credit"),
                    "debit": transaction_data.get("debit"),
                    "transaction_date": transaction_data.get("date"),
                    "source": transaction_data.get("source"),
                    "type": transaction_data.get("type"),
                    "currency": transaction_data.get("currency"),
                    "exchange_rate": transaction_data.get("exchange_rate"),
                    "description": transaction_data.get("description"),
                    "check": transaction_data.get("check"),
                    "client_id": transaction_data.get("client_id"),
                    "matter_id": transaction_data.get("matter_id"),
                    "created_by": transaction_data.get("created_by"),
                    "team_id": transaction_data.get("team_id"),
                    "created_at": transaction_data.get("created_at"),
                    "updated_at": transaction_data.get("updated_at"),
                    "source_id": transaction_data.get("source_id"),
                    "destination_id": transaction_data.get("destination_id"),
                    "transfer_id": transaction_data.get("transfer_id"),
                    "source_item_id": transaction_data.get("source_item_id"),
                    "destination_item_id": transaction_data.get("destination_item_id"),
                    "source_item_type": transaction_data.get("source_item_type"),
                    "destination_item_type": transaction_data.get("destination_item_type"),
                    "payment_id": transaction_data.get("payment_id"),
                    "quickbooks_id": transaction_data.get("quickbooks_id"),
                    "invoice_id": transaction_data.get("invoice_id"),
                    "transaction_type": transaction_data.get("transaction_type"),
                    "source_type": transaction_data.get("source_type"),
                    "note": transaction_data.get("note"),
                    "integration_id": transaction_data.get("integration_id"),
                    "integration_status": transaction_data.get("integration_status"),
                    "integration_account_id": transaction_data.get("integration_account_id"),
                    "quickbooks_journal_id": transaction_data.get("quickbooks_journal_id"),
                    "quickbooks_error": transaction_data.get("quickbooks_error"),
                    "matter_name": transaction_data.get("mattername"),
                    "matter_uuid": transaction_data.get("matteruuid"),
                    "account_name": transaction_data.get("accountname"),
                    "client_name": transaction_data.get("clientname"),
                    "client_uuid": transaction_data.get("clientuuid"),
                    "source_name": transaction_data.get("sourcename"),
                    "destination_name": transaction_data.get("destinationname"),
                }
            )

    print("LAWCUS Reports Client Ledger data inserted into the table successfully.")


def create_reports_trust_ledger_table(cursor):
    """
    Create a table for LAWOCUS Reports Trust Ledger in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_TRUST_LEDGER"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_TRUST_LEDGER (
            KEY VARCHAR2(4000),
            TRANSACTION_ID VARCHAR2(4000),
            ACCOUNT_ID VARCHAR2(4000),
            CREDIT VARCHAR2(4000),
            DEBIT VARCHAR2(4000),
            TRANSACTION_DATE VARCHAR2(4000),
            SOURCE VARCHAR2(4000),
            TYPE VARCHAR2(4000),
            CURRENCY VARCHAR2(4000),
            EXCHANGE_RATE VARCHAR2(4000),
            DESCRIPTION VARCHAR2(4000),
            CHECKS VARCHAR2(4000),
            CLIENT_ID VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            CREATED_AT VARCHAR2(4000),
            UPDATED_AT VARCHAR2(4000),
            SOURCE_ID VARCHAR2(4000),
            DESTINATION_ID VARCHAR2(4000),
            TRANSFER_ID VARCHAR2(4000),
            SOURCE_ITEM_ID VARCHAR2(4000),
            DESTINATION_ITEM_ID VARCHAR2(4000),
            SOURCE_ITEM_TYPE VARCHAR2(4000),
            DESTINATION_ITEM_TYPE VARCHAR2(4000),
            PAYMENT_ID VARCHAR2(4000),
            QUICKBOOKS_ID VARCHAR2(4000),
            INVOICE_ID VARCHAR2(4000),
            TRANSACTION_TYPE VARCHAR2(4000),
            SOURCE_TYPE VARCHAR2(4000),
            NOTE VARCHAR2(4000),
            INTEGRATION_ID VARCHAR2(4000),
            INTEGRATION_STATUS VARCHAR2(4000),
            INTEGRATION_ACCOUNT_ID VARCHAR2(4000),
            QUICKBOOKS_JOURNAL_ID VARCHAR2(4000),
            QUICKBOOKS_ERROR VARCHAR2(4000),
            XERO_ID VARCHAR2(4000),
            XERO_ERRORS VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000),
            ACCOUNT_NAME VARCHAR2(4000),
            CLIENT_NAME VARCHAR2(4000),
            CLIENT_UUID VARCHAR2(4000),
            SOURCE_NAME VARCHAR2(4000),
            DESTINATION_NAME VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Trust Ledger table created successfully.")
    else:
        print("LAWCUS Reports Trust Ledger table already exists.")


def insert_reports_trust_ledger_into_table(cursor, trust_ledger_report):
    """
    Insert LAWOCUS Reports Trust Ledger data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_TRUST_LEDGER (
        KEY, TRANSACTION_ID, ACCOUNT_ID, CREDIT, DEBIT, TRANSACTION_DATE, SOURCE,
        TYPE, CURRENCY, EXCHANGE_RATE, DESCRIPTION, CHECKS, CLIENT_ID, MATTER_ID,
        CREATED_BY, TEAM_ID, CREATED_AT, UPDATED_AT, SOURCE_ID, DESTINATION_ID,
        TRANSFER_ID, SOURCE_ITEM_ID, DESTINATION_ITEM_ID, SOURCE_ITEM_TYPE,
        DESTINATION_ITEM_TYPE, PAYMENT_ID, QUICKBOOKS_ID, INVOICE_ID,
        TRANSACTION_TYPE, SOURCE_TYPE, NOTE, INTEGRATION_ID, INTEGRATION_STATUS,
        INTEGRATION_ACCOUNT_ID, QUICKBOOKS_JOURNAL_ID, QUICKBOOKS_ERROR,
        XERO_ID, XERO_ERRORS, MATTER_NAME, MATTER_UUID, ACCOUNT_NAME,
        CLIENT_NAME, CLIENT_UUID, SOURCE_NAME, DESTINATION_NAME
    ) VALUES (
        :key, :transaction_id, :account_id, :credit, :debit,:transaction_date,
        :source, :type, :currency, :exchange_rate, :description, :check,
        :client_id, :matter_id, :created_by, :team_id,:created_at,:updated_at, :source_id, :destination_id,
        :transfer_id, :source_item_id, :destination_item_id, :source_item_type,
        :destination_item_type, :payment_id, :quickbooks_id, :invoice_id,
        :transaction_type, :source_type, :note, :integration_id, :integration_status,
        :integration_account_id, :quickbooks_journal_id, :quickbooks_error,
        :xero_id, :xero_errors, :matter_name, :matter_uuid, :account_name,
        :client_name, :client_uuid, :source_name, :destination_name
    )
    """

    for trust_data in trust_ledger_report:
        for transaction_data in trust_data.get("transactions", []):
            cursor.execute(
                insert_query,
                {
                    "key": trust_data.get("key"),
                    "transaction_id": transaction_data.get("id"),
                    "account_id": transaction_data.get("account_id"),
                    "credit": transaction_data.get("credit"),
                    "debit": transaction_data.get("debit"),
                    "transaction_date": transaction_data.get("date"),
                    "source": transaction_data.get("source"),
                    "type": transaction_data.get("type"),
                    "currency": transaction_data.get("currency"),
                    "exchange_rate": transaction_data.get("exchange_rate"),
                    "description": transaction_data.get("description"),
                    "check": transaction_data.get("check"),
                    "client_id": transaction_data.get("client_id"),
                    "matter_id": transaction_data.get("matter_id"),
                    "created_by": transaction_data.get("created_by"),
                    "team_id": transaction_data.get("team_id"),
                    "created_at": transaction_data.get("created_at"),
                    "updated_at": transaction_data.get("updated_at"),
                    "source_id": transaction_data.get("source_id"),
                    "destination_id": transaction_data.get("destination_id"),
                    "transfer_id": transaction_data.get("transfer_id"),
                    "source_item_id": transaction_data.get("source_item_id"),
                    "destination_item_id": transaction_data.get("destination_item_id"),
                    "source_item_type": transaction_data.get("source_item_type"),
                    "destination_item_type": transaction_data.get("destination_item_type"),
                    "payment_id": transaction_data.get("payment_id"),
                    "quickbooks_id": transaction_data.get("quickbooks_id"),
                    "invoice_id": transaction_data.get("invoice_id"),
                    "transaction_type": transaction_data.get("transaction_type"),
                    "source_type": transaction_data.get("source_type"),
                    "note": transaction_data.get("note"),
                    "integration_id": transaction_data.get("integration_id"),
                    "integration_status": transaction_data.get("integration_status"),
                    "integration_account_id": transaction_data.get("integration_account_id"),
                    "quickbooks_journal_id": transaction_data.get("quickbooks_journal_id"),
                    "quickbooks_error": transaction_data.get("quickbooks_error"),
                    "xero_id": transaction_data.get("xero_id"),
                    "xero_errors": transaction_data.get("xero_errors"),
                    "matter_name": transaction_data.get("mattername"),
                    "matter_uuid": transaction_data.get("matteruuid"),
                    "account_name": transaction_data.get("accountname"),
                    "client_name": transaction_data.get("clientname"),
                    "client_uuid": transaction_data.get("clientuuid"),
                    "source_name": transaction_data.get("sourcename"),
                    "destination_name": transaction_data.get("destinationname"),
                }
            )

    print("LAWCUS Reports Trust Ledger data inserted into the table successfully.")


def create_reports_time_entries_table(cursor):
    """
    Create a table for LAWOCUS Reports Time Entries in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_TIME_ENTRIES"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_TIME_ENTRIES (
            USER_ID VARCHAR2(4000),
            ENTRY_ID VARCHAR2(4000),
            DATES VARCHAR2(4000),
            DESCRIPTION VARCHAR2(4000),
            TIMESTAMPS VARCHAR2(4000),
            RATE VARCHAR2(4000),
            TOTAL VARCHAR2(4000),
            CREATED_AT VARCHAR2(4000),
            UPDATED_AT VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            TEAM_ID VARCHAR2(4000),
            INVOICE_ID VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            PAYMENT_DATE VARCHAR2(4000),
            ACTIVITY_ID VARCHAR2(4000),
            ACTIVITY_TASK_ID VARCHAR2(4000),
            REAL_TIMESTAMP VARCHAR2(4000),
            IS_NON_BILLABLE VARCHAR2(4000),
            DISCOUNT_VALUE VARCHAR2(4000),
            DISCOUNT_TYPE VARCHAR2(4000),
            LINE_DISCOUNT_TOTAL VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000),
            CLIENT_ID VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Time Entries table created successfully.")
    else:
        print("LAWCUS Reports Time Entries table already exists.")


def insert_reports_time_entries_into_table(cursor, time_entries_report):
    """
    Insert LAWOCUS Reports Time Entries data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_TIME_ENTRIES (
        USER_ID, ENTRY_ID, DATES, DESCRIPTION, TIMESTAMPS, RATE, TOTAL,
        CREATED_AT, UPDATED_AT, CREATED_BY, TEAM_ID, INVOICE_ID, MATTER_ID,
        PAYMENT_DATE, ACTIVITY_ID, ACTIVITY_TASK_ID, REAL_TIMESTAMP,
        IS_NON_BILLABLE, DISCOUNT_VALUE, DISCOUNT_TYPE, LINE_DISCOUNT_TOTAL,
        MATTER_NAME, MATTER_UUID, CLIENT_ID
    ) VALUES (
        :user_id, :entry_id,:date,
        :description, :timestamp, :rate, :total,
        :created_at,:updated_at,
        :created_by, :team_id, :invoice_id, :matter_id,:payment_date,
        :activity_id, :activity_task_id, :real_timestamp,
        :is_non_billable, :discount_value, :discount_type,
        :line_discount_total, :matter_name, :matter_uuid, :client_id
    )
    """

    for user_id, time_entries in time_entries_report.items():
        for time_entry in time_entries:
            cursor.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "entry_id": time_entry.get("id"),
                    "date": time_entry.get("date"),
                    "description": time_entry.get("description"),
                    "timestamp": time_entry.get("timestamp"),
                    "rate": time_entry.get("rate"),
                    "total": time_entry.get("total"),
                    "created_at": time_entry.get("created_at"),
                    "updated_at": time_entry.get("updated_at"),
                    "created_by": time_entry.get("created_by"),
                    "team_id": time_entry.get("team_id"),
                    "invoice_id": time_entry.get("invoice_id"),
                    "matter_id": time_entry.get("matter_id"),
                    "payment_date": time_entry.get("payment_date"),
                    "activity_id": time_entry.get("activity_id"),
                    "activity_task_id": time_entry.get("activity_task_id"),
                    "real_timestamp": time_entry.get("real_timestamp"),
                    "is_non_billable": time_entry.get("is_non_billable"),
                    "discount_value": time_entry.get("discount_value"),
                    "discount_type": time_entry.get("discount_type"),
                    "line_discount_total": time_entry.get("line_discount_total"),
                    "matter_name": time_entry.get("mattername"),
                    "matter_uuid": time_entry.get("matteruuid"),
                    "client_id": time_entry.get("client_id"),
                }
            )

    print("LAWCUS Reports Time Entries data inserted into the table successfully.")


def create_reports_revenue_table(cursor):
    """
    Create a table for LAWOCUS Reports Revenue in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_REVENUE"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_REVENUE (
            USER_ID VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000),
            UNBILLED_TIME VARCHAR2(4000),
            UNBILLED_HOURS VARCHAR2(4000),
            NON_BILLABLE_TIME VARCHAR2(4000),
            NON_BILLABLE_HOURS VARCHAR2(4000),
            UNBILLED_EXPENSES VARCHAR2(4000),
            UNBILLED_FLAT VARCHAR2(4000),
            BILLED_TIME VARCHAR2(4000),
            BILLED_HOURS VARCHAR2(4000),
            BILLED_EXPENSES VARCHAR2(4000),
            BILLED_FLAT VARCHAR2(4000),
            BILLED_TAXES VARCHAR2(4000),
            PAID_TIME VARCHAR2(4000),
            PAID_HOURS VARCHAR2(4000),
            PAID_EXPENSES VARCHAR2(4000),
            PAID_FLAT VARCHAR2(4000),
            PAID_TAXES VARCHAR2(4000),
            WITHOUT_MATTER VARCHAR2(4000),
            MATTER_NUMBER VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Revenue table created successfully.")
    else:
        print("LAWCUS Reports Revenue table already exists.")


def insert_reports_revenue_into_table(cursor, revenue_report):
    """
    Insert LAWOCUS Reports Revenue data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_REVENUE (
        USER_ID, MATTER_ID, MATTER_NAME, MATTER_UUID,
        UNBILLED_TIME, UNBILLED_HOURS, NON_BILLABLE_TIME, NON_BILLABLE_HOURS,
        UNBILLED_EXPENSES, UNBILLED_FLAT, BILLED_TIME, BILLED_HOURS,
        BILLED_EXPENSES, BILLED_FLAT, BILLED_TAXES, PAID_TIME, PAID_HOURS,
        PAID_EXPENSES, PAID_FLAT, PAID_TAXES, WITHOUT_MATTER, MATTER_NUMBER
    ) VALUES (
        :user_id, :matter_id, :matter_name, :matter_uuid,
        :unbilled_time, :unbilled_hours, :non_billable_time, :non_billable_hours,
        :unbilled_expenses, :unbilled_flat, :billed_time, :billed_hours,
        :billed_expenses, :billed_flat, :billed_taxes, :paid_time, :paid_hours,
        :paid_expenses, :paid_flat, :paid_taxes, :without_matter, :matter_number
    )
    """

    for user_id, revenue_entries in revenue_report.items():
        for revenue_entry in revenue_entries:
            cursor.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "matter_id": revenue_entry.get("matter_id"),
                    "matter_name": revenue_entry.get("matter_name"),
                    "matter_uuid": revenue_entry.get("matter_uuid"),
                    "unbilled_time": revenue_entry.get("unbilled_time"),
                    "unbilled_hours": revenue_entry.get("unbilled_hours"),
                    "non_billable_time": revenue_entry.get("non_billable_time"),
                    "non_billable_hours": revenue_entry.get("non_billable_hours"),
                    "unbilled_expenses": revenue_entry.get("unbilled_expenses"),
                    "unbilled_flat": revenue_entry.get("unbilled_flat"),
                    "billed_time": revenue_entry.get("billed_time"),
                    "billed_hours": revenue_entry.get("billed_hours"),
                    "billed_expenses": revenue_entry.get("billed_expenses"),
                    "billed_flat": revenue_entry.get("billed_flat"),
                    "billed_taxes": revenue_entry.get("billed_taxes"),
                    "paid_time": revenue_entry.get("paid_time"),
                    "paid_hours": revenue_entry.get("paid_hours"),
                    "paid_expenses": revenue_entry.get("paid_expenses"),
                    "paid_flat": revenue_entry.get("paid_flat"),
                    "paid_taxes": revenue_entry.get("paid_taxes"),
                    "without_matter": revenue_entry.get("without_matter"),
                    "matter_number": revenue_entry.get("matter_number"),
                }
            )

    print("LAWCUS Reports Revenue data inserted into the table successfully.")


def create_reports_accounts_receivable_table(cursor):
    """
    Create a table for LAWOCUS Reports Accounts Receivable in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_RECEIVABLE"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_RECEIVABLE (
            CLIENT_ID VARCHAR2(4000),
            ID VARCHAR2(4000),
            AMOUNT_DUE VARCHAR2(4000),
            NUMBERS VARCHAR2(4000),
            TOTAL VARCHAR2(4000),
            MATTER_ID VARCHAR2(4000),
            CREATED_BY VARCHAR2(4000),
            ISSUE_DATE VARCHAR2(4000),
            DUE_DATE VARCHAR2(4000),
            STATUS VARCHAR2(4000),
            PRACTICE VARCHAR2(4000),
            RESPONSIBLE_ATTORNEY_ID VARCHAR2(4000),
            MATTER_UUID VARCHAR2(4000),
            MATTER_NAME VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Accounts Receivable table created successfully.")
    else:
        print("LAWCUS Reports Accounts Receivable table already exists.")


def insert_reports_accounts_receivable_into_table(cursor, accounts_receivable_report):
    """
    Insert LAWOCUS Reports Accounts Receivable data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_RECEIVABLE (
        CLIENT_ID, ID, AMOUNT_DUE, NUMBER, TOTAL,
        MATTER_ID, CREATED_BY, ISSUE_DATE, DUE_DATE,
        STATUS, PRACTICE, RESPONSIBLE_ATTORNEY_ID, MATTER_UUID,
        MATTER_NAME
    ) VALUES (
        :client_id, :id, :amount_due, :number, :total,
        :matter_id, :created_by, :issue_date,:due_date, :status,
        :practice, :responsible_attorney_id, :matter_uuid, :matter_name
    )
    """

    for client_id, receivable_entries in accounts_receivable_report.items():
        for receivable_entry in receivable_entries:
            cursor.execute(
                insert_query,
                {
                    "client_id": client_id,
                    "id": receivable_entry.get("id"),
                    "amount_due": receivable_entry.get("amount_due"),
                    "number": receivable_entry.get("number"),
                    "total": receivable_entry.get("total"),
                    "matter_id": receivable_entry.get("matter_id"),
                    "created_by": receivable_entry.get("created_by"),
                    "issue_date": receivable_entry.get("issue_date"),
                    "due_date": receivable_entry.get("due_date"),
                    "status": receivable_entry.get("status"),
                    "practice": receivable_entry.get("practice"),
                    "responsible_attorney_id": receivable_entry.get("responsible_attorney_id"),
                    "matter_uuid": receivable_entry.get("matter_uuid"),
                    "matter_name": receivable_entry.get("matter_name"),
                }
            )

    print("LAWCUS Reports Accounts Receivable data inserted into the table successfully.")


def create_reports_matters_info_table(cursor):
    """
    Create a table for LAWOCUS Reports Matters Info in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_MATTERS_INFO"

    if not table_exists(cursor, table_name):
        table_creation_query = """
        CREATE TABLE LAWCUS_REPORTS_MATTERS_INFO (
            ALL_COUNT VARCHAR2(4000),
            OPEN_COUNT VARCHAR2(4000),
            CLOSED_COUNT VARCHAR2(4000),
            MONTH_COUNT VARCHAR2(4000),
            PREV_MONTH_COUNT VARCHAR2(4000)
        )
        """
        cursor.execute(table_creation_query)
        print("LAWCUS Reports Matters Info table created successfully.")
    else:
        print("LAWCUS Reports Matters Info table already exists.")


def insert_reports_matters_info_into_table(cursor, matters_info_report):
    """
    Insert LAWOCUS Reports Matters Info data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_MATTERS_INFO (
        ALL_COUNT, OPEN_COUNT, CLOSED_COUNT, MONTH_COUNT, PREV_MONTH_COUNT
    ) VALUES (
        :all_count, :open_count, :closed_count, :month_count, :prev_month_count
    )
    """

    cursor.execute(
        insert_query,
        {
            "all_count": matters_info_report.get("all"),
            "open_count": matters_info_report.get("open"),
            "closed_count": matters_info_report.get("closed"),
            "month_count": matters_info_report.get("month"),
            "prev_month_count": matters_info_report.get("prev_month"),
        },
    )

    print("LAWCUS Reports Matters Info data inserted into the table successfully.")
