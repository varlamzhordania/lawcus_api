from utils import table_exists
from logger import logger


def create_reports_payment_collected_table(cursor):
    """
    Create a table for LAWOCUS Reports Payment Collected in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_COLLECTED"

    try:
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
            logger.info("LAWCUS Reports Payment Collected table created successfully.")
        else:
            logger.info("LAWCUS Reports Payment Collected table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Payment Collected table: {e}")


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
        :my_key, :my_payment_id, :my_account_id, :my_credit, :my_debit, :my_date,
        :my_source, :my_type, :my_currency, :my_exchange_rate, :my_description,
        :my_check, :my_client_id, :my_matter_id, :my_created_by, :my_team_id,
        :my_created_at, :my_updated_at, :my_source_id, :my_destination_id,
        :my_transfer_id, :my_source_item_id, :my_destination_item_id,
        :my_source_item_type, :my_destination_item_type, :my_payment_id_col,
        :my_quickbooks_id, :my_invoice_id, :my_transaction_type, :my_source_type,
        :my_note, :my_integration_id, :my_integration_status, :my_integration_account_id,
        :my_quickbooks_journal_id, :my_quickbooks_error, :my_invoice_number,
        :my_client_name, :my_amount, :my_matter_name, :my_responsible_attorney_id
    )
    """

    try:
        for report in payment_collected_reports:
            key = report.get("key")
            payments = report.get("payments", [])

            for payment in payments:
                cursor.execute(
                    insert_query,
                    my_key=key,
                    my_payment_id=payment.get("id"),
                    my_account_id=payment.get("account_id"),
                    my_credit=payment.get("credit"),
                    my_debit=payment.get("debit"),
                    my_date=payment.get("date"),
                    my_source=payment.get("source"),
                    my_type=payment.get("type"),
                    my_currency=payment.get("currency"),
                    my_exchange_rate=payment.get("exchange_rate"),
                    my_description=payment.get("description"),
                    my_check=payment.get("check"),
                    my_client_id=payment.get("client_id"),
                    my_matter_id=payment.get("matter_id"),
                    my_created_by=payment.get("created_by"),
                    my_team_id=payment.get("team_id"),
                    my_created_at=payment.get("created_at"),
                    my_updated_at=payment.get("updated_at"),
                    my_source_id=payment.get("source_id"),
                    my_destination_id=payment.get("destination_id"),
                    my_transfer_id=payment.get("transfer_id"),
                    my_source_item_id=payment.get("source_item_id"),
                    my_destination_item_id=payment.get("destination_item_id"),
                    my_source_item_type=payment.get("source_item_type"),
                    my_destination_item_type=payment.get("destination_item_type"),
                    my_payment_id_col=payment.get("payment_id_col"),
                    my_quickbooks_id=payment.get("quickbooks_id"),
                    my_invoice_id=payment.get("invoice_id"),
                    my_transaction_type=payment.get("transaction_type"),
                    my_source_type=payment.get("source_type"),
                    my_note=payment.get("note"),
                    my_integration_id=payment.get("integration_id"),
                    my_integration_status=payment.get("integration_status"),
                    my_integration_account_id=payment.get("integration_account_id"),
                    my_quickbooks_journal_id=payment.get("quickbooks_journal_id"),
                    my_quickbooks_error=payment.get("quickbooks_error"),
                    my_invoice_number=payment.get("invoice_number"),
                    my_client_name=payment.get("client_name"),
                    my_amount=payment.get("amount"),
                    my_matter_name=payment.get("matter_name"),
                    my_responsible_attorney_id=payment.get("responsible_attorney_id"),
                )

        cursor.connection.commit()
        logger.info("LAWCUS Reports Payment Collected data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Payment Collected data into the table: {e}")


def create_reports_invoice_history_table(cursor):
    """
    Create a table for LAWOCUS Reports Invoice History in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_INVOICE_HISTORY"

    try:
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
            logger.info("LAWCUS Reports Invoice History table created successfully.")
        else:
            logger.info("LAWCUS Reports Invoice History table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Invoice History table: {e}")


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
        :my_id, :my_amount_due, :my_discount, :my_discount_type, :my_type, :my_client_id, :my_number, :my_total,
        :my_sub_total, :my_matter_id, :my_created_by, :my_issue_date,
        :my_due_date, :my_status, :my_practice,
        :my_matter_name, :my_matter_uuid, :my_responsible_attorney_id
    )
    """

    try:
        for client_id, invoices in invoice_history_report.get("list", {}).items():
            for invoice in invoices:
                cursor.execute(
                    insert_query,
                    my_id=invoice.get("id"),
                    my_amount_due=invoice.get("amount_due"),
                    my_discount=invoice.get("discount"),
                    my_discount_type=invoice.get("discount_type"),
                    my_type=invoice.get("type"),
                    my_client_id=invoice.get("client_id"),
                    my_number=invoice.get("number"),
                    my_total=invoice.get("total"),
                    my_sub_total=invoice.get("sub_total"),
                    my_matter_id=invoice.get("matter_id"),
                    my_created_by=invoice.get("created_by"),
                    my_issue_date=invoice.get("issue_date"),
                    my_due_date=invoice.get("due_date"),
                    my_status=invoice.get("status"),
                    my_practice=invoice.get("practice"),
                    my_matter_name=invoice.get("matter_name"),
                    my_matter_uuid=invoice.get("matter_uuid"),
                    my_responsible_attorney_id=invoice.get("responsible_attorney_id")
                )

        cursor.connection.commit()
        logger.info("LAWCUS Reports Invoice History data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Invoice History data into the table: {e}")


def create_reports_matter_balance_table(cursor):
    """
    Create a table for LAWOCUS Reports Matter Balance in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_MATTER_BALANCE"

    try:
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
            logger.info("LAWCUS Reports Matter Balance table created successfully.")
        else:
            logger.info("LAWCUS Reports Matter Balance table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Matter Balance table: {e}")


def insert_reports_matter_balance_into_table(cursor, matter_balance_reports):
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
        :my_key, :my_matter_id, :my_matter_name, :my_description, :my_display_number, :my_client_id,
        :my_responsible_attorney_id, :my_receivable, :my_expenses_in_work, :my_time_entry_in_work,
        :my_flat_fee_in_work, :my_trust
    )
    """

    try:
        for matter_balance_report in matter_balance_reports:
            for matters_data in matter_balance_report:
                for matter_data in matters_data.get("matters", []):
                    cursor.execute(
                        insert_query,
                        my_key=matters_data.get("key"),
                        my_matter_id=matter_data.get("matter_id"),
                        my_matter_name=matter_data.get("matter_name"),
                        my_description=matter_data.get("description"),
                        my_display_number=matter_data.get("display_number"),
                        my_client_id=matter_data.get("client_id"),
                        my_responsible_attorney_id=matter_data.get("responsible_attorney_id"),
                        my_receivable=matter_data.get("receivable"),
                        my_expenses_in_work=matter_data.get("expenses_in_work"),
                        my_time_entry_in_work=matter_data.get("time_entry_in_work"),
                        my_flat_fee_in_work=matter_data.get("flat_fee_in_work"),
                        my_trust=matters_data.get("trust"),
                    )

        cursor.connection.commit()
        logger.info("LAWCUS Reports Matter Balance data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Matter Balance data into the table: {e}")


def create_reports_client_trust_table(cursor):
    """
    Create a table for LAWOCUS Reports Client Trust in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_CLIENT_TRUST"

    try:
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
            logger.info("LAWCUS Reports Client Trust table created successfully.")
        else:
            logger.info("LAWCUS Reports Client Trust table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Client Trust table: {e}")


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

    try:
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

        cursor.connection.commit()
        logger.info("LAWCUS Reports Client Trust data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Client Trust data into the table: {e}")


def create_reports_client_ledger_table(cursor):
    """
    Create a table for LAWOCUS Reports Client Ledger in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_CLIENT_LEDGER"

    try:
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
            logger.info("LAWCUS Reports Client Ledger table created successfully.")
        else:
            logger.info("LAWCUS Reports Client Ledger table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Client Ledger table: {e}")


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
        :my_key, :my_transaction_id, :my_account_id, :my_credit, :my_debit, :my_transaction_date,
        :my_source, :my_type, :my_currency, :my_exchange_rate, :my_description, :my_check,
        :my_client_id, :my_matter_id, :my_created_by, :my_team_id, :my_created_at, :my_updated_at,
        :my_source_id, :my_destination_id, :my_transfer_id, :my_source_item_id,
        :my_destination_item_id, :my_source_item_type, :my_destination_item_type,
        :my_payment_id, :my_quickbooks_id, :my_invoice_id, :my_transaction_type,
        :my_source_type, :my_note, :my_integration_id, :my_integration_status,
        :my_integration_account_id, :my_quickbooks_journal_id, :my_quickbooks_error,
        :my_matter_name, :my_matter_uuid, :my_account_name, :my_client_name, :my_client_uuid,
        :my_source_name, :my_destination_name
    )
    """

    try:
        # Ensure that client_ledger_report is a dictionary
        if not isinstance(client_ledger_report, dict):
            raise ValueError("Invalid data format. Expected a dictionary.")

        # Extract client_id and transactions directly from the dictionary
        client_id, transactions = next(iter(client_ledger_report.items()), (None, []))

        for transaction_data in transactions:
            cursor.execute(
                insert_query,
                my_key=client_id,
                my_transaction_id=transaction_data.get("id"),
                my_account_id=transaction_data.get("account_id"),
                my_credit=transaction_data.get("credit"),
                my_debit=transaction_data.get("debit"),
                my_transaction_date=transaction_data.get("date"),
                my_source=transaction_data.get("source"),
                my_type=transaction_data.get("type"),
                my_currency=transaction_data.get("currency"),
                my_exchange_rate=transaction_data.get("exchange_rate"),
                my_description=transaction_data.get("description"),
                my_check=transaction_data.get("check"),
                my_client_id=transaction_data.get("client_id"),
                my_matter_id=transaction_data.get("matter_id"),
                my_created_by=transaction_data.get("created_by"),
                my_team_id=transaction_data.get("team_id"),
                my_created_at=transaction_data.get("created_at"),
                my_updated_at=transaction_data.get("updated_at"),
                my_source_id=transaction_data.get("source_id"),
                my_destination_id=transaction_data.get("destination_id"),
                my_transfer_id=transaction_data.get("transfer_id"),
                my_source_item_id=transaction_data.get("source_item_id"),
                my_destination_item_id=transaction_data.get("destination_item_id"),
                my_source_item_type=transaction_data.get("source_item_type"),
                my_destination_item_type=transaction_data.get("destination_item_type"),
                my_payment_id=transaction_data.get("payment_id"),
                my_quickbooks_id=transaction_data.get("quickbooks_id"),
                my_invoice_id=transaction_data.get("invoice_id"),
                my_transaction_type=transaction_data.get("transaction_type"),
                my_source_type=transaction_data.get("source_type"),
                my_note=transaction_data.get("note"),
                my_integration_id=transaction_data.get("integration_id"),
                my_integration_status=transaction_data.get("integration_status"),
                my_integration_account_id=transaction_data.get("integration_account_id"),
                my_quickbooks_journal_id=transaction_data.get("quickbooks_journal_id"),
                my_quickbooks_error=transaction_data.get("quickbooks_error"),
                my_matter_name=transaction_data.get("matter_name"),
                my_matter_uuid=transaction_data.get("matter_uuid"),
                my_account_name=transaction_data.get("accountname"),
                my_client_name=transaction_data.get("clientname"),
                my_client_uuid=transaction_data.get("clientuuid"),
                my_source_name=transaction_data.get("sourcename"),
                my_destination_name=transaction_data.get("destinationname"),
            )

        cursor.connection.commit()
        logger.info("LAWCUS Reports Client Ledger data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Client Ledger data into the table: {e}")


def create_reports_trust_ledger_table(cursor):
    """
    Create a table for LAWOCUS Reports Trust Ledger in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_TRUST_LEDGER"

    try:
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
            logger.info("LAWCUS Reports Trust Ledger table created successfully.")
        else:
            logger.info("LAWCUS Reports Trust Ledger table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Trust Ledger table: {e}")


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
        :my_key, :my_transaction_id, :my_account_id, :my_credit, :my_debit,:my_transaction_date,
        :my_source, :my_type, :my_currency, :my_exchange_rate, :my_description, :my_check,
        :my_client_id, :my_matter_id, :my_created_by, :my_team_id,:my_created_at,:my_updated_at, :my_source_id, :my_destination_id,
        :my_transfer_id, :my_source_item_id, :my_destination_item_id, :my_source_item_type,
        :my_destination_item_type, :my_payment_id, :my_quickbooks_id, :my_invoice_id,
        :my_transaction_type, :my_source_type, :my_note, :my_integration_id, :my_integration_status,
        :my_integration_account_id, :my_quickbooks_journal_id, :my_quickbooks_error,
        :my_xero_id, :my_xero_errors, :my_matter_name, :my_matter_uuid, :my_account_name,
        :my_client_name, :my_client_uuid, :my_source_name, :my_destination_name
    )
    """

    try:
        for trust_data in trust_ledger_report:
            for transaction_data in trust_data.get("transactions", []):
                cursor.execute(
                    insert_query,
                    my_key=trust_data.get("key"),
                    my_transaction_id=transaction_data.get("id"),
                    my_account_id=transaction_data.get("account_id"),
                    my_credit=transaction_data.get("credit"),
                    my_debit=transaction_data.get("debit"),
                    my_transaction_date=transaction_data.get("transaction_date"),
                    my_source=transaction_data.get("source"),
                    my_type=transaction_data.get("type"),
                    my_currency=transaction_data.get("currency"),
                    my_exchange_rate=transaction_data.get("exchange_rate"),
                    my_description=transaction_data.get("description"),
                    my_check=transaction_data.get("check"),
                    my_client_id=transaction_data.get("client_id"),
                    my_matter_id=transaction_data.get("matter_id"),
                    my_created_by=transaction_data.get("created_by"),
                    my_team_id=transaction_data.get("team_id"),
                    my_created_at=transaction_data.get("created_at"),
                    my_updated_at=transaction_data.get("updated_at"),
                    my_source_id=transaction_data.get("source_id"),
                    my_destination_id=transaction_data.get("destination_id"),
                    my_transfer_id=transaction_data.get("transfer_id"),
                    my_source_item_id=transaction_data.get("source_item_id"),
                    my_destination_item_id=transaction_data.get("destination_item_id"),
                    my_source_item_type=transaction_data.get("source_item_type"),
                    my_destination_item_type=transaction_data.get("destination_item_type"),
                    my_payment_id=transaction_data.get("payment_id"),
                    my_quickbooks_id=transaction_data.get("quickbooks_id"),
                    my_invoice_id=transaction_data.get("invoice_id"),
                    my_transaction_type=transaction_data.get("transaction_type"),
                    my_source_type=transaction_data.get("source_type"),
                    my_note=transaction_data.get("note"),
                    my_integration_id=transaction_data.get("integration_id"),
                    my_integration_status=transaction_data.get("integration_status"),
                    my_integration_account_id=transaction_data.get("integration_account_id"),
                    my_quickbooks_journal_id=transaction_data.get("quickbooks_journal_id"),
                    my_quickbooks_error=transaction_data.get("quickbooks_error"),
                    my_xero_id=transaction_data.get("xero_id"),
                    my_xero_errors=transaction_data.get("xero_errors"),
                    my_matter_name=transaction_data.get("matter_name"),
                    my_matter_uuid=transaction_data.get("matter_uuid"),
                    my_account_name=transaction_data.get("account_name"),
                    my_client_name=transaction_data.get("client_name"),
                    my_client_uuid=transaction_data.get("client_uuid"),
                    my_source_name=transaction_data.get("source_name"),
                    my_destination_name=transaction_data.get("destination_name"),
                )

        cursor.connection.commit()
        logger.info("LAWCUS Reports Trust Ledger data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Trust Ledger data into the table: {e}")


def create_reports_time_entries_table(cursor):
    """
    Create a table for LAWOCUS Reports Time Entries in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_TIME_ENTRIES"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_REPORTS_TIME_ENTRIES (
                KEY VARCHAR2(4000),
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
            logger.info("LAWCUS Reports Time Entries table created successfully.")
        else:
            logger.info("LAWCUS Reports Time Entries table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Time Entries table: {e}")


def insert_reports_time_entries_into_table(cursor, time_entries_report):
    """
    Insert LAWOCUS Reports Time Entries data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_TIME_ENTRIES (
        KEY, USER_ID, ENTRY_ID, DATES, DESCRIPTION, TIMESTAMPS, RATE, TOTAL,
        CREATED_AT, UPDATED_AT, CREATED_BY, TEAM_ID, INVOICE_ID, MATTER_ID,
        PAYMENT_DATE, ACTIVITY_ID, ACTIVITY_TASK_ID, REAL_TIMESTAMP,
        IS_NON_BILLABLE, DISCOUNT_VALUE, DISCOUNT_TYPE, LINE_DISCOUNT_TOTAL,
        MATTER_NAME, MATTER_UUID, CLIENT_ID
    ) VALUES (
        :my_key, :my_user_id, :my_entry_id, :my_date,
        :my_description, :my_timestamp, :my_rate, :my_total,
        :my_created_at, :my_updated_at,
        :my_created_by, :my_team_id, :my_invoice_id, :my_matter_id, :my_payment_date,
        :my_activity_id, :my_activity_task_id, :my_real_timestamp,
        :my_is_non_billable, :my_discount_value, :my_discount_type,
        :my_line_discount_total, :my_matter_name, :my_matter_uuid, :my_client_id
    )
    """

    try:
        for key, time_entries in time_entries_report.items():
            for time_entry in time_entries:
                cursor.execute(
                    insert_query,
                    my_key=key,
                    my_user_id=time_entry.get("user_id"),
                    my_entry_id=time_entry.get("id"),
                    my_date=time_entry.get("date"),
                    my_description=time_entry.get("description"),
                    my_timestamp=time_entry.get("timestamp"),
                    my_rate=time_entry.get("rate"),
                    my_total=time_entry.get("total"),
                    my_created_at=time_entry.get("created_at"),
                    my_updated_at=time_entry.get("updated_at"),
                    my_created_by=time_entry.get("user_id"),  # Update this line
                    my_team_id=time_entry.get("team_id"),
                    my_invoice_id=time_entry.get("invoice_id"),
                    my_matter_id=time_entry.get("matter_id"),
                    my_payment_date=time_entry.get("payment_date"),
                    my_activity_id=time_entry.get("activity_id"),
                    my_activity_task_id=time_entry.get("activity_task_id"),
                    my_real_timestamp=time_entry.get("real_timestamp"),
                    my_is_non_billable=time_entry.get("is_non_billable"),
                    my_discount_value=time_entry.get("discount_value"),
                    my_discount_type=time_entry.get("discount_type"),
                    my_line_discount_total=time_entry.get("line_discount_total"),
                    my_matter_name=time_entry.get("mattername"),
                    my_matter_uuid=time_entry.get("matteruuid"),
                    my_client_id=time_entry.get("client_id"),
                )

        cursor.connection.commit()
        logger.info("LAWCUS Reports Time Entries data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Time Entries data into the table: {e}")


def create_reports_revenue_table(cursor):
    """
    Create a table for LAWOCUS Reports Revenue in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_REVENUE"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_REPORTS_REVENUE (
                KEY VARCHAR2(4000),
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
            logger.info("LAWCUS Reports Revenue table created successfully.")
        else:
            logger.info("LAWCUS Reports Revenue table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Revenue table: {e}")


def insert_reports_revenue_into_table(cursor, revenue_report):
    """
    Insert LAWOCUS Reports Revenue data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_REVENUE (
        KEY, USER_ID, MATTER_ID, MATTER_NAME, MATTER_UUID,
        UNBILLED_TIME, UNBILLED_HOURS, NON_BILLABLE_TIME, NON_BILLABLE_HOURS,
        UNBILLED_EXPENSES, UNBILLED_FLAT, BILLED_TIME, BILLED_HOURS,
        BILLED_EXPENSES, BILLED_FLAT, BILLED_TAXES, PAID_TIME, PAID_HOURS,
        PAID_EXPENSES, PAID_FLAT, PAID_TAXES, WITHOUT_MATTER, MATTER_NUMBER
    ) VALUES (
        :my_key, :my_user_id, :my_matter_id, :my_matter_name, :my_matter_uuid,
        :my_unbilled_time, :my_unbilled_hours, :my_non_billable_time, :my_non_billable_hours,
        :my_unbilled_expenses, :my_unbilled_flat, :my_billed_time, :my_billed_hours,
        :my_billed_expenses, :my_billed_flat, :my_billed_taxes, :my_paid_time, :my_paid_hours,
        :my_paid_expenses, :my_paid_flat, :my_paid_taxes, :my_without_matter, :my_matter_number
    )
    """

    try:
        for key, revenue_entries in revenue_report.items():
            for revenue_entry in revenue_entries:
                cursor.execute(
                    insert_query,
                    my_key=key,
                    my_user_id=revenue_entry.get("user_id"),
                    my_matter_id=revenue_entry.get("matter_id"),
                    my_matter_name=revenue_entry.get("matter_name"),
                    my_matter_uuid=revenue_entry.get("matter_uuid"),
                    my_unbilled_time=revenue_entry.get("unbilled_time"),
                    my_unbilled_hours=revenue_entry.get("unbilled_hours"),
                    my_non_billable_time=revenue_entry.get("non_billable_time"),
                    my_non_billable_hours=revenue_entry.get("non_billable_hours"),
                    my_unbilled_expenses=revenue_entry.get("unbilled_expenses"),
                    my_unbilled_flat=revenue_entry.get("unbilled_flat"),
                    my_billed_time=revenue_entry.get("billed_time"),
                    my_billed_hours=revenue_entry.get("billed_hours"),
                    my_billed_expenses=revenue_entry.get("billed_expenses"),
                    my_billed_flat=revenue_entry.get("billed_flat"),
                    my_billed_taxes=revenue_entry.get("billed_taxes"),
                    my_paid_time=revenue_entry.get("paid_time"),
                    my_paid_hours=revenue_entry.get("paid_hours"),
                    my_paid_expenses=revenue_entry.get("paid_expenses"),
                    my_paid_flat=revenue_entry.get("paid_flat"),
                    my_paid_taxes=revenue_entry.get("paid_taxes"),
                    my_without_matter=revenue_entry.get("without_matter"),
                    my_matter_number=revenue_entry.get("matter_number"),
                )

        cursor.connection.commit()
        logger.info("LAWCUS Reports Revenue data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Revenue data into the table: {e}")


def create_reports_accounts_receivable_table(cursor):
    """
    Create a table for LAWOCUS Reports Accounts Receivable in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_RECEIVABLE"

    try:
        if not table_exists(cursor, table_name):
            table_creation_query = """
            CREATE TABLE LAWCUS_REPORTS_RECEIVABLE (
                KEY VARCHAR2(4000),
                CLIENT_ID VARCHAR2(4000),
                ID VARCHAR2(4000),
                AMOUNT_DUE VARCHAR2(4000),
                NUMBER VARCHAR2(4000),
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
            logger.info("LAWCUS Reports Accounts Receivable table created successfully.")
        else:
            logger.info("LAWCUS Reports Accounts Receivable table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Accounts Receivable table: {e}")


def insert_reports_accounts_receivable_into_table(cursor, accounts_receivable_report):
    """
    Insert LAWOCUS Reports Accounts Receivable data into the Oracle database.

    Modify the insert query based on your specific requirements.
    """
    insert_query = """
    INSERT INTO LAWCUS_REPORTS_RECEIVABLE (
        KEY, CLIENT_ID, ID, AMOUNT_DUE, NUMBER, TOTAL,
        MATTER_ID, CREATED_BY, ISSUE_DATE, DUE_DATE,
        STATUS, PRACTICE, RESPONSIBLE_ATTORNEY_ID, MATTER_UUID,
        MATTER_NAME
    ) VALUES (
        :my_key, :my_client_id, :my_id, :my_amount_due, :my_number, :my_total,
        :my_matter_id, :my_created_by, :my_issue_date, :my_due_date, :my_status,
        :my_practice, :my_responsible_attorney_id, :my_matter_uuid, :my_matter_name
    )
    """

    try:
        for key, receivable_entries in accounts_receivable_report.items():
            for receivable_entry in receivable_entries:
                cursor.execute(
                    insert_query,
                    my_key=key,
                    my_client_id=receivable_entry.get("client_id"),
                    my_id=receivable_entry.get("id"),
                    my_amount_due=receivable_entry.get("amount_due"),
                    my_number=receivable_entry.get("number"),
                    my_total=receivable_entry.get("total"),
                    my_matter_id=receivable_entry.get("matter_id"),
                    my_created_by=receivable_entry.get("created_by"),
                    my_issue_date=receivable_entry.get("issue_date"),
                    my_due_date=receivable_entry.get("due_date"),
                    my_status=receivable_entry.get("status"),
                    my_practice=receivable_entry.get("practice"),
                    my_responsible_attorney_id=receivable_entry.get("responsible_attorney_id"),
                    my_matter_uuid=receivable_entry.get("matter_uuid"),
                    my_matter_name=receivable_entry.get("matter_name"),
                )

        cursor.connection.commit()
        logger.info("LAWCUS Reports Accounts Receivable data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Accounts Receivable data into the table: {e}")


def create_reports_matters_info_table(cursor):
    """
    Create a table for LAWOCUS Reports Matters Info in the Oracle database if it doesn't exist.

    Modify the table creation query based on your specific requirements.
    """
    table_name = "LAWCUS_REPORTS_MATTERS_INFO"

    try:
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
            logger.info("LAWCUS Reports Matters Info table created successfully.")
        else:
            logger.info("LAWCUS Reports Matters Info table already exists.")
    except Exception as e:
        logger.error(f"Error creating LAWOCUS Reports Matters Info table: {e}")


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

    try:
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
        cursor.connection.commit()
        logger.info("LAWCUS Reports Matters Info data inserted into the table successfully.")
    except Exception as e:
        logger.error(f"Error inserting LAWOCUS Reports Matters Info data into the table: {e}")
