def table_exists(cursor, table_name):
    """
    Check if a table already exists in the Oracle database.

    :param cursor: Oracle database cursor
    :param table_name: Name of the table to check
    :return: True if the table exists, False otherwise
    """
    check_query = f"SELECT count(*) FROM all_tables WHERE table_name = '{table_name.upper()}'"
    cursor.execute(check_query)
    result = cursor.fetchone()

    return result[0] > 0
