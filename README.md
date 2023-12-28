# Lawcus Data Migration

## Overview

This project enables the migration of data from the Lawcus platform to an Oracle database. It consists of scripts to
connect to the Lawcus API, retrieve data for various endpoints, and insert the data into corresponding Oracle database
tables.

## Prerequisites

Ensure that you have the following installed:

- Python 3.x (recommend 3.9+)
- Oracle Instant Client (for database connection)
- Dependencies listed in `requirements.txt`

## Setup

1. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Set up Oracle Instant Client.

3. Configure API access:

   Replace placeholder values in `main.py` for `API_BASE_URL`, `API_ACCESS_TOKEN`, and `API_HEADERS` with your actual
   Lawcus API details.

4. Configure database connection:

   Update database connection details (DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_SID) in `main.py` based on your
   Oracle database.

## Usage

Run the main script to initiate data migration:

```bash
python main.py
```

The script connects to the Oracle database, creates tables for different data types, fetches data from Lawcus API
endpoints, and inserts data into corresponding tables.

## Project Structure

- `main.py`: Main script for data migration.
- `contacts.py`, `matters.py`, ...: Endpoint-specific scripts with functions for creating tables and inserting data.
- `utils.py`: Utility functions used across different scripts.
- `requirements.txt`: List of Python dependencies.