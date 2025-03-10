import pandas as pd
from database import get_db_connection, close_db_connection
from email_sender import send_email
from datetime import datetime

import pandas as pd
from datetime import datetime

def upload_yearly_data(file_name, local_file_path):
    """Uploads yearly stock data from an Excel file to the database."""
    connection, cursor = get_db_connection()
    missing_stocks = []
    message = "Initiating upload of yearly data"

    try:
        xls = pd.ExcelFile(local_file_path)

        for sheet_name in xls.sheet_names:
            df = load_and_process_sheet(xls, sheet_name)
            metadata_mapping = fetch_metadata_mapping(cursor, sheet_name)

            process_stock_data(cursor, df, metadata_mapping, missing_stocks)

        connection.commit()
        message = "Data processing complete."

    except Exception as e:
        message = f"An error occurred: {e}"
        print(message)

    finally:
        close_db_connection(connection, cursor)
        send_email(missing_stocks, message)


def load_and_process_sheet(xls, sheet_name):
    """Loads and processes an Excel sheet, setting column names properly."""
    df = pd.read_excel(xls, sheet_name=sheet_name)
    header_row_idx = 1
    df.columns = df.iloc[header_row_idx]
    return df[header_row_idx + 1:].reset_index(drop=True)


def fetch_metadata_mapping(cursor, sheet_name):
    """Fetches metadata mapping for the given sheet name."""
    cursor.execute(
        "SELECT id, identifier FROM m_stock_metadata WHERE identifier = %s",
        (sheet_name,)
    )
    return {name: meta_id for meta_id, name in cursor.fetchall()}


def process_stock_data(cursor, df, metadata_mapping, missing_stocks):
    """Processes stock data row by row and inserts missing records."""
    for _, row in df.iterrows():
        company_name = row.iloc[1]
        if not isinstance(company_name, str):
            continue

        stock_id = fetch_stock_id(cursor, company_name, missing_stocks)
        if not stock_id:
            continue

        insert_stock_data(cursor, row, stock_id, metadata_mapping)


def fetch_stock_id(cursor, company_name, missing_stocks):
    """Fetches stock ID for a given company."""
    cursor.execute("SELECT id FROM m_stock WHERE ticker = %s", (company_name,))
    stock_result = cursor.fetchone()
    
    if stock_result:
        return stock_result[0]

    print(f"Stock ID not found for {company_name}, skipping...")
    missing_stocks.append(company_name)
    return None


def insert_stock_data(cursor, row, stock_id, metadata_mapping):
    """Inserts missing stock data into the database."""
    current_year = datetime.now().year

    for col_name, value in row.items():
        if not is_valid_column(col_name, value, current_year):
            continue

        metadata_id = metadata_mapping.get(col_name)
        if not metadata_id or data_exists(cursor, stock_id, metadata_id, col_name):
            continue

        cursor.execute(
            """
            INSERT INTO f_fundamental_d (stock_id, metadata_id, data, ltm_years, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (stock_id, metadata_id, value, col_name)
        )
        print(f"Data inserting for {stock_id} - {col_name}")


def is_valid_column(col_name, value, current_year):
    """Checks if the column should be processed."""
    return not pd.isna(value) and col_name in ['LTM', 'LTM-4'] or (
        isinstance(col_name, float) and 1950.0 <= col_name <= current_year
    )


def data_exists(cursor, stock_id, metadata_id, col_name):
    """Checks if the data already exists in the database."""
    cursor.execute(
        "SELECT 1 FROM f_fundamental_d WHERE stock_id = %s AND metadata_id = %s AND ltm_years = %s",
        (stock_id, metadata_id, col_name)
    )
    if cursor.fetchone():
        print(f"Data already exists for stock {stock_id} - {col_name}, skipping...")
        return True
    return False