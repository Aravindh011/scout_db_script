import pandas as pd
from email_sender import send_email
from database import get_db_connection, close_db_connection

def upload_daily_data(file_name, local_file_path):
    """Uploads daily stock data from an Excel file to the database."""

    connection, cursor = get_db_connection()
    missing_stocks = []
    message = "Initiating upload of daily data"

    try:
        df = pd.read_excel(local_file_path, sheet_name=0)
        rows_to_check, stock_names = process_local_excel_file(df)

        # Get metadata_id
        metadata_id = fetch_metadata_id(cursor, file_name)
        if metadata_id is None:
            message = f"Error: {file_name} metadata not found"
            return

        # Get stock mappings
        stock_map = get_all_stock_ids(cursor, stock_names)

        for _, row in rows_to_check.iterrows():
            process_stock_data(cursor, row, stock_names, stock_map, metadata_id, missing_stocks)

        connection.commit()

    except Exception as e:
        message = f"An error occurred: {e}"
        print(message)

    finally:
        close_db_connection(connection, cursor)
        send_email(missing_stocks, message)


def fetch_metadata_id(cursor, file_name):
    """Fetch metadata ID based on file identifier."""
    identifier_map = {"MC_USD": "MC", "PX_USD": "PX", "Vol_USD": "Volume"}
    identifier = next((v for k, v in identifier_map.items() if k in file_name), None)

    if not identifier:
        return None

    cursor.execute("SELECT id FROM m_stock_metadata WHERE identifier = %s", (identifier,))
    result = cursor.fetchone()
    return result[0] if result else None


def process_stock_data(cursor, row, stock_names, stock_map, metadata_id, missing_stocks):
    """Process and insert stock data for a specific date."""
    date_value = row['Dates']
    existing_stock_map = get_existing_stock_data(cursor, metadata_id, date_value)

    missing_stocks_for_date = [s for s in stock_names if s not in existing_stock_map]
    if not missing_stocks_for_date:
        print(f"All stocks already exist for {date_value}. Skipping insertion.")
        return

    insert_missing_stocks(cursor, row, missing_stocks_for_date, stock_map, metadata_id, date_value, missing_stocks)


def insert_missing_stocks(cursor, row, missing_stocks_for_date, stock_map, metadata_id, date_value, missing_stocks):
    """Insert missing stock data into the database."""
    for stock_name in missing_stocks_for_date:
        value = row.get(stock_name)
        if pd.isna(value):
            continue

        stock_id = stock_map.get(stock_name)
        if stock_id is None:
            print(f"Warning: No stock_id found for {stock_name}")
            missing_stocks.append(stock_name)
            continue

        cursor.execute(
            """
            INSERT INTO f_stock_data (stock_id, metadata_id, data, date, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (stock_id, metadata_id, value, date_value)
        )

def process_local_excel_file(df):

    # Stock names from the 2th row (index 1)
    stock_names = df.iloc[2, 1:].dropna().values
    insert_stock_names = df.iloc[2, 1:].dropna().tolist()

    df.columns = ['Dates'] + stock_names.tolist()

    # droping first 4 rows (0, 1, 2, 3)  not needed for the data processing
    df = df.drop(index=[0, 1, 2, 3, 4])

    # The 'Dates' column is the first column, and we will process the remaining columns as stock data
    # Convert the 'Dates' column to datetime
    df['Dates'] = pd.to_datetime(df['Dates'], errors='coerce')

    # Extract the first 10 rows for processing
    rows_to_check = df.head(10)

    return rows_to_check, insert_stock_names


def get_existing_stock_data(cursor, metadata_id, date_value):
    """Fetch all stock names and their IDs for a given date and metadata_id."""
    query = """
        SELECT m_stock.id, m_stock.ticker FROM f_stock_data 
        JOIN m_stock ON f_stock_data.stock_id = m_stock.id
        WHERE f_stock_data.metadata_id = %s AND f_stock_data.date = %s
    """
    cursor.execute(query, (metadata_id, date_value))
    result = cursor.fetchall()
    # Convert to a dictionary {stock_name: stock_id}
    return {row[1]: row[0] for row in result}  # {name -> id}


def get_all_stock_ids(cursor, stock_names):
    stock_query = "SELECT id, ticker FROM m_stock WHERE ticker IN ({})".format(",".join(["%s"] * len(stock_names)))
    
    cursor.execute(stock_query, stock_names)
    fetched_results = cursor.fetchall()

    # Create a mapping of ticker -> stock_id
    stock_map = {ticker: stock_id for stock_id, ticker in fetched_results}

    return stock_map

