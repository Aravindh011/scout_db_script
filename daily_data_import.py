import pandas as pd
from email_sender import send_email
from database import get_db_connection, close_db_connection


def upload_daily_data(file_name, local_file_path):
    # Database connection
    connection, cursor = get_db_connection()
    missing_stocks = []
    message = "Initiating upload of daily data"

    try:
        df = pd.read_excel(local_file_path, sheet_name=0)
        rows_to_check, stock_names = process_local_excel_file(df)

        # Fetch the metadata_id (volume/price/market_cap)
        identifier = None
        if "MC_USD" in file_name:
            identifier = "MC"
        elif "PX_USD" in file_name:
            identifier = "PX"
        elif "Vol_USD" in file_name:
            identifier = "Volume"
        cursor.execute("SELECT id FROM m_stock_metadata WHERE identifier = %s", (identifier,))
        metadata_result = cursor.fetchone()

        if not metadata_result:
            print(f"Error: {file_name} metadata not found")
            message = f"Error: {file_name} metadata not found"
            return
        
        metadata_id = metadata_result[0]

        # Fetch existing stock entries for this date
        missing_stocks = []
        stock_map = get_all_stock_ids(cursor, stock_names)

        for _, row in rows_to_check.iterrows():

            date_value = row['Dates']

            # Fetch existing stock data for this date (name -> id mapping)
            existing_stock_map = get_existing_stock_data(cursor, metadata_id, date_value)
            
            # Find missing stocks
            missing_stocks_for_date = [stock_name for stock_name in stock_names if stock_name not in existing_stock_map]

            if not missing_stocks_for_date:
                print(f"All stocks already exist for {date_value}. Skipping insertion.")
                continue

            # Insert only missing stocks
            for stock_name in missing_stocks_for_date:
                value = row.get(stock_name)
                if pd.isna(value):  # Skip if value is NaN
                    continue

                stock_id = stock_map.get(stock_name)  # Get stock_id from stock_map
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
        connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        message = f"An error occurred: {e}"

    finally:
        close_db_connection(connection, cursor)
        send_email(missing_stocks, message)


def process_local_excel_file(df):
    df = df

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

