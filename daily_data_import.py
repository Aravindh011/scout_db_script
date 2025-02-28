import pandas as pd
from email_sender import send_email
from database import get_db_connection, close_db_connection


def upload_daily_data(file_name, local_file_path):
    # Database connection
    connection, cursor = get_db_connection()
    missing_stocks = []
    message = "Initiating upload of daily data"

    try:
        #aws file
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
        print(f"Metadata ID: {metadata_id}")

        # Fetch the latest date from stock table for the given metadata_id
        latest_date = get_latest_stock_date(cursor, metadata_id)
        print(f"Latest date from stock_table: {latest_date}")

        # converting to a pandas Timestamp for comparison
        if latest_date:
            latest_date = pd.Timestamp(latest_date)

        # Compare and filter new rows
        new_rows = rows_to_check[rows_to_check['Dates'] > latest_date]

        if not new_rows.empty:
            print(f"Found {len(new_rows)} new rows. Inserting into stock_table...")
            missing_stocks = insert_new_rows(cursor, new_rows, stock_names, metadata_id)
            connection.commit()
        else:
            print("No new rows to insert.")

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

    print(rows_to_check)

    return rows_to_check, insert_stock_names


def get_latest_stock_date(cursor, metadata_id):
    query = """
        SELECT MAX(date) AS latest_date 
        FROM f_stock_data 
        WHERE metadata_id = %s
    """
    cursor.execute(query, (metadata_id,))
    result = cursor.fetchone()
    return result[0] if result and result[0] else None


def insert_new_rows(cursor, rows, stock_names, metadata_id):
    stock_query = "SELECT id, name, ticker FROM m_stock WHERE ticker IN ({})".format(",".join(["%s"] * len(stock_names)))
    print("Executing SQL Query:", stock_query)
    print("With Values:", stock_names)
    
    cursor.execute(stock_query, stock_names)
    fetched_results = cursor.fetchall()
    print("Fetched Results from Database:", fetched_results)

    #mapping of ticker -> stock_id
    stock_map = {ticker: stock_id for stock_id, name, ticker in fetched_results}

    # Debugging print
    print("Stock Map:", stock_map)

    missing_stocks = []

    # Insert data into f_stock_data
    for _, row in rows.iterrows():
        date_value = row['Dates']
        if pd.isna(date_value):  # Skip if date is NaN
            continue

        for stock_name, value in row.items():
            if stock_name == 'Dates' or pd.isna(value):  # Skip 'Dates' column and NaN values
                continue

            stock_id = stock_map.get(stock_name)  # Correct mapping to ticker
            if stock_id is None:
                print(f"Warning: No stock_id found for {stock_name}")
                missing_stocks.append(stock_name)
                continue
            print("executing ", value)
            cursor.execute(
                """
                INSERT INTO f_stock_data (stock_id, metadata_id, data, date, created_at)
                VALUES (%s, %s, %s, %s, NOW())
                """,
                (stock_id, metadata_id, value, date_value)
            )

    return missing_stocks

