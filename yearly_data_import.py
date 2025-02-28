import pandas as pd
from database import get_db_connection, close_db_connection
from email_sender import send_email
from datetime import datetime

def upload_yearly_data(file_name, local_file_path):
    connection, cursor = get_db_connection()
    missing_stocks = []
    message = "Initiating upload of yearly data"
    try:
        # Load Excel file
        file_path = local_file_path
        xls = pd.ExcelFile(file_path)

        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)

            header_row_idx = 1

            # Extract column names (years) from identified header row
            df.columns = df.iloc[header_row_idx]
            df = df[header_row_idx + 1:].reset_index(drop=True)

            # getting attribute
            cursor.execute("SELECT id, identifier FROM m_stock_metadata WHERE identifier = %s", (sheet_name,))
            metadata_mapping = {name: meta_id for meta_id, name in cursor.fetchall()}
            print(metadata_mapping)

            for _, row in df.iterrows():
                company_name = row.iloc[1]
                if not isinstance(company_name, str):
                    continue

                cursor.execute("SELECT id FROM m_stock WHERE ticker = %s", (company_name,))
                stock_result = cursor.fetchall()  # Fetch all results to avoid unread result error
                stock_id = 0
                if stock_result:
                    stock_id = stock_result[0][0]  # Extract first ID
                    print("Stock ID:", stock_id)
                else:
                    print(f"Stock ID not found for {company_name}, skipping...")
                    missing_stocks.append(company_name)
                    continue

                # Iterate through each column (starting from index 2 onwards)
                for col_name, value in row.items():
                    if pd.isna(value) or pd.isna(col_name):  # Skip if value is NaN
                        continue

                    current_year = datetime.now().year
                    if col_name in ['LTM', 'LTM-4'] or (isinstance(col_name, float) and 1950.0 <= col_name <= current_year):
                        pass
                    else:
                        continue

                    cursor.execute("SELECT * FROM f_fundamental_d WHERE stock_id = %s AND metadata_id = %s AND ltm_years = %s", (stock_id, metadata_mapping.get(sheet_name), col_name))
                    data_exist = cursor.fetchone()
                    if data_exist:
                        print(f"Data already exists for {company_name} - {col_name}, skipping...")
                        continue

                    cursor.execute("INSERT INTO f_fundamental_d (stock_id, metadata_id, data, ltm_years, created_at) VALUES (%s, %s, %s, %s, NOW())", (stock_id, metadata_mapping.get(sheet_name), value, col_name))
                    print(f"Data inserting for {company_name} - {col_name}")

                    #store in db

            connection.commit()

        print("Data processing complete.")
        message = "Data processing complete."

    except Exception as e:
        print(f"An error occurred: {e}")
        message = f"An error occurred: {e}"

    finally:
        close_db_connection(connection, cursor)
        send_email(missing_stocks, message)
