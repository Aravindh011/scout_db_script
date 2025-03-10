from sftp_import import get_files
import re, os
from daily_data_import import upload_daily_data
from yearly_data_import import upload_yearly_data

def lambda_handler(event, context):

    remote_path = "/remote/folder/"
    local_path = "/tmp/"  # AWS Lambda /tmp/ for temporary storage

    # Download files from SFTP server
    downloaded_files = get_files()

    # Identify the file by name
    for local_file_path in downloaded_files:
        file_name = os.path.basename(local_file_path)
        
        if re.search(r"(MC|Vol|PX)", file_name, re.IGNORECASE):
            upload_daily_data(file_name, local_file_path)

        elif "MENA_Fundamentals" in file_name:
            upload_yearly_data(file_name, local_file_path)
            
        else:
            print(f"No matching function for file: {file_name}")

    return {
        "statusCode": 200,
        "body": "Files processed successfully"
    }