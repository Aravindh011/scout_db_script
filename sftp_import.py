import paramiko, re

def get_files():
    # SFTP Connection Details
    host = "sftp.example.com"
    port = 22
    username = "your_username"
    password = "your_password"

    # Establish Connection
    client = paramiko.Transport((host, port))
    client.connect(username=username, password=password)
    sftp = client.open_sftp()

    # List Files and Download All to /tmp/
    files = sftp.listdir("/remote/folder/")
    downloaded_files = []
    
    for file_name in files:
        local_path = f"/tmp/{file_name}"  # AWS Lambda uses /tmp/ for temporary storage
        sftp.get(f"/remote/folder/{file_name}", local_path)
        downloaded_files.append(local_path)  # Add to list for processing later

    # Close Connection after downloading all files
    sftp.close()
    client.close()

    return downloaded_files
