## DB Scipt
OverviewThe script is designed to process Excel files containing stock data and store relevant data in the database while handling missing stock entries and sending alerts for them.

## Files in the System:
1. main.py: Main entry point of the application that triggers the Lambda function for processing downloaded files from the SFTP server.
2. daily_data_import.py: Handles the daily data import process, specifically for files with identifiers such as MC_USD, PX_USD, and Vol_USD.
3. yearly_data_import.py: Processes yearly stock data and inserts it into the database.
4. database.py: Contains database connection and closure logic for interacting with the MySQL database.
5. email_sender.py: Sends email notifications with the status of data imports, including missing stocks.
6. sftp_import.py: Handles downloading files from an SFTP server to the /tmp/ directory (for use in AWS Lambda).
requirements.txt: Contains the list of dependencies required to run the scripts.
