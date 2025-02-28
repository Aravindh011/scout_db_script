import mysql.connector

# Database connection settings
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '12345678',
    'database': 'scout',
}

def get_db_connection():
    """Establish and return a database connection and cursor."""
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    return connection, cursor

def close_db_connection(connection, cursor):
    """Close the database connection and cursor."""
    if cursor:
        cursor.close()
    if connection:
        connection.close()