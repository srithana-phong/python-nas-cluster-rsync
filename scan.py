import os
import yaml # type: ignore
import mysql.connector # type: ignore
from mysql.connector import Error # type: ignore
import requests # type: ignore

# Load configuration from config.yml
with open("/NAS/config.yml", 'r') as file:
    config = yaml.safe_load(file)

# Configuration variables
path = config['path_scan']
db_server = config['db_server']
username = config['username']
passwd = config['password']
db_port = config['port_db']
db_name = config['db_name']
table_name = config['table_name_scan']

discord_webhook_url = config['discord_webhook_url']
line_access_token = config['line_access_token']

# Function to connect to the database
def create_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=db_server,
            user=username,
            password=passwd,
            port=db_port,
            database=db_name
        )
        if connection.is_connected():
            print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

# Function to drop the table if it exists
def drop_table_if_exists(connection):
    drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor = connection.cursor()
    try:
        cursor.execute(drop_table_query)
        connection.commit()
        print(f"Table `{table_name}` dropped successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

# Function to create table
def create_table(connection):
    create_table_query = f"""
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        file_name VARCHAR(255) NOT NULL,
        file_path TEXT NOT NULL,
        file_size BIGINT NOT NULL,
        file_extension VARCHAR(255) NOT NULL
    )
    """
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table `{table_name}` created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

# Function to insert file details into the table
def insert_file_details(connection, file_name, file_path, file_size, file_extension):
    insert_query = f"""
    INSERT INTO {table_name} (file_name, file_path, file_size, file_extension)
    VALUES (%s, %s, %s, %s)
    """
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query, (file_name, file_path, file_size, file_extension))
        connection.commit()
        print(f"Inserted {file_name} into `{table_name}`")
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

# Function to scan the directory and insert details into the database
def scan_directory_and_log_files(directory_path, connection):
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            file_extension = os.path.splitext(file)[1]
            insert_file_details(connection, file, file_path, file_size, file_extension)

# Notify Discord
def notify_discord(webhook_url, message):
    data = {"content": message}
    response = requests.post(webhook_url, json=data)
    return response.status_code

# Notify LINE
def notify_line(access_token, message):
    url = "https://notify-api.line.me/api/notify"
    headers = {"Authorization": f"Bearer {access_token}"}
    data = {"message": message}
    response = requests.post(url, headers=headers, data=data)
    return response.status_code

# Get table size and row count
def get_table_size_and_count(connection):
    # Query to get the table size
    size_query = f"""
    SELECT table_name AS `Table`,
           round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB`
    FROM information_schema.TABLES
    WHERE table_schema = '{db_name}'
      AND table_name = '{table_name}';
    """
    
    # Query to get the row count
    count_query = f"SELECT COUNT(*) FROM {table_name};"
    
    cursor = connection.cursor()
    
    try:
        # Execute the size query
        cursor.execute(size_query)
        size_result = cursor.fetchone()
        
        # Execute the row count query
        cursor.execute(count_query)
        count_result = cursor.fetchone()
        
        if size_result and count_result:
            table, size = size_result
            row_count = count_result[0]
            print(f"The size of the table '{table}' is {size} MB")
            print(f"The row count of the table '{table}' is {row_count}")
            return size, row_count
        else:
            print(f"Table '{table_name}' not found in database '{db_name}'")
            return None, None

    except Error as e:
        print(f"Error: {e}")
        return None, None
    
    finally:
        cursor.close()

# Main script
def main():
    connection = create_connection()
    if connection:
        drop_table_if_exists(connection)
        create_table(connection)
        scan_directory_and_log_files(path, connection)
        size, row_count = get_table_size_and_count(connection)

        connection.close()

        if size is not None and row_count is not None:
            message = f"Scan File Successful. Table name {table_name}. Table size: {size} MB. Row count: {row_count}."

            # Send to Discord
            discord_status = notify_discord(discord_webhook_url, message)
            print(f"Discord notification status: {discord_status}")

            # Send to LINE
            line_status = notify_line(line_access_token, message)
            print(f"LINE notification status: {line_status}")

if __name__ == "__main__":
    main()
