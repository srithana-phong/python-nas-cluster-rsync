from aifc import Error
import subprocess
import mysql.connector
import os
import requests
from datetime import datetime
import yaml

# Load configuration from YAML file
with open("/NAS/config.yml", "r") as ymlfile:
    config = yaml.safe_load(ymlfile)

# Set variables from config
directory_path = config['directory_path_rsync']
file_path_backup = config['file_path_backup_rsync']
db_ip = config['db_server']
db_port = config['port_db']
db_name = config['db_name']
table_name = config['table_name_rsync']
db_user = config['username']
db_password = config['password']
discord_webhook_url = config['discord_webhook_url']
line_access_token = config['line_access_token']

# Database connection
def connect_db():
    return mysql.connector.connect(
        host=db_ip,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )

# Create the database table if it doesn't exist
def create_table():
    db_conn = connect_db()
    cursor = db_conn.cursor()
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        file_name VARCHAR(255),
        directory_path VARCHAR(255),
        path_backup VARCHAR(255),
        file_extension VARCHAR(10),
        timestamp DATETIME
    )
    """)
    db_conn.commit()
    cursor.close()
    db_conn.close()

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
        
# Insert rsync output into the database
def insert_log(file_name, dir_path, backup_path, file_ext, timestamp):
    db_conn = connect_db()
    cursor = db_conn.cursor()
    cursor.execute(f"""
    INSERT INTO {table_name} (file_name, directory_path, path_backup, file_extension, timestamp)
    VALUES (%s, %s, %s, %s, %s)
    """, (file_name, dir_path, backup_path, file_ext, timestamp))
    db_conn.commit()
    cursor.close()
    db_conn.close()

# Rsync and log file paths
def run_rsync():
    rsync_command = ["rsync", "-avh", directory_path, file_path_backup]
    result = subprocess.run(rsync_command, capture_output=True, text=True)
    output_lines = result.stdout.splitlines()
    
    for line in output_lines:
        if not line.endswith('/'):  # Skip directories
            file_name = os.path.basename(line)
            file_ext = os.path.splitext(file_name)[1]
            full_file_path = os.path.join(file_path_backup, file_name)
            #size = os.path.getsize(full_file_path) if os.path.exists(full_file_path) else 0
            timestamp = datetime.now()
            
            # Insert into database
            insert_log(file_name, directory_path, file_path_backup, file_ext, timestamp)

# Get table size and row count
def get_table_size_and_count():
    db_conn = connect_db()
    cursor = db_conn.cursor()
    cursor.execute(f"SELECT table_name AS `Table`, ROUND((data_length + index_length) / 1024 / 1024, 2) `Size in MB` FROM information_schema.TABLES WHERE table_schema = '{db_name}' AND table_name = '{table_name}';")
    size_result = cursor.fetchone()
    
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_result = cursor.fetchone()
    
    cursor.close()
    db_conn.close()
    
    return size_result, count_result

# Send notification to Discord
def send_discord_notification(message):
    payload = {
        "content": message
    }
    requests.post(discord_webhook_url, json=payload)

# Send notification to Line
def send_line_notification(message):
    headers = {
        "Authorization": f"Bearer {line_access_token}"
    }
    payload = {
        "message": message
    }
    requests.post("https://notify-api.line.me/api/notify", headers=headers, data=payload)

# Main execution
if __name__ == "__main__":

    # Drop table if exists
    drop_table_if_exists()
    # Create table
    create_table()
    
    # Run rsync and log
    run_rsync()
    
    # Get table size and count
    size_info, count_info = get_table_size_and_count()
    message = f"Table {table_name} Size: {size_info[1]} MB, Rows: {count_info[0]}"
    
    # Send notifications
    send_discord_notification(message)
    send_line_notification(message)

    print("Rsync operation and logging complete.")
