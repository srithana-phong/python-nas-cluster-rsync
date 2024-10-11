import os
import zipfile
from datetime import datetime
import mysql.connector
import requests
import yaml

# Load configuration from YAML file
with open("/NAS/config.yml", "r") as ymlfile:
    config = yaml.safe_load(ymlfile)

# Variables from config
db_server = config["db_server"]
username = config["username"]
passwd = config["password"]
port_db = config["port_db"]
db_name = config["db_name"]
node = config["node"]
table_name = config["table_name_zip_file"]
path_dir_file = config["path_dir_file_zip_file"]
path_zip_file = config["path_zip_file"]
date_format = config["date_format"]
discord_webhook_url = config["discord_webhook_url"]
line_access_token = config["line_access_token"]

# Create table SQL
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    zip_name VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

def create_zip_file():
    date_str = datetime.now().strftime(date_format)
    zip_name = f"{node}_zipfile_{date_str}.zip"
    zip_path = os.path.join(path_zip_file, zip_name)

    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, dirs, files in os.walk(path_dir_file):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), path_dir_file))
    
    return zip_name, zip_path

def insert_status_to_db(zip_name, status):
    try:
        connection = mysql.connector.connect(
            host=db_server,
            user=username,
            password=passwd,
            database=db_name,
            port=port_db
        )
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        connection.commit()

        insert_sql = f"INSERT INTO {table_name} (zip_name, status) VALUES (%s, %s)"
        cursor.execute(insert_sql, (zip_name, status))
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def notify_discord(message):
    data = {"content": message}
    response = requests.post(discord_webhook_url, json=data)
    if response.status_code != 204:
        print(f"Failed to send Discord notification: {response.text}")

def notify_line(message):
    headers = {
        "Authorization": f"Bearer {line_access_token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"message": message}
    response = requests.post("https://notify-api.line.me/api/notify", headers=headers, data=data)
    if response.status_code != 200:
        print(f"Failed to send Line notification: {response.text}")

if __name__ == "__main__":
    zip_name, zip_path = create_zip_file()
    status = "Success" if os.path.exists(zip_path) else "Failure"
    insert_status_to_db(zip_name, status)

    message = f"Zip file {zip_name} creation status: {status}"
    notify_discord(message)
    notify_line(message)
