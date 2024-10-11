import os
import datetime
import yaml
import mysql.connector
import requests

# Load configuration from config.yml
with open("/NAS/config.yml", 'r') as file:
    config = yaml.safe_load(file)

db_server = config['db_server']
username = config['username']
passwd = config['password']
db_port = config['port_db']
db_name = config['db_name']
table_name = config["table_name_zfs_snaphot"]
node = config['node']
zpool_name = config['zpool_name_zfs_snaphot']
date_format = config['date_format']

snapshot_name = f"{zpool_name}@{node}_snapshot_{datetime.datetime.now().strftime(date_format)}"

discord_webhook_url = config['discord_webhook_url']
line_access_token = config['line_access_token']

db_config = {
    'host': db_server,
    'user': username,
    'password': passwd,
    'database': db_name,
    'port': db_port
}

def create_snapshot(snapshot_name):
    create_command = f"zfs snapshot {snapshot_name}"
    result = os.system(create_command)
    if result != 0:
        raise Exception(f"Snapshot creation failed with code {result}")

def write_status_to_db(snapshot_name, status):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                snapshot_name VARCHAR(255),
                status VARCHAR(255),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        insert_query = f"INSERT INTO {table_name} (snapshot_name, status) VALUES (%s, %s)"
        cursor.execute(insert_query, (snapshot_name, status))
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def notify_discord(message):
    payload = {"content": message}
    response = requests.post(discord_webhook_url, json=payload)
    if response.status_code != 204:
        print(f"Failed to send Discord notification: {response.status_code}, {response.text}")

def notify_line(message):
    headers = {"Authorization": f"Bearer {line_access_token}"}
    payload = {"message": message}
    response = requests.post("https://notify-api.line.me/api/notify", headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Failed to send Line notification: {response.status_code}, {response.text}")

def remove_old_snapshot():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Count the number of snapshots with '{node}' in their name
        count_query = f"SELECT COUNT(`snapshot_name`) FROM {table_name} WHERE `snapshot_name` LIKE '%{node}%';"
        cursor.execute(count_query)
        snapshot_count = cursor.fetchone()[0]

        if snapshot_count >= 8:
            # Find the oldest snapshot with '{node}' in its name
            oldest_snapshot_query = f"""
                SELECT snapshot_name 
                FROM {table_name} 
                WHERE snapshot_name LIKE '%{node}%' 
                ORDER BY timestamp ASC 
                LIMIT 1;
            """
            cursor.execute(oldest_snapshot_query)
            oldest_snapshot = cursor.fetchone()[0]

            # Remove the oldest snapshot
            remove_command = f"zfs destroy {oldest_snapshot}"
            os.system(remove_command)
            print(f"Removed old snapshot: {oldest_snapshot}")

            # Remove the entry from the database
            delete_query = f"DELETE FROM {table_name} WHERE snapshot_name = %s"
            cursor.execute(delete_query, (oldest_snapshot,))
            connection.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    try:
        create_snapshot(snapshot_name)
        write_status_to_db(snapshot_name, "Success")
        message = f"{snapshot_name} created successfully."
        remove_old_snapshot()  # Remove the oldest snapshot if more than 7 exist
        str(remove_old_snapshot())
    except Exception as e:
        write_status_to_db(snapshot_name, f"Failed: {str(e)}")
        message = f"Snapshot {snapshot_name} creation failed: {str(e)}"
    
    notify_discord(message)
    notify_line(message)
