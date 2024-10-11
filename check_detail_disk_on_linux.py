import psutil # type: ignore
import mysql.connector
import requests
import yaml
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from YAML file
with open("/NAS/config.yml", 'r') as file:
    config = yaml.safe_load(file)

# Variables
mountpoint_to_check = config["mountpoint_to_check_detail_disk"]
db_server = config["db_server"]
username = config["username"]
passwd = config["password"]
port_db = config["port_db"]
db_name = config["db_name"]
node = config["node"]
table_name = config["table_name_check_detail_disk"]
discord_webhook_url = config["discord_webhook_url"]
line_access_token = config["line_access_token"]

# Function to get disk details
def get_disk_details(mountpoint):
    try:
        partitions = psutil.disk_partitions()
        for partition in partitions:
            if partition.mountpoint == mountpoint:
                partition_usage = psutil.disk_usage(partition.mountpoint)
                return {
                    "device": partition.device,
                    "mountpoint": partition.mountpoint,
                    "fstype": partition.fstype,
                    "total_size": partition_usage.total / (1024 * 1024 * 1024),  # GB
                    "used": partition_usage.used / (1024 * 1024 * 1024),  # GB
                    "free": partition_usage.free / (1024 * 1024 * 1024),  # GB
                    "percentage": partition_usage.percent
                }
    except Exception as e:
        logging.error(f"Error getting disk details: {e}")
    return None

# Function to insert or update database
def update_database(details):
    try:
        conn = mysql.connector.connect(
            host=db_server,
            user=username,
            password=passwd,
            database=db_name,
            port=port_db
        )
        cursor = conn.cursor()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            name_node VARCHAR(255),
            device_info VARCHAR(255),
            mountpoint VARCHAR(255),
            disk_type VARCHAR(255),
            total_size FLOAT,
            used FLOAT,
            free FLOAT,
            percentage FLOAT,
            PRIMARY KEY (name_node, mountpoint)
        )
        """
        cursor.execute(create_table_query)

        insert_query = f"""
        REPLACE INTO {table_name} (name_node, device_info, mountpoint, disk_type, total_size, used, free, percentage)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (node, details["device"], details["mountpoint"], details["fstype"], 
                                      details["total_size"], details["used"], details["free"], details["percentage"]))
        conn.commit()
        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

# Function to send notification to Discord
def notify_discord(message):
    try:
        data = {"content": message}
        response = requests.post(discord_webhook_url, json=data)
        return response.status_code
    except requests.RequestException as e:
        logging.error(f"Error sending Discord notification: {e}")
        return None

# Function to send notification to Line
def notify_line(message):
    try:
        headers = {
            "Authorization": f"Bearer {line_access_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"message": message}
        response = requests.post("https://notify-api.line.me/api/notify", headers=headers, data=data)
        return response.status_code
    except requests.RequestException as e:
        logging.error(f"Error sending Line notification: {e}")
        return None

# Main script
details = get_disk_details(mountpoint_to_check)

if details:
    update_database(details)
    message = f"Node name: {node}:\n" \
              f"Disk Details for {mountpoint_to_check}:\n" \
              f"Device: {details['device']}\n" \
              f"Total Size: {details['total_size']:.2f} GB\n" \
              f"Used: {details['used']:.2f} GB\n" \
              f"Free: {details['free']:.2f} GB\n" \
              f"Percentage: {details['percentage']}%"

    discord_status = notify_discord(message)
    line_status = notify_line(message)

    logging.info(f"Discord notification status: {discord_status}")
    logging.info(f"Line notification status: {line_status}")
else:
    logging.warning(f"No details found for mount point: {mountpoint_to_check}")
