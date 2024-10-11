import os
import shutil
import time
import yaml

# Load configuration from YAML file
with open("config.yml", "r") as ymlfile:
    config = yaml.safe_load(ymlfile)

# Define the directories
path_dir = config['path_scan']
path_tmp = config['path_zip_file']
output_file = os.path.join(path_tmp, "compressed_backup.zip")

# Ensure the temporary directory exists
os.makedirs(path_tmp, exist_ok=True)

# Start the timer
start_time = time.time()

# Print path file
print(path_dir)

# Compress the directory
shutil.make_archive(output_file.replace('.zip', ''), 'zip', path_dir)

# Stop the timer
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

print(f"Compression completed in {elapsed_time:.4f} seconds")

# Delete the zip file
os.remove(output_file)
print(f"Deleted the file: {output_file}")
