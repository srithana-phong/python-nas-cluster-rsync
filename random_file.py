import os
import random

# Directory to store the files
output_dir = '/data_node_3/data/files_random'

# List of file extensions to choose from
file_extensions = [
    ".txt",
    ".doc",
    ".pdf",
    ".jpg",
    ".png",
    ".mp3",
    ".mp4",
    ".py",
    ".php",
    ".txt",
    ".yml",
    ".ini",
    ".json",
    ".mkv",
    ".avi",
    ".mov",
    ".mpeg",
    ".gif",
    ".jpg",
    ".jpeg",
    ".png",
    ".tif",
    ".tiff",
    ".svg",
    ".psd",
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".pptx",
    ".zip",
    ".rar",
    ".7z",
    ".xz",
    ".tar",
    ".csv",
    ".sql",
    ".sqlite",
    ".h",
    ".c",
    ".cpp",
    ".js",
    ".ts",
    ".php",
    ".java",
    ".cs",
    ".go",
    ".lua",
    ".html",
    ".css",
    ".bin",

]

# Create the directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Function to generate a random filename with a random extension
def generate_random_filename():
    random_name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZกขฃคฅฆงจฉชซฌญฎฏฐฑฒณดตถทธนบปผฝพฟภมยรลวศษสหฬอฮ0123456789', k=10))
    random_ext = random.choice(file_extensions)
    return random_name + random_ext

# Function to generate a file with random content of 1 KiB
def generate_random_file(filepath):
    with open(filepath, 'wb') as f:
        f.write(os.urandom((2)*1024))  # Write 1024 random bytes (1 KiB)

# Generate 20,000 random files
for i in range(354):
    filename = generate_random_filename()
    filepath = os.path.join(output_dir, filename)
    generate_random_file(filepath)
    if i % 1000 == 0:  # Print progress every 1000 files
        print(f'Generated {i} files')

print('File generation complete.')

