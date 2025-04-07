import boto3
import os
import schedule
import time
import logging
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Configuration
bucket_name = 'tf-cfw-logs'
prefix = ''  #  If you want to check only a specific "folder". For example setting Ubuntu/ here will list only the objects inside the bucket_name/Ubuntu "folder". Empty = no filter
local_download_path = '~/OBSDownloads'
endpoint_url = 'https://obs.eu-central-6001.apistack.one.hu'
interval_minutes = -1  # The script runs every N minutes. N = the number you write here. empty or -1 means one time run
#AK/SK as env variable in .bashrc
#credential file can be used as well, see: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Set up the log formatting
def log_success(message):
    logging.info(f'{Fore.GREEN}[Ok]{Style.RESET_ALL} {message}')

def log_error(message):
    logging.error(f'{Fore.RED}[Error]{Style.RESET_ALL} {message}')

def log_warn(message):
    logging.warning(f'{Fore.YELLOW}[Warn]{Style.RESET_ALL} {message}')

#check if all the config options are provided
def check_configuration():
    missing_config = []
    if not bucket_name:
        missing_config.append('bucket_name')
    if not local_download_path:
        missing_config.append('local_download_path')
    if not endpoint_url:
        missing_config.append('endpoint_url')

    if missing_config:
        log_error(f'Missing configuration values: {", ".join(missing_config)}')
        return False

    if not prefix:
        log_warn('Prefix is not set. Listing all objects in the bucket.')

    return True

# Initialize S3 client
try:
    s3 = boto3.client('s3', endpoint_url=endpoint_url)
except Exception as e:
    log_error(f'Error initializing S3 client: {e}')
    raise

# List all the files in the given bucket
def list_files():
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        return []
    except Exception as e:
        log_error(f'Error listing files: {e}')
        return []

# Download all files from the given bucket and save it local_download_path
def download_file(key):
    try:
        local_file_path = os.path.join(local_download_path, os.path.basename(key))
        s3.download_file(bucket_name, key, local_file_path)
        log_success(f'Downloaded {key} to {local_file_path}')
        return local_file_path
    except Exception as e:
        log_error(f'Error downloading file {key}: {e}')
        return None

# Delete the downloaded file from the bucket
# The deletion only happens if the download was succesful AND the file exists on the local machine
# If the download fails, this part does not run
# check_and_download_files is the responsible part for this check. the download_file and delete_file parts are called from within the check part.
def delete_file(key):
    try:
        s3.delete_object(Bucket=bucket_name, Key=key)
        log_success(f'Deleted {key} from bucket')
    except Exception as e:
        log_warn(f'Error deleting file {key}: {e}')

def check_and_download_files():
    files = list_files()
    if not files:
        log_success('There are no new files in the bucket.')
        return

    for file_key in files:
        local_file_path = download_file(file_key)
        if local_file_path and os.path.exists(local_file_path):
            delete_file(file_key)

def main():
    if not check_configuration():
        log_error('Script terminated due to missing configuration values.')
        return

    if interval_minutes == -1 or interval_minutes is None:
        log_success('Interval is too low. Running one-time operation.')
        check_and_download_files()
    else:
        schedule.every(interval_minutes).minutes.do(check_and_download_files)
        log_success(f'Scheduled to run every {interval_minutes} minutes.')

        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == '__main__':
    main()
