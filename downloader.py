import boto3
import os
import schedule
import time
import logging
import signal
import filelock
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
from colorama import Fore, Style, init
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from itertools import islice
from datetime import datetime, timedelta
import hashlib
import queue
import threading

# Configuration
bucket_name = 'tf-cfw-logs'
prefix = ''  # If you want to check only a specific "folder".
local_download_path = os.path.expanduser('~/OBSDownloads')
endpoint_url = 'https://obs.eu-central-6001.apistack.one.hu'
interval_minutes = -1  # Script runs every N minutes. 0 or lower means one-time run
dry_run = True  # Set to False to perform actual operations
max_attempts = 3  # How many times should we retry the download if it fails
chunk_size = 8192  # Size of chunks to read when calculating checksums (8KB)
batch_size = 10  # Number of files to process in a batch
max_workers = 5  # Maximum number of concurrent downloads

@dataclass
class DownloadResult:
    key: str
    success: bool
    local_path: Optional[str] = None
    error: Optional[str] = None

class FileMetadataCache:
    def __init__(self, ttl_seconds=300):  # 5 minute cache TTL
        self.cache = {}
        self.ttl = timedelta(seconds=ttl_seconds)
        self._lock = threading.RLock()  # Add lock for thread safety

    def get(self, key):
        with self._lock:
            if key in self.cache:
                entry = self.cache[key]
                if datetime.now() - entry['timestamp'] < self.ttl:
                    return entry['data']
                del self.cache[key]
        return None

    def set(self, key, data):
        with self._lock:
            self.cache[key] = {
                'data': data,
                'timestamp': datetime.now()
            }

# Initialize colorama
init(autoreset=True)

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S')

# Set up log formatting
def log_success(message):
    logging.info(f'{Fore.GREEN}[Ok]{Style.RESET_ALL} {message}')

def log_error(message):
    logging.error(f'{Fore.RED}[Error]{Style.RESET_ALL} {message}')

def log_warn(message):
    logging.warning(f'{Fore.YELLOW}[Warn]{Style.RESET_ALL} {message}')

# Check if all the config options are provided
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

    # Create download directory if it doesn't exist
    try:
        os.makedirs(local_download_path, exist_ok=True)
        log_success(f'Download directory ensured: {local_download_path}')
    except Exception as e:
        log_error(f'Failed to create download directory: {e}')
        return False

    if not prefix:
        log_warn('Prefix is not set. Listing all objects in the bucket.')

    return True

# Initialize S3 client with retry configuration
try:
    s3_config = Config(
        retries={'max_attempts': max_attempts, 'mode': 'standard'},
        connect_timeout=10,
        read_timeout=30
    )
    s3 = boto3.client('s3', endpoint_url=endpoint_url, config=s3_config)
    log_success(f'S3 client initialized with endpoint {endpoint_url}')
except Exception as e:
    log_error(f'Error initializing S3 client: {e}')
    raise

# Initialize cache
metadata_cache = FileMetadataCache()

# Transaction log to track file operations
# mondjuk nem sok értelmét látom hirtelen ezeket az operation-öket logolni, de ha már egyszer meg lett csinálva, akkor legyen itt...
class TransactionLog:
    def __init__(self, log_file=None):
        if log_file is None:
            log_file = os.path.join(local_download_path, 'transaction_log.txt')
        self.log_file = log_file
        self._lock = threading.Lock()
        
    def log_operation(self, operation, key, success, error=None):
        with self._lock:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(self.log_file, 'a') as f:
                log_entry = f"{timestamp} | {operation} | {key} | {'Success' if success else 'Failed'}"
                if error:
                    log_entry += f" | {error}"
                f.write(f"{log_entry}\n")

transaction_log = TransactionLog()

def get_file_metadata(key):
    """Get file metadata with caching"""
    cached = metadata_cache.get(key)
    if cached:
        return cached

    try:
        metadata = s3.head_object(Bucket=bucket_name, Key=key)
        metadata_cache.set(key, metadata)
        return metadata
    except Exception as e:
        log_error(f'Error getting metadata for {key}: {e}')
        return None

def list_files_paginated():
    """List all files in the bucket with pagination"""
    all_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                all_keys.extend([obj['Key'] for obj in page['Contents']])
        
        if all_keys:
            log_success(f'Found {len(all_keys)} files in bucket')
        else:
            log_success('No files found in bucket')
            
        return all_keys
    except Exception as e:
        log_error(f'Error listing files: {e}')
        return []

def verify_checksum(local_file_path, s3_checksum):
    """Verify checksum using streaming approach to handle large files"""
    hash_md5 = hashlib.md5()
    try:
        with open(local_file_path, 'rb') as f:
            # Read file in chunks to avoid consuming a lot of memory
            for chunk in iter(lambda: f.read(chunk_size), b''):
                hash_md5.update(chunk)
        
        local_checksum = hash_md5.hexdigest()
        # S3 ETag is wrapped in quotes
        s3_checksum = s3_checksum.strip('"')
        
        return local_checksum == s3_checksum
    except Exception as e:
        log_error(f'Error verifying checksum: {e}')
        return False

class ProgressPercentage:
    def __init__(self, key, size):
        self._key = key
        self._size = size
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._last_print = 0

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            
            # Only update console every 10% to reduce spam
            if percentage - self._last_print >= 10 or percentage >= 100:
                self._last_print = int(percentage / 10) * 10
                print(f"\r{self._key}: {self._seen_so_far}/{self._size} ({percentage:.1f}%)", end="", flush=True)
                if percentage >= 100:
                    print()  # New line after completion

def download_file(key):
    """Download a file with error handling and retry logic"""
    local_file_path = os.path.join(local_download_path, os.path.basename(key))
    lock_file = f"{local_file_path}.lock"
    
    try:
        if dry_run:
            log_success(f'[Dry-Run] Would download {key}')
            transaction_log.log_operation("download", key, True, "dry-run")
            return DownloadResult(key=key, success=True)

        # Use file lock to prevent multiple processes downloading the same file
        with filelock.FileLock(lock_file, timeout=120):
            # Check if file already exists and has correct checksum
            metadata = get_file_metadata(key)
            if not metadata:
                error_msg = "Failed to get metadata"
                transaction_log.log_operation("metadata", key, False, error_msg)
                return DownloadResult(key=key, success=False, error=error_msg)
            
            s3_checksum = metadata.get('ETag', '').strip('"')
            file_size = metadata.get('ContentLength', 0)
            
            # Check if file exists and has correct size/checksum
            if os.path.exists(local_file_path):
                if os.path.getsize(local_file_path) == file_size and verify_checksum(local_file_path, s3_checksum):
                    log_success(f'File {key} already exists with correct checksum')
                    transaction_log.log_operation("download", key, True, "already exists")
                    return DownloadResult(key=key, success=True, local_path=local_file_path)
                else:
                    log_warn(f'File {key} exists but is incomplete or corrupted, redownloading')
            
            # Download with retry logic
            for attempt in range(max_attempts):
                try:
                    # Create progress callback
                    progress_callback = ProgressPercentage(key, file_size)
                    
                    # Download the file
                    s3.download_file(
                        Bucket=bucket_name,
                        Key=key,
                        Filename=local_file_path,
                        Callback=progress_callback
                    )
                    
                    # Verify the downloaded file
                    if verify_checksum(local_file_path, s3_checksum):
                        log_success(f'Downloaded {key} to {local_file_path}')
                        transaction_log.log_operation("download", key, True)
                        return DownloadResult(key=key, success=True, local_path=local_file_path)
                    else:
                        log_warn(f'Attempt {attempt+1}/{max_attempts}: Checksum verification failed for {key}')
                        if os.path.exists(local_file_path):
                            os.remove(local_file_path)
                except Exception as e:
                    log_warn(f'Attempt {attempt+1}/{max_attempts}: Error downloading {key}: {e}')
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)
            
            # If we reach here, all attempts failed
            error_msg = f"Failed after {max_attempts} attempts"
            transaction_log.log_operation("download", key, False, error_msg)
            return DownloadResult(key=key, success=False, error=error_msg)
# This part is important. Its there to prevent raceconditions
# We lock the file to prevent multiple processes from downloading the same file at the same time
# Without timeout, it would hang indefinitely if another process which holds the lock crashes
    except filelock.Timeout:
        error_msg = f"Lock timeout - another process is downloading {key}"
        transaction_log.log_operation("download", key, False, error_msg)
        return DownloadResult(key=key, success=False, error=error_msg)
    
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        transaction_log.log_operation("download", key, False, error_msg)
        return DownloadResult(key=key, success=False, error=error_msg)
    finally:
        # Clean up lock file if it exists
        lock_path = f"{local_file_path}.lock"
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except:
                pass

def delete_file(key):
    """Delete a file from S3 with error handling"""
    try:
        if dry_run:
            log_success(f'[Dry-Run] Would delete {key}')
            transaction_log.log_operation("delete", key, True, "dry-run")
            return True
        
        s3.delete_object(Bucket=bucket_name, Key=key)
        log_success(f'Deleted {key} from bucket')
        transaction_log.log_operation("delete", key, True)
        return True
    
    except Exception as e:
        error_msg = f'Error deleting file: {str(e)}'
        log_warn(error_msg)
        transaction_log.log_operation("delete", key, False, error_msg)
        return False

def batch_process_files(files, batch_size=10):
    """Process files in batches"""
    total_files = len(files)
    processed = 0
    results = {}
    
    while processed < total_files:
        # Get next batch of files
        batch = list(islice(files, processed, processed + batch_size))
        batch_results = {}  # Local results for this batch
        
        with ThreadPoolExecutor(max_workers=min(len(batch), max_workers)) as executor:
            # Submit batch for processing
            future_to_key = {executor.submit(download_file, key): key for key in batch}
            
            # Process results as they complete
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    result = future.result()
                    batch_results[key] = result
                except Exception as e:
                    log_error(f"Unexpected error processing {key}: {e}")
                    batch_results[key] = DownloadResult(key=key, success=False, error=str(e))
        
        # Update results after all batch operations complete
        results.update(batch_results)
        processed += len(batch)
        log_success(f'Processed {processed}/{total_files} files')
    
    return results

def check_and_download_files():
    """Main function to check for new files and download them"""
    try:
        log_success("Starting file download process")
        files = list_files_paginated()
        
        if not files:
            log_success('No files found to download.')
            return
        
        # Process files in batches
        download_results = batch_process_files(files, batch_size=batch_size)
        
        # Count successes and failures
        successes = sum(1 for result in download_results.values() if result.success)
        failures = sum(1 for result in download_results.values() if not result.success)
        
        log_success(f"Download summary: {successes} successful, {failures} failed")
        
        # Process successful downloads and delete from bucket
        for key, result in download_results.items():
            if result.success and result.local_path and os.path.exists(result.local_path):
                delete_file(key)
    
    except Exception as e:
        log_error(f"Error in check_and_download_files: {e}")

# This part is responsible for exiting the script gracefully
def handle_exit(signum, frame):
    log_success('Exiting script gracefully...')
    exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

def main():
    log_success(f"OBS Downloader starting - Version One :)")
    
    if not check_configuration():
        log_error('Script terminated due to missing configuration values.')
        return
    
    if interval_minutes <= 0:
        log_success('Running one-time operation.')
        check_and_download_files()
    else:
        schedule.every(interval_minutes).minutes.do(check_and_download_files)
        log_success(f'Scheduled to run every {interval_minutes} minutes.')
        
        # Run once immediately
        check_and_download_files()
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            log_success("Script stopped by user")
        except Exception as e:
            log_error(f"Unexpected error in main loop: {e}")

if __name__ == '__main__':
    main()
