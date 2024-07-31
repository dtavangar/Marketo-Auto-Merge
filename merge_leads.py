import requests
import json
import logging
import time
import csv
import yaml
import os
from datetime import datetime, timedelta, timezone

__author__ = "Damon Tavangar"
__license__ = "GPL"
__version__ = "1.0.6"
__maintainer__ = "Damon Tavangar"
__email__ = "tavangar@2017@gmail.com"
__status__ = "Production"

# Load settings from the appropriate YAML file based on the environment
env = os.getenv('ENV', 'dev')  # Default to 'dev' if ENV is not set
settings_file = f'settings.{env}.yaml'

try:
    with open(settings_file, 'r') as file:
        settings = yaml.safe_load(file)
    if settings is None:
        raise ValueError(f"Failed to load settings from {settings_file}")
except Exception as e:
    raise RuntimeError(f"Error loading settings file {settings_file}: {e}")

# Extract settings
JOB_END_DAYS = settings.get('job_end_days', 31)
START_DAYS_BEFORE_END = settings.get('start_days_before_end', 90)
LOG_NAME = settings.get('log_name', 'merge.log')
LOG_LOCATION = settings.get('log_location', './')
LOG_PATH = os.path.join(LOG_LOCATION, LOG_NAME)
AUTH_ENDPOINT = settings.get('auth_endpoint', '/identity/oauth/token')
BULK_EXPORT_CREATE_ENDPOINT = settings.get('bulk_export_create_endpoint', '/bulk/v1/leads/export/create.json')
BULK_EXPORT_ENQUEUE_ENDPOINT = settings.get('bulk_export_enqueue_endpoint', '/bulk/v1/leads/export/{}/enqueue.json')
BULK_EXPORT_STATUS_ENDPOINT = settings.get('bulk_export_status_endpoint', '/bulk/v1/leads/export/{}/status.json')
BULK_EXPORT_FILE_ENDPOINT = settings.get('bulk_export_file_endpoint', '/bulk/v1/leads/export/{}/file.json')
MERGE_ENDPOINT = settings.get('merge_endpoint', '/rest/v1/leads/{}/merge.json')
FIELDS = settings.get('fields', ["id", "Email", "createdAt"])
CUSTOM_FIELD_NAME = settings.get('custom_field_name', 'clientid')
USE_CUSTOM_FIELD = settings.get('use_custom_field', True)
if USE_CUSTOM_FIELD:
    FIELDS.append(CUSTOM_FIELD_NAME)
FILTER_DATE_FORMAT = settings.get('filter_date_format', '%Y-%m-%dT%H:%M:%SZ')
MERGE_IN_CRM = settings.get('merge_in_crm', 'false')
DOWNLOAD_LOCATION = settings.get('download_location', './downloads/')

# Ensure download directory exists
os.makedirs(DOWNLOAD_LOCATION, exist_ok=True)

# Get CLIENT_ID, CLIENT_SECRET, and MUNCHKIN_ID from environment variables
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
MUNCHKIN_ID = os.getenv('MUNCHKIN_ID')

# Validate configuration
if not CLIENT_ID or not CLIENT_SECRET or not MUNCHKIN_ID:
    raise ValueError("CLIENT_ID, CLIENT_SECRET, and MUNCHKIN_ID must be set in environment variables.")

# Construct base_url
BASE_URL = f"https://{MUNCHKIN_ID}.mktorest.com"

# Configure logging
logging.basicConfig(filename=LOG_PATH, level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize API call counter
api_call_count = 0

def get_access_token():
    global api_call_count
    try:
        logging.info('Requesting access token...')
        response = requests.get(f"{BASE_URL}{AUTH_ENDPOINT}", params={
            'grant_type': 'client_credentials',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        })
        api_call_count += 1
        response.raise_for_status()
        token_data = response.json()
        if not token_data.get('access_token'):
            logging.error(f'Failed to retrieve access token: {token_data}')
            return None
        logging.info('Successfully retrieved access token')
        return token_data.get('access_token')
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to get access token: {e}')
        raise

def create_bulk_export_job(token, start_date, end_date):
    global api_call_count
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    data = {
        "format": "CSV",
        "fields": FIELDS,
        "filter": {
            "createdAt": {
                "startAt": start_date.strftime(FILTER_DATE_FORMAT),
                "endAt": end_date.strftime(FILTER_DATE_FORMAT)
            }
        }
    }
    try:
        logging.info(f'Creating bulk export job for date range: {start_date} to {end_date}')
        response = requests.post(f"{BASE_URL}{BULK_EXPORT_CREATE_ENDPOINT}", headers=headers, json=data)
        api_call_count += 1
        response.raise_for_status()
        job_id = response.json().get('result', [{}])[0].get('exportId')
        if not job_id:
            logging.error(f'Bulk export job creation failed: {response.json()}')
            return None
        logging.info(f'Bulk export job created with ID: {job_id}')
        return job_id
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to create bulk export job: {e}')
        raise

def enqueue_bulk_export_job(token, job_id):
    global api_call_count
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    try:
        logging.info(f'Enqueueing bulk export job {job_id}...')
        response = requests.post(f"{BASE_URL}{BULK_EXPORT_ENQUEUE_ENDPOINT.format(job_id)}", headers=headers)
        api_call_count += 1
        response.raise_for_status()
        logging.info(f'Bulk export job {job_id} enqueued successfully')
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to enqueue bulk export job: {e}')
        raise

def check_bulk_export_status(token, job_id):
    global api_call_count
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(f"{BASE_URL}{BULK_EXPORT_STATUS_ENDPOINT.format(job_id)}", headers=headers)
        api_call_count += 1
        response.raise_for_status()
        status = response.json().get('result', [{}])[0].get('status')
        return status
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to check bulk export job status: {e}')
        raise

def download_bulk_export_file(token, job_id):
    global api_call_count
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(f"{BASE_URL}{BULK_EXPORT_FILE_ENDPOINT.format(job_id)}", headers=headers)
        api_call_count += 1
        response.raise_for_status()
        content = response.content.decode('utf-8')
        leads = content.splitlines()
        logging.info(f'Downloaded {len(leads) - 1} leads from bulk export file')
        
        # Save file to the download location
        file_path = os.path.join(DOWNLOAD_LOCATION, f'leads_{job_id}.csv')
        with open(file_path, 'w') as file:
            file.write(content)
        
        logging.info(f'Saved downloaded leads to {file_path}')
        return file_path
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to download bulk export file: {e}')
        raise

def parse_csv_leads(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        parsed_leads = [row for row in reader]
    return parsed_leads

def find_duplicates(leads):
    lead_dict = {}
    for lead in leads:
        key = (lead.get('Email'), lead.get(CUSTOM_FIELD_NAME) if USE_CUSTOM_FIELD else None)
        if key in lead_dict:
            lead_dict[key].append(lead)
        else:
            lead_dict[key] = [lead]
    duplicates = {k: v for k, v in lead_dict.items() if len(v) > 1}
    logging.info(f'Found {len(duplicates)} sets of duplicates')
    return duplicates

def merge_leads(token, primary_lead_id, duplicate_lead_ids):
    global api_call_count
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    params = {
        'leadIds': ','.join(duplicate_lead_ids),
        'mergeInCrm': MERGE_IN_CRM
    }
    try:
        logging.info(f'Merging leads: {duplicate_lead_ids} into lead: {primary_lead_id}')
        response = requests.post(f"{BASE_URL}{MERGE_ENDPOINT.format(primary_lead_id)}", headers=headers, params=params)
        api_call_count += 1
        response.raise_for_status()
        merge_result = response.json()
        if merge_result.get('success'):
            logging.info(f'Successfully merged leads: {duplicate_lead_ids} into lead: {primary_lead_id}')
            return True
        else:
            logging.error(f'Merge failed: {merge_result}')
            return False
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to merge leads: {e}')
        return False

def main():
    start_time = time.time()
    total_merged_leads = 0
    merge_details = []

    try:
        token = get_access_token()
        if not token:
            logging.error('Failed to retrieve access token')
            return

        end_date = datetime.now(timezone.utc) + timedelta(days=14)
        start_date = end_date - timedelta(days=START_DAYS_BEFORE_END)
        all_leads = []

        while start_date < end_date:
            job_end_date = start_date + timedelta(days=JOB_END_DAYS)
            if job_end_date > end_date:
                job_end_date = end_date

            logging.info(f'Processing leads from {start_date} to {job_end_date}')

            job_id = create_bulk_export_job(token, start_date, job_end_date)
            if not job_id:
                logging.error('Failed to create bulk export job')
                return

            enqueue_bulk_export_job(token, job_id)

            backoff = 60
            while True:
                status = check_bulk_export_status(token, job_id)
                if status == 'Completed':
                    break
                elif status in ['Failed', 'Cancelled']:
                    logging.error(f'Bulk export job {job_id} did not complete successfully. Status: {status}')
                    return
                time.sleep(backoff)
                backoff = min(backoff * 2, 960)

            file_path = download_bulk_export_file(token, job_id)
            leads = parse_csv_leads(file_path)
            if leads:
                all_leads.extend(leads)

            start_date = job_end_date + timedelta(seconds=1)

        if not all_leads:
            logging.info('No leads found')
            return

        duplicates = find_duplicates(all_leads)
        for key, dups in duplicates.items():
            primary_lead = dups[0]
            primary_lead_id = primary_lead['id']
            duplicate_lead_ids = [lead['id'] for lead in dups[1:]]
            success = merge_leads(token, primary_lead_id, duplicate_lead_ids)
            if success:
                total_merged_leads += len(duplicate_lead_ids)
                merge_details.append(f'Lead ID {primary_lead_id} merged with {duplicate_lead_ids}')
            else:
                merge_details.append(f'Failed to merge lead ID {primary_lead_id} with {duplicate_lead_ids}')

        logging.info(f'Total merged leads: {total_merged_leads}')
        logging.info(f'Merge details: {merge_details}')

    except Exception as e:
        logging.error(f'An error occurred: {e}')
    finally:
        end_time = time.time()
        logging.info(f'Total execution time: {end_time - start_time} seconds')
        logging.info(f'Total API calls made: {api_call_count}')

if __name__ == "__main__":
    main()
