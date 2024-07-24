import json
import gspread
from os import path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import csv
from datetime import datetime, timedelta
import pandas as pd
import jwt
import time
import requests
import gzip

# Android data processing
def process_android_data():
    json_file = 'augmented-web-429814-m4-225446bc272f.json'
    cloud_storage_bucket = 'pubsite_prod_7437328891382826399'
    report_directory = "stats/installs/"

    credentials = service_account.Credentials.from_service_account_file(
        json_file,
        scopes=['https://www.googleapis.com/auth/devstorage.read_only']
    )

    storage = build('storage', 'v1', credentials=credentials)

    request = storage.objects().list(bucket=cloud_storage_bucket, prefix=report_directory)
    response = request.execute()

    latest_file = None
    latest_date = datetime.min

    for item in response.get('items', []):
        if item['name'].endswith('.csv'):
            file_date = datetime.strptime(item['updated'], "%Y-%m-%dT%H:%M:%S.%fZ")
            if file_date > latest_date:
                latest_date = file_date
                latest_file = item['name']

    if not latest_file:
        print("No CSV files found in the specified directory.")
        return None

    print(f"Latest CSV file: {latest_file}")

    request = storage.objects().get_media(
        bucket=cloud_storage_bucket,
        object=latest_file
    )
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    file.seek(0)
    content = file.read().decode('utf-16le')
    reader = csv.reader(content.splitlines(), delimiter=',')
    return list(reader)

# iOS data processing
def process_ios_data():
    ISSUER_ID = '13bae064-5789-4eee-8111-7c8fa59554de'
    KEY_ID = '64DH23TSCC'
    KEY_FILE_PATH = 'AuthKey_64DH23TSCC.p8'
    VENDOR_NUMBER = '92333400'

    def generate_token():
        with open(KEY_FILE_PATH, 'r') as key_file:
            key = key_file.read()

        expiration_time = int(time.time()) + 1200

        payload = {
            'iss': ISSUER_ID,
            'exp': expiration_time,
            'aud': 'appstoreconnect-v1'
        }

        return jwt.encode(payload, key, algorithm='ES256', headers={'kid': KEY_ID})

    token = generate_token()

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/a-gzip'
    }

    report_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    params = {
        'filter[frequency]': 'DAILY',
        'filter[reportDate]': report_date,
        'filter[reportSubType]': 'SUMMARY',
        'filter[reportType]': 'SALES',
        'filter[vendorNumber]': VENDOR_NUMBER,
        'filter[version]': '1_0'
    }

    url = 'https://api.appstoreconnect.apple.com/v1/salesReports'

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        with open('sales_report.gz', 'wb') as f:
            f.write(response.content)

        with gzip.open('sales_report.gz', 'rt') as f:
            csv_content = f.read()

        lines = csv_content.split('\n')
        headers = lines[0].split('\t')
        data = [line.split('\t') for line in lines[1:] if line]

        df = pd.DataFrame(data, columns=headers)

        date_columns = ['Begin Date', 'End Date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        numeric_columns = ['Units', 'Developer Proceeds']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

# Google Sheets connection
def connect_to_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = service_account.Credentials.from_service_account_file(
        'augmented-web-429814-m4-ed2308c1ad5d.json', scopes=scope
    )
    return gspread.authorize(credentials)

# Main execution
if __name__ == "__main__":
    gc = connect_to_sheets()
    sheet_name = 'App topline'

    # Process Android data
    android_data = process_android_data()
    if android_data:
        android_worksheet = gc.open(sheet_name).worksheet('android')
        android_worksheet.clear()
        android_worksheet.update('A1', android_data)
        print("Android data written to Google Sheet successfully!")

    # Process iOS data
    ios_df = process_ios_data()
    if ios_df is not None:
        ios_worksheet = gc.open(sheet_name).worksheet('apple')
        ios_worksheet.clear()

        ios_df['Begin Date'] = ios_df['Begin Date'].dt.strftime('%Y-%m-%d')
        ios_df['End Date'] = ios_df['End Date'].dt.strftime('%Y-%m-%d')
        ios_data = [ios_df.columns.tolist()] + ios_df.values.tolist()
        ios_worksheet.update('A1', ios_data)
        print("iOS data written to Google Sheet successfully!")

        total_units = ios_df['Units'].sum() if 'Units' in ios_df.columns else "Units column not found"
        total_proceeds = ios_df['Developer Proceeds'].sum() if 'Developer Proceeds' in ios_df.columns else "Developer Proceeds column not found"
        print(f"\nTotal iOS Units: {total_units}")
        print(f"Total iOS Developer Proceeds: {total_proceeds}")