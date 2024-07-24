import jwt
import time
import requests
import gzip
import csv
import io
from datetime import datetime, timedelta
import pandas as pd
import gspread
from os import path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Your credentials
ISSUER_ID = '13bae064-5789-4eee-8111-7c8fa59554de'
KEY_ID = '64DH23TSCC'
KEY_FILE_PATH = 'AuthKey_64DH23TSCC.p8'
VENDOR_NUMBER = '92333400'

# Connect to Google Sheets
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
credentials = service_account.Credentials.from_service_account_file(
    'augmented-web-429814-m4-ed2308c1ad5d.json', scopes=scope
)
gc = gspread.authorize(credentials)
sheet_name = 'App topline'
worksheet_name = 'apple'

# Connect to the Google Sheet
worksheet = gc.open(sheet_name).worksheet(worksheet_name)

# Generate JWT token
def generate_token():
    with open(KEY_FILE_PATH, 'r') as key_file:
        key = key_file.read()

    expiration_time = int(time.time()) + 1200  # 20 minutes from now

    payload = {
        'iss': ISSUER_ID,
        'exp': expiration_time,
        'aud': 'appstoreconnect-v1'
    }

    token = jwt.encode(payload, key, algorithm='ES256', headers={'kid': KEY_ID})
    return token

# Make API request for INSTALLS report
def get_installs_report():
    token = generate_token()

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/a-gzip'
    }

    # Set the date for the previous year (INSTALLS reports are yearly)
    current_year = datetime.now().year
    report_date = f"{current_year-1}"

    params = {
        'filter[frequency]': 'YEARLY',
        'filter[reportDate]': report_date,
        'filter[reportSubType]': 'SUMMARY_INSTALL_TYPE',  # or SUMMARY_TERRITORY or SUMMARY_CHANNEL
        'filter[reportType]': 'INSTALLS',
        'filter[vendorNumber]': VENDOR_NUMBER,
        'filter[version]': '1_1'  # Use the latest version available
    }

    url = 'https://api.appstoreconnect.apple.com/v1/salesReports'

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        # Save the gzip file
        with open('installs_report.gz', 'wb') as f:
            f.write(response.content)
        return "Report downloaded successfully as 'installs_report.gz'"
    else:
        return f"Error: {response.status_code}, {response.text}"

def process_installs_report():
    with gzip.open('installs_report.gz', 'rt') as f:
        content = f.read()

    print("Report content:")
    print(content)

    # Parse the content manually
    lines = content.split('\n')
    data = {}
    for line in lines:
        if ':' in line:
            key, value = line.split(':', 1)
            data[key.strip()] = value.strip()

    # Create a DataFrame from the parsed data
    df = pd.DataFrame([data])

    print("\nDataFrame info:")
    print(df.info())

    print("\nDataFrame content:")
    print(df)

    return df

def upload_to_google_sheet(df):
    # Convert DataFrame to list of lists
    data = [df.columns.tolist()] + df.values.tolist()

    # Clear existing data in the sheet
    worksheet.clear()

    # Update the sheet with new data
    worksheet.update('A1', data)

    print(f"Data uploaded successfully to {sheet_name}, worksheet: {worksheet_name}")

# Main execution
if __name__ == "__main__":
    result = get_installs_report()
    print(result)

    if "Report downloaded successfully" in result:
        df = process_installs_report()

        # No need to calculate total installs as this report seems to provide annual data

        # Upload data to Google Sheet
        upload_to_google_sheet(df)
    else:
        print("Failed to download the report. Please check your credentials and try again.")