import os
from dotenv import load_dotenv
import time
import requests
import csv
from io import StringIO

from sp_api.api import Reports
from sp_api.base import Marketplaces, Client

# --- Load Environment Variables ---
load_dotenv()

# --- SP-API Credentials ---
credentials = {
    'lwa_app_id': os.getenv('SPAPI_CLIENT_ID'),
    'lwa_client_secret': os.getenv('SPAPI_CLIENT_SECRET'),
    'aws_access_key': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    'lwa_refresh_token': os.getenv('SPAPI_REFRESH_TOKEN')
}

MARKETPLACE_ID = os.getenv('MARKETPLACE_ID') # e.g., ATVPDKIKX0DER for US

# --- Configuration ---
REPORT_TYPE = 'GET_FLAT_FILE_OPEN_LISTINGS_DATA'
POLL_INTERVAL_SECONDS = 30
MAX_POLL_ATTEMPTS = 60

def get_listing_report(marketplace_id_string: str, report_type: str = REPORT_TYPE):
    """
    Requests, monitors, and downloads a specific listing report from Amazon SP-API.
    Dynamically selects the Marketplace enum based on the provided marketplace ID string.
    """
    try:
        # Dynamically find the Marketplace enum member
        marketplace_enum = None
        for marketplace in Marketplaces:
            if marketplace.marketplace_id == marketplace_id_string:
                marketplace_enum = marketplace
                break

        if not marketplace_enum:
            print(f"Error: Could not find Marketplace enum for ID: {marketplace_id_string}")
            print("Please ensure the MARKETPLACE_ID in your .env is correct and supported by the SDK.")
            return None

        # Initialize the Reports API client
        reports_client = Reports(
            marketplace=marketplace_enum, # Use the dynamically determined enum
            refresh_token=credentials['lwa_refresh_token'],
            credentials=credentials
        )

        print(f"Requesting report type: {report_type} for marketplace: {marketplace_id_string} ({marketplace_enum.name})...")

        # 1. Request the report
        create_report_response = reports_client.create_report(
            reportType=report_type,
            marketplaceIds=[marketplace_id_string]
            # dataStartTime='YYYY-MM-DDTHH:MM:SSZ', # Optional: for historical data
            # dataEndTime='YYYY-MM:SSZ',   # Optional
        )
        report_id = create_report_response.payload.get('reportId')

        if not report_id:
            print(f"Error: Could not obtain reportId from create_report response: {create_report_response.payload}")
            return None

        print(f"Report requested. reportId: {report_id}. Polling for completion...")

        # 2. Monitor Report Processing
        report_document_id = None
        for attempt in range(MAX_POLL_ATTEMPTS):
            time.sleep(POLL_INTERVAL_SECONDS)
            print(f"Polling report status (Attempt {attempt + 1}/{MAX_POLL_ATTEMPTS})...")

            get_report_response = reports_client.get_report(reportId=report_id)
            processing_status = get_report_response.payload.get('processingStatus')

            if processing_status == 'DONE':
                report_document_id = get_report_response.payload.get('reportDocumentId')
                print(f"Report processing complete! reportDocumentId: {report_document_id}")
                break
            elif processing_status in ['FATAL', 'CANCELLED']:
                print(f"Report processing failed with status: {processing_status}")
                print(f"Report details: {get_report_response.payload}")
                return None
            else:
                print(f"Report still {processing_status}...")

        if not report_document_id:
            print("Report did not complete in time or failed.")
            return None

        # 3. Retrieve Report Document
        print(f"Retrieving report document for {report_document_id}...")
        get_report_doc_response = reports_client.get_report_document(
            reportDocumentId=report_document_id
        )

        download_url = get_report_doc_response.payload.get('url')
        compression_algorithm = get_report_doc_response.payload.get('compressionAlgorithm')

        if not download_url:
            print(f"Error: Could not get download URL for report document: {get_report_doc_response.payload}")
            return None

        print(f"Downloading report from: {download_url}")

        # 4. Download the Report
        report_content = requests.get(download_url).content

        # Handle decompression if necessary
        if compression_algorithm == 'GZIP':
            import gzip
            report_content = gzip.decompress(report_content)
        elif compression_algorithm == 'ZLIB':
            import zlib
            report_content = zlib.decompress(report_content)
        # Add other compression algorithms if needed

        report_text = report_content.decode('utf-8')

        # 5. Process the Report (Example: print first few lines and parse as CSV)
        print("\n--- Report Content Preview (First 10 lines) ---")
        lines = report_text.splitlines()
        for i, line in enumerate(lines[:10]):
            print(line)
        if len(lines) > 10:
            print(f"... (total {len(lines)} lines)")

        print("\n--- Parsing Report Data (Example: CSV reader) ---")
        reader = csv.reader(StringIO(report_text), delimiter='\t')
        header = next(reader)
        print(f"Report Header: {header}")

        parsed_data = []
        for row in reader:
            parsed_data.append(row)

        print(f"\nSuccessfully parsed {len(parsed_data)} data rows.")
        return parsed_data

    except Exception as e:
        print(f"An error occurred during report extraction: {e}")
        return None

if __name__ == "__main__":
    if not all([credentials.get('lwa_app_id'), credentials.get('lwa_client_secret'),
                credentials.get('lwa_refresh_token'), credentials.get('aws_access_key'),
                credentials.get('aws_secret_key'), MARKETPLACE_ID]):
        print("Please ensure all SP-API credentials (LWA, AWS IAM) and MARKETPLACE_ID are set in your .env file.")
    else:
        print(f"Starting report extraction for Marketplace ID: {MARKETPLACE_ID}")
        all_listings_data = get_listing_report(MARKETPLACE_ID, REPORT_TYPE)

        if all_listings_data:
            print(f"Extracted {len(all_listings_data)} listing entries.")
        else:
            print("Failed to extract listing report.")
