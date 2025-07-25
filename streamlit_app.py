import streamlit as st
import os
from dotenv import load_dotenv
import time
import requests
import csv
from io import StringIO, BytesIO
import pandas as pd
import gzip # For GZIP decompression
import zlib # For ZLIB decompression

from sp_api.api import Reports
from sp_api.base import Marketplaces, Client
from sp_api.base.exceptions import SellingApiException # Import for specific error handling

# --- Configuration ---
# Mapping of user-friendly report names to SP-API report types
REPORT_TYPES_MAP = {
    "All Active Listings (Flat File)": 'GET_FLAT_FILE_OPEN_LISTINGS_DATA',
    "All Listings (Flat File)": 'GET_MERCHANT_LISTINGS_ALL_DATA',
    "Inventory (Flat File)": 'GET_FLAT_FILE_ALL_INVENTORY_DATA',
    "Sales and Traffic (ASIN)": 'GET_SALES_AND_TRAFFIC_REPORT', # Example, might require specific parameters or a different API
    "Settlement Report (V2 Flat File)": 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2',
    "Seller Feedback": 'GET_SELLER_FEEDBACK_DATA',
    "FBA Fulfilled Inventory": 'GET_FBA_FULFILLMENT_CURRENT_INVENTORY_DATA',
    "FBA Daily Inventory History": 'GET_FBA_FULFILLMENT_INVENTORY_ARCHIVE_DATA',
    "FBA Customer Returns": 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA',
    "FBA Reimbursements": 'GET_FBA_REIMBURSEMENTS_DATA',
    "Reserved Inventory": 'GET_RESERVED_INVENTORY_REPORT',
    "Manage FBA Inventory": 'GET_AFN_INVENTORY_DATA',
    "Open Orders (Flat File)": 'GET_FLAT_FILE_ACTIONABLE_ORDER_DATA',
    "Canceled Orders (Flat File)": 'GET_FLAT_FILE_ORDER_REPORT_DATA_SHIPPING', # This is a general order report, specific canceled might need filtering
    "Pending Orders (Flat File)": 'GET_FLAT_FILE_PENDING_ORDERS_DATA',
    "Returns (Flat File)": 'GET_FLAT_FILE_RETURNS_REPORT_BY_RETURN_DATE',
    # Add more report types as needed from Amazon SP-API documentation
}

POLL_INTERVAL_SECONDS = 10 # How often to check report status (can be increased for production)
MAX_POLL_ATTEMPTS = 120 # Max attempts (e.g., 20 minutes total wait for 10-sec interval)

# --- Load Environment Variables (Credentials) ---
# This function loads credentials once and caches them for efficiency.
@st.cache_resource
def load_credentials():
    load_dotenv()
    creds = {
        'lwa_app_id': os.getenv('SPAPI_CLIENT_ID'),
        'lwa_client_secret': os.getenv('SPAPI_CLIENT_SECRET'),
        'aws_access_key': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'lwa_refresh_token': os.getenv('SPAPI_REFRESH_TOKEN')
    }
    # Basic validation for essential credentials
    if not all([creds['lwa_app_id'], creds['lwa_client_secret'], creds['lwa_refresh_token'],
                 creds['aws_access_key'], creds['aws_secret_key']]):
        st.error("Missing one or more SP-API credentials in your .env file. Please check SPAPI_CLIENT_ID, SPAPI_CLIENT_SECRET, SPAPI_REFRESH_TOKEN, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.")
        st.stop() # Stop execution if credentials are not found
    return creds

# --- Helper function to map marketplace ID string to Marketplaces enum ---
def get_marketplace_enum(marketplace_id_string: str):
    """Converts a marketplace ID string to its corresponding Marketplaces enum."""
    for marketplace in Marketplaces:
        if marketplace.marketplace_id == marketplace_id_string:
            return marketplace
    return None

# --- Main function to get the Amazon report ---
@st.cache_data(show_spinner=False) # Cache data, but control spinner manually
def get_amazon_report(marketplace_id_string: str, credentials: dict, report_type_api_name: str):
    """
    Requests, monitors, and downloads a specific report from Amazon SP-API.
    Returns a pandas DataFrame or None if an error occurs.
    """
    marketplace_enum = get_marketplace_enum(marketplace_id_string)
    if not marketplace_enum:
        st.error(f"Unsupported Marketplace ID: {marketplace_id_string}. Please ensure it's a valid Amazon marketplace ID supported by the SDK.")
        return None

    try:
        # Initialize the Reports API client
        reports_client = Reports(
            marketplace=marketplace_enum,
            refresh_token=credentials['lwa_refresh_token'],
            credentials=credentials
        )

        st.info(f"Requesting report type: `{report_type_api_name}` for marketplace: `{marketplace_id_string}` (`{marketplace_enum.name}`). This may take a few minutes...")

        # 1. Request the report
        with st.spinner("Initiating report request..."):
            create_report_response = reports_client.create_report(
                reportType=report_type_api_name,
                marketplaceIds=[marketplace_id_string]
            )
            report_id = create_report_response.payload.get('reportId')

        if not report_id:
            st.error(f"Error: Could not obtain reportId from create_report response: {create_report_response.payload}")
            return None

        st.info(f"Report requested. Report ID: `{report_id}`. Polling for completion...")

        # 2. Monitor Report Processing
        report_document_id = None
        progress_bar = st.progress(0)
        status_text = st.empty()

        for attempt in range(MAX_POLL_ATTEMPTS):
            time.sleep(POLL_INTERVAL_SECONDS)
            progress = (attempt + 1) / MAX_POLL_ATTEMPTS
            progress_bar.progress(progress)

            get_report_response = reports_client.get_report(reportId=report_id)
            processing_status = get_report_response.payload.get('processingStatus')
            status_text.text(f"Report status: {processing_status} (Attempt {attempt + 1}/{MAX_POLL_ATTEMPTS})")

            if processing_status == 'DONE':
                report_document_id = get_report_response.payload.get('reportDocumentId')
                st.success(f"Report processing complete! Report Document ID: `{report_document_id}`")
                break
            elif processing_status in ['FATAL', 'CANCELLED']:
                st.error(f"Report processing failed with status: {processing_status}")
                st.json(get_report_response.payload) # Show full payload for debugging
                return None
            else:
                pass # Continue polling

        if not report_document_id:
            st.warning("Report did not complete in time or failed to retrieve document ID.")
            return None

        # 3. Retrieve Report Document
        with st.spinner(f"Retrieving report document for `{report_document_id}`..."):
            get_report_doc_response = reports_client.get_report_document(
                reportDocumentId=report_document_id
            )

        download_url = get_report_doc_response.payload.get('url')
        compression_algorithm = get_report_doc_response.payload.get('compressionAlgorithm')

        if not download_url:
            st.error(f"Error: Could not get download URL for report document: {get_report_doc_response.payload}")
            return None

        st.info(f"Downloading report from Amazon...")

        # 4. Download the Report
        with st.spinner("Downloading report content..."):
            report_content_bytes = requests.get(download_url).content

        # Handle decompression
        if compression_algorithm == 'GZIP':
            st.info("Decompressing GZIP content...")
            report_content_bytes = gzip.decompress(report_content_bytes)
        elif compression_algorithm == 'ZLIB':
            st.info("Decompressing ZLIB content...")
            report_content_bytes = zlib.decompress(report_content_bytes)
        # Add other compression algorithms if needed, or default to raw bytes

        # Attempt to decode, handling potential errors
        try:
            report_text = report_content_bytes.decode('utf-8')
        except UnicodeDecodeError:
            st.warning("UTF-8 decoding failed, trying 'latin-1' (common for some Amazon reports).")
            report_text = report_content_bytes.decode('latin-1') # Fallback for other encodings

        # 5. Process the Report into a DataFrame
        if not report_text.strip():
            st.warning("Downloaded report is empty.")
            return pd.DataFrame() # Return empty DataFrame

        # Use StringIO to treat the string content as a file for pandas
        # Most Amazon flat files are tab-delimited (TSV)
        df = pd.read_csv(StringIO(report_text), sep='\t', quoting=csv.QUOTE_NONE, encoding='utf-8', on_bad_lines='warn')

        st.success(f"Successfully extracted {len(df)} entries for report type: `{report_type_api_name}`.")
        return df

    except SellingApiException as se:
        st.error(f"SP-API Error: {se.code} - {se.message}")
        if se.details:
            st.json(se.details)
        return None
    except requests.exceptions.RequestException as re:
        st.error(f"Network or Request Error: {re}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

# --- Streamlit App Layout ---
st.set_page_config(layout="wide", page_title="Amazon SP-API Report Generator")

st.title("📦 Amazon SP-API Dynamic Report Generator")
st.markdown("Select a marketplace and a report type to generate and view the data.")

# Load credentials at the start (cached)
spapi_credentials = load_credentials()

# Get available marketplaces from the SDK's Marketplaces enum
# Create a dictionary for display: {"US (ATVPDKIKX0DER)": "ATVPDKIKX0DER", ...}
marketplace_options = {f"{m.name} ({m.marketplace_id})": m.marketplace_id for m in Marketplaces}
sorted_marketplace_options_keys = sorted(marketplace_options.keys())

# User selects marketplace
selected_marketplace_display = st.selectbox(
    "Select Marketplace:",
    options=sorted_marketplace_options_keys,
    index=sorted_marketplace_options_keys.index(f"US ({Marketplaces.US.marketplace_id})") if f"US ({Marketplaces.US.marketplace_id})" in sorted_marketplace_options_keys else 0,
    help="Choose the Amazon marketplace for which you want to retrieve the report."
)
selected_marketplace_id = marketplace_options[selected_marketplace_display]

st.write(f"Selected Marketplace ID: `{selected_marketplace_id}`")

# User selects report type
selected_report_display_name = st.selectbox(
    "Select Report Type:",
    options=list(REPORT_TYPES_MAP.keys()),
    index=list(REPORT_TYPES_MAP.keys()).index("All Active Listings (Flat File)"), # Default selection
    help="Choose the type of report you want to generate."
)
selected_report_api_name = REPORT_TYPES_MAP[selected_report_display_name]

st.write(f"Selected SP-API Report Type: `{selected_report_api_name}`")


# Button to trigger report generation
if st.button("Generate Report", help=f"Click to request and download the '{selected_report_display_name}' report."):
    if spapi_credentials: # Only proceed if credentials were loaded successfully
        with st.status(f"Generating '{selected_report_display_name}' report...", expanded=True) as status:
            # Pass the selected API report name to the function
            df_report = get_amazon_report(selected_marketplace_id, spapi_credentials, selected_report_api_name)
            if df_report is not None:
                st.session_state['current_report_df'] = df_report
                st.session_state['current_report_name'] = selected_report_display_name
                st.session_state['current_report_api_name'] = selected_report_api_name
                status.update(label="Report Generation Complete!", state="complete", expanded=False)
            else:
                st.session_state['current_report_df'] = None
                st.session_state['current_report_name'] = None
                st.session_state['current_report_api_name'] = None
                status.update(label="Report Generation Failed", state="error", expanded=False)
    else:
        st.error("Cannot generate report: SP-API credentials are not loaded.")

# Display report data if available in session state
if 'current_report_df' in st.session_state and st.session_state['current_report_df'] is not None:
    report_display_name = st.session_state.get('current_report_name', "Generated Report")
    report_api_name = st.session_state.get('current_report_api_name', "UNKNOWN_REPORT_TYPE")

    st.subheader(f"{report_display_name} for {selected_marketplace_display}")

    if not st.session_state['current_report_df'].empty:
        # Display the DataFrame in a scrollable table
        st.dataframe(st.session_state['current_report_df'], use_container_width=True, height=500) # Height for scrollability

        # Provide download button
        csv_buffer = BytesIO()
        # Use a more robust encoding for CSV download if UTF-8 causes issues with specific reports
        st.session_state['current_report_df'].to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_buffer.seek(0) # Rewind the buffer to the beginning

        # Create a clean file name from the report display name
        safe_report_name = report_display_name.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").lower()
        file_name = f"amazon_{safe_report_name}_{selected_marketplace_id}_{time.strftime('%Y%m%d-%H%M%S')}.csv"

        st.download_button(
            label="Download Report as CSV",
            data=csv_buffer,
            file_name=file_name,
            mime="text/csv",
            help="Download the displayed report data as a CSV file."
        )
    else:
        st.info(f"No data found for the selected '{report_display_name}' report for {selected_marketplace_display}.")
elif 'current_report_df' in st.session_state and st.session_state['current_report_df'] is None:
    st.info("Report generation was attempted but failed or returned no data. Check error messages above.")

st.markdown("---")
st.markdown("### How to Run This Application:")
st.markdown("1.  **Save the code:** Save the code above as `app.py` (or any `.py` file).")
st.markdown("2.  **Create `.env`:** In the same directory, create a file named `.env` and populate it with your actual SP-API credentials (Client ID, Client Secret, Refresh Token, AWS Access Key ID, AWS Secret Access Key).")
st.code("""
SPAPI_CLIENT_ID="amzn1.developer.LWA-XXXX"
SPAPI_CLIENT_SECRET="amzn1.developer.LWA-secret-XXXX"
SPAPI_REFRESH_TOKEN="Atzr|RQFN-XXXX"
SELLER_ID="A123ABCDEFGH" # Your Amazon Seller ID (not directly used in this report, but good to have)
MARKETPLACE_ID="ATVPDKIKX0DER" # Example: US marketplace ID (used as default selection)
AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID" # Required for SigV4 signing
AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY" # Required for SigV4 signing
""")
st.markdown("3.  **Install libraries:** Open your terminal/command prompt, navigate to the directory, activate your virtual environment (if using), and run:")
st.code("pip install streamlit python-dotenv requests pandas python-amazon-sp-api")
st.markdown("4.  **Run the Streamlit app:**")
st.code("streamlit run app.py")
st.markdown("5.  **Access in browser:** Streamlit will open a new tab in your web browser with the application.")
