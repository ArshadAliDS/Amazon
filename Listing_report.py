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
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiException

# --- Configuration ---
REPORT_TYPES_MAP = {
    "All Active Listings (Flat File)": 'GET_FLAT_FILE_OPEN_LISTINGS_DATA',
    "All Listings (Flat File)": 'GET_MERCHANT_LISTINGS_ALL_DATA',
    "Inventory (Flat File)": 'GET_FLAT_FILE_ALL_INVENTORY_DATA',
    "Manage FBA Inventory": 'GET_AFN_INVENTORY_DATA',
    "FBA Fulfilled Inventory": 'GET_FBA_FULFILLMENT_CURRENT_INVENTORY_DATA',
    "FBA Daily Inventory History": 'GET_FBA_FULFILLMENT_INVENTORY_ARCHIVE_DATA',
    "FBA Customer Returns": 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA',
    "FBA Reimbursements": 'GET_FBA_REIMBURSEMENTS_DATA',
    "Reserved Inventory": 'GET_RESERVED_INVENTORY_REPORT',
    "Open Orders (Flat File)": 'GET_FLAT_FILE_ACTIONABLE_ORDER_DATA',
    "Pending Orders (Flat File)": 'GET_FLAT_FILE_PENDING_ORDERS_DATA',
    "Returns (Flat File)": 'GET_FLAT_FILE_RETURNS_REPORT_BY_RETURN_DATE',
    "Canceled Orders (Flat File)": 'GET_FLAT_FILE_ORDER_REPORT_DATA_SHIPPING',
    "Settlement Report (V2 Flat File)": 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2',
    "Seller Feedback": 'GET_SELLER_FEEDBACK_DATA',
    "Sales and Traffic (ASIN)": 'GET_SALES_AND_TRAFFIC_REPORT',
}

POLL_INTERVAL_SECONDS = 15
MAX_POLL_ATTEMPTS = 120

# --- Load Environment Variables ---
load_dotenv()
APP_PASSWORD = os.getenv("Password")

@st.cache_resource
def load_credentials():
    creds = {
        'lwa_app_id': os.getenv('SPAPI_CLIENT_ID'),
        'lwa_client_secret': os.getenv('SPAPI_CLIENT_SECRET'),
        'aws_access_key': os.getenv('AWS_ACCESS_KEY_ID'),
        'aws_secret_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'refresh_tokens': {
            'na': os.getenv('SPAPI_REFRESH_TOKEN_NA'),
            'eu': os.getenv('SPAPI_REFRESH_TOKEN_EU'),
            'fe': os.getenv('SPAPI_REFRESH_TOKEN_FE'),
        }
    }
    if not all([creds['lwa_app_id'], creds['lwa_client_secret'], creds['aws_access_key'], creds['aws_secret_key']]):
        st.error("Missing one or more core SP-API credentials in your .env file.")
        st.stop()
    if not any(creds['refresh_tokens'].values()):
        st.error("No refresh tokens found. Please add regional refresh tokens to your .env file.")
        st.stop()
    return creds

# --- Helper function ---
def get_marketplace_enum(marketplace_id_string: str):
    for marketplace in Marketplaces:
        if marketplace.marketplace_id == marketplace_id_string:
            return marketplace
    return None

# --- Main report generation function ---
@st.cache_data(show_spinner=False)
def get_amazon_report(marketplace_id_string: str, credentials: dict, report_type_api_name: str):
    marketplace_enum = get_marketplace_enum(marketplace_id_string)
    if not marketplace_enum:
        st.error(f"Unsupported Marketplace ID: {marketplace_id_string}.")
        return None

    endpoint_url = marketplace_enum.endpoint
    if "sellingpartnerapi-eu" in endpoint_url:
        region_code = 'eu'
    elif "sellingpartnerapi-fe" in endpoint_url:
        region_code = 'fe'
    else:
        region_code = 'na'

    refresh_token_for_region = credentials['refresh_tokens'].get(region_code)

    if not refresh_token_for_region:
        st.error(f"Refresh token for region '{region_code.upper()}' not found. Please set `SPAPI_REFRESH_TOKEN_{region_code.upper()}` in your .env file.")
        return None

    try:
        reports_client = Reports(
            marketplace=marketplace_enum,
            refresh_token=refresh_token_for_region,
            credentials=credentials
        )
        st.info(f"Requesting report: `{report_type_api_name}` for `{marketplace_enum.name}`. This may take a few minutes...")

        create_report_response = reports_client.create_report(reportType=report_type_api_name, marketplaceIds=[marketplace_id_string])
        report_id = create_report_response.payload.get('reportId')
        if not report_id:
            st.error(f"Error: Could not obtain reportId: {create_report_response.payload}")
            return None
        st.info(f"Report requested (ID: `{report_id}`). Polling for completion...")

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
                st.success(f"Report processing complete!")
                break
            elif processing_status in ['FATAL', 'CANCELLED']:
                st.error(f"Report processing failed with status: {processing_status}")
                st.json(get_report_response.payload)
                return None
        else: # This else belongs to the for loop, runs if loop finishes without break
             st.warning("Report did not complete in time.")
             return None

        get_report_doc_response = reports_client.get_report_document(reportDocumentId=report_document_id)
        download_url = get_report_doc_response.payload.get('url')
        compression_algorithm = get_report_doc_response.payload.get('compressionAlgorithm')
        if not download_url:
            st.error(f"Error: Could not get download URL: {get_report_doc_response.payload}")
            return None

        report_content_bytes = requests.get(download_url).content
        if compression_algorithm == 'GZIP':
            report_content_bytes = gzip.decompress(report_content_bytes)

        try:
            report_text = report_content_bytes.decode('utf-8')
        except UnicodeDecodeError:
            report_text = report_content_bytes.decode('latin-1')

        if not report_text.strip():
            return pd.DataFrame()
        df = pd.read_csv(StringIO(report_text), sep='\t', quoting=csv.QUOTE_NONE, on_bad_lines='warn')
        st.success(f"Successfully extracted {len(df)} entries from the report.")
        return df

    except SellingApiException as se:
        st.error(f"SP-API Error: {se.code} - {se.message}")
        if se.details: st.json(se.details)
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None

# --- Streamlit App Layout ---
st.set_page_config(layout="wide", page_title="Amazon SP-API Report Generator")

if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

if not APP_PASSWORD:
    st.error("FATAL: 'Password' is not set in the .env file. The application cannot start.")
    st.stop()

def login_form():
    st.title("🔐 Report Generator Login")
    with st.form("login_form"):
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")
        if submitted:
            if password == APP_PASSWORD:
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error("Incorrect password")

def main_app():
    st.title("📦 Amazon SP-API Dynamic Report Generator")
    st.markdown("Select a marketplace and a report type, then click **Generate Report**.")

    spapi_credentials = load_credentials()

    with st.container(border=True):
        col1, col2 = st.columns(2)
        marketplace_options = {f"{m.name} ({m.marketplace_id})": m.marketplace_id for m in Marketplaces}
        sorted_marketplace_options_keys = sorted(marketplace_options.keys())
        default_marketplace_index = sorted_marketplace_options_keys.index(f"US ({Marketplaces.US.marketplace_id})") if f"US ({Marketplaces.US.marketplace_id})" in sorted_marketplace_options_keys else 0

        with col1:
            selected_marketplace_display = st.selectbox(
                "Select Marketplace:",
                options=sorted_marketplace_options_keys,
                index=default_marketplace_index
            )
        with col2:
            selected_report_display_name = st.selectbox(
                "Select Report Type:",
                options=list(REPORT_TYPES_MAP.keys())
            )

    selected_marketplace_id = marketplace_options[selected_marketplace_display]
    selected_report_api_name = REPORT_TYPES_MAP[selected_report_display_name]

    if st.button("Generate Report", use_container_width=True, type="primary"):
        if spapi_credentials:
            with st.status(f"Generating '{selected_report_display_name}' for {selected_marketplace_display}...", expanded=True) as status:
                df_report = get_amazon_report(selected_marketplace_id, spapi_credentials, selected_report_api_name)
                st.session_state['current_report_df'] = df_report
                st.session_state['current_report_name'] = selected_report_display_name
                st.session_state['current_marketplace_display'] = selected_marketplace_display
                if df_report is not None:
                    status.update(label="Report Generation Complete!", state="complete", expanded=False)
                else:
                    status.update(label="Report Generation Failed", state="error", expanded=False)
        else:
            st.error("Cannot generate report: SP-API credentials are not loaded.")

    st.markdown("---")

    if 'current_report_df' in st.session_state and st.session_state['current_report_df'] is not None:
        report_display_name = st.session_state.get('current_report_name')
        marketplace_display = st.session_state.get('current_marketplace_display')
        st.header(f"Results: {report_display_name} for {marketplace_display}")
        if not st.session_state['current_report_df'].empty:
            st.dataframe(st.session_state['current_report_df'], use_container_width=True, height=500)
            csv_buffer = StringIO()
            st.session_state['current_report_df'].to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            safe_report_name = report_display_name.replace(" ", "_").replace("(", "").replace(")", "").lower()
            file_name = f"amazon_{marketplace_options[marketplace_display]}_{safe_report_name}_{time.strftime('%Y%m%d')}.csv"
            st.download_button(label="Download Report as CSV", data=csv_data, file_name=file_name, mime="text/csv")
        else:
            st.info(f"The '{report_display_name}' report for {marketplace_display} was generated but contained no data.")
    elif 'current_report_df' in st.session_state:
        st.info("Report generation was attempted but failed or was cancelled. Check messages above for details.")
    
    st.markdown("---")
    with st.expander("Show .env Configuration Example"):
        st.markdown("Please ensure your `.env` file is configured with your regional refresh tokens and a password.")
        st.code("""
# Example .env file

SPAPI_CLIENT_ID="amzn1.application-oa2-client.xxxxxxxx"
SPAPI_CLIENT_SECRET="xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"

# Add refresh tokens for each region you sell in
SPAPI_REFRESH_TOKEN_NA="Atzr|YOUR_NORTH_AMERICA_REFRESH_TOKEN"
SPAPI_REFRESH_TOKEN_EU="Atzr|YOUR_EUROPE_REFRESH_TOKEN"
# SPAPI_REFRESH_TOKEN_FE="Atzr|YOUR_FAR_EAST_REFRESH_TOKEN" # Optional

# Add a password to protect the app
Password="your_secure_password"
""")

# --- Main app flow ---
if not st.session_state['authenticated']:
    login_form()
else:
    main_app()
