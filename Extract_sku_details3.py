import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta
import time # Import time module for delays

# Load environment variables from .env file
# Ensure your .env file is in the same directory as this script.
load_dotenv()

# --- SP-API Configuration from .env ---
SP_API_CLIENT_ID = os.getenv("SPAPI_CLIENT_ID")
SP_API_CLIENT_SECRET = os.getenv("SPAPI_CLIENT_SECRET")
APP_PASSWORD = os.getenv("Password") # Get the password from .env

# Load specific regional refresh tokens
SP_API_REFRESH_TOKEN_NA = os.getenv("SPAPI_REFRESH_TOKEN_NA")
SP_API_REFRESH_TOKEN_EU = os.getenv("SPAPI_REFRESH_TOKEN_EU")
SP_API_REFRESH_TOKEN_FE = os.getenv("SPAPI_REFRESH_TOKEN_FE")

# Load specific regional Seller IDs
SP_API_SELLER_ID_NA = os.getenv("SELLER_ID_NA")
SP_API_SELLER_ID_EU = os.getenv("SELLER_ID_EU")
SP_API_SELLER_ID_FE = os.getenv("SELLER_ID_FE")

# These are required for SigV4 signing, although this script primarily uses LWA tokens
SP_API_AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SP_API_AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


# Base URLs for SP-API
SP_API_BASE_URLS = {
    'na': 'https://sellingpartnerapi-na.amazon.com', # North America (US, CA, MX)
    'eu': 'https://sellingpartnerapi-eu.amazon.com', # Europe (UK, DE, ES, FR, IT, TR, AE, SA)
    'fe': 'https://sellingpartnerapi-fe.amazon.com'  # Far East (JP, AU, IN, SG, CN, BR)
}
LWA_TOKEN_URL = 'https://api.amazon.com/auth/o2/token'

# Global variable to store access token and its expiry per REGION GROUP
# Will store {region_group: {"token": ..., "expires_at": ...}}
access_token_info = {}

# Mapping of country names to Amazon Marketplace IDs, their region group, and Seller ID env var names
MARKETPLACE_DETAILS_MAP = {
    "US (United States)": {"id": "ATVPDKIKX0DER", "region_group": "na", "seller_id_env": "SELLER_ID_NA"},
    "CA (Canada)": {"id": "A2EUQ1J6SYM78R", "region_group": "na", "seller_id_env": "SELLER_ID_NA"},
    "MX (Mexico)": {"id": "A1AM78C6CP7M78", "region_group": "na", "seller_id_env": "SELLER_ID_NA"},
    "UK (United Kingdom)": {"id": "A1F83G8C2ARO7P", "region_group": "eu", "seller_id_env": "SELLER_ID_EU"},
    "DE (Germany)": {"id": "A1PA6795UKMFR9", "region_group": "eu", "seller_id_env": "SELLER_ID_EU"},
    "ES (Spain)": {"id": "A1RKKUPIHCS9HS", "region_group": "eu", "seller_id_env": "SELLER_ID_EU"},
    "FR (France)": {"id": "A13V1IB3VIYZZH", "region_group": "eu", "seller_id_env": "SELLER_ID_EU"},
    "IT (Italy)": {"id": "APJ6JRA9NG5V4", "region_group": "eu", "seller_id_env": "SELLER_ID_EU"},
    "IN (India)": {"id": "A21TJRUUN4KGV", "region_group": "fe", "seller_id_env": "SELLER_ID_FE"},
    "AU (Australia)": {"id": "A39IBJ37V3C1DG", "region_group": "fe", "seller_id_env": "SELLER_ID_FE"},
    "JP (Japan)": {"id": "A1VC38T7YXB528", "region_group": "fe", "seller_id_env": "SELLER_ID_FE"},
    "AE (United Arab Emirates)": {"id": "A2VIGQ35RCS4UG", "region_group": "eu", "seller_id_env": "SELLER_ID_EU"},
    "BR (Brazil)": {"id": "A2Q3Y263D00KWC", "region_group": "na", "seller_id_env": "SELLER_ID_NA"}, # Brazil uses the NA endpoint
    "CN (China)": {"id": "AAHKV2X7AFYLW", "region_group": "fe", "seller_id_env": "SELLER_ID_FE"},
    "TR (Turkey)": {"id": "A33AVAJ2PDY3G", "region_group": "eu", "seller_id_env": "SELLER_ID_EU"},
    "SA (Saudi Arabia)": {"id": "A17E79C6D8DWX5", "region_group": "eu", "seller_id_env": "SELLER_ID_EU"},
    "SG (Singapore)": {"id": "A19VAU5U5O7RUS", "region_group": "fe", "seller_id_env": "SELLER_ID_FE"}
}


def get_access_token(selected_region_group):
    """
    Retrieves or refreshes the LWA (Login With Amazon) access token for a specific region group.
    Caches the token until it expires to minimize API calls.
    """
    global access_token_info

    if selected_region_group not in access_token_info:
        access_token_info[selected_region_group] = {"token": None, "expires_at": None}

    if access_token_info[selected_region_group]["token"] and \
       access_token_info[selected_region_group]["expires_at"] and \
       datetime.now() < access_token_info[selected_region_group]["expires_at"]:
        return access_token_info[selected_region_group]["token"]

    st.info(f"Attempting to refresh Amazon SP-API access token for region group: {selected_region_group}...")

    specific_refresh_token = os.getenv(f"SPAPI_REFRESH_TOKEN_{selected_region_group.upper()}")

    if not all([SP_API_CLIENT_ID, SP_API_CLIENT_SECRET, specific_refresh_token]):
        st.error(f"Missing one or more SP-API LWA credentials for region group '{selected_region_group}' in your .env file. Please check SPAPI_CLIENT_ID, SPAPI_CLIENT_SECRET, and SPAPI_REFRESH_TOKEN_{selected_region_group.upper()}.")
        return None

    try:
        response = requests.post(
            LWA_TOKEN_URL,
            data={
                'grant_type': 'refresh_token',
                'refresh_token': specific_refresh_token,
                'client_id': SP_API_CLIENT_ID,
                'client_secret': SP_API_CLIENT_SECRET
            }
        )
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data['access_token']
        expires_in = token_data['expires_in']

        access_token_info[selected_region_group]["token"] = access_token
        access_token_info[selected_region_group]["expires_at"] = datetime.now() + timedelta(seconds=expires_in - 60)

        st.success(f"Amazon SP-API access token for region group {selected_region_group} refreshed successfully!")
        return access_token
    except requests.exceptions.RequestException as e:
        st.error(f"Error refreshing access token for region group {selected_region_group}: {e}")
        st.error(f"Response status: {e.response.status_code if e.response else 'N/A'}, Response body: {e.response.text if e.response else 'N/A'}")
        st.error(f"Please verify your SPAPI_CLIENT_ID, SPAPI_CLIENT_SECRET, and SPAPI_REFRESH_TOKEN_{selected_region_group.upper()} in your .env file.")
        return None

def get_sp_api_headers(access_token):
    """
    Constructs the necessary HTTP headers for SP-API requests.
    """
    return {
        'x-amz-access-token': access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

def get_product_details(sku, marketplace_id, seller_id, selected_country_name):
    """
    Retrieves product details for a given SKU using the SP-API Listings Items API.
    """
    region_group = MARKETPLACE_DETAILS_MAP.get(selected_country_name, {}).get("region_group")
    if not region_group:
        st.error(f"Invalid country selected: {selected_country_name}. Cannot determine region group.")
        return {"status": "error", "message": "Invalid country selected."}

    access_token = get_access_token(region_group)
    if not access_token:
        st.error("Cannot proceed: Failed to obtain SP-API access token.")
        return {"status": "error", "message": "Failed to obtain SP-API access token."}

    base_url = SP_API_BASE_URLS.get(region_group)
    if not base_url:
        st.error(f"Could not determine base URL for marketplace '{selected_country_name}'.")
        return {"status": "error", "message": f"Could not determine base URL for marketplace '{selected_country_name}'."}

    url = f"{base_url}/listings/2021-08-01/items/{seller_id}/{sku}"
    params = {
        "marketplaceIds": marketplace_id,
        "includedData": "summaries,attributes",
        "issueLocale": "en_US"
    }

    try:
        st.info(f"Attempting to retrieve details for SKU: {sku} from {selected_country_name}...")
        response = requests.get(url, headers=get_sp_api_headers(access_token), params=params)
        response.raise_for_status()
        response_data = response.json()

        if response_data.get('issues'):
            issue_messages = [f"Code: {issue.get('code', 'N/A')}, Message: {issue.get('message', 'N/A')}" for issue in response_data['issues']]
            st.warning(f"Retrieved details with warnings: {'; '.join(issue_messages)}")

        product_details = {
            "Product Name": "N/A", "Product Type": "N/A", "Keywords": "N/A",
            "Platinum Keywords": "N/A", "Description": "N/A", "Bullet Points": "N/A",
            "Main Image URL": None
        }

        # Extract data from 'summaries'
        if response_data.get('summaries') and len(response_data['summaries']) > 0:
            summary = response_data['summaries'][0]
            product_details["Product Name"] = summary.get('itemName', 'N/A')
            product_details["Product Type"] = summary.get('productType', 'N/A')

        # Extract data from 'attributes'
        if response_data.get('attributes'):
            attributes = response_data['attributes']
            
            # Helper function to extract value from potentially complex attribute format
            def extract_attribute_value(data):
                if not data or not isinstance(data, list): return None
                first_item = data[0]
                if isinstance(first_item, dict): return first_item.get('value')
                return None

            # Keywords (generic_keyword or search_terms)
            keywords_raw = attributes.get('generic_keyword') or attributes.get('search_terms')
            if keywords_raw and isinstance(keywords_raw, list):
                extracted_keywords = [item.get('value') for item in keywords_raw if isinstance(item, dict) and 'value' in item]
                if extracted_keywords:
                    product_details["Keywords"] = ", ".join(extracted_keywords)
            
            # Description
            description_val = extract_attribute_value(attributes.get('product_description'))
            if description_val: product_details["Description"] = description_val

            # Bullet Points
            bullet_points_raw = attributes.get('bullet_point')
            if bullet_points_raw and isinstance(bullet_points_raw, list):
                extracted_bps = [item.get('value') for item in bullet_points_raw if isinstance(item, dict) and 'value' in item]
                if extracted_bps:
                    product_details["Bullet Points"] = "\n- ".join(extracted_bps)

            # Main Image
            image_raw = attributes.get('main_product_image_locator')
            if image_raw and isinstance(image_raw, list) and len(image_raw) > 0:
                first_image = image_raw[0]
                if isinstance(first_image, dict):
                    product_details["Main Image URL"] = first_image.get('media_location')


        st.success(f"Successfully retrieved details for SKU: {sku}.")
        return {"status": "success", "message": "Product details retrieved.", "data": product_details}

    except requests.exceptions.HTTPError as e:
        error_detail = e.response.text if e.response else "No response text"
        st.error(f"Error retrieving details for SKU {sku}: HTTP Error {e.response.status_code}. Reason: {e.response.reason}.")
        try:
            st.error(f"DEBUG: API Error Response Body: {json.dumps(e.response.json(), indent=2)}")
        except json.JSONDecodeError:
            st.error(f"DEBUG: API Error Response Body (raw): {e.response.text}")
        return {"status": "error", "message": f"HTTP Error {e.response.status_code}: {error_detail}"}
    except Exception as e:
        st.error(f"An unhandled error occurred while retrieving details for SKU {sku}: {e}")
        return {"status": "error", "message": f"An unhandled error occurred: {e}"}

# --- Streamlit User Interface (UI) ---
st.set_page_config(page_title="Amazon Product Details Extractor", layout="wide")

# Custom CSS for styling
st.markdown("""
<style>
    .stApp { background: #f0f2f6; color: #333; }
    .stButton>button { background-color: #FF9900; color: white; border-radius: 8px; border: none; padding: 10px 20px; font-weight: bold; }
    .stButton>button:hover { background-color: #e68a00; }
    h1, h2, h3 { color: #2c3e50; font-family: 'Inter', sans-serif; }
    .main-header { text-align: center; padding-bottom: 20px; border-bottom: 2px solid #e0e0e0; margin-bottom: 30px; }

    /* MODIFIED: Explicitly style all inputs and selectbox for visibility on light background */
    .stTextInput input, .stSelectbox div[data-baseweb="select"] {
        background-color: #FFFFFF !important;
        border: 1px solid #CCCCCC !important;
        border-radius: 8px;
    }
    .stTextInput input {
        color: #000000 !important;
    }
    .stTextInput label, .stSelectbox label {
        color: #2c3e50 !important; /* Ensure labels are visible */
        font-weight: bold;
    }
    
    /* Style for the results text areas to have a dark theme */
    .stTextArea textarea {
        background-color: #212529;
        color: #f8f9fa;
        border: 1px solid #495057;
        border-radius: 8px;
        font-family: monospace;
    }
</style>
""", unsafe_allow_html=True)

# Session state to manage authentication
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

# Password authentication
if not st.session_state['authenticated']:
    st.markdown("<h2 style='text-align: center; color: #2c3e50;'>Access Product Extractor</h2>", unsafe_allow_html=True)
    password_input = st.text_input("Enter Password:", type="password", key="password_input", help="Enter the password from your .env file.")
    
    if password_input:
        if password_input == APP_PASSWORD:
            st.success("Authentication successful! Loading application...")
            st.session_state['authenticated'] = True
            time.sleep(0.5)
            st.rerun()
        else:
            st.error("Incorrect password. Please try again.")
else: # If authenticated, display the main application
    st.markdown("<div class='main-header'><h1>📦 Amazon Product Details Extractor by SKU</h1></div>", unsafe_allow_html=True)
    
    with st.expander("Show Instructions & .env Configuration Example"):
        st.markdown("""
        This application allows you to retrieve existing product details from your Amazon Seller Central account by providing a Seller SKU.
        
        ### **Instructions:**
        1.  **Configure your `.env` file:** Create a file named `.env` in the same directory as this script and populate it with your credentials. **Do NOT share this file.**
        2.  **Enter the password** you set in the `.env` file to access the tool.
        3.  **Select the Country/Marketplace** from the dropdown.
        4.  **Enter the SKU** for the product you want to look up.
        5.  Click the **"Get Product Details"** button.

        ### Example `.env` Content
        ```
SPAPI_CLIENT_ID="amzn1.application-oa2-client.xxxxxxxxxxxxxxxxxxxxxxxx"
SPAPI_CLIENT_SECRET="amzn1.application-oa2-client.xxxxxxxxxxxxxxxxxxxxxxxx"
SPAPI_REFRESH_TOKEN_NA="Atzr|YOUR_NORTH_AMERICA_REFRESH_TOKEN_HERE"
SPAPI_REFRESH_TOKEN_EU="Atzr|YOUR_EUROPE_REFRESH_TOKEN_HERE"
SPAPI_REFRESH_TOKEN_FE="Atzr|YOUR_FAR_EAST_REFRESH_TOKEN_HERE"
AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"
SELLER_ID_NA="Axxxxxxxxxxxxxxx"
SELLER_ID_EU="Axxxxxxxxxxxxxxx"
SELLER_ID_FE="Axxxxxxxxxxxxxxx"
Password="your_secure_password"
        ```
        """, unsafe_allow_html=True)
    
    # Use a container with a border for better visual grouping
    with st.container(border=True):
        st.header("Retrieve Product Details by SKU")

        # User Inputs
        col1, col2 = st.columns([1, 2])
        with col1:
            selected_country_name = st.selectbox(
                "Select Country/Marketplace:",
                options=list(MARKETPLACE_DETAILS_MAP.keys()),
                index=list(MARKETPLACE_DETAILS_MAP.keys()).index("US (United States)"),
                help="Choose the Amazon marketplace."
            )
        with col2:
            retrieve_sku = st.text_input("Enter SKU to retrieve details:", key="retrieve_sku_input")

        # Get the correct Seller ID based on selected country
        seller_id_env_var = MARKETPLACE_DETAILS_MAP[selected_country_name]["seller_id_env"]
        selected_seller_id = os.getenv(seller_id_env_var)

        if not selected_seller_id:
            st.error(f"Seller ID for this region is not configured. Please set '{seller_id_env_var}' in your .env file.")
        
        if st.button("Get Product Details", help="Retrieve product details from Amazon for the given SKU."):
            if retrieve_sku and selected_seller_id:
                with st.spinner(f"Retrieving details for SKU: {retrieve_sku}..."):
                    selected_marketplace_id = MARKETPLACE_DETAILS_MAP[selected_country_name]["id"]
                    details_result = get_product_details(
                        sku=retrieve_sku.strip(),
                        marketplace_id=selected_marketplace_id,
                        seller_id=selected_seller_id,
                        selected_country_name=selected_country_name
                    )
                    
                    if details_result["status"] == "success":
                        st.subheader(f"Results for SKU: {retrieve_sku}")
                        product_data = details_result["data"]
                        
                        res_col1, res_col2 = st.columns([1, 2])
                        
                        with res_col1:
                            if product_data["Main Image URL"]:
                                st.image(product_data["Main Image URL"], caption="Main Product Image", use_container_width=True)
                            else:
                                st.info("No main image found.")
                        
                        with res_col2:
                            st.write(f"**Product Name:** {product_data['Product Name']}")
                            st.write(f"**Product Type:** {product_data['Product Type']}")
                            st.text_area("Keywords:", value=product_data['Keywords'], height=100)
                            st.text_area("Description:", value=product_data['Description'], height=150)
                            st.text_area("Bullet Points:", value=f"- {product_data['Bullet Points']}", height=150)

                    else:
                        st.error(f"Failed to retrieve product details: {details_result['message']}")
            else:
                st.warning("Please enter a SKU. Ensure a Seller ID is configured for the selected country.")