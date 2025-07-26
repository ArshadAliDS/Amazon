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

# --- App Configuration from .env ---
APP_PASSWORD = os.getenv("Password") # Get the password from .env

# Base URLs for SP-API
SP_API_BASE_URLS = {
    'na': 'https://sellingpartnerapi-na.amazon.com', # North America (US, CA, MX)
    'eu': 'https://sellingpartnerapi-eu.amazon.com', # Europe (UK, DE, ES, FR, IT, TR, AE, SA)
    'fe': 'https://sellingpartnerapi-fe.amazon.com'  # Far East (JP, AU, IN, SG, CN, BR)
}
LWA_TOKEN_URL = 'https://api.amazon.com/auth/o2/token'

# Global variable to store access token and its expiry per (REGION GROUP, ACCOUNT)
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


def get_access_token(selected_region_group, selected_account):
    """
    Retrieves or refreshes the LWA access token for a specific account and region group.
    """
    global access_token_info
    cache_key = (selected_region_group, selected_account)

    if cache_key not in access_token_info:
        access_token_info[cache_key] = {"token": None, "expires_at": None}

    if access_token_info[cache_key]["token"] and \
       access_token_info[cache_key]["expires_at"] and \
       datetime.now() < access_token_info[cache_key]["expires_at"]:
        return access_token_info[cache_key]["token"]

    st.info(f"Attempting to refresh token for Account: '{selected_account}', Region: '{selected_region_group}'...")
    account_prefix = selected_account.upper() + "_"
    
    client_id_var = f"{account_prefix}SPAPI_CLIENT_ID"
    client_secret_var = f"{account_prefix}SPAPI_CLIENT_SECRET"
    refresh_token_var = f"{account_prefix}SPAPI_REFRESH_TOKEN_{selected_region_group.upper()}"
    
    client_id = os.getenv(client_id_var)
    client_secret = os.getenv(client_secret_var)
    specific_refresh_token = os.getenv(refresh_token_var)

    if not all([client_id, client_secret, specific_refresh_token]):
        st.error(f"Missing one or more SP-API LWA credentials for account '{selected_account}' and region '{selected_region_group}'.")
        st.error(f"Please check your .env file for: {client_id_var}, {client_secret_var}, and {refresh_token_var}.")
        return None

    try:
        response = requests.post(LWA_TOKEN_URL, data={'grant_type': 'refresh_token','refresh_token': specific_refresh_token,'client_id': client_id,'client_secret': client_secret})
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data['access_token']
        expires_in = token_data['expires_in']

        access_token_info[cache_key]["token"] = access_token
        access_token_info[cache_key]["expires_at"] = datetime.now() + timedelta(seconds=expires_in - 60)

        st.success(f"Access token for Account '{selected_account}', Region '{selected_region_group}' refreshed successfully!")
        return access_token
    except requests.exceptions.RequestException as e:
        st.error(f"Error refreshing access token for Account '{selected_account}': {e}")
        st.error(f"Response status: {e.response.status_code if e.response else 'N/A'}, Response body: {e.response.text if e.response else 'N/A'}")
        st.error(f"Please verify your credentials in the .env file.")
        return None

def get_sp_api_headers(access_token):
    return {'x-amz-access-token': access_token,'Content-Type': 'application/json','Accept': 'application/json'}

def get_product_details(sku, marketplace_id, seller_id, selected_country_name, selected_account):
    """Retrieves product listing details for a given SKU."""
    region_group = MARKETPLACE_DETAILS_MAP.get(selected_country_name, {}).get("region_group")
    access_token = get_access_token(region_group, selected_account)
    if not access_token: return {"status": "error", "message": "Failed to obtain SP-API access token."}
    
    base_url = SP_API_BASE_URLS.get(region_group)
    url = f"{base_url}/listings/2021-08-01/items/{seller_id}/{sku}"
    params = {"marketplaceIds": marketplace_id, "includedData": "summaries,attributes", "issueLocale": "en_US"}

    try:
        st.info(f"Retrieving listing details for SKU: {sku}...")
        response = requests.get(url, headers=get_sp_api_headers(access_token), params=params)
        response.raise_for_status()
        response_data = response.json()

        product_details = {"Product Name": "N/A", "Product Type": "N/A", "Keywords": "N/A", "Description": "N/A", "Bullet Points": "N/A", "Main Image URL": None}

        if response_data.get('summaries'):
            summary = response_data['summaries'][0]
            product_details["Product Name"] = summary.get('itemName', 'N/A')
            product_details["Product Type"] = summary.get('productType', 'N/A')

        if response_data.get('attributes'):
            attributes = response_data['attributes']
            def extract_attribute_value(data):
                if not data or not isinstance(data, list): return None
                return data[0].get('value') if isinstance(data[0], dict) else None

            keywords_raw = attributes.get('generic_keyword') or attributes.get('search_terms')
            if keywords_raw and isinstance(keywords_raw, list):
                product_details["Keywords"] = ", ".join([item.get('value', '') for item in keywords_raw if item.get('value')])

            description_val = extract_attribute_value(attributes.get('product_description'))
            if description_val: product_details["Description"] = description_val

            bullet_points_raw = attributes.get('bullet_point')
            if bullet_points_raw and isinstance(bullet_points_raw, list):
                product_details["Bullet Points"] = "\n- ".join([item.get('value', '') for item in bullet_points_raw if item.get('value')])
            
            image_raw = attributes.get('main_product_image_locator')
            if image_raw and isinstance(image_raw, list) and image_raw[0].get('media_location'):
                product_details["Main Image URL"] = image_raw[0].get('media_location')

        st.success(f"Successfully retrieved listing details for SKU: {sku}.")
        return {"status": "success", "data": product_details}
    except requests.exceptions.HTTPError as e:
        st.error(f"Error retrieving details for SKU {sku}: HTTP {e.response.status_code}. The SKU may not exist for the selected account/marketplace.")
        st.error(f"DEBUG: {e.response.text}")
        return {"status": "error", "message": f"HTTP Error {e.response.status_code}"}
    except Exception as e:
        st.error(f"An unhandled error occurred while retrieving details for SKU {sku}: {e}")
        return {"status": "error", "message": f"An unhandled error occurred: {e}"}

# --- REVISED FUNCTION ---
def get_product_pricing(sku, marketplace_id, selected_account, selected_country_name):
    """
    Retrieves pricing details for a given SKU using the getListingOffers endpoint.
    This is an alternative to the getPricing endpoint.
    """
    region_group = MARKETPLACE_DETAILS_MAP.get(selected_country_name, {}).get("region_group")
    access_token = get_access_token(region_group, selected_account)
    if not access_token: 
        return {"status": "error", "message": "Failed to obtain SP-API access token."}

    base_url = SP_API_BASE_URLS.get(region_group)
    
    # --- MODIFICATION START ---
    # The SKU is now part of the URL path itself
    url = f"{base_url}/products/pricing/v0/listings/{sku}/offers"
    # Parameters are simplified
    params = {"MarketplaceId": marketplace_id, "ItemCondition": "New"}
    # --- MODIFICATION END ---

    try:
        # st.info(f"Retrieving pricing for SKU: {sku} (using getListingOffers)...")
        response = requests.get(url, headers=get_sp_api_headers(access_token), params=params)
        response.raise_for_status()
        pricing_data = response.json()
        # st.info(f"Response status code: {pricing_data}")
        
        # Default pricing
        price_info = {"price": "N/A", "shipping": "N/A", "total": "N/A", "currency": ""}

        # Extract the price from the response structure
        payload = pricing_data.get('payload', {})
        offers = payload.get('Offers', [])
        # st.info(f"Offers found: {offers}")
        if offers:
            offer = offers[0] # Get the first offer
            buying_price = offer# Get BuyingPrice from the offer
            # st.info(f"Buying price details: {buying_price}")
            listing_price_info = buying_price.get('ListingPrice', {})
            shipping_price_info = buying_price.get('Shipping', {})
            landed_price_info = buying_price.get('LandedPrice', {})
            # st.info(f"Listing price info: {listing_price_info}, Shipping price info: {shipping_price_info}, Landed price info: {landed_price_info}")
            price_info["price"] = listing_price_info.get('Amount', 'N/A')
            price_info["shipping"] = shipping_price_info.get('Amount', 0.0) # Default to 0 if not present
            price_info["total"] = price_info["price"] + price_info["shipping"] #landed_price_info.get('Amount', 'N/A') # Use LandedPrice for total
            price_info["currency"] = listing_price_info.get('CurrencyCode', '')
            # st.info(f"Retrieved pricing for SKU: {sku} - Price: {price_info['price']}, Shipping: {price_info['shipping']}, Total: {price_info['total']},Price Currency: {price_info['currency']}")
        # st.success(f"Successfully retrieved pricing for SKU: {sku}.")
        return {"status": "success", "data": price_info}
    except requests.exceptions.HTTPError as e:
        # Check if the error is 403 again
        if e.response.status_code == 403:
             st.error("FATAL: Received 403 Forbidden error again. This confirms a permission issue with the 'Pricing' role on your account. Please pursue your Amazon Support case.")
        else:
            st.warning(f"Could not retrieve pricing for SKU {sku}. HTTP {e.response.status_code}. The item may not have an active offer.")
        return {"status": "error", "message": f"Could not retrieve pricing info: {e.response.text}"}
    except Exception as e:
        st.warning(f"An unhandled error occurred while retrieving pricing for SKU {sku}: {e}")
        return {"status": "error", "message": f"An unhandled error occurred: {e}"}

# --- NEW FUNCTION FOR CURRENCY CONVERSION ---
def get_inr_conversion(amount, from_currency):
    # st.info(f"Converting {amount} {from_currency} to INR...")
    """
    Converts a given amount from a source currency to INR using a free API.
    Returns the converted amount or None if conversion fails.
    """

    # st.info(from_currency)
    # if from_currency == "INR" or not isinstance(amount, (int, float)) or amount <= 0:
    #     return None
    
    try:
        # st.info(f"Converting2 {amount} {from_currency} to INR...")
        # Using Frankfurter API, which is free and requires no API key
        url = f"https://api.frankfurter.app/latest?amount={amount}&from={from_currency}&to=INR"
        response = requests.get(url, timeout=5) # Use a timeout to prevent long waits
        response.raise_for_status()
        data = response.json()
        # st.info(f"Converted {amount} {from_currency} to INR successfully.")
        return data.get('rates', {}).get('INR')
    except Exception:
        # st.info(f"Failed to convert {amount} {from_currency} to INR. The conversion service may be down or unavailable.")
        # Fails silently to not clutter the UI if the conversion service is down
        return None

# --- Streamlit User Interface (UI) ---
st.set_page_config(page_title="Amazon Product Details Extractor", layout="wide")

st.markdown("""<style>...your_css_here...</style>""", unsafe_allow_html=True) # CSS hidden for brevity

if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

if not st.session_state['authenticated']:
    st.markdown("<h2 style='text-align: center;'>Access Product Extractor</h2>", unsafe_allow_html=True)
    password_input = st.text_input("Enter Password:", type="password", key="password_input")
    if password_input:
        if password_input == APP_PASSWORD:
            st.session_state['authenticated'] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
else:
    st.markdown("<div class='main-header'><h1>📦 Amazon Product Details Extractor</h1></div>", unsafe_allow_html=True)
    
    with st.expander("Show Instructions & .env Configuration Example"):
        st.markdown("""...instructions_and_env_example...""", unsafe_allow_html=True) # Content hidden for brevity

    with st.container(border=True):
        st.header("Retrieve Product Details by SKU")
        
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            selected_account = st.selectbox("Select Account:", options=["Frienemy", "aport"])
        with col2:
            selected_country_name = st.selectbox("Select Country/Marketplace:", options=list(MARKETPLACE_DETAILS_MAP.keys()), index=0)
        with col3:
            retrieve_sku = st.text_input("Enter SKU to retrieve details:", key="retrieve_sku_input")

        account_prefix = selected_account.upper() + "_"
        seller_id_env_key = MARKETPLACE_DETAILS_MAP[selected_country_name]["seller_id_env"]
        full_seller_id_env_var = f"{account_prefix}{seller_id_env_key}"
        selected_seller_id = os.getenv(full_seller_id_env_var)
        
        if not selected_seller_id:
            st.error(f"Seller ID not configured. Please set '{full_seller_id_env_var}' in your .env file.")
        
        if st.button("Get Product Details"):
            if retrieve_sku and selected_seller_id and selected_account:
                with st.spinner(f"Retrieving all details for SKU: {retrieve_sku}..."):
                    selected_marketplace_id = MARKETPLACE_DETAILS_MAP[selected_country_name]["id"]
                    
                    details_result = get_product_details(sku=retrieve_sku.strip(), marketplace_id=selected_marketplace_id, seller_id=selected_seller_id, selected_country_name=selected_country_name, selected_account=selected_account)
                    
                    pricing_data = None
                    if details_result["status"] == "success":
                        pricing_result = get_product_pricing(sku=retrieve_sku.strip(), marketplace_id=selected_marketplace_id, selected_account=selected_account, selected_country_name=selected_country_name)
                        if pricing_result["status"] == "success":
                            pricing_data = pricing_result["data"]
                    
                    if details_result["status"] == "success":
                        st.subheader(f"Results for SKU: {retrieve_sku} (Account: {selected_account})")
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
                            
                            st.divider()
                            if pricing_data:
                                # Determine currency symbol, default to currency code if not a major one
                                currency_symbols = {"USD": "$", "GBP": "£", "EUR": "€", "JPY": "¥", "INR": "₹"}
                                currency_symbol = currency_symbols.get(pricing_data['currency'], pricing_data['currency'])

                                price_cols = st.columns(3)
                                with price_cols[0]:
                                    st.metric("Listing Price", f"{currency_symbol}{pricing_data['price']}" if pricing_data['price'] != 'N/A' else 'N/A')
                                    # --- NEW: INR CONVERSION DISPLAY ---
                                    inr_price = get_inr_conversion(pricing_data['price'], pricing_data['currency'])
                                    # st.info(pricing_data['currency'] )
                                    if inr_price:
                                        st.caption(f"Approx. **₹{inr_price:,.2f}**") # Display as a caption below the total price

                                with price_cols[1]:
                                    st.metric("Shipping", f"{currency_symbol}{pricing_data['shipping']}" if pricing_data['shipping'] != 'N/A' else 'N/A')
                                    # --- NEW: INR CONVERSION DISPLAY ---
                                    inr_price = get_inr_conversion(pricing_data['shipping'], pricing_data['currency'])
                                    # st.info(pricing_data['currency'] )
                                    if inr_price:
                                        st.caption(f"Approx. **₹{inr_price:,.2f}**") # Display as a caption below the total price
                                with price_cols[2]:
                                    total_price_str = f"{currency_symbol}{pricing_data['total']}" if pricing_data['total'] != 'N/A' else 'N/A'
                                    st.metric("Total Price", total_price_str)                                    
                                    # --- NEW: INR CONVERSION DISPLAY ---
                                    inr_price = get_inr_conversion(pricing_data['total'], pricing_data['currency'])
                                    # st.info(pricing_data['currency'] )
                                    if inr_price:
                                        st.caption(f"Approx. **₹{inr_price:,.2f}**") # Display as a caption below the total price

                            else:
                                st.info("Pricing information could not be retrieved.")
                            st.divider()

                            st.text_area("Keywords:", value=product_data['Keywords'], height=100)
                            st.text_area("Description:", value=product_data['Description'], height=150)
                            st.text_area("Bullet Points:", value=f"- {product_data['Bullet Points']}", height=150)

                    else:
                        st.error(f"Failed to retrieve product details: {details_result['message']}")
            else:
                st.warning("Please enter a SKU and ensure required configurations are set.")
