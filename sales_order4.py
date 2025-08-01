import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
import time
from io import StringIO
import requests
import gzip
from datetime import date, timedelta
import json
import numpy as np

# Import sp-api clients and exceptions
from sp_api.api import Reports, Orders, CatalogItems
from sp_api.base import Marketplaces, SellingApiException

# --- Initial Configuration ---
load_dotenv()
APP_PASSWORD = os.getenv("Password")

# --- Helper Functions ---

@st.cache_resource
def get_sp_api_credentials(selected_account: str):
    """Loads SP-API credentials for the selected account from the .env file."""
    account_prefix = selected_account.upper() + "_"
    creds = {
        'lwa_app_id': os.getenv(f"{account_prefix}SPAPI_CLIENT_ID"),
        'lwa_client_secret': os.getenv(f"{account_prefix}SPAPI_CLIENT_SECRET"),
        'aws_access_key': os.getenv(f"{account_prefix}AWS_ACCESS_KEY_ID"),
        'aws_secret_key': os.getenv(f"{account_prefix}AWS_SECRET_ACCESS_KEY"),
    }
    creds['regional_tokens'] = {
        'na': os.getenv(f"{account_prefix}SPAPI_REFRESH_TOKEN_NA"),
        'eu': os.getenv(f"{account_prefix}SPAPI_REFRESH_TOKEN_EU"),
        'fe': os.getenv(f"{account_prefix}SPAPI_REFRESH_TOKEN_FE"),
    }
    if not all(k for k in [creds['lwa_app_id'], creds['lwa_client_secret'], creds['aws_access_key'], creds['aws_secret_key']]):
        st.error(f"Missing one or more core SP-API credentials for '{selected_account}'. Please check your .env file.")
        st.stop()
    return creds

def get_marketplace_enum(marketplace_id_string: str):
    """Converts a marketplace ID string to its corresponding Marketplaces enum."""
    for marketplace in Marketplaces:
        if marketplace.marketplace_id == marketplace_id_string:
            return marketplace
    return None

def get_currency_code_for_marketplace(marketplace_id: str) -> str:
    """Returns the currency code for a given marketplace ID."""
    currency_map = {
        "ATVPDKIKX0DER": "USD", "A2EUQ1WTGCTBG2": "CAD", "A1AM78C64UM0Y8": "MXN",
        "A1F83G8C2ARO7P": "GBP", "A1PA6795UKMFR9": "EUR", "APJ6JRA9NG5V4": "EUR",
        "A1RKKUPIHCS9HS": "EUR", "A13V1IB3VIYZZH": "EUR", "A1805IZSGTT6HS": "EUR",
        "A1C3SOZRARQ6R3": "PLN", "A2NODRKZP88ZB9": "SEK", "A33AVAJ2PDY3EV": "TRY",
        "A2VIGQ35RCS4UG": "AED", "A17E79C6D8DWNP": "SAR", "ARBP9OOSHTCHU": "EGP"
    }
    return currency_map.get(marketplace_id, "USD")

@st.cache_data(ttl=3600)
def get_conversion_rates(base_currency="USD"):
    """Fetches latest conversion rates against a base currency. Falls back gracefully."""
    try:
        url = f"https://api.frankfurter.app/latest?from={base_currency}"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        rates = data.get('rates', {})
        rates[base_currency] = 1.0 # Add base currency to rates
        st.session_state['conversion_failed'] = False
        return rates
    except Exception as e:
        st.warning(f"Could not fetch currency conversion rates: {e}. Sales values will be shown in local currencies.")
        st.session_state['conversion_failed'] = True
        return {base_currency: 1.0}

def get_refresh_token_for_region(credentials, marketplace):
    """Gets the correct refresh token based on the marketplace's region."""
    endpoint = marketplace.endpoint
    if "eu" in endpoint: region = 'eu'
    elif "fe" in endpoint: region = 'fe'
    else: region = 'na'
    token = credentials['regional_tokens'].get(region)
    if not token:
        st.warning(f"Refresh token for region '{region.upper()}' not found. Skipping this region.")
        return None
    return token

def download_and_process_report(reports_client, report_id):
    """Polls for, downloads, and processes a report."""
    for _ in range(20):
        time.sleep(15)
        report_status_payload = reports_client.get_report(report_id).payload
        status = report_status_payload['processingStatus']
        if status in ['DONE', 'FATAL', 'CANCELLED']:
            break
    if status != 'DONE':
        st.error(f"Report generation failed with status: {status}")
        return None

    doc_id = report_status_payload['reportDocumentId']
    report_doc_response = reports_client.get_report_document(doc_id)
    doc_payload = report_doc_response.payload
    report_url = doc_payload.get('url')
    compression = doc_payload.get('compressionAlgorithm')

    if not report_url:
        st.error("Failed to get report download URL.")
        return None

    response = requests.get(report_url)
    response.raise_for_status()
    decompressed_content = gzip.decompress(response.content) if compression == 'GZIP' else response.content
    try:
        return decompressed_content.decode('utf-8')
    except UnicodeDecodeError:
        return decompressed_content.decode('latin-1')

# --- Core Business Logic ---

@st.cache_data(ttl=3600, show_spinner=False)
def get_sales_summary_data(_credentials, marketplace_ids, start_date, end_date, selected_account):
    """Fetches, combines, and converts sales data from multiple marketplaces."""
    all_sales_data = []
    conversion_rates = get_conversion_rates("USD")

    for marketplace_id in marketplace_ids:
        marketplace = get_marketplace_enum(marketplace_id)
        if not marketplace:
            st.warning(f"Invalid Marketplace ID '{marketplace_id}' skipped.")
            continue

        creds = _credentials.copy()
        creds['refresh_token'] = get_refresh_token_for_region(creds, marketplace)
        if not creds['refresh_token']:
            continue

        try:
            reports_client = Reports(credentials=creds, marketplace=marketplace)
            st.info(f"Requesting 'Sales and Traffic' report for Account: '{selected_account}', Marketplace: {marketplace.name}...")
            
            report_options = {"dateGranularity": "DAY"}
            report_response = reports_client.create_report(
                reportType='GET_SALES_AND_TRAFFIC_REPORT',
                dataStartTime=start_date.isoformat(),
                dataEndTime=end_date.isoformat(),
                marketplaceIds=[marketplace_id],
                reportOptions=report_options
            )
            report_id = report_response.payload['reportId']
            
            report_text = download_and_process_report(reports_client, report_id)
            if not report_text: continue

            report_json = json.loads(report_text)
            sales_by_date = report_json.get('salesAndTrafficByDate', [])
            
            if not sales_by_date:
                st.warning(f"Sales and Traffic report for {marketplace.name} contained no data.")
                continue

            currency_code = get_currency_code_for_marketplace(marketplace_id)
            rate = conversion_rates.get(currency_code, 1.0)

            records = [
                {
                    'Date': item['date'], 
                    'Sales (Local Currency)': item['salesByDate']['orderedProductSales']['amount'], 
                    'Sales (USD)': item['salesByDate']['orderedProductSales']['amount'] / rate,
                    'Units Ordered': item['salesByDate']['unitsOrdered'],
                    'Marketplace': marketplace.name,
                    'Currency': currency_code
                } for item in sales_by_date
            ]
            all_sales_data.extend(records)

        except SellingApiException as e:
            st.error(f"SP-API Error for {marketplace.name}: {e.code} - {e.message}")
        except Exception as e:
            st.error(f"An unexpected error occurred for {marketplace.name}: {e}")

    if not all_sales_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_sales_data)
    df['Date'] = pd.to_datetime(df['Date'])
    return df

def get_product_details_for_order(_credentials, marketplace_id, items):
    """Fetches rich details (image, pricing) for a list of order items."""
    marketplace = get_marketplace_enum(marketplace_id)
    creds = _credentials.copy()
    creds['refresh_token'] = get_refresh_token_for_region(creds, marketplace)

    asins = [item['ASIN'] for item in items]
    skus = [item['SellerSKU'] for item in items]
    
    # --- Fetch Catalog Info (Images) ---
    catalog_info = {}
    try:
        catalog_client = CatalogItems(credentials=creds, marketplace=marketplace)
        # Using a direct _request method for robustness against library versions.
        response = catalog_client._request(
            path="/catalog/2022-04-01/items",
            params={
                "marketplaceId": marketplace_id,
                "identifiers": ",".join(asins),
                "identifiersType": "ASIN",
                "includedData": "images"
            }
        )
        for item in response.payload.get('items', []):
            asin = item['asin']
            images_data = item.get('images', [])
            main_image_url = None
            if images_data:
                first_locale_images = images_data[0].get('images', [])
                main_image = next((img['link'] for img in first_locale_images if img.get('variant') == 'MAIN'), None)
                if main_image:
                    main_image_url = main_image
            catalog_info[asin] = {'Image URL': main_image_url}
    except SellingApiException as e:
        st.warning(f"Could not fetch catalog images: {e.message}")
    except Exception as e:
        st.error(f"An unexpected error occurred in get_product_catalog_info: {e}")

    # --- Fetch Pricing Info ---
    pricing_info = {}
    try:
        generic_client = Orders(credentials=creds, marketplace=marketplace)
        for sku in skus:
            response = generic_client._request(
                path=f"/products/pricing/v0/listings/{sku}/offers",
                params={"MarketplaceId": marketplace_id, "ItemCondition": "New"}
            )
            offer = response.payload.get('payload', {}).get('Offers', [{}])[0]
            pricing_info[sku] = {
                'Listing Price': offer.get('ListingPrice', {}).get('Amount'),
                'Shipping Price': offer.get('Shipping', {}).get('Amount', 0.0),
                'Landed Price': offer.get('LandedPrice', {}).get('Amount')
            }
            time.sleep(1)
    except SellingApiException as e:
        st.warning(f"Could not fetch pricing details: {e.message}")

    # --- Combine all data ---
    for item in items:
        item.update(catalog_info.get(item['ASIN'], {}))
        item.update(pricing_info.get(item['SellerSKU'], {}))
    
    return items


def get_order_details(_credentials, marketplace_id, order_id):
    """Fetches non-PII details and enriches with catalog and pricing data."""
    marketplace = get_marketplace_enum(marketplace_id)
    creds = _credentials.copy()
    creds['refresh_token'] = get_refresh_token_for_region(creds, marketplace)

    try:
        orders_client = Orders(credentials=creds, marketplace=marketplace)
        order = orders_client.get_order(order_id).payload
        order_items_payload = orders_client.get_order_items(order_id).payload
        order_items = order_items_payload.get('OrderItems', [])

        if order_items:
            order_items = get_product_details_for_order(creds, marketplace_id, order_items)
        
        return order, order_items

    except SellingApiException as e:
        st.error(f"SP-API Error while fetching order {order_id}: {e.code} - {e.message}")
        return None, None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None, None

# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Amazon Sales Dashboard")

def login_page():
    st.title("🔐 Sales Dashboard Login")
    with st.form("login_form"):
        password = st.text_input("Password", type="password")
        if st.form_submit_button("Login"):
            if password == APP_PASSWORD:
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error("Incorrect password.")

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



def main_dashboard():
    st.title("📈 Amazon Sales Dashboard")

    st.sidebar.header("Configuration")
    selected_account = st.sidebar.selectbox("Select Account", ["Frienemy", "aport"])
    
    na_eu_marketplaces = {
        "United States": "ATVPDKIKX0DER", "Canada": "A2EUQ1WTGCTBG2", "Mexico": "A1AM78C64UM0Y8",
        "United Kingdom": "A1F83G8C2ARO7P", "Germany": "A1PA6795UKMFR9", "Italy": "APJ6JRA9NG5V4",
        "Spain": "A1RKKUPIHCS9HS", "France": "A13V1IB3VIYZZH", "Netherlands": "A1805IZSGTT6HS",
        "Poland": "A1C3SOZRARQ6R3", "Sweden": "A2NODRKZP88ZB9", "Turkey": "A33AVAJ2PDY3EV",
        "United Arab Emirates": "A2VIGQ35RCS4UG", "Saudi Arabia": "A17E79C6D8DWNP", "Egypt": "ARBP9OOSHTCHU"
    }
    marketplace_options_with_all = {"All NA & EU Marketplaces": "ALL", **na_eu_marketplaces}
    selected_marketplace_display = st.sidebar.selectbox("Select Marketplace", marketplace_options_with_all.keys())
    selected_marketplace_id = marketplace_options_with_all[selected_marketplace_display]
    
    st.header("Sales Performance Summary")
    
    today = date.today()
    yesterday = today - timedelta(days=1)
    default_start = yesterday - timedelta(days=29)
    
    summary_cols = st.columns(2)
    with summary_cols[0]:
        start_date = st.date_input("Start Date", default_start, min_value=today - timedelta(days=365), max_value=yesterday)
    with summary_cols[1]:
        end_date = st.date_input("End Date", yesterday, min_value=start_date, max_value=yesterday)

    if st.button("📊 Analyze Sales", type="primary"):
        credentials = get_sp_api_credentials(selected_account)
        
        marketplace_ids_to_fetch = []
        if selected_marketplace_id == "ALL":
            marketplace_ids_to_fetch = list(na_eu_marketplaces.values())
        else:
            marketplace_ids_to_fetch = [selected_marketplace_id]

        df_sales = get_sales_summary_data(credentials, marketplace_ids_to_fetch, start_date, end_date, selected_account)
        st.session_state['df_sales'] = df_sales

    if 'df_sales' in st.session_state and st.session_state['df_sales'] is not None:
        df_master = st.session_state['df_sales']
        if not df_master.empty:
            
            st.subheader("Filter Results")
            all_markets = df_master['Marketplace'].unique()
            selected_markets = st.multiselect("Filter by Marketplace:", options=all_markets, default=list(all_markets))
            
            if not selected_markets:
                st.warning("Please select at least one marketplace to display data.")
                df_filtered = pd.DataFrame()
            else:
                df_filtered = df_master[df_master['Marketplace'].isin(selected_markets)]

            if not df_filtered.empty:
                if st.session_state.get('conversion_failed', False):
                    st.warning("Could not fetch live currency rates. Sales totals are a mix of local currencies and may not be accurate.")
                
                total_sales_usd = df_filtered['Sales (USD)'].sum()
                total_units = df_filtered['Units Ordered'].sum()
                num_days = (end_date - start_date).days + 1
                avg_daily_sales_usd = total_sales_usd / num_days

                kpi_cols = st.columns(3)
                kpi_cols[0].metric("Total Sales", f"${total_sales_usd:,.2f} USD")
                kpi_cols[1].metric("Total Units Sold", f"{total_units:,.0f}")
                kpi_cols[2].metric("Avg. Daily Sales", f"${avg_daily_sales_usd:,.2f} USD")

                st.subheader("Sales Trend (in USD)")
                
                time_granularity = st.radio(
                    "Select Time Granularity:",
                    ("Daily", "Weekly", "Monthly", "Yearly"),
                    horizontal=True,
                    key="granularity_selector"
                )

                df_pivot = df_filtered.pivot_table(index='Date', columns='Marketplace', values='Sales (USD)', aggfunc='sum')

                if time_granularity == "Weekly":
                    df_chart_data = df_pivot.resample('W-MON').sum()
                elif time_granularity == "Monthly":
                    df_chart_data = df_pivot.resample('M').sum()
                elif time_granularity == "Yearly":
                    df_chart_data = df_pivot.resample('Y').sum()
                else: # Daily
                    df_chart_data = df_pivot
                
                st.line_chart(df_chart_data)

            else:
                st.info("No sales data found for the selected filters.")
        else:
            st.info("No sales data found for the selected period.")
    
    st.divider()

    st.header("Order Details Lookup")
    
    if selected_marketplace_id == "ALL":
        st.info("Please select a specific marketplace from the sidebar to look up an order.")
    else:
        order_id_input = st.text_input("Enter Amazon Order ID:", placeholder="e.g., 123-1234567-1234567")
        if st.button("🔍 Search Order"):
            if order_id_input:
                credentials = get_sp_api_credentials(selected_account)
                with st.spinner(f"Searching for order {order_id_input}..."):
                    order_details, order_items = get_order_details(credentials, selected_marketplace_id, order_id_input)
                    # st.info(order_details)
                    # st.info(order_items)
                    if order_details and order_items:
                        st.success(f"Details found for Order ID: {order_details['AmazonOrderId']}")
                        # Display Order Summary
                        order_cols = st.columns(6)
                        order_cols[0].metric("Order Status", order_details['OrderStatus'])
                        order_cols[1].metric("Purchase Date", pd.to_datetime(order_details['PurchaseDate']).strftime('%d-%b-%Y'))
                        order_cols[2].metric("Last Delivery Date", pd.to_datetime(order_details['EarliestDeliveryDate']).strftime('%d-%b-%Y'))
                        order_cols[3].metric("Total Items", len(order_items))
                        order_cols[4].metric("Order Total", f"${order_details.get('OrderTotal', {}).get('Amount', 'N/A')}")
                        order_cols[5].metric("MarketPlace", f"{order_details['SalesChannel']}")
                        
                        st.subheader("Items in this Order")
                        
                        for item in order_items:
                            with st.container(border=True):
                                col1, col2 = st.columns([1, 3])
                                with col1:
                                    if item.get("Image URL"):
                                        st.image(item["Image URL"])
                                    else:
                                        st.caption("No Image")
                                with col2:
                                    st.write(f"**{item.get('Title', 'N/A')}**")
                                    st.text(f"SKU: {item.get('SellerSKU', 'N/A')}")
                                    st.text(f"ASIN: {item.get('ASIN', 'N/A')}")
                                    st.text(f"Quantity: {item.get('QuantityOrdered', 0)}")
                                    
                                    

                                    amount_value = item.get('ItemPrice', {})
                                    # st.info(amount_value)
                                    shipping_value = item.get('ShippingPrice', {}).get('Amount')
                                    total_value = order_details.get('OrderTotal', {}).get('Amount', 'N/A')
                                    currency_symbols = {"USD": "$", "GBP": "£", "EUR": "€", "JPY": "¥", "INR": "₹"}
                                    currency_symbol1 = currency_symbols.get(amount_value['CurrencyCode'], amount_value['CurrencyCode'])
                                    # st.info(f"Price: {currency_symbol1}")
                                    price_info_cols = st.columns(3)
                                    # st.info(order_details.get('OrderTotal', {}).get('Amount', 'N/A'))
                                    


                                    with price_info_cols[0]:
                                        st.metric("Listing Price", f"${item.get('ItemPrice', {}).get('Amount')}")
                                        inr_price = get_inr_conversion(item.get('ItemPrice', {}).get('Amount'), currency_symbol1)
                                        if inr_price:
                                            st.caption(f"Approx. **₹{inr_price:,.2f}**") # Display as a caption below the total price

                                    with price_info_cols[1]:
                                        st.metric("Shipping", f"${item.get('ShippingPrice', {}).get('Amount')}")
                                        inr_price = get_inr_conversion(item.get('ShippingPrice', {}).get('Amount'), currency_symbol1)
                                        if inr_price:
                                            st.caption(f"Approx. **₹{inr_price:,.2f}**") # Display as a caption below the total price
                                    
                                    with price_info_cols[2]:
                                        st.metric("Landed Price", f"${order_details.get('OrderTotal', {}).get('Amount', 'N/A')}")
                                        inr_price = get_inr_conversion(order_details.get('OrderTotal', {}).get('Amount', 'N/A'), currency_symbol1)
                                        if inr_price:
                                            st.caption(f"Approx. **₹{inr_price:,.2f}**") # Display as a caption below the total price

                    else:
                        st.warning("Could not retrieve details for this Order ID. Please check the ID and selected marketplace.")
            else:
                st.warning("Please enter an Order ID.")

# --- Main App Logic ---
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False

if not APP_PASSWORD:
    st.error("FATAL: 'Password' is not set in the .env file. Application cannot start.")
else:
    if st.session_state['authenticated']:
        main_dashboard()
    else:
        login_page()
