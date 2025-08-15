import streamlit as st
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import date, timedelta, datetime
import time
from sp_api.api import Finances
from sp_api.base import Marketplaces, SellingApiRequestThrottledException
from io import StringIO
import requests # Added for manual download
import plotly.express as px

# --- Page Configuration ---
st.set_page_config(
    page_title="SP-API Financial Dashboard",
    page_icon="💰",
    layout="wide",
)

# --- Load Environment Variables ---
# This loads the credentials from your .env file.
load_dotenv()
APP_PASSWORD = os.getenv("Password")


# --- Detailed Marketplace Mappings ---
MARKETPLACE_DETAILS_MAP = {
    "US (United States)": {"id": "ATVPDKIKX0DER", "region_group": "na", "api_name": "www.amazon.com"},
    "CA (Canada)": {"id": "A2EUQ1WTGCTBG2", "region_group": "na", "api_name": "www.amazon.ca"},
    "MX (Mexico)": {"id": "A1AM78C64UM0Y8", "region_group": "na", "api_name": "www.amazon.com.mx"},
    "BR (Brazil)": {"id": "A2Q3Y263D00KWC", "region_group": "na", "api_name": "www.amazon.com.br"},
    "GB (United Kingdom)": {"id": "A1F83G8C2ARO7P", "region_group": "eu", "api_name": "www.amazon.co.uk"},
    "DE (Germany)": {"id": "A1PA6795UKMFR9", "region_group": "eu", "api_name": "www.amazon.de"},
    "ES (Spain)": {"id": "A1RKKUPIHCS9HS", "region_group": "eu", "api_name": "www.amazon.es"},
    "FR (France)": {"id": "A13V1IB3VIYZZH", "region_group": "eu", "api_name": "www.amazon.fr"},
    "IT (Italy)": {"id": "APJ6JRA9NG5V4", "region_group": "eu", "api_name": "www.amazon.it"},
    "NL (Netherlands)": {"id": "A1805IZSGTT6HS", "region_group": "eu", "api_name": "www.amazon.nl"},
    "SE (Sweden)": {"id": "A2NODRKZP88ZB9", "region_group": "eu", "api_name": "www.amazon.se"},
    "PL (Poland)": {"id": "A1C3SOZRARQ6R3", "region_group": "eu", "api_name": "www.amazon.pl"},
    "BE (Belgium)": {"id": "AMEN7PMS3EDWL", "region_group": "eu", "api_name": "www.amazon.com.be"},
    "TR (Turkey)": {"id": "A33AVAJ2PDY3G", "region_group": "eu", "api_name": "www.amazon.com.tr"},
    "AE (United Arab Emirates)": {"id": "A2VIGQ35RCS4UG", "region_group": "eu", "api_name": "www.amazon.ae"},
    "SA (Saudi Arabia)": {"id": "A17E79C6D8DWX5", "region_group": "eu", "api_name": "www.amazon.sa"},
    "IN (India)": {"id": "A21TJRUUN4KGV", "region_group": "eu", "api_name": "www.amazon.in"},
    "JP (Japan)": {"id": "A1VC38T7YXB528", "region_group": "fe", "api_name": "www.amazon.co.jp"},
    "AU (Australia)": {"id": "A39IBJ37V3C1DG", "region_group": "fe", "api_name": "www.amazon.com.au"},
    "SG (Singapore)": {"id": "A19VAU5U5O7RUS", "region_group": "fe", "api_name": "www.amazon.sg"},
    "CN (China)": {"id": "AAHKV2X7AFYLW", "region_group": "fe", "api_name": "www.amazon.cn"},
}

# A map to get a representative marketplace for each region to set the correct endpoint
REGION_REPRESENTATIVE_MARKETPLACE = {
    "na": Marketplaces.US,
    "eu": Marketplaces.DE,
    "fe": Marketplaces.JP,
}

# --- Core Functions ---

@st.cache_data(ttl=3600) # Cache the rates for 1 hour
def get_dynamic_conversion_rates(currencies, target_currency="INR"):
    """Fetches latest conversion rates for a list of currencies against a target currency."""
    st.info(f"Fetching conversion rates for {currencies} to {target_currency}...")
    rates = {}
    for currency in currencies:
        if currency == target_currency:
            rates[currency] = 1.0
            continue
        try:
            url = f"https://api.frankfurter.app/latest?from={currency}&to={target_currency}"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            rates[currency] = data['rates'][target_currency]
        except Exception as e:
            st.warning(f"Could not fetch rate for {currency}: {e}. Using 1.0 as fallback.")
            rates[currency] = 1.0
    st.success("Successfully fetched all required exchange rates.")
    return rates

def get_credentials_for_region(region_group: str, account_name: str):
    """
    Retrieves credentials from environment variables for a specific region group and account.
    """
    region_upper = region_group.upper()
    account_upper = account_name.upper()
    try:
        creds = {
            "lwa_app_id": os.environ[f"{account_upper}_SPAPI_CLIENT_ID"],
            "lwa_client_secret": os.environ[f"{account_upper}_SPAPI_CLIENT_SECRET"],
            "aws_secret_key": os.environ[f"{account_upper}_AWS_SECRET_ACCESS_KEY"],
            "aws_access_key": os.environ[f"{account_upper}_AWS_ACCESS_KEY_ID"],
        }
        refresh_token = os.environ[f"{account_upper}_SPAPI_REFRESH_TOKEN_{region_upper}"]
        return creds, refresh_token
    except KeyError as e:
        st.error(f"❌ Critical Error: Missing credential in .env file for account '{account_upper}' and region '{region_upper}': {e}")
        return None, None

def process_financial_events(financial_events_payload):
    """
    Parses the JSON payload from the Finances API and calculates the required financial columns.
    """
    st.info("Parsing financial events and calculating totals...")
    processed_records = []
    
    shipment_events = financial_events_payload.get('FinancialEvents', {}).get('ShipmentEventList', [])
    
    for event in shipment_events:
        order_id = event.get('AmazonOrderId')
        posted_date = event.get('PostedDate')
        marketplace_name = event.get('MarketplaceName')
        
        for item in event.get('ShipmentItemList', []):
            sku = item.get('SellerSKU')
            quantity = item.get('QuantityShipped')
            
            item_price = 0
            shipping_price = 0
            amazon_fees = 0

            for charge in item.get('ItemChargeList', []):
                charge_type = charge.get('ChargeType')
                amount = charge.get('ChargeAmount', {}).get('CurrencyAmount', 0)
                if charge_type == 'Principal':
                    item_price += amount
                elif charge_type == 'ShippingCharge':
                    shipping_price += amount
            
            for fee in item.get('ItemFeeList', []):
                amount = fee.get('FeeAmount', {}).get('CurrencyAmount', 0)
                amazon_fees += amount
            
            currency = next((c.get('ChargeAmount', {}).get('CurrencyCode') for c in item.get('ItemChargeList', []) if c.get('ChargeAmount')), None)

            total_revenue = item_price + shipping_price
            net_proceeds = total_revenue + amazon_fees
            
            processed_records.append({
                'amazon-order-id': order_id,
                'purchase-date': posted_date,
                'sales-channel': marketplace_name,
                'sku': sku,
                'quantity-purchased': quantity,
                'currency': currency,
                'Total Revenue': total_revenue,
                'Net Proceeds': net_proceeds,
                'Amazon Fees': amazon_fees,
            })
            
    if not processed_records:
        st.warning("No shipment events found in the financial data for this period.")
        return pd.DataFrame()

    df = pd.DataFrame(processed_records)
    st.success(f"Successfully parsed {len(df)} item-level financial records.")
    return df

def convert_df_to_inr(df, rates):
    """Converts financial columns of a DataFrame to INR, keeping original columns."""
    if not rates:
        st.warning("No exchange rates available. Skipping conversion.")
        return df
        
    st.info("Converting financial columns to INR...")
    
    df['rate'] = df['currency'].map(rates).fillna(1.0)
    df['Total Revenue (INR)'] = df['Total Revenue'] * df['rate']
    df['Net Proceeds (INR)'] = df['Net Proceeds'] * df['rate']
    df['Amazon Fees (INR)'] = df['Amazon Fees'] * df['rate']
    
    # Also convert expense columns if they exist
    if 'Expenses' in df.columns:
        df['Expenses (INR)'] = df['Expenses'] * df['rate']
    if 'Courier Charges' in df.columns:
        df['Courier Charges (INR)'] = df['Courier Charges'] * df['rate']
    
    df.drop(columns=['rate'], inplace=True) # Drop the helper rate column
    
    return df


def fetch_financial_data_chunk(start_date, end_date, region, account_name, status_placeholder):
    """
    Fetches a single chunk of financial events for the specified date range.
    """
    all_events_df = pd.DataFrame()
    
    status_placeholder.info(f"▶️ Processing region: {region.upper()} for chunk {start_date} to {end_date}...")
    credentials, refresh_token = get_credentials_for_region(region, account_name)
    if not credentials:
        status_placeholder.error(f"Could not retrieve credentials for account '{account_name}' and region '{region.upper()}'.")
        return None

    try:
        representative_marketplace = REGION_REPRESENTATIVE_MARKETPLACE[region]
        finances_api = Finances(credentials=credentials, refresh_token=refresh_token, marketplace=representative_marketplace)
        
        next_token = None
        
        with st.spinner(f"Fetching financial events for {region.upper()}..."):
            while True:
                st.info(f"Fetching page of events from {start_date}...")
                response = finances_api.list_financial_events(
                    PostedAfter=pd.to_datetime(start_date).isoformat(),
                    PostedBefore=pd.to_datetime(end_date).isoformat(),
                    NextToken=next_token
                )
                
                page_df = process_financial_events(response.payload)
                if not page_df.empty:
                    all_events_df = pd.concat([all_events_df, page_df], ignore_index=True)
                
                next_token = response.payload.get('FinancialEvents', {}).get('NextToken')
                if not next_token:
                    st.success("All pages for this chunk have been processed.")
                    break
                
                st.info("More events available, fetching next page...")
                time.sleep(2)

    except Exception as e:
        st.error(f"❌ An unexpected error occurred while fetching financial data for region '{region.upper()}': {str(e)}")
        return None

    return all_events_df if not all_events_df.empty else None

def fetch_financial_data_in_chunks(start_date, end_date, account_name, status_placeholder):
    """
    Splits a long date range into 30-day chunks and fetches data for each chunk across all NA and EU regions.
    """
    all_data_df = pd.DataFrame()
    
    regions_to_process = ["na", "eu"]
    st.info(f"Will process data for the following regions: {regions_to_process}")

    for region in regions_to_process:
        st.markdown(f"--- \n### Processing Region: {region.upper()} for account: {account_name.title()}")
        current_start_date = start_date
        while current_start_date <= end_date:
            chunk_end_date = current_start_date + timedelta(days=29)
            if chunk_end_date > end_date:
                chunk_end_date = end_date
            
            st.info(f"--- Processing chunk: {current_start_date.strftime('%Y-%m-%d')} to {chunk_end_date.strftime('%Y-%m-%d')} ---")
            
            chunk_df = fetch_financial_data_chunk(current_start_date, chunk_end_date, region, account_name, status_placeholder)
            
            if chunk_df is not None and not chunk_df.empty:
                all_data_df = pd.concat([all_data_df, chunk_df], ignore_index=True)

            current_start_date = chunk_end_date + timedelta(days=1)
            
            if current_start_date <= end_date:
                st.info("--- Pausing for 5 seconds to respect API rate limits... ---")
                time.sleep(5)
            
    return all_data_df

def build_dashboard(df):
    """Builds and displays the dashboard components."""
    st.header("Financial Dashboard (Values in INR)")

    # --- Data Preparation and Filters ---
    df['purchase-date-dt'] = pd.to_datetime(df['purchase-date'])
    
    st.subheader("Dashboard Filters")
    
    all_channels = ['All Channels'] + sorted(df['sales-channel'].unique().tolist())
    selected_channels = st.multiselect("Filter by Sales Channel", options=all_channels, default=['All Channels'])

    time_agg_options = {'Daily': 'D', 'Monthly': 'M', 'Quarterly': 'Q', 'Yearly': 'Y'}
    time_agg_selection = st.selectbox("Aggregate Time Period", options=list(time_agg_options.keys()))
    
    if 'All Channels' in selected_channels or not selected_channels:
        filtered_df = df
    else:
        filtered_df = df[df['sales-channel'].isin(selected_channels)]

    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return

    # --- KPIs ---
    total_revenue_inr = filtered_df['Total Revenue (INR)'].sum()
    total_fees_inr = filtered_df['Amazon Fees (INR)'].sum()
    
    expenses_sum = filtered_df['Expenses (INR)'].sum() if 'Expenses (INR)' in filtered_df.columns else 0
    courier_sum = filtered_df['Courier Charges (INR)'].sum() if 'Courier Charges (INR)' in filtered_df.columns else 0
    total_expenses_inr = expenses_sum + courier_sum
    
    total_net_inr = total_revenue_inr + total_fees_inr - total_expenses_inr # Fees are negative
    unique_orders = filtered_df['amazon-order-id'].nunique()

    st.subheader("Performance Overview")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Total Revenue", f"₹{total_revenue_inr:,.2f}")
    kpi2.metric("Amazon Fees", f"₹{total_fees_inr:,.2f}")
    kpi3.metric("Other Expenses", f"₹{total_expenses_inr:,.2f}")
    kpi4.metric("Net Proceeds", f"₹{total_net_inr:,.2f}")
    
    st.markdown("---")

    # --- Charts ---
    tab1, tab2, tab3 = st.tabs(["Performance Over Time", "By Sales Channel", "Top SKUs"])

    with tab1:
        st.subheader(f"{time_agg_selection} Performance (INR)")
        resample_code = time_agg_options[time_agg_selection]
        
        agg_cols = {'Total Revenue (INR)': 'sum', 'Net Proceeds (INR)': 'sum', 'Amazon Fees (INR)': 'sum'}
        if 'Expenses (INR)' in filtered_df.columns:
            agg_cols['Expenses (INR)'] = 'sum'
        if 'Courier Charges (INR)' in filtered_df.columns:
            agg_cols['Courier Charges (INR)'] = 'sum'

        time_data = filtered_df.set_index('purchase-date-dt').resample(resample_code).agg(agg_cols).reset_index()
        
        fig_time = px.line(time_data, x='purchase-date-dt', y=list(agg_cols.keys()),
                           title=f"Financials Over Time ({time_agg_selection})", labels={'purchase-date-dt': 'Date', 'value': 'Amount (INR)'})
        st.plotly_chart(fig_time, use_container_width=True)

    with tab2:
        st.subheader("Performance by Sales Channel (INR)")
        channel_data = filtered_df.groupby('sales-channel').agg({
            'Total Revenue (INR)': 'sum',
            'Net Proceeds (INR)': 'sum'
        }).reset_index().sort_values('Total Revenue (INR)', ascending=False)
        
        fig_channel = px.bar(channel_data, x='sales-channel', y=['Total Revenue (INR)', 'Net Proceeds (INR)'],
                             title="Revenue and Net Proceeds by Sales Channel", barmode='group',
                             labels={'sales-channel': 'Sales Channel', 'value': 'Amount (INR)'})
        st.plotly_chart(fig_channel, use_container_width=True)

    with tab3:
        st.subheader("Top 10 SKUs by Total Revenue (INR)")
        sku_data = filtered_df.groupby('sku').agg({
            'Total Revenue (INR)': 'sum',
            'quantity-purchased': 'sum',
            'Net Proceeds (INR)': 'sum'
        }).reset_index().sort_values('Total Revenue (INR)', ascending=False).head(10)
        st.dataframe(sku_data.style.format({
            'Total Revenue (INR)': '₹{:,.2f}',
            'Net Proceeds (INR)': '₹{:,.2f}'
        }))

    with st.expander("View Processed Data"):
        st.dataframe(filtered_df)

def login_page():
    st.title("🔐 Dashboard Login")
    with st.form("login_form"):
        password = st.text_input("Password", type="password")
        if st.form_submit_button("Login"):
            if password == APP_PASSWORD:
                st.session_state['authenticated'] = True
                st.rerun()
            else:
                st.error("Incorrect password.")

def main_dashboard():
    st.title("💰 Amazon SP-API Financial Dashboard")
    st.markdown("""
    This application connects to your Amazon Seller Central account using the **Finances API** to download and visualize a detailed breakdown of your order transactions. All financial values are converted to INR.
    """)
    st.markdown("---")

    # --- Sidebar for Inputs ---
    with st.sidebar:
        st.header("⚙️ Extraction Parameters")
        
        account_name = st.selectbox(
            "Select Account",
            ("Frienemy", "Aport")
        )
        
        today = date.today()
        date_range = st.date_input(
            "Select Date Range",
            value=(today - timedelta(days=30), today),
            max_value=today
        )
        
        st.header("Optional Expenses")
        expense_file = st.file_uploader(
            "Upload Courier Charges & Expenses (CSV/Excel)",
            type=['csv', 'xlsx']
        )
        
        process_button = st.button("🚀 Extract & Display Data", type="primary", use_container_width=True)

    # --- Main Panel for Output ---
    status_placeholder = st.container()

    if process_button:
        if len(date_range) != 2:
            st.warning("Please select both a start and end date.")
        else:
            start_date, end_date = date_range
            if start_date > end_date:
                st.error("Error: The start date cannot be after the end date.")
            else:
                financial_df = fetch_financial_data_in_chunks(start_date, end_date, account_name, status_placeholder)
                
                if financial_df is not None and not financial_df.empty:
                    if expense_file:
                        st.info("Processing uploaded expense file...")
                        try:
                            expense_df = pd.read_csv(expense_file) if expense_file.name.endswith('.csv') else pd.read_excel(expense_file)
                            required_cols = ['amazon-order-id', 'Expenses', 'Courier Charges']
                            if all(col in expense_df.columns for col in required_cols):
                                financial_df = pd.merge(financial_df, expense_df[required_cols], on='amazon-order-id', how='left')
                                financial_df[['Expenses', 'Courier Charges']] = financial_df[['Expenses', 'Courier Charges']].fillna(0)
                                st.success("Successfully merged expense data.")
                            else:
                                st.error(f"Expense file must contain the columns: {', '.join(required_cols)}")
                        except Exception as e:
                            st.error(f"Error processing expense file: {e}")
                    
                    unique_currencies = financial_df['currency'].unique().tolist()
                    rates = get_dynamic_conversion_rates(unique_currencies, target_currency="INR")
                    
                    converted_df = convert_df_to_inr(financial_df, rates)
                    
                    try:
                        converted_df['purchase-date'] = pd.to_datetime(converted_df['purchase-date']).dt.strftime('%d-%b-%y')
                    except Exception as e:
                        st.warning(f"Could not format 'purchase-date': {e}")
                    
                    st.session_state.financial_df = converted_df
                else:
                    st.error("Operation complete, but no financial data was loaded. Please check the logs above for errors.")
                    st.session_state.financial_df = None

    # --- Dashboard Display ---
    if 'financial_df' in st.session_state and st.session_state.financial_df is not None:
        build_dashboard(st.session_state.financial_df)
    elif not process_button:
        st.info("Select an account and a date range, then click 'Extract & Display Data' to begin.")

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
