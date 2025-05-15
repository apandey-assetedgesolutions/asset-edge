import streamlit as st
import json
import os
import yaml
import requests
from automation.apis.process_documents import APIClient
from dotenv import load_dotenv
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

load_dotenv()

def load_config():
    try:
        with open("config.yaml", "r") as file:
            return yaml.safe_load(file)
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"Error loading config file: {e}")
        return None

config = load_config()
if not config:
    print("Configuration file is missing or invalid.")
    exit(1)



# Set page configuration and theme
st.set_page_config(
    page_title="Fund Data Review & Submission",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state variables
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'current_step' not in st.session_state:
    st.session_state.current_step = 0
if 'asset_id' not in st.session_state:
    st.session_state.asset_id = None
if 'uploaded_steps' not in st.session_state:
    st.session_state.uploaded_steps = set()
if 'active_section' not in st.session_state:
    st.session_state.active_section = None

# Function to set active section
def set_active_section(idx):
    st.session_state.active_section = idx

# Initialize API client
client = APIClient()

# Example API endpoints
liquidity_lock_type_ep = config["apis"]["liquidity_lock_type"]
liquidity_required_notice_frequency_ep = config["apis"]["liquidity_required_notice_frequency"]
liquidity_lockup_frequency_ep = config["apis"]["liquidity_lockup_frequency"]
liquidity_investor_gate_frequency_ep = config["apis"]["liquidity_investor_gate_frequency"]
liquidity_redemption_frequency_ep = config["apis"]["liquidity_redemption_frequency"]


# Authenticate User (Pre-filled for the example)
user_email = "scoughlin@assetedgesolutions.com"
token = client.authenticate(email=user_email)
if not token:
    st.error("Authentication failed. Please check credentials.")
    st.stop()
st.session_state.authenticated = True

# CSS for custom styling
st.markdown("""
<style>
    .success-message {
        padding: 10px;
        background-color: #d4edda;
        color: #155724;
        border-radius: 5px;
        margin-bottom: 10px;
    }
    .sidebar-header {
        font-size: 1.2em;
        font-weight: bold;
        margin-bottom: 20px;
    }
    .uploaded-section {
        color: #28a745;
    }
    .current-section {
        color: #007bff;
    }
    .main-header {
        color: #343a40;
        margin-bottom: 20px;
    }
    .sub-header {
        color: #495057;
        margin: 15px 0;
    }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("""
        <style>
            div.stButton > button {
                text-align: left !important;
                justify-content: flex-start !important;
            }
        </style>
    """, unsafe_allow_html=True)


# Load data if authenticated
if st.session_state.authenticated and not st.session_state.data_loaded:
    try:
        with open('collected_data.json', 'r') as f:
            all_data = json.load(f)
        st.session_state.all_data = all_data
        st.session_state.data_loaded = True
        st.session_state.total_steps = len(all_data)
        # Set active section to first section if not set
        if st.session_state.active_section is None and len(all_data) > 0:
            st.session_state.active_section = 0
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.stop()

# Function to handle submission of all remaining sections
def handle_submit_all_remaining():
    all_data = st.session_state.all_data
    remaining_indices = [i for i in range(len(all_data)) if i not in st.session_state.uploaded_steps]
    
    if not remaining_indices:
        st.success("All sections have already been uploaded!")
        return
    
    errors = []
    
    with st.spinner("Submitting all remaining sections..."):
        for idx in remaining_indices:
            step_data = all_data[idx]
            success = handle_section_submission(step_data, step_data)
            
            if success:
                st.session_state.uploaded_steps.add(idx)
            else:
                errors.append(f"Failed to submit {step_data['data_type']}")
    
    if errors:
        st.error("Encountered errors during submission:")
        for error in errors:
            st.write(f"- {error}")
    else:
        st.success("All sections submitted successfully!")

def add_track_result_db(section_data):
    for track in section_data['track_result']:
        client.post_request(endpoint=track['endpoint'],payload=track['payload'])

# Function to handle submission of a specific section
def handle_section_submission(section_data, edited_data=None):
    endpoint = section_data['endpoint']
    payload = edited_data['payload'] if edited_data else section_data['payload']
    
    try:
        # Handle the case where we need to call the asset creation method
        if endpoint == "upload_asset":
            asset_id = client.upload_asset(payload)
            add_track_result_db(section_data)
            if asset_id:
                st.session_state.asset_id = asset_id
                return True
            else:
                st.error("Failed to create asset")
                return False
        
        # Handle service providers case
        elif endpoint == "service_providers":
            all_success = True
            for provider in payload:
                # Update the URL to use the actual asset_id
                if st.session_state.asset_id:

                    url = provider['url']
                    parsed_url = urlparse(url)
                    query_params = parse_qs(parsed_url.query)

                    # Update the CompanyId
                    query_params['AssetId'] = [st.session_state.asset_id]
                    query_params['CompanyId'] = [provider['company_id']] 
                    # Rebuild the URL
                    new_query = urlencode(query_params, doseq=True)
                    new_url = urlunparse(parsed_url._replace(query=new_query))
                    response = client.post_request(endpoint=new_url)
                    add_track_result_db(section_data)
                    if not response:
                        st.error(f"Failed to add service provider: {provider['company_type']}")
                        all_success = False
                        
            return all_success
        
        elif endpoint == "share_class":
            for pay in payload:
                if st.session_state.asset_id:
                    new_payload = pay
                    new_payload["assetId"] = st.session_state.asset_id
                    response = client.post_request(
                        endpoint="/AssetShareClass/InsertOrUpdateShareClass",
                        payload=new_payload
                    )
                    add_track_result_db(section_data)
                    if not response:
                        st.error(f"Failed to add service provider: {response}")                                    
                        return False
            
            return True
        
        elif endpoint == "liquidity_terms":
            liquidity_shared_cls_Ids = client.get_request(f"/AssetShareClass/GetShareClassListByAssetId/{st.session_state.asset_id},false")
            for pay in payload:
                for liquidity_shared_cls in liquidity_shared_cls_Ids:
                    if liquidity_shared_cls['ShareClassName'] == pay['class_name']:
                        shareClassid = liquidity_shared_cls['ShareClassId']
                        new_payload = pay['payload']
                        new_payload['shareClassid'] = shareClassid
                        response = client.post_request(
                            endpoint="/Liquidity/InsertOrUpdateLiquidityRedemptionTerms",
                            payload=new_payload
                        )
                        add_track_result_db(section_data)
                        if not response:
                            st.error(f"Failed to add service provider: {response}")                                    
                            return False
            return True

        # Handle regular API calls
        else:
            # If we have an asset_id, update any references to it
            if st.session_state.asset_id and isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict) and 'assetId' in item:
                        item['assetId'] = st.session_state.asset_id
                    elif isinstance(item, dict) and 'entityId' in item:
                        item['entityId'] = st.session_state.asset_id
                        response = client.post_request(endpoint=endpoint, payload=item)
                        add_track_result_db(section_data)
                        if not response:
                            st.error(f"Failed to add service provider: {response}")                                    
                            return False
        
            return True
        
    except Exception as e:
        st.error(f"Error submitting data: {e}")
        return False

# Function to display and enable editing of section data based on data type
def display_section_data(section_data, is_readonly=False):
    data_type = section_data['data_type']
    payload = section_data['payload']
    source = section_data['source_details']
    edited_data = {'data_type': data_type, 'endpoint': section_data['endpoint'], 'payload': payload}
    
    # Display data editor based on data type
    if 'Key-Value Pairs' in data_type:
        edited_data['payload'] = display_key_value_pairs(payload, is_readonly)
    
    elif 'Asset Creation Data' in data_type:
        edited_data['payload'] = display_asset_creation(payload,source,is_readonly)
    
    elif 'Share Class Creation' in data_type:
        edited_data['payload'] = display_share_classes(payload,source, is_readonly)
    
    elif 'Liquidity Terms Creation' in data_type:
        edited_data['payload'] = display_liquidity_terms(payload,source, is_readonly)
    
    elif 'Asset Returns Creation' in data_type:
        edited_data['payload'] = display_asset_returns(payload, is_readonly)
    
    elif 'Service Providers' in data_type:
        edited_data['payload'] = display_service_providers(payload, is_readonly)

    else:
        # For other data types, just display as JSON
        st.json(payload)
    
    return edited_data

def display_key_value_pairs(payload, is_readonly):
    if isinstance(payload, list):
        data_df = pd.DataFrame([{"Key": item.get("keyName", ""), "Value": item.get("keyValue", "")} 
                for item in payload 
                if "keyName" in item or "keyValue" in item])
        
        if not data_df.empty:
            edited_df = st.data_editor(data_df, disabled=is_readonly, use_container_width=True)
            
            # Update the original data with edited values if not readonly
            if not is_readonly:
                for i, row in edited_df.iterrows():
                    if i < len(payload):
                        payload[i]["keyValue"] = row["Value"]
    else:
        st.write("Key-Value data is not in expected format.")
        st.json(payload)
    
    return payload

def display_asset_creation(payload, source, is_readonly):
    if isinstance(payload, dict):
        source_details = source

        # Create form columns for better layout
        col1, col2 = st.columns(2)

        with col1:
            asset_name = st.text_input(
                "Asset Name",
                payload.get('assetName', ''),
                disabled=is_readonly,
                help=f"Source: {source_details.get('full_name_source', 'N/A')},\n Page: {source_details.get('full_name_source_page_label', 'N/A')}"
            )

            security_type = st.text_input(
                "Security Type",
                payload.get('securityType', ''),
                disabled=is_readonly,
                help=f"Source: {source_details.get('security_type_source', 'N/A')}, Page: {source_details.get('security_type_source_page_label', 'N/A')}"
            )

            strategy = st.text_input(
                "Strategy",
                payload.get('strategy', ''),
                disabled=is_readonly,
                help=f"Source: {source_details.get('strategy_value_source', 'N/A')}, Page: {source_details.get('strategy_value_source_page_label', 'N/A')}"
            )

        with col2:
            abbr_name = st.text_input(
                "Abbreviation Name",
                payload.get('abbrName', ''),
                disabled=is_readonly,
                help=f"Source: {source_details.get('abbreviation_source', 'N/A')}, Page: {source_details.get('abbreviation_source_page_label', 'N/A')}"
            )

            effective_date = st.text_input(
                "Inception Date",
                payload.get('effectiveDate', ''),
                disabled=is_readonly,
                help=f"Source: {source_details.get('date_of_inception_source', 'N/A')}, Page: {source_details.get('date_of_inception_source_page_label', 'N/A')}"
            )

        # Update payload with edited values
        if not is_readonly:
            payload['assetName'] = asset_name
            payload['abbrName'] = abbr_name
            payload['securityType'] = security_type
            payload['effectiveDate'] = effective_date
            payload['strategy'] = strategy
    else:
        st.write("Asset data is not in expected format.")
        st.json(payload)

    return payload

def display_share_classes(payload, source, is_readonly):
    if isinstance(payload, list):
        if len(payload) == 0:
            st.warning("No share classes found for this section.")
        else:
            for i, share_class in enumerate(payload):
                share_class_name = share_class.get('shareClassName', '')
                if not share_class_name:
                    continue

                source_info = source.get('classes', [])[i] if i < len(source.get('classes', [])) else {}

                with st.expander(f"Share Class {i+1}: {share_class_name}", expanded=True):
                    col1, col2 = st.columns(2)

                    with col1:
                        name = st.text_input(
                            "Share Class Name",
                            share_class_name,
                            key=f"sc_name_{i}",
                            disabled=is_readonly,
                            help=f"Source: {source_info.get('name_source_file', 'N/A')}, Page: {source_info.get('name_source_page_label', 'N/A')}"
                        )
                        min_investment = st.text_input(
                            "Min Investment",
                            share_class.get('minInvestment', ''),
                            key=f"sc_min_{i}",
                            disabled=is_readonly,
                            help=f"Source: {source_info.get('minimum_investment_source_file', 'N/A')}, Page: {source_info.get('minimum_investment_source_page_label', 'N/A')}"
                        )

                    with col2:
                        fee_details = share_class.get('feeDetails', {})

                        mgmt_fee = st.text_input(
                            "Management Fee",
                            fee_details.get('mgmtFee', ''),
                            key=f"sc_mgmt_{i}",
                            disabled=is_readonly,
                            help=f"Source: {source_info.get('management_fee_source_file', 'N/A')}, Page: {source_info.get('management_fee_source_page_label', 'N/A')}"
                        )
                        perf_fee = st.text_input(
                            "Performance Fee",
                            fee_details.get('perfFee', ''),
                            key=f"sc_perf_{i}",
                            disabled=is_readonly,
                            help=f"Source: {source_info.get('performance_fee_source_file', 'N/A')}, Page: {source_info.get('performance_fee_source_page_label', 'N/A')}"
                        )
                        hurdle = st.text_input(
                            "Hurdle Value",
                            fee_details.get('hurdleValue', ''),
                            key=f"sc_hurdle_{i}",
                            disabled=is_readonly,
                            help=f"Source: {source_info.get('hurdle_value_source_file', 'N/A')}, Page: {source_info.get('hurdle_value_source_page_label', 'N/A')}"
                        )

                    if not is_readonly:
                        payload[i]['shareClassName'] = name
                        payload[i]['minInvestment'] = min_investment

                        if 'feeDetails' not in payload[i]:
                            payload[i]['feeDetails'] = {}

                        payload[i]['feeDetails']['mgmtFee'] = mgmt_fee
                        payload[i]['feeDetails']['perfFee'] = perf_fee
                        payload[i]['feeDetails']['hurdleValue'] = hurdle
    else:
        st.warning("Payload is not in the expected list format for Share Classes.")
        st.json(payload)

    return payload
import streamlit as st

def display_liquidity_terms(payload, source, is_readonly):
    # Fetch necessary data from APIs and check for None
    all_liquidity_lock = client.get_request(liquidity_lock_type_ep)
    if all_liquidity_lock is None:
        print("API call failed for liquidity lock types.")
        all_liquidity_lock = []

    all_notice_frequency = client.get_request(liquidity_required_notice_frequency_ep)
    if all_notice_frequency is None:
        print("API call failed for notice frequencies.")
        all_notice_frequency = []

    all_lockup_frequency = client.get_request(liquidity_lockup_frequency_ep)
    if all_lockup_frequency is None:
        print("API call failed for lockup frequencies.")
        all_lockup_frequency = []

    all_redemption_frequency = client.get_request(liquidity_redemption_frequency_ep)
    if all_redemption_frequency is None:
        print("API call failed for redemption frequencies.")
        all_redemption_frequency = []

    all_investor_gate_frequency = client.get_request(liquidity_investor_gate_frequency_ep)
    if all_investor_gate_frequency is None:
        print("API call failed for investor gate frequencies.")
        all_investor_gate_frequency = []

    # Map Enum values to names
    lock_type_dict = {lock["EnumValue"]: lock["EnumName"] for lock in all_liquidity_lock}
    notice_frequency_dict = {notice["EnumValue"]: notice["EnumName"] for notice in all_notice_frequency}
    lockup_frequency_dict = {lockup["EnumValue"]: lockup["EnumName"] for lockup in all_lockup_frequency}
    redemption_frequency_dict = {redemption["FrequencyId"]: redemption["FrequencyName"] for redemption in all_redemption_frequency}
    investor_gate_frequency_dict = {gate["FrequencyId"]: gate["FrequencyName"] for gate in all_investor_gate_frequency}

    if isinstance(payload, list):
        for i, liquidity_class in enumerate(payload):
            class_name = liquidity_class.get('class_name', f'Class {i+1}')
            with st.expander(f"Liquidity Terms: {class_name}", expanded=True):
                term_payload = liquidity_class.get('payload', {})
                source_info = source.get('classes', [])[i] if i < len(source.get('classes', [])) else {}

                # Columns for UI
                col1, col2 = st.columns(2)

                with col1:
                    # Handle Required Notice (numeric input, not frequency)
                    required_notice = term_payload.get('requiredNotice', '')
                    required_notice_frequency = term_payload.get('noticeFrequency', '')
                    selected_notice = notice_frequency_dict.get(required_notice_frequency, '')

                    required_notice_freq = st.text_input(
                        "Required Notice (Days)",
                        value=required_notice,
                        key=f"lt_notice_{i}",
                        disabled=is_readonly,
                        help=f"Source: {source_info.get('required_notice_source_file', 'N/A')}, "
                             f"Page: {source_info.get('required_notice_source_page_label', 'N/A')}"
                    )

                    # Handle Lock Type (dropdown)
                    lock_type = term_payload.get('lockType', '')
                    selected_lock = lock_type_dict.get(lock_type, '')

                    lock_type_selected = st.selectbox(
                        "Lock Type",
                        options=list(lock_type_dict.values()),
                        index=list(lock_type_dict.values()).index(selected_lock) if selected_lock else 0,
                        key=f"lt_lock_type_{i}",
                        disabled=is_readonly,
                        help=f"Source: {source_info.get('lockup_types_source_file', 'N/A')}, "
                             f"Page: {source_info.get('lockup_types_source_page_label', 'N/A')}"
                    )

                    # Handle Investor Gate Percent (text input)
                    investor_gate_percent = st.text_input(
                        "Investor Gate Percent",
                        term_payload.get('investorGatePercent', ''),
                        key=f"lt_gate_percent_{i}",
                        disabled=is_readonly,
                        help=f"Source: {source_info.get('investor_gate_percent_source_file', 'N/A')}, "
                             f"Page: {source_info.get('investor_gate_percent_source_page_label', 'N/A')}"
                    )

                                        # Handle Redemption Frequency (dropdown)
                    redemption_frequency = term_payload.get('redemptionFrequencyId', '')
                    selected_redemption = redemption_frequency_dict.get(redemption_frequency, '')

                    redemption_freq = st.selectbox(
                        "Redemption Frequency",
                        options=list(redemption_frequency_dict.values()),
                        index=list(redemption_frequency_dict.values()).index(selected_redemption) if selected_redemption else 0,
                        key=f"lt_redemption_freq_{i}",
                        disabled=is_readonly,
                        help=f"Source: {source_info.get('redemption_frequency_source_file', 'N/A')}, "
                             f"Page: {source_info.get('redemption_frequency_source_page_label', 'N/A')}"
                    )

                with col2:
                    # Handle Notice Frequency (dropdown, different from required notice)
                    selected_notice_freq = st.selectbox(
                        "Notice Frequency",
                        options=list(notice_frequency_dict.values()),
                        index=list(notice_frequency_dict.values()).index(selected_notice) if selected_notice else 0,
                        key=f"lt_notice_freq_{i}",
                        disabled=is_readonly,
                        help=f"Source: {source_info.get('required_notice_source_file', 'N/A')}, "
                             f"Page: {source_info.get('required_notice_source_page_label', 'N/A')}"
                    )

                    # Handle Lockup Frequency (dropdown)
                    lockup_frequency = term_payload.get('lockupFrequency', '')
                    selected_lockup = lockup_frequency_dict.get(lockup_frequency, '')

                    lockup_freq = st.selectbox(
                        "Lockup Frequency",
                        options=list(lockup_frequency_dict.values()),
                        index=list(lockup_frequency_dict.values()).index(selected_lockup) if selected_lockup else 0,
                        key=f"lt_lockup_freq_{i}",
                        disabled=is_readonly,
                        help=f"Source: {source_info.get('lockup_frequency_source_file', 'N/A')}, "
                             f"Page: {source_info.get('lockup_frequency_source_page_label', 'N/A')}"
                    )

                    # Handle Investor Gate Frequency (dropdown)
                    investor_gate_frequency = term_payload.get('investorGateFrequency', '')
                    selected_investor_gate = investor_gate_frequency_dict.get(investor_gate_frequency, '')

                    investor_gate_freq = st.selectbox(
                        "Investor Gate Frequency",
                        options=list(investor_gate_frequency_dict.values()),
                        index=list(investor_gate_frequency_dict.values()).index(selected_investor_gate) if selected_investor_gate else 0,
                        key=f"lt_gate_freq_{i}",
                        disabled=is_readonly,
                        help=f"Source: {source_info.get('investor_gate_frequency_source_file', 'N/A')}, "
                             f"Page: {source_info.get('investor_gate_frequency_source_page_label', 'N/A')}"
                    )

                # If not in readonly mode, update the payload with the selected values
                if not is_readonly:

                  # Reverse lookup: find EnumValue from EnumName
                    notice_freq_id = next((k for k, v in notice_frequency_dict.items() if v == selected_notice_freq), '')
                    lock_type_id = next((k for k, v in lock_type_dict.items() if v == lock_type_selected), '')
                    redemption_freq_id = next((k for k, v in redemption_frequency_dict.items() if v == redemption_freq), '')
                    lockup_freq_id = next((k for k, v in lockup_frequency_dict.items() if v == lockup_freq), '')
                    investor_gate_freq_id = next((k for k, v in investor_gate_frequency_dict.items() if v == investor_gate_freq), '')
                   
                    # st.write(f"lock_type_selected :{payload[i]['payload']} {lock_type_id}")
                    # st.write(f"selected_notice_freq : {notice_freq_id}")

                    payload[i]['payload']['requiredNotice'] = required_notice_freq
                    payload[i]['payload']['requiredNoticeFrequencyId'] = notice_freq_id
                    payload[i]['payload']['redemptionFrequencyId'] = redemption_freq_id
                    payload[i]['payload']['lockType'] = lock_type_id
                    payload[i]['payload']['lockupFrequencyId'] = lockup_freq_id
                    payload[i]['payload']['investorGatePercent'] = investor_gate_percent
                    payload[i]['payload']['investorGateFrequencyId'] = investor_gate_freq_id


    else:
        st.json(payload)

    return payload


def display_asset_returns(payload, is_readonly):
    if isinstance(payload, list):
        returns_data = []
        for return_entry in payload:
            returns_data.append({
                "Date": return_entry.get('valuationDate', ''),
                "Return Value": return_entry.get('rorValue', '')
            })
        
        returns_df = pd.DataFrame(returns_data)
        edited_returns = st.data_editor(returns_df, disabled=is_readonly, use_container_width=True)
        
        # Update the original payload if not readonly
        if not is_readonly:
            for i, (_, row) in enumerate(edited_returns.iterrows()):
                if i < len(payload):
                    payload[i]['valuationDate'] = row['Date']
                    payload[i]['rorValue'] = row['Return Value']
    else:
        st.json(payload)
    
    return payload

def get_company_types():
    return client.get_request('/Assets/GetCompanyTypes')

def get_companies_by_type(company_type_id):
    return client.get_request(f'/Assets/GetCompanyByType/{company_type_id}')

def display_service_providers(payload, is_readonly):
    if not isinstance(payload, list):
        st.write("Service provider data is not in expected format.")
        st.json(payload)
        return payload

    st.subheader("Service Providers")
    grouped = {}

    for provider in payload:
        company_type = provider.get('company_type', '')
        grouped.setdefault(company_type, []).append(provider)

    # Temporary holder for deletion
    if 'delete_index' not in st.session_state:
        st.session_state.delete_index = None

    for company_type, providers in grouped.items():
        with st.expander(f"Company Type: {company_type}", expanded=True):
            for idx, provider in enumerate(providers):
                company_type_id = provider.get('company_type_id', '')
                company_id = str(provider.get('company_id', ''))

                companies = get_companies_by_type(company_type_id)
                options = {str(c['CompanyID']): c['CompanyName'] for c in companies}
                option_keys = list(options.keys())
                selected_id = company_id if company_id in option_keys else (option_keys[0] if option_keys else '')

                cols = st.columns([4, 1])  # Dropdown + delete button

                with cols[0]:
                    selected_company_id = st.selectbox(
                        label=f"Company Name {idx + 1}",
                        options=option_keys,
                        format_func=lambda x: options.get(x, x),
                        index=option_keys.index(selected_id) if selected_id in option_keys else 0,
                        key=f"{company_type}_{idx}",
                        disabled=is_readonly
                    )

                    if not is_readonly:
                        provider['company_id'] = selected_company_id

                with cols[1]:
                    if not is_readonly:
                        if st.button("🗑️", key=f"remove_btn_{company_type}_{idx}"):
                            st.session_state.delete_index = (company_type, idx)

            # Confirm delete if any
            if st.session_state.delete_index and st.session_state.delete_index[0] == company_type:
                del_idx = st.session_state.delete_index[1]
                if del_idx < len(providers):
                    confirm_cols = st.columns([3, 1, 1])
                    with confirm_cols[0]:
                        st.warning(f"Confirm delete for Company Name {del_idx + 1}?")
                    with confirm_cols[1]:
                        if st.button("Yes", key=f"confirm_delete_{company_type}_{del_idx}"):
                            item_to_remove = providers[del_idx]
                            grouped[company_type].pop(del_idx)
                            if item_to_remove in payload:
                                payload.remove(item_to_remove)
                            st.session_state.delete_index = None
                            st.rerun()
                    with confirm_cols[2]:
                        if st.button("No", key=f"cancel_delete_{company_type}_{del_idx}"):
                            st.session_state.delete_index = None

            # Add new provider
            if not is_readonly:
                if st.button(f"➕ Add Provider to {company_type}", key=f"add_{company_type}"):
                    companies = get_companies_by_type(providers[0]['company_type_id']) if providers else []
                    first_company = companies[0] if companies else {'CompanyID': '', 'CompanyName': ''}
                    new_provider = {
                        'company_type': company_type,
                        'company_type_id': providers[0]['company_type_id'] if providers else '',
                        'company_id': str(first_company['CompanyID']),
                        'url': f"/Assets/InsertUpdateServiceProvider?assetCompanyXRefId=0&CompanyId={first_company['CompanyID']}&CompanyTypeId={providers[0]['company_type_id']}&AssetId=6000"

                    }
                    payload.append(new_provider)
                    grouped[company_type].append(new_provider)
                    st.rerun()

    return payload


# Main application layout with sidebar
if st.session_state.authenticated and st.session_state.data_loaded:
    all_data = st.session_state.all_data
    
    # Filter out sections with empty or whitespace-only data_type
    filtered_data = [section for section in all_data if section['data_type'] and section['data_type'].strip()]
    
    # Sidebar for navigation
    with st.sidebar:
        st.markdown('<div class="sidebar-header">Fund Data Sections</div>', unsafe_allow_html=True)
        
        # Create navigation sidebar with standard buttons
        for idx, section_data in enumerate(filtered_data):
            section_title = section_data['data_type']
            
            # Determine section status (uploaded, current, or normal)
            if idx in st.session_state.uploaded_steps:
                button_label = f"✅ {section_title}"
                button_type = "secondary"
            elif idx == st.session_state.active_section:
                button_label = f"➡️ {section_title}"
                button_type = "primary"
            else:
                button_label = f"📋 {section_title}"
                button_type = "secondary"
            
            if st.button(button_label, key=f"section_btn_{idx}", use_container_width=True, type=button_type):
                set_active_section(idx)
                st.rerun()

        # Add a divider and overall progress
        st.divider()
        progress_percentage = len(st.session_state.uploaded_steps) / len(filtered_data) * 100
        st.progress(progress_percentage / 100)
        st.write(f"Overall Progress: {len(st.session_state.uploaded_steps)}/{len(filtered_data)} sections")
        
        # Submit all remaining button in sidebar
        # if st.button("Submit All Remaining Sections", use_container_width=True):
        #     handle_submit_all_remaining()

    # Main content area
    st.title("Fund Data Review & Submission")
    st.write("Review, edit, and confirm the data before submitting it to the database. Note that the displayed page number may vary by ±1.")
    
    # Display currently selected section
    if st.session_state.active_section is not None:
        # Ensure active section is within bounds of filtered_data
        section_idx = st.session_state.active_section
        if section_idx < len(filtered_data):
            section_data = filtered_data[section_idx]
            section_title = section_data['data_type']

            # Section header
            st.header(f"{section_title}", anchor=f"section-{section_idx}")
            
            # Check if section is already uploaded
            is_uploaded = section_idx in st.session_state.uploaded_steps
            
            if is_uploaded:
                st.success("✅ This section has been successfully uploaded to the database.")
            
            # Display and allow editing of the section data
            edited_data = display_section_data(section_data, is_uploaded)
            
            # Section navigation and action buttons
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                if section_idx > 0:
                    if st.button("⬅️ Previous Section", key=f"prev_btn_{section_idx}"):
                        set_active_section(section_idx - 1)
                        st.rerun()
            
            with col3:
                if section_idx < len(filtered_data) - 1:
                    if st.button("Next Section ➡️", key=f"next_btn_{section_idx}"):
                        set_active_section(section_idx + 1)
                        st.rerun()
            
            with col2:
                if not is_uploaded:
                    if st.button("Upload This Section", use_container_width=True, type="primary", 
                               key=f"upload_btn_{section_idx}"):
                        success = handle_section_submission(section_data, edited_data)
                        if success:
                            st.session_state.uploaded_steps.add(section_idx)
                            st.success(f"Successfully uploaded {section_title}")
                            st.rerun()
                else:
                    st.button("✅ Uploaded", disabled=True, use_container_width=True, 
                            key=f"uploaded_btn_{section_idx}")
        else:
            st.warning("Selected section is no longer available.")
