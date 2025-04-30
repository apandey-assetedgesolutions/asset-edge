import os
import sys
import warnings
import yaml
import requests
import json
import re
from dotenv import load_dotenv
from step6_crew import run_crew_step6
from step1_crew import run_crew_step1
from step1_2_crew import run_crew_security_strategy
from step_share_class_crew import run_crew_fund_terms
from step_liquidity_terms_crew import run_liquidity_terms_crew
from step_service_providers import run_company_validation_crew
from automation.apis.process_documents import APIClient, PDFHandler
import subprocess 
from datetime import datetime
import openlit

# Function to store data for later review instead of immediate API submission
def store_data_for_review(data_type, payload, endpoint=None,source_details=""):
    """
    Store data for later review instead of immediate API submission
    
    Args:
        data_type: Type of data being stored (string)
        payload: Data to be stored
        endpoint: API endpoint where this data would be sent (optional)
    
    Returns:
        Dict containing the data type, payload, and endpoint
    """
    return {
        "data_type": data_type,
        "payload": payload,
        "endpoint": endpoint,
        "source_details":source_details
    }

# Initialize global storage for all data to be reviewed
all_collected_data = []

load_dotenv()
# Suppress warnings
warnings.filterwarnings('ignore')
import base64

LANGFUSE_PUBLIC_KEY=os.getenv("LANGFUSE_PUBLIC_KEY")
LANGFUSE_SECRET_KEY=os.getenv("LANGFUSE_SECRET_KEY")
LANGFUSE_AUTH=base64.b64encode(f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode()).decode()

os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://cloud.langfuse.com/api/public/otel" # EU data region
# os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://us.cloud.langfuse.com/api/public/otel" # US data region
os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = f"Authorization=Basic {LANGFUSE_AUTH}"

openlit.init()
# Load configuration
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

# Extract configurations
user_email = config["usercred"]["user"]
unprocessed_docs_endpoint = config["apis"]["unprocessed_documents"]
get_document = config["apis"]["get_documents"]
InsertDocKeyValues = config["apis"]["InsertDocKeyValues"]
GetAllSteps = config["apis"]["GetAllSteps"]
InsertStepResult = config["apis"]["InsertStepResult"]
dropdown_asset_types = config["apis"]["dropdown_asset_types"]
dropdown_strategy = config["apis"]["dropdown_strategy"]
# Liquidity Terms Api
liquidity_lock_type_ep = config["apis"]["liquidity_lock_type"]
liquidity_required_notice_frequency_ep = config["apis"]["liquidity_required_notice_frequency"]
liquidity_lockup_frequency_ep = config["apis"]["liquidity_lockup_frequency"]
liquidity_investor_gate_frequency_ep = config["apis"]["liquidity_investor_gate_frequency"]
liquidity_redemption_frequency_ep = config["apis"]["liquidity_redemption_frequency"]

# Initialize API client
client = APIClient()

print("Document Processing Pipeline")
# Authenticate User
token = client.authenticate(email=user_email)
if not token:
    print("Authentication failed. Please check credentials.")
    exit(1)
print("Authentication successful!")

# Fetch unprocessed documents
unprocessed_documents = client.get_document_id(unprocessed_docs_endpoint)
for doc in unprocessed_documents:
    try:
        document_id = doc.get("DocumentId")
        if not document_id:
            print("Skipping entry due to missing DocumentId")
            continue

        response = client.make_request(f"{get_document}/{document_id}")
        document_response = response
        output_path = doc.get("ActivityId")
        document_name = document_response.get("DocumentName")
        document_content = document_response.get("DocumentContent")

        if output_path and document_content and document_name:
            # PDFHandler.save_base64_as_pdf(document_content, output_path, document_name)
            print(f"Saved document: {output_path}/{document_name}")
        else:
            print(f"Warning: Missing DocumentName or DocumentContent for ID {document_id}")

    except requests.exceptions.RequestException as e:
        print(f"Request error while processing document {document_id}: {e}")
    except KeyError as e:
        print(f"Missing expected key in response for Document ID {document_id}: {e}")
    except Exception as e:
        print(f"Unexpected error processing Document ID {document_id}: {e}")

# subprocess.run([sys.executable, "vector_store.py"], check=True)
print("Vector store updated successfully.")

if not unprocessed_documents:
    print("No unprocessed documents found.")
else:
    print("Unprocessed Documents:", unprocessed_documents)

activity_id = "1863"
# Step 1 Starts: Dropdown API Calls
all_asset_types = client.get_request(dropdown_asset_types)
print("all_asset_types:", all_asset_types)
asset_type_names = [asset['AssetTypeName'] for asset in all_asset_types]
# print("asset_type_names:", asset_type_names)
all_strategy = client.get_request(dropdown_strategy)
print("all_strategy:", all_strategy)
strategy_values = [strategy['ClassificationValue'] for strategy in all_strategy]
# print("strategy_values:", strategy_values)

step1_result = run_crew_step1(activity_id)
step1_2_result = run_crew_security_strategy(activity_id,asset_type_names,strategy_values)
print(f"step1_result: {step1_result}")
print(f"step1_2_result: {step1_2_result}")
data1 = step1_result.to_dict()
data2 = step1_2_result.to_dict()

def get_ids(data2, all_strategy, all_asset_types):
    result = {
        "security_type_id": None,
        "strategy_value_id": None
    }

    # Loop to find security_type_id
    for item in all_strategy:
        if item["ClassificationValue"] == data2.get("strategy_value"):
            result["strategy_value_id"] = item["ClassificationId"]
            break

    # Loop to find strategy_value_id
    for item in all_asset_types:
        if item["AssetTypeName"] == data2.get("security_type"):
            result["security_type_id"] = item["AssetTypeId"]
            break

    return result

id_str_type = get_ids(data2, all_strategy, all_asset_types)
data2.update(id_str_type)

combined_result = {**data1, **data2}
# step1_asset_result = json.dumps(combined_result, indent=4)
#----------------------------Verification---------------------------------------
step1_asset_result = json.dumps(combined_result, indent=4)

# Convert JSON string back to dict before using `.get()`
step1_asset_dict = json.loads(step1_asset_result)

# Modify the full_name
original_name = step1_asset_dict.get("full_name", "")
step1_asset_dict["full_name"] = f"GenAI Test 2 - {original_name}"

# Convert back to JSON string if needed
step1_asset_result_1 = json.dumps(step1_asset_dict, indent=4)
step1_asset_result = json.loads(step1_asset_result_1)

# Step 1 
genAIDocumentId = 510
# Create a list of key-value entries
batch_payload = [
    {
        "genAIDocumentId": genAIDocumentId,
        "keyName": key,
        "keyValue": value
    }
    for key, value in step1_asset_result.items()
    if key not in {"security_type_id", "strategy_value_id"}
]

# # Store data instead of API call
# all_collected_data.append(store_data_for_review(
#     "", 
#     batch_payload,
#     InsertDocKeyValues
# ))

# Fetch all steps
try:
    Get_All_Steps = client.get_request(GetAllSteps)
    print("Fetched Steps:", Get_All_Steps)
except Exception as e:
    print(f"Error fetching steps: {e}")

def get_step_id_by_name(steps_list, step_name):
    for step in steps_list:
        if step['StepName'] == step_name:
            return step['StepId']
    return None

step_name = "Name Value Pair Insert"
step_id = get_step_id_by_name(Get_All_Steps, step_name)
print(f"Step ID for '{step_name}':", step_id)

# Store step results instead of API call
InsertStepResult_payload = [
    {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True, "processMessage": "Success"}]

# all_collected_data.append(store_data_for_review(
#     "", 
#     InsertStepResult_payload,
#     InsertStepResult
# ))

print("Step 2: Asset creation")
# Generate asset data, but store for review instead of uploading
formatted_data = client.format_asset_data(step1_asset_result)
all_collected_data.append(store_data_for_review(
    "Asset Creation Data", 
    formatted_data,
    "upload_asset", # Special handling for this method call
    step1_asset_result
))

# Store a fixed asset ID for now (will be updated after actual API call)
asset_id = 6000  # Placeholder value

step_name = "Asset Creation"
step_id = get_step_id_by_name(Get_All_Steps, step_name)
print(f"Step ID for Asset Creation'{step_name}':", step_id)

# Store step results instead of API call
InsertStepResult_payload = [
    {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True, "processMessage": "Success"}]

# all_collected_data.append(store_data_for_review(
#     "", 
#     InsertStepResult_payload,
#     InsertStepResult
# ))

#--------------------Share Class creation --------------------------------------------------------
step4_result = run_crew_fund_terms(activity_id) #share class
print(f"Step 4 Result : {step4_result}")

# Prepare a new list to store cleaned classes
cleaned_classes = []
for cls in step4_result['classes']:
    cleaned_class = {
        'name': cls['name'],
        'management_fee': float(cls['management_fee'].replace('%', '')),
        'performance_fee': float(cls['performance_fee'].replace('%', '')),
        'hurdle_value': float(cls['hurdle_value'].split('%')[0]),
        'minimum_investment': int(cls['minimum_investment'].replace('$', '').replace(',', ''))
    }
    cleaned_classes.append(cleaned_class)

# Store each share class key-value pairs
for idx, class_info in enumerate(cleaned_classes):
    current_doc_id = genAIDocumentId + idx

    batch_payload = [
        {
            "genAIDocumentId": current_doc_id,
            "keyName": key,
            "keyValue": value
        }
        for key, value in class_info.items()
    ]

    # Store data instead of API call
    # all_collected_data.append(store_data_for_review(
    #     f"", 
    #     batch_payload,
    #     InsertDocKeyValues
    # ))

# Store share class payloads
current_iso_datetime = datetime.utcnow().isoformat()
share_class_payloads = []

for idx, class_info in enumerate(cleaned_classes):
    is_default = (idx == 0)

    share_class_payload = {
        "shareClassId": 0,
        "shareClassName": class_info["name"],
        "assetId": asset_id,
        "portfolioId": None,
        "isDefault": is_default,
        "inceptionDate": current_iso_datetime,
        "effectiveDate": current_iso_datetime,
        "minInvestment": class_info["minimum_investment"],
        "subscriptionFrequencyId": None,
        "subscriptionCurrencyIdList": "",
        "taxReportingId": None,
        "votingShares": False,
        "newIssues": False,
        "trackingFrequencyId": None,
        "trackingById": None,
        "accredited": False,
        "qualifiedPurchaser": False,
        "qualifiedClient": False,
        "initialNAV": None,
        "businessDays": False,
        "modifiedBy": 0,
        "liquidityTermsAbrev": None,
        "feeDetails": {
            "shareClassId": 0,
            "mgmtFeeTierId": 0,
            "mgmtFeeTierDesc": None,
            "mgmtFee": str(class_info["management_fee"]),
            "mgmtFeeFrequencyId": None,
            "isMgmtFeeFreqPassThrough": False,
            "perfFeeTierId": 0,
            "perfFeeTierDesc": None,
            "perfFee": str(class_info["performance_fee"]),
            "perfFeePaymentFrequencyId": None,
            "perfFeeAccrualFrequencyId": None,
            "hurdleRateId": None,
            "hurdleValue": str(class_info["hurdle_value"]),
            "hurdleRateBenchMarkId": 0,
            "lossRecovery": False,
            "lossRecoveryResetId": None,
            "modifiedBy": 0
        }
    }
    
    share_class_payloads.append(share_class_payload)

share_class_creation = step4_result.to_dict()
step_4_share_class = json.dumps(share_class_creation, indent=4)
step_4_share_class = json.loads(step_4_share_class)

# Store share class creation data
all_collected_data.append(store_data_for_review(
    "Share Class Creation", 
    share_class_payloads,
    "share_class",
    step_4_share_class
))

step_name = "Share Class Creation"
step_id = get_step_id_by_name(Get_All_Steps, step_name)
print(f"Step ID for Share Class Creation'{step_name}':", step_id)

# Store step results instead of API call
InsertStepResult_payload = [
    {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True, "processMessage": "Success"}]

# all_collected_data.append(store_data_for_review(
#     "", 
#     InsertStepResult_payload,
#     InsertStepResult
# ))

#----------------------------------------------------------------------------
print("Liquidity Terms Creation")
all_liquidity_lock = client.get_request(liquidity_lock_type_ep)
lock_type = [lock_type["EnumName"] for lock_type in all_liquidity_lock]
print(lock_type)

all_notice_frequency = client.get_request(liquidity_required_notice_frequency_ep)
notice_frequency_type = [notice_frequency["EnumName"] for notice_frequency in all_notice_frequency]
print(notice_frequency_type)

all_lockup_frequency = client.get_request(liquidity_lockup_frequency_ep)
lockup_frequency_type = [lockup_frequency["EnumName"] for lockup_frequency in all_lockup_frequency]
print(lockup_frequency_type)

all_redemption_frequency = client.get_request(liquidity_redemption_frequency_ep)
redemption_frequency_type = [redemption_frequency["FrequencyName"] for redemption_frequency in all_redemption_frequency]
print(redemption_frequency_type)

all_investor_gate_frequency = client.get_request(liquidity_investor_gate_frequency_ep)
investor_gate_frequency_type = [investor_gate_frequency["FrequencyName"] for investor_gate_frequency in all_investor_gate_frequency]
print(investor_gate_frequency_type)

liquidity_terms_result = run_liquidity_terms_crew(
    activity_id,
    lock_type,
    notice_frequency_type,
    lockup_frequency_type,
    redemption_frequency_type,
    investor_gate_frequency_type
)
print(f"Liquidity Terms Response : {liquidity_terms_result}")

cleaned_liquidity_classes = []

for cls in liquidity_terms_result['classes']:
    cleaned_class = {
        'name': cls.get('name'),  
        'required_notice': int(cls['required_notice']),
        'notice_frequency': cls['notice_frequency'],
        'redemption_frequency': cls['redemption_frequency'],
        'lockup_types': cls['lockup_types'],
        'lockup_frequency': cls['lockup_frequency'],
        'investor_gate_percent': float(cls['investor_gate_percent'].replace('%', '')),
        'investor_gate_frequency': cls['investor_gate_frequency']
    }
    cleaned_liquidity_classes.append(cleaned_class)

# Store each liquidity class key-value pairs
for idx, class_info in enumerate(cleaned_liquidity_classes):
    current_doc_id = genAIDocumentId + idx

    batch_payload = [
        {
            "genAIDocumentId": current_doc_id,
            "keyName": key,
            "keyValue": value
        }
        for key, value in class_info.items()
    ]

    # Store data instead of API call
    # all_collected_data.append(store_data_for_review(
    #     f"", 
    #     batch_payload,
    #     InsertDocKeyValues
    # ))

def get_enum_id(all_data, frequency_name):
    for frequency in all_data:
        if frequency["EnumName"] == frequency_name:
            return frequency["EnumValue"]
    return None

def get_frequency_id(all_data, frequency_name):
    for frequency in all_data:
        if frequency["FrequencyName"] == frequency_name:
            return frequency["FrequencyId"]
    return None

# This API call would normally be done here but we'll just store a placeholder
# for now since we don't have the actual asset_id yet
liquidity_shared_cls_Ids = []  # Placeholder for now

# For now, associate each liquidity class with each share class based on name
liquidity_terms_payloads = []

for liquidity_class in cleaned_liquidity_classes:
    class_name = liquidity_class.get("name")
    requiredNoticeFrequencyName = liquidity_class.get("notice_frequency")
    redemptionFrequencyName = liquidity_class.get("redemption_frequency")
    lockTypeName = liquidity_class.get("lockup_types")
    lockupFrequencyName = liquidity_class.get("lockup_frequency")
    investorGateFrequencyName = liquidity_class.get("investor_gate_frequency")
    investorGatePercent = liquidity_class.get("investor_gate_percent")
    requiredNotice = liquidity_class.get("required_notice")

    requiredNoticeFrequencyId = get_enum_id(all_notice_frequency, requiredNoticeFrequencyName)
    lockupFrequencyId = get_enum_id(all_lockup_frequency, lockupFrequencyName)
    lockType = get_enum_id(all_liquidity_lock, lockTypeName)
    redemptionFrequencyId = get_frequency_id(all_redemption_frequency, redemptionFrequencyName)
    investorGateFrequencyId = get_frequency_id(all_investor_gate_frequency, investorGateFrequencyName)

    # We'll use a placeholder shareClassId for now
    shareClassid = 0  # Will be updated in Phase Two
    
    liquidity_share_class_payload = {
        "redemptionTermsId": 0,
        "shareClassid": shareClassid,
        "lockType": lockType,
        "penaltyPercent": 0,
        "redemptionFeePercent": 0,
        "rollingLockup": False,
        "anniversary": False,
        "redemptionFrequencyId": redemptionFrequencyId,
        "lockupFrequencyId": lockupFrequencyId,
        "lockupStart": 0,
        "lockupEnd": 0,
        "requiredNoticeFrequencyId": requiredNoticeFrequencyId,
        "requiredNotice": requiredNotice,
        "firstRedemptionMonth": 3,
        "investorGateFrequencyId": investorGateFrequencyId,
        "investorGatePercent": investorGatePercent,
        "investorGateCapResetFrequencyId": 0,
        "investorGateMaxCapPercent": 0,
        "investorGateUseNav": False,
        "assetGateFrequencyId": 0,
        "assetGatePercent": 0,
        "notes": "string",
        "modifiedBy": 0
    }
    
    liquidity_terms_payloads.append({
        "class_name": class_name,
        "payload": liquidity_share_class_payload
    })

liquidity_terms_result_1 = liquidity_terms_result.to_dict()
liquidity_terms_result_1 = json.dumps(liquidity_terms_result_1, indent=4)
liquidity_terms_result_1 = json.loads(liquidity_terms_result_1)

# Store liquidity terms creation data
all_collected_data.append(store_data_for_review(
    "Liquidity Terms Creation", 
    liquidity_terms_payloads,
    "liquidity_terms",
    liquidity_terms_result_1
))

step_name = "Liquidity Creation"
step_id = get_step_id_by_name(Get_All_Steps, step_name)
print(f"Step ID for Liquidity Creation'{step_name}':", step_id)

# Store step results instead of API call
InsertStepResult_payload = [
    {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True, "processMessage": "Success"}]

# all_collected_data.append(store_data_for_review(
#     "", 
#     InsertStepResult_payload,
#     InsertStepResult
# ))

#----------------------------------------------------------------------------
print("Step 6: Asset returns creation")
step6_result = run_crew_step6(activity_id)
print(f"step1_result: {step1_result}")
print(f"step1_2_result: {step1_2_result}")
print(f"step4_result: {step4_result}")
print(f"step6_result: {step6_result}")

base_payload = {
    "rorValuationId": 0,
    "navValuationId": 0,
    "entityTypeId": 1,
    "entityId": asset_id,  # Will be updated with actual asset_id
    "entityName": "string",
    "frequencyId": 3,
    "valuationDate": "",  # will be updated
    "rorValue": 0.0,      # will be updated
    "navValue": 0,
    "estimateActual": "string",
    "modifiedBy": 0,
    "modifiedByName": "string",
    "modifiedDate": "2025-04-10T13:03:24.491Z",
    "entityMasterId": 0
}

# Prepare asset returns payloads
asset_returns_payloads = []
for record in step6_result['records']:
    assert_return_payload = base_payload.copy()
    assert_return_payload["valuationDate"] = record["valuationDate"]
    assert_return_payload["rorValue"] = record["rorValue"]
    
    asset_returns_payloads.append(assert_return_payload)

# Store asset returns data
all_collected_data.append(store_data_for_review(
    "Asset Returns Creation", 
    asset_returns_payloads,
    "/AssetValuation/InsertUpdateAssetValuation"
))

# Store returns key-value pairs
batch_payload = [
    {
        "genAIDocumentId": genAIDocumentId,
        "keyName": "returns_creation",
        "keyValue": json.dumps(step6_result['records'])
    }
]

# all_collected_data.append(store_data_for_review(
#     "", 
#     batch_payload,
#     InsertDocKeyValues
# ))

step_name = "Returns Creation"
step_id = get_step_id_by_name(Get_All_Steps, step_name)
print(f"Step ID for Returns Creation'{step_name}':", step_id)

# Store step results instead of API call
InsertStepResult_payload = [
    {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True, "processMessage": "Success"}]

# all_collected_data.append(store_data_for_review(
#     "", 
#     InsertStepResult_payload,
#     InsertStepResult
# ))

# Service providers
#----------------------------------------------------------------------------
def get_company_types():
    return client.get_request('/Assets/GetCompanyTypes')
def get_companies_by_type(company_type_id):
    return client.get_request(f'/Assets/GetCompanyByType/{company_type_id}')

service_provider_response = run_company_validation_crew(activity_id)
print(service_provider_response)

company_types = get_company_types()

service_provider_payloads = []
for company_type in company_types:
    res_company_names = service_provider_response[f'{company_type['CompanyType']}']
    type_id = company_type['CompanyTypeID']
    if not res_company_names:
        print(f"{company_type['CompanyType']} is empty")
    else:
        company_names = get_companies_by_type(type_id)
        for company_name in company_names:
            if company_name['CompanyName'] in res_company_names:
                company_id = company_name['CompanyID']
                service_provider_payloads.append({
                    "company_type": company_type["CompanyType"],
                    "company_id": company_id,
                    "company_type_id": type_id,
                    "url": f"/Assets/InsertUpdateServiceProvider?assetCompanyXRefId=0&CompanyId={company_id}&CompanyTypeId={type_id}&AssetId={asset_id}"
                })

# Store service provider data
all_collected_data.append(store_data_for_review(
    "Service Providers", 
    service_provider_payloads,
    "service_providers"  # Special handling for these URLs
))

# Save all collected data to a file
with open('collected_data.json', 'w') as f:
    json.dump(all_collected_data, f, indent=4)

print("\n\nPhase One Complete!")
print(f"All data has been collected and stored in 'collected_data.json'")
print(f"Total collection items: {len(all_collected_data)}")
print("You can now review this data before proceeding to Phase Two for API submissions.")