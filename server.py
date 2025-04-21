import os
import sys
import warnings
import yaml
import requests
import json
import re
from step6_crew import run_crew_step6
from step1_crew import run_crew_step1
from step1_2_crew import run_crew_security_strategy
from step_share_class_crew import run_crew_fund_terms
from service_providers import ServiceProviderProcessor
from automation.apis.process_documents import APIClient, PDFHandler
import subprocess 
from datetime import datetime
# Suppress warnings
warnings.filterwarnings('ignore')

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
# genAIDocumentId=114
# Preprocessing steps
# Update Processed For All - once file downloaded and stored in vector database
# try:
#     UpdateProcessedForAll_payload = [doc["ActivityId"] for doc in unprocessed_documents]
#     if UpdateProcessedForAll_payload:
#         GenAI/UpdateProcessedForDoc/{genAIDocumentId}
#         UpdateProcessedForAll_url = f"/GenAI/UpdateProcessedForAll/{UpdateProcessedForAll_payload}"
#         Update_Processed_ForAll = client.post_request(endpoint=UpdateProcessedForAll_url)
#         print("Update Processed For All:", Update_Processed_ForAll)
#     else:
#         print("No documents to update.")
# except Exception as e:
#     print(f"Error updating processed documents: {e}")

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
step1_asset_dict["full_name"] = f"GenAI Test 5 - {original_name}"

# Convert back to JSON string if needed
step1_asset_result_1 = json.dumps(step1_asset_dict, indent=4)
step1_asset_result = json.loads(step1_asset_result_1)
#----------------------------Verification---------------------------------------

# step1_asset_result = {
#     "full_name": "Caligan Partners Onshore LP",
#     "abbreviation": "CPOL",
#     "date_of_inception": "2022-02-01",
#     "security_type": "Mutual Fund",
#     "strategy_value": "Long/Short Equity"
# }

# Step 1 
genAIDocumentId = 123
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
# print(batch_payload)   
# API call
response = client.post_request(endpoint=InsertDocKeyValues, payload=batch_payload)
print("Batch insert response Asset details:", response)
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

# Insert step results
try:
    InsertStepResult_payload = [
        {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True , "processMessage": "Success"}]
    Insert_Step_Result = client.post_request(endpoint=InsertStepResult, payload=InsertStepResult_payload)
    print("Inserted Step Results:", Insert_Step_Result)
except Exception as e:
    print(f"Error inserting step results: {e}")
print("Step 2: Asset creation")
# Upload extracted data
try:
    formatted_data = client.format_asset_data(step1_asset_result)
    asset_id = client.upload_asset(formatted_data)
    print("asset_id:", asset_id)
except Exception as e:
    print(f"Error uploading data: {e}")

step_name = "Asset Creation"
step_id = get_step_id_by_name(Get_All_Steps, step_name)
print(f"Step ID for Asset Creation'{step_name}':", step_id)

# Insert step results
try:
    InsertStepResult_payload = [
        {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True , "processMessage": "Success"}]
    Insert_Step_Result = client.post_request(endpoint=InsertStepResult, payload=InsertStepResult_payload)
    print("Inserted Step Results:", Insert_Step_Result)
except Exception as e:
    print(f"Error inserting step results: {e}")
#--------------------Share Class creation --------------------------------------------------------
step4_result = run_crew_fund_terms(activity_id) #share class
print(step4_result)

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

        # API call per class
        response = client.post_request(endpoint=InsertDocKeyValues, payload=batch_payload)
        print(f"Inserted values for {class_info['name']} (DocID: {current_doc_id}): {response}")
# ******Payload insert for user interface
current_iso_datetime = datetime.utcnow().isoformat()
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

    try:
        response = client.post_request(
            endpoint="/AssetShareClass/InsertOrUpdateShareClass",
            payload=share_class_payload
        )
        print(f"✅ Success: {class_info['name']} inserted. Response: {response}")
    except Exception as e:
        print(f"❌ Failed for {class_info['name']}: Exception - {e}")
# ******
step_name = "Share Class Creation"
step_id = get_step_id_by_name(Get_All_Steps, step_name)
print(f"Step ID for Returns Creation'{step_name}':", step_id)

# Insert step results
try:
    InsertStepResult_payload = [
        {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True , "processMessage": "Success"}]
    Insert_Step_Result = client.post_request(endpoint=InsertStepResult, payload=InsertStepResult_payload)
    print("Inserted Step Results:", Insert_Step_Result)
except Exception as e:
    print(f"Error inserting step results: {e}")
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
    "entityId": 56746,
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

# Loop through and send payloads
for record in step6_result['records']:
    assert_return_payload = base_payload.copy()
    assert_return_payload["valuationDate"] = record["valuationDate"]
    assert_return_payload["rorValue"] = record["rorValue"]
    assert_return_payload["entityId"] = asset_id

    try:
        response  = client.post_request(endpoint= "/AssetValuation/InsertUpdateAssetValuation", payload=assert_return_payload)
        print(f"✅ Success: {record['valuationDate']} inserted. Response: {response}")
    except Exception as e:
        print(f"❌ Failed for {record['valuationDate']}: {response.status_code} - {response.text}")
        print(f"Exception: {Exception}")
# Create a list of key-value entries
batch_payload = [
    {
        "genAIDocumentId": genAIDocumentId,
        "keyName": "returns_creation",
        "keyValue": json.dumps(step6_result['records'])
    }
]
# print(batch_payload)   
# API call
response = client.post_request(endpoint=InsertDocKeyValues, payload=batch_payload)
print("Batch insert response Asset details:", response)

step_name = "Returns Creation"
step_id = get_step_id_by_name(Get_All_Steps, step_name)
print(f"Step ID for Returns Creation'{step_name}':", step_id)

# Insert step results
try:
    InsertStepResult_payload = [
        {"activityId": activity_id, "genAIDocumentId": genAIDocumentId, "stepId": step_id, "processResult": True , "processMessage": "Success"}]
    Insert_Step_Result = client.post_request(endpoint=InsertStepResult, payload=InsertStepResult_payload)
    print("Inserted Step Results:", Insert_Step_Result)
except Exception as e:
    print(f"Error inserting step results: {e}")


# Service providers

def get_company_types():
    return client.get_request('/Assets/GetCompanyTypes')
def get_companies_by_type(company_type_id):
    return client.get_request(f'/Assets/GetCompanyByType/{company_type_id}')


def update_service_provider(verified_info):
    asset_id = 56748
    try:
        company_types = get_company_types()
        for role, matched_name in verified_info.items():
            for company_type in company_types:
                if company_type["CompanyType"] == role:
                    type_id = company_type["CompanyTypeID"]
                    companies = get_companies_by_type(type_id)
                    
                    if isinstance(matched_name, list):  # e.g., Prime Broker
                        for name in matched_name:
                            for company in companies:
                                if name.lower() in company["CompanyName"].lower():
                                    company_id = company["CompanyID"]
                                    assetCompanyXRefId = 0  # Let backend handle new insert
                                    url = f"/Assets/InsertUpdateServiceProvider?assetCompanyXRefId={assetCompanyXRefId}&CompanyId={company_id}&CompanyTypeId={type_id}&AssetId={asset_id}"
                                    response = client.post_request(endpoint=url)
                                    print(f"Updated {role} with {company['CompanyName']} => {response}")
                    else:
                        for company in companies:
                            if matched_name and matched_name.lower() in company["CompanyName"].lower():
                                company_id = company["CompanyID"]
                                assetCompanyXRefId = 0  # Let backend handle new insert
                                url = f"/Assets/InsertUpdateServiceProvider?assetCompanyXRefId={assetCompanyXRefId}&CompanyId={company_id}&CompanyTypeId={type_id}&AssetId={asset_id}"
                                response = client.post_request(endpoint=url)
                                print(f"Updated {role} with {company['CompanyName']} => {response}")
    except Exception as e:
        print(f"Exception: {e}")



def run_all():
    service_provider = ServiceProviderProcessor()
    service_provider.create_vector_store(activity_id)

    print("Extracted Providers:", service_provider.extracted_info)
    verified_info = service_provider.call_agent_to_verify()
    print("Verified by LLM:\n", verified_info)
    update_service_provider(verified_info)

run_all()


"""
print("Step3 Asset Attributes: ")
Attribute_creation_payload = {
  "assetId": upload_response,
  "assetAttributeId": 0,
  "assetAttributeHFId": 0,
  "assetAttributePEId": 0,
  "assetAttributeMSId": 0,
  "assetFamilyId": 0,
  "assetFamily": "string",
  "chartOfAccountId": 0,
  "chartOfAccount": "string",
  "operatingCurrencyId": 0,
  "operatingCurrency": "string",
  "legalStructureId": 0,
  "legalStructure": f"{extracted_json.get("ODD Rating", "Not Found")}",
  "domicileCountryId": 0,
  "domicileCountry": "string",
  "quarterlyLetter": True,
  "assetSecurityTypeId": 0,
  "assetSecurityType": "string",
  "auditCompletionDate": "2025-04-07T08:45:08.678Z",
  "investmentTypeId": 0,
  "investmentType": "string",
  "investmentPack": True,
  "assetFactSheet": True,
  "allowsSidePocket": True,
  "investsInOtherFundsId": 0,
  "investsInOtherFunds": "string",
  "exposureReportFrequencyId": 0,
  "exposureReportFrequency": "string",
  "personalCapital": "string",
  "accceptsManagedAccounts": True,
  "investsInManagedAccount": True,
  "tradingCity": "string",
  "prohibitsUSInvestors": True,
  "filingStatusId": 0,
  "filingStatus": "string",
  "keyMan": True,
  "investorLetterFrequencyId": 0,
  "investorLetterFrequency": "string",
  "perfEstimateFrequencyId": 0,
  "perfEstimateFrequency": "string",
  "bloombergId": "string",
  "reportingFrequencyId": 0,
  "reportingFrequency": "string",
  "fiscalYear": "2025-04-07T08:45:08.679Z",
  "taxId": "string",
  "cimaId": "string",
  "formDNumber": "string",
  "vintageYear": "2025-04-07T08:45:08.679Z",
  "targetFundRaise": 0,
  "estimatedFirstCloseDate": "2025-04-07T08:45:08.679Z",
  "estimatedFirstCloseAmount": 0,
  "estimatedSecondCloseDate": "2025-04-07T08:45:08.679Z",
  "estimatedSecondCloseAmount": 0,
  "offerCoInvestment": True,
  "singleDealFund": True,
  "continuationFund": True,
  "issuerId": 0,
  "issuer": "string",
  "sharesOutstanding": 0,
  "cusip": "string",
  "isin": "string",
  "managementFee": 0,
  "fundFeeExpenses": 0,
  "otherExpenses": 0,
  "expenseRatio": 0,
  "distributionFrequencyId": 0,
  "distributionFrequency": "string",
  "isMarketable": True,
  "modifiedBy": 0,
  "isClientSpecific": True
}
try:
    AttributesHF_creation  = client.post_request(endpoint= "/Assets/InsertUpdateAssetAttributesHF", payload=Attribute_creation_payload)
    print(AttributesHF_creation)
except:
    AttributesPE_creation  = client.post_request(endpoint= "/Assets/InsertUpdateAssetAttributesPE", payload=Attribute_creation_payload)
    print(AttributesPE_creation)

#Create Benchmarks
Create_bmk = {
  "assetId": upload_response,
  "benchMarkXRefId": 0,
  "benchMarkXRefTypeId": 0,
  "entityTypeId": 0,
  "entityId": upload_response,
  "isFixedRate": True,
  "bmEntityTypeId": 0,
  "bmEntityId": 0,
  "bmValue": 0,
  "isMrktEqBM": True,
  "sortOrder": 0,
  "modifiedBy": 0
}
try:
    asset_create_bmk  = client.post_request(endpoint= "/Assets/InsertUpdateBMSettings", payload=Create_bmk)
    print(asset_create_bmk)
except Exception as e:
    print(f"Exception: {Exception}")

#Create service provider
assetCompanyXRefId = 1
CompanyId = 1
CompanyTypeId = 1
create_service_provider_url = f"/Assets/InsertUpdateServiceProvider?assetCompanyXRefId={assetCompanyXRefId}&CompanyId={CompanyId}&CompanyTypeId={CompanyTypeId}&AssetId={upload_response}"

try:
    create_service_provider  = client.post_request(endpoint= create_service_provider_url)
    print(create_service_provider)
except Exception as e:
    print(f"Exception: {Exception}")


print("Step 4: share class")

shareclass_payload = {
    "shareClassId": 0,
    "shareClassName": "Default",
    "assetId": upload_response,
    "portfolioId": None,
    "inceptionDate": "2025-04-03T09:11:51.332Z",
    "effectiveDate": "2025-04-03T09:11:51.332Z",
    "minInvestment": None,
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
    "modifiedBy": 38,
    "liquidityTermsAbrev": None,
    "feeDetails": {
        "shareClassId": 0,
        "mgmtFeeTierId": 0,
        "mgmtFeeTierDesc": None,
        "mgmtFeeFrequencyId": None,
        "isMgmtFeeFreqPassThrough": False,
        "perfFeeTierId": 0,
        "perfFeeTierDesc": None,
        "perfFeePaymentFrequencyId": None,
        "perfFeeAccrualFrequencyId": None,
        "hurdleRateId": None,
        "hurdleValue": None,
        "hurdleRateBenchMarkId": 0,
        "lossRecovery": False,
        "lossRecoveryResetId": None,
        "modifiedBy": 38
    }
}
try:
    share_class_creation  = client.post_request(endpoint= "/AssetShareClass/InsertOrUpdateShareClass", payload=shareclass_payload)
    print(share_class_creation)
except Exception as e:
    print(f"Exception: {Exception}")

print("Step 5: Liquitity creation")
Liquitity_creation_payload = {
  "redemptionTermsId": 0,
  "shareClassid": 0,
  "lockType": 0,
  "penaltyPercent": 0,
  "redemptionFeePercent": 0,
  "rollingLockup": True,
  "anniversary": True,
  "redemptionFrequencyId": 0,
  "lockupFrequencyId": 0,
  "lockupStart": 0,
  "lockupEnd": 0,
  "requiredNoticeFrequencyId": 0,
  "requiredNotice": 0,
  "firstRedemptionMonth": 0,
  "investorGateFrequencyId": 0,
  "investorGatePercent": 0,
  "investorGateCapResetFrequencyId": 0,
  "investorGateMaxCapPercent": 0,
  "investorGateUseNav": True,
  "assetGateFrequencyId": 0,
  "assetGatePercent": 0,
  "notes": "string",
  "modifiedBy": 0
}

try:
    Liquitity_creation  = client.post_request(endpoint= "/Liquidity/InsertOrUpdateLiquidityRedemptionTerms", payload=Liquitity_creation_payload)
    print(Liquitity_creation)
except Exception as e:
    print(f"Exception: {Exception}")
"""