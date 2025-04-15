import os
import requests
import json
import yaml
import base64
import warnings
from datetime import datetime, timezone

formatted_time = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'


with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)


base_url = config["apis"]["base_url"]
get_document = config["apis"]["get_documents"]
auth_endpoint = config["apis"]["authentication"]
create_asset = config['apis']['upload_asset_API_ENDPOINT']


# unprocessed_docs_url = f"{base_url}/{unprocessed_docs_endpoint}"


warnings.filterwarnings('ignore')

class APIClient:
    def __init__(self, base_url=base_url):
        self.base_url = base_url
        self.access_token = None

    def authenticate(self, email, param_value="string", is_addin=True):
        """Authenticate and retrieve the access token."""
        url = f"{self.base_url}{auth_endpoint}"
        payload = {
            "paramValue": param_value,
            "emailAddress": email,
            "isAddin": str(is_addin).lower()
        }
        headers = {
            "accept": "*/*",
            "Content-Type": "application/json-patch+json"
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
            response.raise_for_status()
            auth_response = response.json()
            self.access_token = auth_response.get("token")
            print("Authentication successful.")
            return self.access_token
        except requests.exceptions.RequestException as e:
            print("Authentication failed:", e)
            return None

    def make_request(self, endpoint, method="GET", payload=None):
        """Make an authenticated request to the specified endpoint."""
        if not self.access_token:
            print("Access token not available. Please authenticate first.")
            return None
        
        url = f"{self.base_url}{endpoint}"
        # print(url)
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        try:
            if method == "POST":
                response = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
            else:
                response = requests.get(url, headers=headers, data=json.dumps(payload), verify=False)
            
            # response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request to {endpoint} failed:", e)
            return None
    
    def get_document_id(self, endpoint, payload=None):
        """Make an authenticated request to the specified endpoint."""
        if not self.access_token:
            print("Access token not available. Please authenticate first.")
            return None

        url = f"{self.base_url}{endpoint}"
        # print(f"Requesting: {url}")

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }

        try:
            response = requests.get(url, headers=headers, verify=False)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request to {url} failed: {e}")
            return {"error": str(e)}

    def format_asset_data(self, extracted_data):
        
        current_time = datetime.now(timezone.utc).isoformat()
        is_active = 1
        return {
            "assetId": 0,
            "securityTypeId": f"{extracted_data.get('security_type_id', 0)}",
            "securityType": f"{extracted_data.get('security_type', 'Not Found')}",
            "assetClassId": 0,
            "assetClass": "string",
            "assetName": f"{extracted_data.get('full_name', 'Not Found')}",
            "strategyId": f"{extracted_data.get('strategy_value_id', 0)}",
            "strategy": f"{extracted_data.get('strategy_value', 'Not Found')}",
            "substrategyId": 0,
            "substrategy": "string",
            "strategyDescription": "string",
            "abbrName": f"{extracted_data.get('abbreviation', 'Not Found')}",
            "effectiveDate": f"{extracted_data.get('date_of_inception', 'Not Found')}",
            "overrideStrategy": "string",
            "investmentStatusId": 0,
            "investmentStatus": "string",
            "bloombergId": "string",
            "valuationDate": current_time,
            "regionId": 0,
            "region": "string",
            "subregionId": 0,
            "subregion": "string",
            "assetStatusId": 0,
            "assetStatus": "string",
            "primaryAnalystId": 0,
            "primaryAnalyst": "string",
            "secondaryAnalystId": 0,
            "secondaryAnalyst": "string",
            "sectorId": 0,
            "sector": "string",
            "subSectorId": 0,
            "subSector": "string",
            "accountTypeId": 0,
            "primaryBM": "string",
            "secondaryBM": "string",
            "modifiedBy": 0,
            "managerContactId": 0,
            "managerContact": "string",
            "returnsList": [
                {
                    "dateType": "string",
                    "returnAmount": 0,
                    "returnDate": current_time,
                    "returnFreq": 1,
                    "isFinal": "true",
                    "roR": 0,
                    "roRNullable": 0,
                    "entityName": "string",
                    "reportDate": current_time,
                    "date": current_time,
                    "entityId": 0
                }
            ],
            "assetsList": [
                {
                    "assetType": "string",
                    "assetValue": 0,
                    "assetId": 0,
                    "assetName": f"{extracted_data.get('full_name', 'Not Found')}",
                    "accountTypeId": 0,
                    "isMarketable": "true"
                }
            ],
            "portfolioHoldings": [
                {
                    "familyName": "string",
                    "basketId": 0,
                    "basketName": "string",
                    "investmentValue": 0
                }
            ],
            "transactionList": [
                {
                    "transTypeId": 0,
                    "transName": "string",
                    "transDate": "2025-03-18T10:42:03.951Z",
                    "transValue": 0
                }
            ],
            "rptDate": "2025-03-18T10:42:03.951Z",
            "ytdTWR": 0,
            "oneYrTWR": 0,
            "threeYrTWR": 0,
            "fiveYrTWR": 0,
            "itdtwr": 0,
            "itdStdDev": 0,
            "iddRating": "string",
            "iddRatingValue": 0,
            "iddDate": "2025-03-18T10:42:03.951Z",
            "IsActive": is_active,
            "oddRating": "string",
            "oddRatingValue": 0,
            "oddDate": "2025-03-18T10:42:03.951Z",
            "isClientSpecific": "true"
        }
    
    def upload_asset(self, asset_data):
        url = f"{self.base_url}{create_asset}"
        print(url)
        headers = {
            'accept': '*/*',
            'Authorization': f"Bearer {self.access_token}",
            'Content-Type': 'application/json-patch+json'
        }
        response = requests.post(url, headers=headers, data=json.dumps(asset_data), verify=False)
        # Debugging output
        print(f"Status Code: {response.status_code}")
        print(f"Response Text: {response.text}")

        # Check if response has content before calling .json()
        if response.status_code != 200:
            raise ValueError(f"API Error: {response.status_code}, Response: {response.text}")

        try:
            return response.json()
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON response: {response.text}")
    
        
    def get_request(self, endpoint, payload=None):
        """Make an authenticated request to the specified endpoint."""
        if not self.access_token:
            print("Access token not available. Please authenticate first.")
            return None
        
        url = f"{self.base_url}{endpoint}"
        print(url)
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        try:
            # if method == "POST":
            #     response = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
            # else:
            response = requests.get(url, headers=headers, data=json.dumps(payload), verify=False)
            
            # response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request to {endpoint} failed:", e)
            return None
    
    def post_request(self, endpoint, payload=None):
        """Make an authenticated request to the specified endpoint."""
        if not self.access_token:
            print("Access token not available. Please authenticate first.")
            return None
        
        url = f"{self.base_url}{endpoint}"
        print(url)
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        try:
            # if method == "POST":
            response = requests.post(url, headers=headers, data=json.dumps(payload), verify=False)
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request to {endpoint} failed:", e)
            return None

        


class PDFHandler:
    @staticmethod
    def save_base64_as_pdf(base64_content, output_path, document_name):
        try:
            # Convert output_path to string (in case it's numeric)
            output_folder = os.path.join("data", str(output_path)) 
            
            # Create the output directory if it doesn't exist
            os.makedirs(output_folder, exist_ok=True)
            
            # Decode the base64 content
            pdf_data = base64.b64decode(base64_content)
            
            # Build the full file path
            file_path = os.path.join(output_folder, document_name)
            
            # Save the PDF
            with open(file_path, "wb") as pdf_file:
                pdf_file.write(pdf_data)
            
            print(f"PDF saved successfully at: {file_path}")
        except Exception as e:
            print(f"Failed to save PDF: {e}")



