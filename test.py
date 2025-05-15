
import yaml
from automation.apis.process_documents import APIClient, PDFHandler
from step_liquidity_terms_crew import run_liquidity_terms_crew
from step1_2_crew import run_crew_security_strategy

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
user_email = config["usercred"]["user"]
dropdown_asset_types = config["apis"]["dropdown_asset_types"]
dropdown_strategy = config["apis"]["dropdown_strategy"]

client = APIClient()
token = client.authenticate(email=user_email)


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

step1_2_result = run_crew_security_strategy(activity_id,asset_type_names,strategy_values)
print(f"step1_2_result: {step1_2_result}")