import os
import yaml
from langchain_openai import OpenAIEmbeddings ,ChatOpenAI
from langchain_openai import AzureChatOpenAI
from langchain_huggingface import HuggingFaceEmbeddings
from automation.apis.process_documents import APIClient, PDFHandler

from dotenv import load_dotenv
load_dotenv()

os.environ["AZURE_API_KEY"] = os.getenv('AZURE_OPENAI_API_KEY')
os.environ["AZURE_API_BASE"] = os.getenv('AZURE_OPENAI_ENDPOINT')
os.environ["OPENAI_API_VERSION"] = "2023-03-15"
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

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
sp_get_company_types = config['apis']['service_provider_get_company_types']
sp_get_companies_by_type = config['apis']['service_provider_get_companies_by_type']


client = APIClient()
token = client.authenticate(email=user_email)

def choose_LLM(llm_model):
    llm = ""
    if llm_model == 1:
        llm = ChatOpenAI(
            model="gpt-4o-mini",
        )
    else:
        llm = AzureChatOpenAI(
            deployment_name="gpt-4o-mini",
            model_name="azure/gpt-4o-mini",
            temperature=0.9,
            top_p=0.9
        )
    return llm


def choose_Embeddings(embedding_model):
    embeddings = ""
    if embedding_model == 1:
        embeddings = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))
    else:
        embeddings = HuggingFaceEmbeddings(
            model_name='sentence-transformers/all-MiniLM-L12-v2',
            model_kwargs={'device': 'cpu'}
        )
    return embeddings

llm_model = config["model"]["openai_llm"]
embedding_model = config["model"]["openai_embeddings"]

llm = choose_LLM(llm_model)
embedding = choose_Embeddings(embedding_model)

print(f"Choosen LLM MODEL is : {llm}\n")
print(f"Choosen Embedding MODEL is : {embedding}\n")