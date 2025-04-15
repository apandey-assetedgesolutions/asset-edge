import os
from dotenv import load_dotenv
import glob
from langchain_community.document_loaders import UnstructuredFileLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma

load_dotenv()
# Set the base directory where your client folders are located
base_folder = "data"
# Define the list of client folders
client_folders = [
    client for client in os.listdir(base_folder)
    if os.path.isdir(os.path.join(base_folder, client)) and not client.startswith('.')
]

# Set the base directory where your client folders are located
base_folder = "data"
# Instantiate the embeddings model
embedding = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))

# Loop through each client folder
for client in client_folders:
    print(f"Processing client: {client}")
    
    # Define the client folder path
    client_folder_path = os.path.join(base_folder, client)
    
    # Get all PDF and XLSX files in this folder
    pdf_files = glob.glob(os.path.join(client_folder_path, "*.pdf"))
    xlsx_files = glob.glob(os.path.join(client_folder_path, "*.xlsx"))
    file_list = pdf_files + xlsx_files
    
    # Initialize an empty list to hold documents
    documents = []
    
    # Process each file individually
    for file_path in file_list:
        try:
            # Use UnstructuredFileLoader to load the file
            loader = UnstructuredFileLoader(file_path)
            docs = loader.load()  # Returns list of Document objects
            documents.extend(docs)
            print(f"Loaded file: {file_path}")
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
    
    if not documents:
        print(f"No documents found for {client}. Moving to next folder.")
        continue

    # Split documents into chunks
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    split_docs = text_splitter.split_documents(documents)
    
    # Define persistence directory for this client's Chroma collection
    persist_directory = os.path.join("chroma_db", client)
    
    # Create and persist the Chroma vector store
    vector_db = Chroma.from_documents(
        documents=split_docs,
        embedding=embedding,
        persist_directory=persist_directory
    )
    
    print(f"Client {client} processed and stored in Chroma vector store at: {persist_directory}\n")

print("All client folders have been processed.")