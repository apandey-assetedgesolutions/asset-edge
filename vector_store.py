import os
from dotenv import load_dotenv
import glob
from langchain_community.document_loaders import PyPDFLoader
from langchain_unstructured  import UnstructuredLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain.docstore.document import Document

# Load environment variables
load_dotenv()

# Base folder where client folders are stored
base_folder = "data"

# Get list of client folders
client_folders = [
    client for client in os.listdir(base_folder)
    if os.path.isdir(os.path.join(base_folder, client)) and not client.startswith('.')
]

# Initialize OpenAI Embeddings
embedding = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))

# Minimal metadata builder
def minimal_metadata(doc, file_path, page_label=None):
    return {
        "page_label": page_label if page_label is not None else -1,
        "source_file": os.path.basename(file_path),
        "source": file_path
    }

# Loop through each client folder
for client in client_folders:
    print(f"\n📂 Processing client: {client}")
    
    client_folder_path = os.path.join(base_folder, client)
    
    # Gather PDF and XLSX files
    pdf_files = glob.glob(os.path.join(client_folder_path, "*.pdf"))
    xlsx_files = glob.glob(os.path.join(client_folder_path, "*.xlsx"))
    file_list = pdf_files + xlsx_files

    documents = []

    for file_path in file_list:
        try:
            if file_path.lower().endswith(".pdf"):
                loader = PyPDFLoader(file_path)
                page_docs = loader.load_and_split()

                for i, doc in enumerate(page_docs):
                    doc.metadata = minimal_metadata(doc, file_path, page_label=i + 1)

                documents.extend(page_docs)
                print(f"✅ Loaded PDF: {file_path}")

            elif file_path.lower().endswith(".xlsx"):
                loader = UnstructuredLoader(file_path)
                xlsx_docs = loader.load()

                for doc in xlsx_docs:
                    doc.metadata = minimal_metadata(doc, file_path, page_label=None)

                documents.extend(xlsx_docs)
                print(f"✅ Loaded XLSX: {file_path}")

        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")

    if not documents:
        print(f"⚠️ No documents found for {client}. Skipping...")
        continue

    # Apply chunking only for XLSX (if needed)
    non_pdf_docs = [doc for doc in documents if doc.metadata["page_label"] == -1]
    pdf_docs = [doc for doc in documents if doc.metadata["page_label"] != -1]

    if non_pdf_docs:
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
        split_non_pdf_docs = text_splitter.split_documents(non_pdf_docs)
    else:
        split_non_pdf_docs = []

    # Merge all
    final_docs = pdf_docs + split_non_pdf_docs

    # Persist into Chroma
    persist_directory = os.path.join("chroma_db", client)
    vector_db = Chroma.from_documents(
        documents=final_docs,
        embedding=embedding,
        persist_directory=persist_directory
    )

    print(f"✅ Client {client} stored in Chroma DB: {persist_directory}")

print("\n🎉 All client folders processed successfully!")
