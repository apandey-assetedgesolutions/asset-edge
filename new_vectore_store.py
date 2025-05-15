import os
import glob
import json
import time
from datetime import timedelta
from dotenv import load_dotenv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from unstructured.partition.pdf import partition_pdf
from langchain.docstore.document import Document
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma

# Load environment variables
load_dotenv()
embedding = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))

base_folder = "data"
client_folders = [
    client for client in os.listdir(base_folder)
    if os.path.isdir(os.path.join(base_folder, client)) and not client.startswith('.')
]

def minimal_metadata(file_path, page_number):
    return {
        "page_number": page_number,
        "source_file": os.path.basename(file_path),
        "source": file_path
    }

def clean_element(el):
    val = el.to_dict()

    if 'metadata' in val:
        for key in [
            'coordinates', 'system', 'layout_width', 'layout_height',
            'last_modified', 'filetype', 'languages',
            'detection_class_prob', 'image_base64','page_number'
        ]:
            val['metadata'].pop(key, None)

    val.pop('element_id', None)
    return val

def save_grouped_pages_to_json(page_dict, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(page_dict, f, indent=2)
    print(f"📁 Grouped page content saved to: {output_file}")

def process_pdf_grouped_by_page(file_path):
    documents = []
    page_grouped = defaultdict(list)
    print(f"🧵 [Thread] Starting PDF: {file_path}")

    try:
        chunks = partition_pdf(
            filename=file_path,
            infer_table_structure=True,
            strategy="hi_res",
            extract_image_block_types=["Image"],
            extract_image_block_to_payload=True,
            chunking_strategy="by_title",
            max_characters=10000,
            combine_text_under_n_chars=2000,
            new_after_n_chars=6000
        )
        print(f"🔍 Parsed {len(chunks)} chunks from {os.path.basename(file_path)}")

        for chunk in chunks:
            if not chunk.metadata.orig_elements:
                continue

            for el in chunk.metadata.orig_elements:
                try:
                    page_num = getattr(el.metadata, "page_number", -1)
                    cleaned = clean_element(el)
                    # page_num = cleaned.get("metadata", {}).get("page_number", -1)
                    page_grouped[page_num].append(cleaned)
                except Exception as e:
                    print(f"⚠️ Error cleaning element: {e}")

        # Save grouped pages for inspection
        json_filename = os.path.splitext(os.path.basename(file_path))[0] + "_grouped_by_page.json"
        save_grouped_pages_to_json(page_grouped, json_filename)

        # Create one Document per page
        for page_number, elements in page_grouped.items():
            doc = Document(
                page_content=json.dumps(elements, ensure_ascii=False),
                metadata=minimal_metadata(file_path, page_number)
            )
            documents.append(doc)

    except Exception as e:
        print(f"❌ Error processing PDF {file_path}: {e}")

    return documents

# Main processing loop
for client in client_folders:
    print(f"\n📂 Processing client: {client}")
    start_time = time.time()

    client_path = os.path.join(base_folder, client)
    pdf_files = glob.glob(os.path.join(client_path, "*.pdf"))
    all_documents = []

    if not pdf_files:
        print(f"⚠️ No PDF files found for client {client}. Skipping...")
        continue

    print(f"📝 Found {len(pdf_files)} PDF files. Starting parallel processing...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_pdf_grouped_by_page, pdf): pdf for pdf in pdf_files}
        total_files = len(futures)
        completed = 0

        for future in as_completed(futures):
            pdf_path = futures[future]
            try:
                result = future.result()
                all_documents.extend(result)
                completed += 1

                elapsed = time.time() - start_time
                avg_per_file = elapsed / completed
                remaining = avg_per_file * (total_files - completed)

                print(f"✅ [Done] {os.path.basename(pdf_path)}: {len(result)} pages loaded.")
                print(f"⏱️ Elapsed: {timedelta(seconds=int(elapsed))}, Estimated remaining: {timedelta(seconds=int(remaining))}")

            except Exception as e:
                print(f"❌ Thread error on {pdf_path}: {e}")

    if not all_documents:
        print(f"⚠️ No documents found for {client}. Skipping...")
        continue

    persist_dir = os.path.join("chroma_db_2", client)
    Chroma.from_documents(
        documents=all_documents,
        embedding=embedding,
        persist_directory=persist_dir
    )

    total_time = time.time() - start_time
    print(f"🟩 [Client Summary] {client}: {len(all_documents)} page-docs stored to ChromaDB.")
    print(f"⏳ Time taken for {client}: {timedelta(seconds=int(total_time))}")

print("\n🟢 All clients processed successfully! Pages stored to ChromaDB.")
