import os
import streamlit as st
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
import pandas as pd

# Load environment variables
load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

# Sidebar menu
st.sidebar.title("Navigation")
menu_option = st.sidebar.radio("Go to", ["📄 All Documents", "🔍 Similarity Search"])

# Common: Select collection
collection_names = ["1863", "2105", "2106"]
selected_collection = st.sidebar.selectbox("Select a Collection", collection_names)

# Cached vector DB loader
@st.cache_resource
def load_vector_db(collection_name):
    embeddings = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))
    return Chroma(
        embedding_function=embeddings,
        persist_directory=f"./chroma_db/{collection_name}",
    )

vector_db = load_vector_db(selected_collection)

# -------------------------------------
# 📄 All Documents Page
# -------------------------------------
if menu_option == "📄 All Documents":
    st.title(f"📄 Documents in Collection: {selected_collection}")
    try:
        ids = vector_db.get().get("ids", [])
        if ids:
            docs = vector_db.get_by_ids(ids)
            # Group by source
            grouped_docs = {}
            for doc in docs:
                source = doc.metadata.get("source", "Unknown")
                grouped_docs.setdefault(source, []).append(doc)
            
            for source, docs_group in grouped_docs.items():
                chunk_count = len(docs_group)
                with st.expander(f"Source: {source} ({chunk_count} chunks)"):
                    data = [{"Page Content": doc.page_content} for doc in docs_group]
                    df = pd.DataFrame(data)
                    st.table(df)
        else:
            st.warning("No documents found in the selected collection.")
    except Exception as e:
        st.error(f"Error loading documents: {e}")

# -------------------------------------
# 🔍 Similarity Search Page
# -------------------------------------
elif menu_option == "🔍 Similarity Search":
    st.title("🔍 Similarity Search")
    st.subheader(f"Searching in Collection: {selected_collection}")
    
    k = st.number_input("Enter number of results to return (k)", min_value=1, value=3)
    query = st.text_input("Enter your query")

    if query:
        try:
            results = vector_db.similarity_search(query, k=int(k))
            if results:
                grouped_results = {}
                for doc in results:
                    source = doc.metadata.get("source", "Unknown")
                    grouped_results.setdefault(source, []).append(doc)

                for source, docs_group in grouped_results.items():
                    with st.expander(f"Results from Source: {source}"):
                        result_data = [{"Page Content": doc.page_content} for doc in docs_group]
                        result_df = pd.DataFrame(result_data)
                        st.table(result_df)
            else:
                st.info("No similar results found.")
        except Exception as e:
            st.error(f"Error during similarity search: {e}")
