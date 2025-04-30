import os
import sys
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain_openai import AzureChatOpenAI
from crewai.tools import tool
from pydantic import BaseModel, Field
from typing import List , Optional
import json
from datetime import date
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="onnxruntime.*")
load_dotenv()
from collections import defaultdict

from utills import llm , embedding

llm = llm
embeddings = embedding

def run_crew_step1(collection_name):

    collection_name = collection_name  # Default fallback
    max_iterations = 1

    # Initialize ChromaDB once
    def initialize_chroma(folder_path: str = "chroma_db", collection_name: str = collection_name):
        # embeddings = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))
        return Chroma(
            persist_directory=os.path.join(folder_path, collection_name),
            embedding_function=embeddings
        )

    # Initialize ChromaDB instance
    chroma_db = initialize_chroma()

    # Define your output validation model
    class FundMetadataModel(BaseModel):
        full_name: str = Field(..., description="Official full name of the fund")
        full_name_source: str = Field(..., description="Source File of the full name, in Document Metadata")
        full_name_source_page_label: int = Field(..., description="Page labels for the full name are taken from where you find the full name in the Document Metadata.")

        abbreviation: str = Field(..., description="Short form abbreviation of the fund name")
        abbreviation_source: str = Field(..., description="Source File of the abbreviation, in Document Metadata")
        abbreviation_source_page_label: int = Field(..., description="Page labels for the abbreviation are taken from where the abbreviation is found in the Document Metadata.")
        
        date_of_inception: str = Field("not found", description="Fund inception date in YYYY-MM-DD format or 'not found'")
        date_of_inception_source: str = Field(..., description="Source File of the inception date, in Document Metadata")
        date_of_inception_source_page_label: int = Field(..., description="Page labels for the date_of_inception are taken from where the date_of_inception is found in the Document Metadata.")

    def convert_crewoutput_to_dict(crew_output):
        """Convert CrewOutput object to serializable dictionary"""
        return {
            'full_name': crew_output.full_name,
            'abbreviation': crew_output.abbreviation,
            'date_of_inception': crew_output.date_of_inception
        }
    class DocumentChunkRetrieverInput(BaseModel):
        query: str = ""

    @tool
    def document_chunks_retriever(input: DocumentChunkRetrieverInput) -> str:  # Add default value
        """Retrieves first 5 document chunks from each file in collection"""
        try:
            all_chunks = []
            
            # Get all unique sources from ChromaDB metadata
            all_metadata = chroma_db.get(include=["metadatas"])["metadatas"]
            grouped_chunks = defaultdict(list)
            page_labels_by_source = defaultdict(set)

            sources = {meta['source_file'] for meta in all_metadata if meta}

            for source in sources:
                results = chroma_db.get(
                    where={"source_file": source},
                    limit=4,
                    include=["documents", "metadatas"]
                )

                documents = results.get("documents", [])
                metadatas = results.get("metadatas", [])

                for doc, meta in zip(documents, metadatas):
                    grouped_chunks[source].append(doc)

                    # Collect page_label only for the fetched documents
                    page_label = meta.get("page_label") if meta else None
                    if page_label:
                        page_labels_by_source[source].add(page_label)

            # Merge grouped docs into final chunks with correct page_label list
            all_chunks = [
                f"Content: {' '.join(docs)}\n"
                f"Source File: {src}\n"
                f"Page Labels: {list(page_labels_by_source.get(src, []))}"
                for src, docs in grouped_chunks.items()
            ]
                        
            return "\n\n--- DOCUMENT CHUNK ---\n".join(all_chunks)
        except Exception as e:
            return f"ERROR|FAILED_PERMANENTLY|{str(e)}"

    class InceptionRetrieverInput(BaseModel):
        query: str = ""

    @tool
    def inception_date_retriever() -> str:  # Add default empty string
        """Retrieves top 5 document chunks related to fund inception dates"""
        try:
            results = chroma_db.similarity_search(
                "inception date established founded effective date",
                k=4
            )

            return "\n\n--- DOCUMENT CHUNK ---\n".join(
                [
                    f"Content: {doc.page_content}\n"
                    f"Source File: {doc.metadata.get('source_file', 'unknown')}\n"
                    f"Page Label: {doc.metadata.get('page_label', '[]')}"
                    for doc in results
                ]
            )
        except Exception as e:
            return f"ERROR|FAILED_PERMANENTLY|{str(e)}"

    # Define your agent with enhanced prompting
    fund_metadata_agent = Agent(
        role="Financial Document Analyst",
        goal="Accurately extract fund names and abbreviations from document content",
        verbose=True,
        memory=True,
        tools=[document_chunks_retriever],
        backstory=(
            "Expert financial document analyst with rigorous attention to detail. "
            "Specializes in identifying official fund names and abbreviations "
            "from complex legal and financial documents."
        ),
        allow_delegation=False,
        max_iterations=max_iterations,
        llm = llm,
        # llm=LLM(temperature=0.01)  # Use lower temperature for accuracy
    )

    # Define the extraction task with explicit instructions
    metadata_task = Task(
        description=(
            "Analyze the provided document chunks to identify:\n"
            "1. Full official name of the fund (look for phrases like 'hereinafter referred to as')\n"
            "2. Official abbreviation (search for terms in ALL CAPS, 'the Fund' references, or defined terms)\n\n"
            "Important Rules:\n"
            "- Names are CASE-SENSITIVE - preserve exact capitalization\n"
            "- Never invent names - return 'not found' if uncertain\n"
            "- Prioritize definitions from introductory sections\n"
            "- Verify names appear in multiple locations for consistency\n"
            "- Ignore document metadata - only use text content\n"
            "- Watch for legal entity identifiers (L.P., LLC, Ltd.)"
        ),
        agent=fund_metadata_agent,
        expected_output=(
            "Valid JSON containing:\n"
            "- full_name: The complete legal name of the fund\n"
            "- abbreviation: The official short form abbreviation\n"
            "Example:\n"
            '''{
                "full_name": "Ibex Israel Public Equity Fund L.P.",
                "full_name_source": "file_name.pdf",
                "full_name_source_page_label": -1,
                "abbreviation": "ibex"
            }'''
        ),
        output_json=FundMetadataModel,
        output_parser=lambda x: FundMetadataModel.parse_raw(x).json()
    )

    # Specialized Agent for Dates
    date_analyst = Agent(
        role="Temporal Data Specialist",
        goal="Accurately identify dates of inception from financial documents",
        verbose=True,
        tools=[inception_date_retriever],
        backstory=(
            "Expert in temporal pattern recognition with a focus on financial documents. "
            "Specializes in identifying exact dates from complex legal language."
        ),
        allow_delegation=False,
        max_iterations=max_iterations,
        llm = llm
    )

    date_task = Task(
        description=(
            "Using previous context and date-specific analysis:\n"
            "1. Extract inception date from date-related chunks\n"
            "2. Convert dates to ISO format (YYYY-MM-DD)\n"
            "3. Combine with previous fund naming information\n"
            "4. Return FULL STRUCTURE with all three fields"
        ),
        agent=date_analyst,
        expected_output=(
            "Complete JSON containing ALL fields:\n"
            "- full_name (from previous task)\n" 
            "- abbreviation (from previous task)\n"
            "- date_of_inception (current analysis)\n"
            "Example:\n"
            '''{
                "full_name": "Tata consultancy services",
                "full_name_source": "file_name.pdf",
                "full_name_source_page_label": -1,
                "abbreviation": "TCS",
                "abbreviation_source": "file_name.pdf",
                "abbreviation_source_page_label": -1
            }'''
        ),
        context=[metadata_task],
        output_json=FundMetadataModel,  # Add this line
        output_parser=lambda x: FundMetadataModel.parse_raw(x).json()
    )

    # Keep crew setup the same
    financial_crew = Crew(
        agents=[fund_metadata_agent, date_analyst],
        tasks=[metadata_task, date_task],
        process=Process.sequential,
        verbose=True
    )


    result = financial_crew.kickoff() 
    return result

# res = run_crew_step1("1863")
# print(res)