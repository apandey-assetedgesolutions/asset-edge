import os
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from langchain_chroma import Chroma
from crewai.tools import tool
from pydantic import BaseModel, Field
from typing import List
import json

load_dotenv()
from utills import llm , embedding

llm = llm
embeddings = embedding

def run_crew_fund_terms(collection_name):

    collection_name = collection_name
    max_iterations = 3


    # Initialize ChromaDB
    def initialize_chroma():
        # embeddings = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))
        return Chroma(
            persist_directory=f"chroma_db/{collection_name}",
            embedding_function=embeddings
        )

    chroma_db = initialize_chroma()

    # Pydantic Models
    class FundClass(BaseModel):
        name: str = Field(..., description="Official class name exactly as stated in the document")
        name_source_file: str = Field(..., description="Find Source File of the class name, in Document Metadata")
        name_source_page_label: int = Field(..., description="Page label where the class name is found in the Document Metadata")
        
        management_fee: str = Field("not found", description="Exact management fee percentage (e.g., '9.5%')")
        management_fee_source_file: str = Field(..., description="Find Source File of the management fee, in Document Metadata")
        management_fee_source_page_label: int = Field(..., description="Page label where the management fee is found in the Document Metadata")

        performance_fee: str = Field("not found", description="Base performance fee percentage only (e.g., '100%')")
        performance_fee_source_file: str = Field(..., description="Find Source File of the performance fee, in Document Metadata")
        performance_fee_source_page_label: int = Field(..., description="Page label where the performance fee is found in the Document Metadata")

        hurdle_value: str = Field("not found", description="Exact hurdle rate percentage if specified (e.g., '5%')")
        hurdle_value_source_file: str = Field(..., description="Find Source File of the hurdle value, in Document Metadata")
        hurdle_value_source_page_label: int = Field(..., description="Page label where the hurdle value is found in the Document Metadata")

        minimum_investment: str = Field(
            "not found",
            description="Exact initial minimum investment converted to full numerical format. "
            "Convert abbreviations: $5m → $5000000, £3k → £3000. "
            "Only take the first value if multiple are present (e.g., '$5m (initial)' → '$5000000')"
        )
        minimum_investment_source_file: str = Field(..., description="Find Source File of the minimum investment, in Document Metadata")
        minimum_investment_source_page_label: int = Field(..., description="Page label where the minimum investment is found in the Document Metadata")


    class FundClassesModel(BaseModel):
        classes: List[FundClass] = Field(..., description="List of fund share classes")

    # Custom Tools
    @tool
    def fund_terms_retriever() -> str:
        """Retrieves document chunks about fund terms with enhanced context"""
        try:
            results = chroma_db.similarity_search(
                "class management fee performance fee hurdle rate minimum investment",
                k=10  # Increased from 7 to 10 for more context
            )
            return "\n\n--- SECURITY CONTEXT ---\n".join([
                f"Content : {doc.page_content}\nPage Number : {doc.metadata['page_label']}\nSource File : \'{doc.metadata['source_file']}\'"
                for doc in results
            ])
            
        except Exception as e:
            return f"ERROR|FAILED_PERMANENTLY|{str(e)}"

    # Agent Definition
    fund_analyst = Agent(
        role="Fund Terms Specialist",
        goal="Accurately extract share class terms from legal documents",
        verbose=True,
        tools=[fund_terms_retriever],
        backstory=(
            "Expert financial analyst with deep expertise in fund documentation. "
            "Specializes in identifying share class structures and associated terms "
            "from complex legal agreements and prospectuses."
        ),
        allow_delegation=False,
        max_iterations=max_iterations,
        llm=llm,
        memory=True
    )

    # Task Definition
    terms_task = Task(
        description=(
            "Analyze document chunks to identify all share classes and their terms:\n"
            "1. Identify all class references (Class A, B, C etc.)\n"
            "2. Extract exact numerical values for fees and investments\n"
            "3. Cross-validate information across adjacent chunks (before/after)\n"
            "4. Prioritize values from later document sections for updates\n"
            "5. Reject any explanatory text - only keep %/$ values\n\n"
            "Critical Enhancements:\n"
            "- Verify values in both current chunk and neighboring chunks\n"
            "- Resolve conflicts using most recent mention in document\n"
            "- Strictly exclude any non-numerical explanations\n"
            "- Handle tiered structures as separate entries"
            "- Try to retrieve max 2 Class other than that as of now not needed, 2 should be accurate"
            "6. For minimum investments:\n"
            "   a) Convert abbreviated values (e.g., 67m → 67000000). Do NOT write 'million'\n"
            "   b) Take ONLY the initial value before any parentheses/commas\n"
            "   c) Preserve original currency symbols\n"
            "   d) Try to retrieve accurate values, don't hallucinate any values"
        ),
        agent=fund_analyst,
        expected_output=(
            "JSON array of class objects with:\n"
            "- name\n"
            "- management_fee\n"
            "- performance_fee (base percentage only)\n"
            "- hurdle_rate\n"
            "- minimum_investment\n"
            "Example:\n"
            '''{
                "classes": [
                    {
                        "name": "fund term defined in document",
                        "management_fee": "%",
                        "performance_fee": "%",
                        "hurdle_value": "%",
                        "minimum_investment": "$10000"
                    },
                    {
                        "name": "fund term defined in document",
                        "management_fee": "%",
                        "performance_fee": "%",
                        "hurdle_value": "not found",
                        "minimum_investment": "$"
                    }
                ]
            }'''
        ),
        output_json=FundClassesModel,
        output_parser=lambda x: FundClassesModel.parse_raw(x).json()
    )

    # Crew Setup
    terms_crew = Crew(
        agents=[fund_analyst],
        tasks=[terms_task],
        process=Process.sequential,
        verbose=True
    )

    result = terms_crew.kickoff()
    return result

# if __name__ == "__main__":
#     collection = "1863"
#     benchmarks_json = run_crew_fund_terms(collection)
#     print(benchmarks_json)