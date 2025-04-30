import os
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from langchain_chroma import Chroma
from crewai.tools import tool
from pydantic import BaseModel, Field
from typing import List
import warnings

from utills import llm, embedding  # Custom utilities

warnings.filterwarnings("ignore", category=UserWarning)
load_dotenv()


def run_liquidity_terms_crew(
    collection_name: str,
    lockup_types: List[str],
    notice_frequencies: List[str],
    lockup_frequencies: List[str],
    redemption_frequencies: List[str],
    gate_frequencies: List[str]
):
    def initialize_chroma():
        return Chroma(
            persist_directory=f"chroma_db/{collection_name}",
            embedding_function=embedding
        )

    chroma_db = initialize_chroma()

    # --- Pydantic Models ---
    class LiquidityClass(BaseModel):
        name: str = Field(..., description="Official class name exactly as stated in the document")
        name_source_file: str = Field(..., description="Find Source File of the class name, in Document Metadata")
        name_source_page_label: int = Field(..., description="Page label where the class name is found in the Document Metadata")

        required_notice: int = Field(..., description="Notice period duration in numeric format")
        required_notice_source_file: str = Field(..., description="Find Source File of the required notice, in Document Metadata")
        required_notice_source_page_label: int = Field(..., description="Page label where the required notice is found in the Document Metadata")

        notice_frequency: str = Field(..., description="Must match one of the allowed values")
        notice_frequency_source_file: str = Field(..., description="Find Source File of the notice frequency, in Document Metadata")
        notice_frequency_source_page_label: int = Field(..., description="Page label where the notice frequency is found in the Document Metadata")

        redemption_frequency: str = Field(..., description="Select only from the predefined list")
        redemption_frequency_source_file: str = Field(..., description="Find Source File of the redemption frequency, in Document Metadata")
        redemption_frequency_source_page_label: int = Field(..., description="Page label where the redemption frequency is found in the Document Metadata")

        lockup_types: str = Field(..., description="Select only from the predefined list")
        lockup_types_source_file: str = Field(..., description="Find Source File of the lockup types, in Document Metadata")
        lockup_types_source_page_label: int = Field(..., description="Page label where the lockup types are found in the Document Metadata")

        lockup_frequency: str = Field(..., description="Select only from the predefined list")
        lockup_frequency_source_file: str = Field(..., description="Find Source File of the lockup frequency, in Document Metadata")
        lockup_frequency_source_page_label: int = Field(..., description="Page label where the lockup frequency is found in the Document Metadata")

        investor_gate_percent: str = Field(..., description="Exact percentage as stated in the document (e.g., '15%')")
        investor_gate_percent_source_file: str = Field(..., description="Find Source File of the investor gate percent, in Document Metadata")
        investor_gate_percent_source_page_label: int = Field(..., description="Page label where the investor gate percent is found in the Document Metadata")

        investor_gate_frequency: str = Field(..., description="Select only from the predefined list")
        investor_gate_frequency_source_file: str = Field(..., description="Find Source File of the investor gate frequency, in Document Metadata")
        investor_gate_frequency_source_page_label: int = Field(..., description="Page label where the investor gate frequency is found in the Document Metadata")

    class LiquidityTermsModel(BaseModel):
        classes: List[LiquidityClass] = Field(..., description="List of fund share classes with liquidity terms")

    # --- Tool ---
    @tool
    def liquidity_terms_retriever() -> str:
        """Retrieves relevant liquidity & redemption info chunks"""
        results = chroma_db.similarity_search(
            "required notice period redemption lockup investor gate liquidity",
            k=10
        )
        return "\n\n--- SECURITY CONTEXT ---\n".join([
            f"Content : {doc.page_content}\nPage Number : {doc.metadata['page_label']}\nSource File : \'{doc.metadata['source_file']}\'"
            for doc in results
        ])

    # --- Agent ---
    liquidity_agent = Agent(
        role="Liquidity Specialist",
        goal="Extract liquidity terms per share class",
        verbose=True,
        tools=[liquidity_terms_retriever],
        backstory="An expert in financial legal documents with focus on redemption and liquidity structures.",
        allow_delegation=False,
        llm=llm,
        max_iterations=3,
        memory=True
    )

    # --- Task ---
    liquidity_task = Task(
        description=f"""
        Extract class-wise liquidity details. For each class, identify:
        - Name
        - required_notice (numeric only)
        - notice_frequency (choose from: {notice_frequencies})
        - redemption_frequency (choose from: {redemption_frequencies})
        - lockup_types (choose from: {lockup_types})
        - lockup_frequency (choose from: {lockup_frequencies})
        - investor_gate_percent (as a % string like '15%')
        - investor_gate_frequency (choose from: {gate_frequencies})

        Rules:
        - Only values stated directly in document
        - Ignore hypothetical/explained text
        - If not found, return 'not found'
        - Handle a maximum of 2 accurate classes
        """,
        agent=liquidity_agent,
        expected_output="Valid JSON matching LiquidityTermsModel schema",
        output_json=LiquidityTermsModel,
        output_parser=lambda x: LiquidityTermsModel.parse_raw(x).json()
    )

    # --- Crew ---
    liquidity_crew = Crew(
        agents=[liquidity_agent],
        tasks=[liquidity_task],
        process=Process.sequential,
        verbose=True
    )

    result = liquidity_crew.kickoff()
    return result
