from pydantic import BaseModel, Field
from crewai import Agent, Task, Crew, Process
from langchain_chroma import Chroma
from crewai.tools import tool
from typing import List
import os
from dotenv import load_dotenv
import warnings
from pydantic import create_model

from utills import llm, embedding,client,sp_get_company_types,sp_get_companies_by_type  # your existing utils

warnings.filterwarnings("ignore", category=UserWarning)
load_dotenv()
# --- Your service provider setup ---
def get_company_types():
    return client.get_request(sp_get_company_types)

def get_companies_by_type(company_type_id):
    return client.get_request(f'{sp_get_companies_by_type}{company_type_id}')

def build_company_dict():
    company_types = get_company_types()
    company_dict = {}

    for t_company in company_types:
        company_id = t_company['CompanyTypeID']
        company_type = t_company['CompanyType']
        company_names = get_companies_by_type(company_id)

        if company_type not in company_dict:
            company_dict[company_type] = []

        for n_company in company_names:
            company_dict[company_type].append(n_company['CompanyName'])

    return company_dict
# --- 🔁 Main Crew Logic ---
def run_company_validation_crew(collection_name: str):
    company_dict = build_company_dict()

    # Step 1: Create dynamic Pydantic model
    fields = {
        key: (list[str], Field(..., description=f"List of confirmed companies for '{key}'"))
        for key in company_dict
    }
    DynamicCompanyModel = create_model("DynamicCompanyModel", **fields)

    # Step 2: Chroma DB
    def initialize_chroma():
        return Chroma(
            persist_directory=f"chroma_db/{collection_name}",
            embedding_function=embedding
        )
    chroma_db = initialize_chroma()

    # Step 3: Tool to retrieve related chunks
    @tool
    def company_info_retriever() -> str:
        """Retrieves relevant company mentions from the documents"""
        keywords = " ".join([name for names in company_dict.values() for name in names])
        results = chroma_db.similarity_search(keywords, k=15)
        return "\n\n--- DOCUMENT CHUNK ---\n".join([doc.page_content for doc in results])

    # Step 4: Agent
    company_agent = Agent(
        role="Service Provider Checker",
        goal="Check if listed companies are mentioned as official service providers",
        tools=[company_info_retriever],
        backstory="You're a legal assistant checking service providers against a fund document.",
        llm=llm,
        verbose=True,
        memory=True,
        allow_delegation=False,
        max_iterations=3
    )

    # Step 5: Task (Updated Prompt)
    company_task = Task(
        description=f"""
        Go through the document and check if the following companies are officially mentioned in context.

        Return ONLY those that are clearly stated. Ignore hypothetical or indirect mentions.

        Format the result as JSON grouped by company type:
        - Keys should be: {list(company_dict.keys())}
        - Values should be a list of company names found in the document for that type
        - Only return exact matches from the list of companies provided below for each type
        - If a company name is found in the document, return it exactly as stated in the list (e.g., 'Deloitte' not 'Deloitte LLP').
        - If no matches are found for a given type, return an empty list for that type.
        - Only return companies as listed in the provided company_dict below:
        {company_dict}
        """,
        agent=company_agent,
        expected_output="Valid JSON as per the dynamic company schema",
        output_pydantic=DynamicCompanyModel
    )

    # Step 6: Crew Setup
    company_crew = Crew(
        agents=[company_agent],
        tasks=[company_task],
        process=Process.sequential,
        verbose=True
    )

    # Run the crew and get the result
    result = company_crew.kickoff()
    return result
