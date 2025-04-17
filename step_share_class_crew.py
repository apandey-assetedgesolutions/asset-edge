def run_crew_fund_terms(collection_name):
    import os
    from dotenv import load_dotenv
    from crewai import Agent, Task, Crew, Process
    from langchain_chroma import Chroma
    from langchain_openai import OpenAIEmbeddings
    from crewai.tools import tool
    from pydantic import BaseModel, Field
    from typing import List
    import json

    load_dotenv()
    collection_name = collection_name
    max_iterations = 3

    # Initialize ChromaDB
    def initialize_chroma():
        embeddings = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))
        return Chroma(
            persist_directory=f"chroma_db/{collection_name}",
            embedding_function=embeddings
        )

    chroma_db = initialize_chroma()

    # Pydantic Models
    class FundClass(BaseModel):
        name: str = Field(..., description="Official class name")
        management_fee: str = Field("not found", description="Management fee percentage")
        performance_fee: str = Field("not found", description="Performance fee percentage without conditions")
        hurdle_value: str = Field("not found", description="Hurdle value percentage if specified")
        minimum_investment: str = Field("not found", description="Minimum investment amount")

    class FundClassesModel(BaseModel):
        classes: List[FundClass] = Field(..., description="List of fund share classes")

    # Custom Tools
    @tool
    def fund_terms_retriever(query: str = "") -> str:
        """Retrieves relevant document chunks about fund classes and terms"""
        try:
            results = chroma_db.similarity_search(
                "class management fee performance fee incentive fee hurdle rate minimum investment",
                k=7
            )
            return "\n\n--- DOCUMENT CHUNK ---\n".join([doc.page_content for doc in results])
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
        memory=True
    )

    # Task Definition
    terms_task = Task(
        description=(
            "Analyze document chunks to identify all share classes and their terms:\n"
            "1. Find all class references (Class A, B, C etc.)\n"
            "2. Extract management fee for each class\n"
            "3. Identify performance/incentive fees (without conditions)\n"
            "4. Extract hurdle rate if specified\n"
            "5. Determine minimum investment amounts\n\n"
            "Critical Requirements:\n"
            "- Maintain exact numerical values and percentages\n"
            "- Separate fee percentages from conditions\n"
            "- Preserve currency symbols and units\n"
            "- Cross-validate information across document sections\n"
            "- Report 'not found' for missing elements\n"
            "- Handle tiered structures separately"
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
                        "minimum_investment": "$"
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

if __name__ == "__main__":
    collection = "2106"
    benchmarks_json = run_crew_fund_terms(collection)
    print(benchmarks_json)