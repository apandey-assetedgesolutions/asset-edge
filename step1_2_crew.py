from crewai import Agent, Task, Crew, Process
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from crewai.tools import tool
from pydantic import BaseModel, Field
import os
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_openai import AzureChatOpenAI
os.environ["AZURE_API_KEY"] = os.getenv('OPENAI_API_KEY')
os.environ["AZURE_API_BASE"] = os.getenv('AZURE_OPENAI_ENDPOINT')
os.environ["OPENAI_API_VERSION"] = "2023-03-15"

llm = AzureChatOpenAI(
    deployment_name="gpt-4o-mini",
    model_name="azure/gpt-4o-mini",
    temperature=0.9,
    top_p=0.9
)

embeddings = HuggingFaceEmbeddings(
model_name='sentence-transformers/all-MiniLM-L12-v2',
model_kwargs={'device': 'cpu'}
)

def run_crew_security_strategy(collection_name, asset_type_names, strategy_values):
    max_iterations = 1

    # Define output model
    class InvestmentAttributes(BaseModel):
        security_type: str = Field(..., description="Matched security type from predefined list or N/A")
        strategy_value: str = Field(..., description="Matched strategy value from predefined list or N/A")

    # Initialize ChromaDB
    def initialize_chroma(collection_name):
        # embeddings = OpenAIEmbeddings(
        #     openai_api_key=os.getenv("OPENAI_API_KEY")
        # )
        return Chroma(
            persist_directory=f"chroma_db/{collection_name}",
            embedding_function=embeddings
        )

    chroma_db = initialize_chroma(collection_name)

    # Enhanced search tool with dynamic query generation
    @tool
    def security_type_search():
        """Search for security types using expanded terminology"""
        query_terms = [
            "fund type", "investment vehicle", "security classification",
            "asset structure", "financial instrument type",
            *asset_type_names  # Include all dropdown values
        ]
        results = chroma_db.max_marginal_relevance_search(
            " ".join(query_terms), 
            k=10,  # Increased context window
            fetch_k=20  # Broader initial search
        )
        return "\n\n--- SECURITY CONTEXT ---\n".join([doc.page_content for doc in results])

    @tool
    def strategy_value_search():
        """Search for strategies using concept-based matching"""
        query_terms = [
            "investment strategy", "portfolio allocation",
            "asset mix", "investment approach",
            *[f'"{value}"' for value in strategy_values]  # Include all strategy values
        ]
        results = chroma_db.max_marginal_relevance_search(
            " ".join(query_terms),
            k=10,
            fetch_k=20,
            lambda_mult=0.6  # Balance diversity/relevance
        )
        return "\n\n--- STRATEGY CONTEXT ---\n".join([doc.page_content for doc in results])

    # Enhanced Security Agent
    security_analyst = Agent(
        role="Security Classification Specialist",
        goal="Match document context to closest security type from dropdown options",
        backstory=(
            "Expert financial instrument classifier with deep knowledge of "
            "security structures and regulatory terminology. Skilled in "
            "interpreting legal definitions and matching to standardized categories."
        ),
        tools=[security_type_search],
        verbose=True,
        max_iterations=max_iterations,  # More analysis cycles
        memory=True,
        llm_kwargs={"temperature": 0.1}  # More deterministic
    )

    security_task = Task(
        description=f"""Analyze document context to identify security type:
        Available Options: {asset_type_names}
        
        Rules:
        1. Match document descriptions to closest option
        2. Consider both explicit mentions and implicit characteristics
        3. Prioritize legal structure over marketing terms
        4. If uncertain after analysis, return N/A
        
        Examples:
        - "The fund is structured as a limited partnership" → "Private Equity Fund"
        - "Invests in publicly traded stocks" → "Stock"
        - "CD-XXXX certificate" → "Certificate of Deposit"
        """,
        agent=security_analyst,
        expected_output="One security type from the dropdown list or N/A",
        output_json=InvestmentAttributes
    )

    # Enhanced Strategy Agent
    strategy_analyst = Agent(
        role="Investment Strategy Analyst",
        goal="Match investment approach to closest strategy value from dropdown options",
        backstory=(
            "Experienced strategy mapper with expertise in translating "
            "portfolio descriptions to standardized strategy categories. "
            "Skilled in interpreting allocation tables and investment mandates."
        ),
        tools=[strategy_value_search],
        verbose=True,
        max_iterations=max_iterations,
        memory=True,
        llm_kwargs={"temperature": 0.1}
    )

    strategy_task = Task(
        description=f"""Identify investment strategy:
        Available Options: {strategy_values}
        
        Rules:
        1. Match context to closest strategy category
        2. Consider allocation percentages and geographic focus
        3. Look for strategy descriptions in prospectus sections
        4. If no clear match after analysis, return N/A
        
        Examples:
        - "60% developed market stocks" → "Developed Market Equities"
        - "Focus on private debt instruments" → "Private Credit"
        - "Energy sector investments" → "Energy"
        """,
        agent=strategy_analyst,
        expected_output="One strategy value from the dropdown list or N/A",
        output_json=InvestmentAttributes
    )

    # Enhanced Validation Agent
    final_validator = Agent(
        role="Financial Data Validator",
        goal="Ensure accurate mapping to dropdown values",
        backstory=(
            "Expert in financial data validation with strict adherence to "
            "classification standards. Cross-checks context against options."
        ),
        verbose=True,
        memory=True,
        max_iter=3,
        llm_kwargs={"temperature": 0}
    )

    validation_task = Task(
        description=f"""Final validation:
        1. Cross-reference security type with context from {security_type_search.name}
        2. Verify strategy value against {strategy_value_search.name} results
        3. Ensure matches align with dropdown options:
           Security Types: {asset_type_names}
           Strategies: {strategy_values}
        4. Return N/A only if no reasonable match exists
        """,
        agent=final_validator,
        context=[security_task, strategy_task],
        expected_output="Valid JSON with accurate matches or N/A",
        output_json=InvestmentAttributes
    )

    crew = Crew(
        agents=[security_analyst, strategy_analyst, final_validator],
        tasks=[security_task, strategy_task, validation_task],
        process=Process.sequential,
        verbose=True,
        manager_llm_kwargs={"temperature": 0}
    )

    return crew.kickoff()