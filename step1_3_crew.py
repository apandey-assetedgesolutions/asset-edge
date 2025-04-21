import os
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from crewai.tools import tool
from pydantic import BaseModel, Field
import json

load_dotenv()

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


def run_benchmark_crew(collection_name):
    class BenchmarkModel(BaseModel):
        Benchmark_1: str = Field("N/A", description="Primary performance benchmark")
        Benchmark_2: str = Field("N/A", description="Secondary performance benchmark")

    def initialize_chroma():
        # embeddings = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))
        return Chroma(
            persist_directory=f"chroma_db/{collection_name}",
            embedding_function=embeddings
        )

    chroma_db = initialize_chroma()

    @tool
    def table_retriever() -> str:
        """Priority retrieval of table-containing document chunks"""
        try:
            results = chroma_db.similarity_search(
                "table performance comparison benchmark returns YTD 1Y 3Y",
                k=5
            )
            table_chunks = [doc.page_content for doc in results if 'table' in doc.page_content.lower()]
            return "\n\n--- TABLE CHUNK ---\n".join(table_chunks) if table_chunks else fallback_retriever()
        except Exception as e:
            return f"ERROR|FAILED_PERMANENTLY|{str(e)}"

    @tool
    def fallback_retriever() -> str:
        """Secondary retrieval for non-table benchmark mentions"""
        results = chroma_db.similarity_search("benchmark index comparison", k=3)
        return "\n\n--- TEXT CHUNK ---\n".join([doc.page_content for doc in results])

    benchmark_agent = Agent(
        role="Table-First Benchmark Analyst",
        goal="Extract benchmark names from performance tables with time-series data or from the Important disclosure",
        backstory=(
            "Expert in financial table analysis with focus on time-series comparisons. "
            "Specializes in identifying benchmark indices from structured performance data."
        ),
        tools=[table_retriever, fallback_retriever],
        verbose=True,
        allow_delegation=False,
        max_iterations=3
    )

    benchmark_task = Task(
        description=(
            "Analyze documents following this strict workflow:\n"
            "1. FIRST inspect all table structures for benchmark names\n"
            "2. Also check from the Important disclosure sections"
            "3. Look for columns containing 'Benchmark' or index names\n"
            "4. Verify associated time periods (YTD, 1Y, 3Y)\n"
            "5. Only consider benchmarks with numerical comparisons\n"
            "6. Return maximum 2 benchmarks meeting all criteria\n\n"
            "RULES:\n"
            "- Prioritize tables over text\n"
            "- Require time-series data presence\n"
            "- Case-sensitive exact names\n"
            "- Reject benchmarks without numerical comparisons"
        ),
        agent=benchmark_agent,
        expected_output=(
            "JSON with exactly two benchmark names:\n"
            '{"Benchmark_1": "Index Name", "Benchmark_2": "Index Name"}'
        ),
        output_json=BenchmarkModel
    )

    analysis_crew = Crew(
        agents=[benchmark_agent],
        tasks=[benchmark_task],
        process=Process.sequential,
        verbose=True
    )

    result = analysis_crew.kickoff()
    return result

# if __name__ == "__main__":
#     collection = "1863"  # Replace with your collection name
#     benchmarks_json = run_benchmark_crew(collection)
#     print(benchmarks_json)
