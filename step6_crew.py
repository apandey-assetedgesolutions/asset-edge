import os
import warnings
from datetime import datetime
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from langchain_chroma import Chroma
from crewai.tools import tool
from pydantic import BaseModel, Field
from typing import List

# Silence warnings
warnings.filterwarnings("ignore", category=UserWarning, module="onnxruntime.*")

load_dotenv()
from utills import llm, embedding

# ChromaDB and Embedding
embeddings = embedding


class TimeSeriesRecord(BaseModel):
    valuationDate: str = Field(..., pattern=r"\d{4}-\d{2}-\d{2}T00:00:00Z")
    rorValue: float = Field(..., ge=-100.0, le=100.0)


class TimeSeriesCollection(BaseModel):
    records: list[TimeSeriesRecord] = Field(..., description="Array of time series records")

def run_crew_step6(collection_name):
    max_iterations = 1

    def initialize_chroma(folder_path: str = "chroma_db_2", collection_name: str = collection_name):
        return Chroma(
            persist_directory=os.path.join(folder_path, collection_name),
            embedding_function=embeddings
        )

    # Initialize ChromaDB instance
    chroma_db = initialize_chroma()

    @tool
    def performance_table_retriever(query: str = "") -> str:
        """Retrieves monthly performance table chunks with source tracking."""
        global source_counts
        try:
            results = chroma_db.similarity_search(
                "monthly returns performance table net of fees YTD",
                k=3
            )
            source_counts.clear()
            chunks = []
            for doc in results:
                chunks.append(doc.page_content)
                if 'source' in doc.metadata:
                    source_counts[doc.metadata['source']] += 1
            return "\n\n--- DOCUMENT CHUNK ---\n".join(chunks)
        except Exception as e:
            return f"ERROR|FAILED_PERMANENTLY|{str(e)}"

    @tool
    def document_chunks_retriever(query = "") -> str:
        """Retrieves first 5 document chunks from each file in the collection."""
        try:
            all_chunks = []
            all_metadata = chroma_db.get(include=["metadatas"])["metadatas"]
            sources = {meta['source'] for meta in all_metadata if meta}
            for source in sources:
                results = chroma_db.get(
                    where={"source": source},
                    limit=2,
                    include=["documents"]
                )
                all_chunks.extend(results.get("documents", []))
            return "\n\n--- DOCUMENT CHUNK ---\n".join(all_chunks)
        except Exception as e:
            return f"ERROR|FAILED_PERMANENTLY|{str(e)}"

    time_agent = Agent(
        role="Financial Table Processor",
        goal="Extract monthly return values from all performance tables (type : Tables)",
        verbose=True,
        tools=[document_chunks_retriever],
        backstory=(
            "You are an expert in financial data analysis and fund reporting(check with object of table text and text_as_html compare both)" 
            "You are a perfectionist and have never made a single mistake in your lifetime." "Your task is to extract accurate and complete performance metrics from any provided data."
            "you never skip any fields."
            "If a data point is unavailable or missing, explicitly write 'null' instead of omitting it."
            "Ensure the output is structured, consistent, and reflects your high standards for accuracy and completeness."),
        llm_kwargs={"temperature": 0.1},
        max_iterations=1,
        llm= llm,
        early_stopping_method="force_final_answer",
        memory=True
    )

    time_task = Task(
        description=(
            "You are provided with fund performance tables containing monthly returns. "
            "Extract *all* available monthly returns for each year, excluding any XBI or RUT values. "
            "For each entry, format it like this:\n"
            "{'valuationDate': 'YYYY-MM-DDT00:00:00Z', 'rorValue': float}\n"
            "Use the last calendar day of each month (e.g., January 2000 → 2000-01-31T00:00:00Z).\n"
            "Return output as JSON with a 'records' key and a list of entries."
        ),
        agent=time_agent,
        expected_output="""{
          "records": [
            {"valuationDate": "2000-01-31T00:00:00Z", "rorValue": 1.59},
            {"valuationDate": "2000-02-29T00:00:00Z", "rorValue": 11.30},
            ...
          ]
        }""",
        output_json=TimeSeriesCollection
    )

    time_series_crew = Crew(
        agents=[time_agent],
        tasks=[time_task],
        process=Process.sequential,
        verbose=True
    )

    result = time_series_crew.kickoff()
    return result


# Example usage
if __name__ == "__main__":
    res = run_crew_step6("1863")  # Replace "A" with your actual Chroma collection name
    print(res)
