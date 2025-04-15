def run_crew_step6(collection_name):
    import os
    import sys
    from dotenv import load_dotenv
    from crewai import Agent, Task, Crew, Process
    from langchain_chroma import Chroma
    from langchain_openai import OpenAIEmbeddings
    from crewai.tools import tool
    from pydantic import BaseModel, Field
    from typing import List
    import json
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning, module="onnxruntime.*")

    collection_name = collection_name
    max_iterations = 1

    def initialize_chroma(folder_path: str = "chroma_db", collection_name: str = collection_name):
        embeddings = OpenAIEmbeddings(openai_api_key=os.getenv("OPENAI_API_KEY"))
        return Chroma(
            persist_directory=os.path.join(folder_path, collection_name),
            embedding_function=embeddings
        )

    # Initialize ChromaDB instance
    chroma_db = initialize_chroma()

    class TimeSeriesRecord(BaseModel):
        valuationDate: str = Field(..., pattern=r"\d{4}-\d{2}-\d{2}T00:00:00Z")
        rorValue: float = Field(..., ge=-100.0, le=100.0)

    class TimeSeriesCollection(BaseModel):
        records: list[TimeSeriesRecord] = Field(..., description="Array of time series records")

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
    def document_chunks_retriever(query: str = "") -> str:
        """Retrieves first 5 document chunks from each file in the collection."""
        try:
            all_chunks = []
            all_metadata = chroma_db.get(include=["metadatas"])["metadatas"]
            sources = {meta['source'] for meta in all_metadata if meta}
            for source in sources:
                results = chroma_db.get(
                    where={"source": source},
                    limit=5,
                    include=["documents"]
                )
                all_chunks.extend(results.get("documents", []))
            return "\n\n--- DOCUMENT CHUNK ---\n".join(all_chunks)
        except Exception as e:
            return f"ERROR|FAILED_PERMANENTLY|{str(e)}"

    time_agent = Agent(
        role="Financial Table Processor",
        goal="Extract all monthly return values from performance tables",
        verbose=True,
        tools=[document_chunks_retriever],
        backstory=(
            "Expert in financial data extraction with meticulous attention to table structures."
        ),
        max_iterations=1,
        early_stopping_method="force_final_answer",
        memory=True
    )

    time_task = Task(
        description=(
            "Analyze all performance tables and extract every monthly value..."
        ),
        agent=time_agent,
        expected_output="""{
          "records": [
            {"valuationDate": "2024-01-31T00:00:00Z", "rorValue": 1.59},
            {"valuationDate": "2024-02-29T00:00:00Z", "rorValue": 11.30}
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