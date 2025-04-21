import os
import json
import warnings
import re
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from langchain_chroma import Chroma
from langchain_community.document_loaders import PyPDFLoader, UnstructuredExcelLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings
from crewai import Crew, Task, Agent, Process
from crewai.tools import tool

warnings.filterwarnings('ignore')
load_dotenv()

os.environ["AZURE_API_KEY"] = os.getenv('OPENAI_API_KEY')
os.environ["AZURE_API_BASE"] = os.getenv('AZURE_OPENAI_ENDPOINT')
os.environ["OPENAI_API_VERSION"] = "2023-03-15"

llm = AzureChatOpenAI(
    deployment_name="gpt-4o-mini",
    model_name="azure/gpt-4o-mini",
    temperature=0.9,
    top_p=0.9
)

# llm = AzureChatOpenAI(
#     deployment_name="gpt-4o-mini",
#     model_name="azure/gpt-4o-mini",
#     temperature=0.9,
#     top_p=0.9
# )

persist_directory = "./chroma_db"

embeddings = HuggingFaceEmbeddings(
    model_name='sentence-transformers/all-MiniLM-L12-v2',
    model_kwargs={'device': 'cpu'}
)

class ServiceProviderProcessor:

    def __init__(self,persist_path=persist_directory):
        # self.basepath = basepath
        self.persist_path = persist_path
        # self.client = client
        # self.asset_id = asset_id
        self.documents = []
        self.vector_db = None
        self.extracted_info = {}
    
    
    def create_vector_store(self, collection_name):
        self.vector_db = Chroma(
            persist_directory=os.path.join(self.persist_path, collection_name),
            embedding_function=embeddings
        )


    def rag_tool(self, question):
        docs = self.vector_db.similarity_search(question, k=5)
        return "\n\n".join([doc.page_content for doc in docs])

    def call_agent_to_verify(self):
        @tool
        def rag_tool(question: str) -> str:
            """Search relevant context from document chunks for the given question."""
            return self.rag_tool(question)

        retriever_agent = Agent(
            role="Retriever",
            goal="Extract and verify service providers from document",
            backstory="You ensure extracted service provider data is correct and structured.",
            verbose=True,
            allow_delegation=False,
            llm=llm,
            tools=[rag_tool]
        )

        task = Task(
            description=(
                "Extract the following service provider info from document: Administrator, Custodian, Auditor, "
                "Legal Counsel, Prime Broker, Transfer Agent, Valuation Agent, Tax Advisor. Return JSON format:\n"
                '{ "Service Providers": { "Administrator": "Caligan Partners LP", ... } }'
            ),
            expected_output="Cleaned JSON containing extracted service providers.",
            agent=retriever_agent
        )

        rag_crew = Crew(agents=[retriever_agent], tasks=[task], process=Process.sequential, verbose=True)
        result = rag_crew.kickoff(inputs={"question": "Provide me the Service Provider in a fund?"})

        try:
            # Attempt to parse the raw output
            raw_output = result.raw  # Assuming 'raw' contains the raw string output
            print("Agent Raw Output:\n", raw_output)
            # If the output is wrapped in markdown or contains extra characters, clean it
            cleaned_output = raw_output.strip()
            # Parse the JSON
            parsed = json.loads(cleaned_output)
            return parsed.get("Service Providers", {})
        except Exception as e:
            print("Agent JSON Parse Error:", e)
            return {}

