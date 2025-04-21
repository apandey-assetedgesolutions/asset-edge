import os
import json
import warnings
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

persist_directory = "./chroma_db"

embeddings = HuggingFaceEmbeddings(
    model_name='sentence-transformers/all-MiniLM-L12-v2',
    model_kwargs={'device': 'cpu'}
)

class AssetAttributeProcessor:

    def __init__(self, persist_path=persist_directory):
        self.persist_path = persist_path
        self.vector_db = None

    def create_vector_store(self, collection_name):
        self.vector_db = Chroma(
            persist_directory=os.path.join(self.persist_path, collection_name),
            embedding_function=embeddings
        )

    def rag_tool(self, question):
        docs = self.vector_db.similarity_search(question, k=5)
        return "\n\n".join([doc.page_content for doc in docs])

    def call_agent_to_verify_attributes(self):
        @tool
        def rag_tool(question: str) -> str:
            """Search relevant context from document chunks for the given question."""
            return self.rag_tool(question)

        retriever_agent = Agent(
            role="Asset Analyst",
            goal="Extract and verify all key attributes of financial assets from fund documentation",
            backstory="You analyze fund documents and extract structured metadata for each asset.",
            verbose=True,
            allow_delegation=False,
            llm=llm,
            tools=[rag_tool]
        )

        task = Task(
            description=(
                "Extract the following asset attributes from the document: "
                "Asset ID, Asset Name, Asset Type, Asset Class, Market Value, Cost Basis, "
                "Currency, Issuer, Maturity Date (if applicable), Coupon Rate, Liquidity, "
                "Geographical Exposure, ESG Score, and Risk Metrics. "
                "Return a clean JSON format like this:\n"
                '{ "Assets": [ { "Asset ID": "A001", "Asset Name": "US Treasury Bond", '
                '"Asset Type": "Fixed Income", "Currency": "USD", ... } ] }'
            ),
            expected_output="Structured JSON containing key asset attributes.",
            agent=retriever_agent
        )

        rag_crew = Crew(agents=[retriever_agent], tasks=[task], process=Process.sequential, verbose=True)
        result = rag_crew.kickoff(inputs={"question": "Extract and structure all asset attributes from the document."})

        try:
            raw_output = result.raw
            print("Agent Raw Output:\n", raw_output)
            cleaned_output = raw_output.strip()
            parsed = json.loads(cleaned_output)
            return parsed.get("Assets", [])
        except Exception as e:
            print("Agent JSON Parse Error:", e)
            return []
