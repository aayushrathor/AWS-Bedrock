from typing import Type
import boto3
from langchain_core.tools import BaseTool
from pydantic.v1 import BaseModel, Field
import streamlit as st

## We will be suing Titan Embeddings Model To generate Embedding
from langchain_community.embeddings import BedrockEmbeddings
from langchain.llms.bedrock import Bedrock
from langchain.chat_models.bedrock import BedrockChat

## Data Ingestion
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFDirectoryLoader

# Vector Embedding And Vector Store
from langchain.vectorstores import FAISS

## LLm Models
from langchain.prompts import PromptTemplate
from langchain.chains import RetrievalQA

from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain.agents.agent_toolkits import create_retriever_tool
from langchain_core.utils.function_calling import convert_to_openai_tool
from langchain.agents import AgentExecutor, OpenAIFunctionsAgent, tool

import yfinance as yf

## Bedrock Clients
bedrock = boto3.client(service_name="bedrock-runtime")
bedrock_embeddings = BedrockEmbeddings(
    model_id="amazon.titan-embed-image-v1", client=bedrock
)


## Data ingestion
def loader():
    loader = PyPDFDirectoryLoader("data")
    documents = loader.load()
    docs = [doc for doc in documents if doc.page_content.strip()]
    return docs


def data_ingestion(documents):
    # - in our testing Character split works better with this PDF data set
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=1000)

    docs = text_splitter.split_documents(documents)
    return docs


## Vector Embedding and vector store
def get_vector_store(docs):
    vectorstore_faiss = FAISS.from_documents(docs, bedrock_embeddings)
    vectorstore_faiss.save_local("faiss_index")


def get_titan_llm():
    ##create the Anthropic Model
    llm = BedrockChat(
        model_id="amazon.titan-text-express-v1",
        client=bedrock,
        model_kwargs={"maxTokenCount": 4096},
    )
    return llm


def get_mistral_llm():
    ##create the Anthropic Model
    llm = Bedrock(
        model_id="mistral.mistral-7b-instruct-v0:2",
        client=bedrock,
        model_kwargs={"max_tokens": 1000},
    )
    return llm


def get_llm_transformer(llm):
    transformer = LLMGraphTransformer(llm=llm)
    return transformer


def graph_documents(llm, docs):
    transformer = get_llm_transformer(llm)
    print(f"\n\ntype of docs: {type(docs)}, length: {len(docs)}, docs: {docs[:3]}\n\n")
    graph_docs = transformer.convert_to_graph_documents(documents=docs)
    print(f"Nodes; {graph_docs[0].nodes}")
    print(f"relationships; {graph_docs[0].relationships}")
    return graph_docs


# llm = get_mistral_llm()
# docs = loader()
# graph_documents(llm, docs)


def get_current_stock_price(ticker):
    ticker_data = yf.Ticker(ticker)
    recent = ticker_data.history(period="1d")
    return recent.iloc[0]["Close"]


class CurrentStockPriceInput(BaseModel):
    """Inputs for get_current_stock_price"""

    ticker: str = Field(description="Ticker symbol of the stock")


class CurrentStockPriceTool(BaseTool):
    name = "get_current_stock_price"
    description = "Useful when you want to get current stock price"
    args_schema: Type[BaseModel] = CurrentStockPriceInput  # type: ignore

    def _run(self, ticker):
        return get_current_stock_price(ticker)

    def _arun(self, ticker):
        raise NotImplementedError("func get_current_stock_price did not support async.")


prompt_template = """

Human: Use the following pieces of context to provide a 
concise answer to the question at the end but usse atleast summarize with 
250 words with detailed explaantions. If you don't know the answer, 
just say that you don't know, don't try to make up an answer.

{agent_scratchpad}

<context>
{context}
</context>

Question: {question}

Assistant:"""

PROMPT = PromptTemplate(
    template=prompt_template, input_variables=["context", "question"]
)


def get_response_llm(llm, vectorstore_faiss, query):
    qa = RetrievalQA.from_chain_type(
        llm=llm,
        chain_type="stuff",
        retriever=vectorstore_faiss.as_retriever(
            search_type="similarity", search_kwargs={"k": 3}
        ),
        return_source_documents=True,
        chain_type_kwargs={"prompt": PROMPT},
    )
    answer = qa({"query": query})
    return answer["result"]


def configure_retriever():
    docs = loader()
    splitter = data_ingestion(docs)
    vectorstore = FAISS.from_documents(splitter, bedrock_embeddings)
    return vectorstore.as_retriever(search_kwargs={"k": 4})


latest_stock_price = convert_to_openai_tool(get_current_stock_price)

# print("init retriever tool")
# retriever_tool = create_retriever_tool(
#     configure_retriever(),
#     "search_docs",
#     "searches and returns documents and information from the documents",
# )
#
# print("init tools")
# tools = [retriever_tool, latest_stock_price]
# print("init openai functions")
# agent = OpenAIFunctionsAgent(llm=get_mistral_llm(), tools=tools, prompt=PROMPT)
# print("init agent_executor")
# agent_executor = AgentExecutor(
#     agent=agent, tools=tools, verbose=True, return_intermediate_steps=True
# )
# print("questions: Hello")
# agent_executor.invoke({"input": "Hello?", "agent_scratchpad": ""})
# print("questions: What is machine learning")
# agent_executor.invoke({"input": "What is machine learning?", "agent_scratchpad": ""})
# print("questions: AAPL stock price")
# agent_executor.invoke(
#     {"input": "What is the latest stock price of AAPL?", "agent_scratchpad": ""}
# )


def main():
    st.set_page_config("Chat PDF")

    st.header("Chat with PDF using AWS Bedrock💁")

    user_question = st.text_input("Ask a Question from the PDF Files")

    with st.sidebar:
        st.title("Update Or Create Vector Store:")

        if st.button("Vectors Update"):
            with st.spinner("Processing..."):
                docs = data_ingestion(loader())
                get_vector_store(docs)
                st.success("Done")

    if st.button("Titan Output"):
        with st.spinner("Processing..."):
            faiss_index = FAISS.load_local(
                "faiss_index", bedrock_embeddings, allow_dangerous_deserialization=True
            )
            llm = get_titan_llm()

            # faiss_index = get_vector_store(docs)
            st.write(get_response_llm(llm, faiss_index, user_question))
            st.success("Done")

    if st.button("Mistral Output"):
        with st.spinner("Processing..."):
            faiss_index = FAISS.load_local(
                "faiss_index", bedrock_embeddings, allow_dangerous_deserialization=True
            )
            llm = get_mistral_llm()

            # faiss_index = get_vector_store(docs)
            st.write(get_response_llm(llm, faiss_index, user_question))
            st.success("Done")


if __name__ == "__main__":
    main()
