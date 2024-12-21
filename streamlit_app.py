[10:13 pm, 20/12/2024] Aanchal: import streamlit as st  # Import Python packages
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import pandas as pd
import json

pd.set_option("max_colwidth", None)

# Default Values
NUM_CHUNKS = 3  # Number of chunks to use for context

# Service parameters
CORTEX_SEARCH_DATABASE = "MEDICAL_DATA"
CORTEX_SEARCH_SCHEMA = "DATA"
CORTEX_SEARCH_SERVICE = "CC_MEDICAL_DOC_SEARCH_SERVICE"

# Columns to query in the service
COLUMNS = [
    "chunk",
    "relative_path"
]

session = get_active_session()
root = Root(session)

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

### Functions

def config_options():
    st.sidebar.selectbox('Select your model:', (
        'mixtral-8x7b', 'snowflake-arctic', 'mistral-large', 'llama3-8b', 'llama3-70b',
        'reka-flash', 'mistral-7b', 'llama2-70b-chat', 'gemma-7b'
    ), key="model_name")
    st.sidebar.expander("Session State").write(st.session_state)

def get_similar_chunks_search_service(query):
    # Fetching the search results from the service
    try:
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
        # Print response for debugging
        st.sidebar.json(response.json())  # View the raw response in the sidebar
        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                st.sidebar.write(f"Found {len(results)} chunks")
            else:
                st.sidebar.write("No chunks found for the query.")
        else:
            st.sidebar.write("Error fetching data from the search service.")
        return response.json()  # Return the raw response for further processing
    except Exception as e:
        st.sidebar.write(f"Error: {str(e)}")
        return []


def create_prompt(myquestion):
    if st.session_state.rag == 1:
        prompt_context = get_similar_chunks_search_service(myquestion)
        
        # Check if the 'results' key is available in the response
        if 'results' in prompt_context and prompt_context['results']:
            # Properly format the context for the prompt
            formatted_context = json.dumps(prompt_context['results'], indent=2)  # Convert list to JSON string
            prompt = f"""
               You are a medical assistant that extracts information from the CONTEXT provided
               between <context> and </context> tags.
               When answering the question contained between <question> and </question> tags,
               be concise and do not hallucinate.
               If you don’t have the information, just say so.
               Only answer the question if you can extract it from the CONTEXT provided.

               Do not mention the CONTEXT used in your answer.

               <context>
               {formatted_context}
               </context>
               <question>
               {myquestion}
               </question>
               Answer: 
            """
            # Get the relative paths from the context (if available)
            relative_paths = set(item['relative_path'] for item in prompt_context['results'])
        else:
            prompt = f"""
            [0]
            'Question:  
              {myquestion} 
              Answer: '
            """
            relative_paths = "None"
    
   
    return prompt, relative_paths

def complete(myquestion):
    prompt, relative_paths = create_prompt(myquestion)
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    df_response = session.sql(cmd, params=[st.session_state.model_name, prompt]).collect()
    return df_response, relative_paths

def main():
    st.title(f":speech_balloon: Chat Medical Assistant with Snowflake Cortex")
    st.write("This i…
[10:26 pm, 20/12/2024] Aanchal: create or replace stage medical_data_stage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );
CREATE OR REPLACE CORTEX SEARCH SERVICE CC_MEDICAL_DOC_SEARCH_SERVICE
ON chunk
ATTRIBUTES doc_id, relative_path, chunk
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 minute'
AS (
    SELECT doc_id,               -- Document ID or name
           relative_path,        -- Path of the file in the stage
           chunk,                -- Extracted text chunk (searchable content)
           file_url              -- Original file URL (for referencing the document)
    FROM medical_docs_chunks    -- Table containing the document chunks
);