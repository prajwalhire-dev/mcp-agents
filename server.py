import os
import json
import sqlite3
from mcp.server.fastmcp import FastMCP
from anthropic import Anthropic
from dotenv import load_dotenv
import pandas as pd
from typing import Dict

# Load environment variables from .env file
load_dotenv()

# initialse MCP server
mcp = FastMCP(
    name="SQLQueryAgent",
)

# Initialize the Anthropic client
anthropic_client = Anthropic(
    api_key=os.getenv("ANTHROPIC_API_KEY"),)

# --- Absolute Paths for Data Files ---
# This ensures the server can find the files regardless of how it's started.
BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "data", "electric_vehicle_data.db")
DATA_DICT_PATH = os.path.join(BASE_DIR, "data", "data_dictionary.csv")

# --- Helper Function ---
def get_data_dictionary_description():
    """Reads the data dictionary CSV and formats it into a string for the AI."""
    try:
        df = pd.read_csv(DATA_DICT_PATH)
        description = "This is the data dictionary. It explains the columns in the database tables:\n"
        for _, row in df.iterrows():
            description += f"- Column '{row['Column Header']}' (also called '{row['Business Header']}'): {row['Definition']}. Example: {row['Example']}\n"
        return description
    except FileNotFoundError:
        return "Data dictionary file not found. I will proceed without it."
    except Exception as e:
        return f"Error reading data dictionary: {e}"
    
def _parse_llm_json_response(llm_text_response: str) -> Dict:
    """
    A robust helper function to extract a JSON object from an LLM text response.
    """
    try:
        #find the start and end of the JSON object
        start_index = llm_text_response.find("{")
        end_index = llm_text_response.rfind("}") + 1
        if start_index != -1 and end_index != 0:
            json_str = llm_text_response[start_index:end_index]
            # Parse the JSON string to ensure it's valid
            return json.loads(json_str)
    except json.JSONDecodeError as e:
        return {"error": f"JSON decoding error: {e}"}
    return {"error": "No valid JSON found in the response."}

def get_database_schema(db_path):

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    #give a list of all tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]
    str_table_names = ", ".join(table_names)
    str_table_names = str_table_names.replace(" ", "")

    schema_description = f"Database schema contains the following tables: {str_table_names}. Each table contains various columns with specific data types."
    for table_name in table_names:
        # Get the column names and types for each table
        schema_description += f"\n\nTable: {table_name}\nColumns:\n"
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        for col in columns:
            column_name = col[1]
            column_type = col[2]
            schema_description += f"{column_name} ({column_type})\n"
    # Close the database connection
    conn.close()
    # Return the schema description
    return schema_description
# --- Tool 1: NER Generator ---
@mcp.tool()
def ner_generator_dynamic(question: str) -> str: #returns a JSON string
    """
    Analyzes a question to extract key entities (tables, columns, filters)
    needed to form a database query. Uses a data dictionary for context.
    """
    data_dictionary = get_data_dictionary_description()
    prompt = f"""
    You are a data analyst. Your job is to extract key entities from a user's question.
    Use the provided data dictionary to understand the columns.

    Data Dictionary:
    {data_dictionary}

    User Question: "{question}"

    Extract the necessary components to answer the question. Your output MUST be a single JSON object with keys: "table", "columns_to_select", and "filters".
    - "table": The table name, which is always a county name (e.g., "King").
    - "columns_to_select": A list of columns the user wants to see.
    - "filters": A dictionary of filters to apply, where the key is the column name and value is the condition.

    """
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        json_str = response.content[0].text
        parsed_dict = _parse_llm_json_response(json_str)
        return json.dumps(parsed_dict)
        
    except Exception as e:
        return json.dumps({"error": f"Error in ner_generator_dynamic: {e}"})
    
# --- Tool 2: Create SQL ---
@mcp.tool()
def create_sql(question: str, ner_dict: Dict) -> str:#returns a JSON string
    """
    Creates a full SQLite query by combining the user's question and the
    extracted entities from the ner_generator_dynamic tool.
    """
    ner_json = json.dumps(ner_dict, indent=2)
    prompt = f"""
    You are an expert SQLite developer. Create a single, valid SQLite query to answer the user's question.
    Understand the user's intent and the context provided by the extracted entities.
    The query may be complex, using window functions (like ROW_NUMBER(), PARTITION BY), subqueries, or other advanced features.

    User's Question: "{question}"
    Extracted Entities: {ner_json}

    Your output MUST be the raw SQLite query text, and nothing else. Do not wrap it in JSON or markdown.

    """
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        
        # The raw SQL query is extracted from the response.
        raw_sql_query = response.content[0].text

        # We now reliably create the JSON object in Python.
        sql_dict = {"sql_query": raw_sql_query}
        
        return json.dumps(sql_dict)
    except Exception as e:
        return json.dumps({"error": f"LLM Error in create_sql: {e}"})

# --- Tool 3 : Validate SQL agent ---    
@mcp.tool()
def validator_sql_agent(question: str, ner_dict: Dict, generated_query_dict: Dict) -> str: #return a JSON string
    """
    Validates a generated SQL query for correctness, syntax, and hallucinations against the schema.
    Returns a corrected/validated version as a JSON string.
    """
    schema_info = get_database_schema(DB_PATH)
    generated_query_json = json.dumps(generated_query_dict) #here it converts the dict to a JSON string
    prompt = f"""
    You are a SQL validator and debugger. Your task is to check if the provided SQL query correctly answers the user's question and is syntactically correct for SQLite.
    Strictly follow the schema information provided to ensure no hallucinations or incorrect names.

    Provided Information:
    1.  User's Original Question: "{question}"
    2.  Extracted Entities (for context): {json.dumps(ner_dict, indent=2)}
    3.  Generated SQL Query to Validate: {generated_query_json}
    4.  Database Schema Information: {schema_info}

    Your Tasks:
    1.  Check for syntax errors.
    2.  Check for "hallucinated" or incorrect column and table names by comparing against the schema.
    3.  Ensure the query logic accurately reflects the user's question (e.g., if they ask for "top 3", there should be an ORDER BY and LIMIT 3).
 
    
    Your output MUST be a single JSON object with one key: "sql_query", containing the final, validated, and potentially corrected query.
    """
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet-20241022", max_tokens=1024, messages=[{"role": "user", "content": prompt}]
        )
        parsed_dict = _parse_llm_json_response(response.content[0].text)
        return json.dumps(parsed_dict)
    except Exception as e:
        return json.dumps({"error": f"LLM Error in validator_sql_agent: {e}"})

# --- Tool 4: Run SQLite Query---
@mcp.tool()
def run_sqlite_query(sql_dict: Dict) -> str:  #returns a JSON string
    """Executes a SQL query and returns the data as a JSON string."""
    try:
        #json_str = sql_json[sql_json.find('{') : sql_json.rfind('}') + 1]
        sql_query = sql_dict.get("sql_query")
        # data = json.loads(json_str)
        # sql_query = data.get("sql_query")

        # if data.get("error"):
        #     return json.dumps({"error": f"Cannot execute due to previous error: {data['error']}", "data": []})
        if not sql_query:
            return json.dumps({"error": "No SQL query provided.", "data": []})

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        conn.close()

        formatted_results = [dict(zip(column_names, row)) for row in results]
        return json.dumps({"data": formatted_results})
        # print(f"Query executed successfully. Results: {formatted_results}")
        # return {"data": formatted_results}
      
    except Exception as e:
        return json.dumps({"error": f"Database query failed: {e}", "data": []})

# --- Tool 5: Handle Error Agent (NEW) ---
@mcp.tool()
def handle_error_agent(failed_sql_query_dict: Dict, error_message: str) -> str: #returns a JSON string
    """
    Attempts to fix a failed SQL query based on the specific error message from the database.
    """
    failed_sql_query = failed_sql_query_dict.get("sql_query", "Query not provided")
    prompt = f"""
    You are a highly skilled SQLite expert debugging a query.
    
    The following SQL query failed to execute:
    ```sql
    {failed_sql_query}
    ```

    It produced this specific error message:
    `{error_message}`

    Task:
    1.  Carefully analyze the query and the error message.
    2.  Provide a corrected SQLite query that resolves the identified error.
    
    Your output MUST be a single JSON object with one key: "sql_query", containing only the corrected query.
    """
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet-20241022", max_tokens=1024, messages=[{"role": "user", "content": prompt}]
        )
        parsed_dict = _parse_llm_json_response(response.content[0].text)

        return json.dumps(parsed_dict)
    except Exception as e:
        return json.dumps({"error": f"LLM Error in handle_error_agent: {e}"})

# --- Tool 6: Generate Final Answer ---
@mcp.tool()
def generate_final_answer(question: str, query_result_dict: Dict) -> str:
    """Takes the database results and generates a human-readable answer."""
    query_result_json = json.dumps(query_result_dict, indent=2) 

    prompt = f"""
    You are a helpful assistant. Answer the user's question based on the provided data.
    If the data contains an error, explain it simply. If the data is empty, say so.

    Original Question: "{question}"
    Data from Database: {query_result_json}
    """
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        print(f"Final Answer Result: {response.content[0].text}")
        return response.content[0].text
    except Exception as e:
        return f"Error formulating final answer: {e}"

# --- Run Server ---
if __name__ == "__main__":
    print("MCP server with multi-step AI pipeline is starting...")
    mcp.run(transport="stdio")