import os
import asyncio
import json
from contextlib import AsyncExitStack
from typing import Any, Dict, List, Optional
import nest_asyncio
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

nest_asyncio.apply()

# Load environment variables from .env file
load_dotenv()

class SQLAgentClient:
    """Orchestrates a multi-step AI pipeline to answer questions using a database."""

    def __init__(self):
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()
        self.stdio: Optional[Any] = None
        self.write: Optional[Any] = None
        print("SQL Agent Client initialized.")

    async def connect(self, server_script_path: str = "server.py"):
        """Connects to the MCP server."""
        print(f"Connecting to MCP server: {server_script_path}...")
        server_params = StdioServerParameters(command="python", args=[server_script_path])
        try:
            stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
            self.stdio, self.write = stdio_transport
            self.session = await self.exit_stack.enter_async_context(ClientSession(self.stdio, self.write))
            await self.session.initialize()

            tools_result = await self.session.list_tools()
            print("\nSuccessfully connected. Available tools:")
            for tool in tools_result.tools:
                print(f"  - {tool.name}")
        except Exception as e:
            print(f"Failed to connect: {e}")
            raise

    async def ask(self, question: str) -> str:
        """Processes a question through the full pipeline with a validation/retry loop."""
        if not self.session:
            return "Error: Not connected to the server."

        print(f"\nProcessing question: \"{question}\"")
        try:
            # Step 1: Extract Entities with NER
            print("Step 1: Calling ner_generator_dynamic...")
            ner_result = await self.session.call_tool("ner_generator_dynamic", {"question": question})
            ner_dict = json.loads(ner_result.content[0].text) 
            print(f" -> NER Result: {ner_dict}")

            # Step 2: Create SQL Query
            print("Step 2: Calling create_sql...")
            sql_result = await self.session.call_tool("create_sql", {"question": question, "ner_dict": ner_dict})
            sql_dict = json.loads(sql_result.content[0].text) 
            print(f"SQL output : {sql_result.content[0]}") 
            print(f" -> SQL Created: {sql_dict}")

            # Step 3: Validate SQL Query
            print("Step 3: Calling validator_sql_agent...")
            validated_result = await self.session.call_tool("validator_sql_agent", {
                "question": question,
                "ner_dict": ner_dict,
                "generated_query_dict": sql_dict
            })
            validated_sql_dict = json.loads(validated_result.content[0].text)
            print(f" -> Validated SQL: {validated_sql_dict}")
            
            # --- Retry Loop ---
            max_retries = 3
            for i in range(max_retries):
                # Step 4: Run Query on Database
                print(f"Step 4 (Attempt {i+1}/{max_retries}): Calling run_sqlite_query...")
                db_result = await self.session.call_tool("run_sqlite_query", {"sql_dict": validated_sql_dict})
                db_dict = json.loads(db_result.content[0].text)  
                print(f" -> Database Result: {db_dict}")
                

                # Check if the database query was successful
                if "error" not in db_dict:
                    print(" -> Database query executed successfully.")
                    # Step 6: Generate Final Answer
                    print("Step 6: Calling generate_final_answer...")
                    final_answer = await self.session.call_tool("generate_final_answer", {
                        "question": question, 
                        "query_result_dict": db_dict
                    })
                    return final_answer.content[0].text
                
                # If there was an error, proceed to the error handler
                error_message = db_dict.get("error", "Unknown database error")
                print(f" -> Database query failed with error: {error_message}")
                
                # Step 5: Handle Error
                print("Step 5: Calling handle_error_agent to fix the query...")
                error_handler_result = await self.session.call_tool("handle_error_agent", {
                    "failed_sql_query_dict": validated_sql_dict,
                    "error_message": error_message
                })
                # Update the query with the fixed version for the next loop attempt
                validated_sql_dict = json.loads(error_handler_result.content[0].text)
                print(f" -> Received new query from error handler: {validated_sql_dict}")

            return f"Failed to execute the query after {max_retries} attempts. Last error: {db_dict.get('error')}"

        except Exception as e:
            return f"A critical error occurred in the pipeline: {e}"

    async def cleanup(self):
        """Closes the connection."""
        print("\nCleaning up and closing connection...")
        await self.exit_stack.aclose()
        print("Connection closed.")

async def main():
    """Main function to run the client."""
    client = SQLAgentClient()
    try:
        await client.connect()
        
        # --- Example Question ---
        question = "How many vehicles are there in King county?"
        # question = "What is maximum base MSRP in King county?"
        # question="How many distinct vehicle Makes are in the King table?"
        # question = "What is the minimum Electric Range in the King table?"
        # question = "Find the vehicle with the second highest Electric Range per City in King?"
        question = "Find Make and Model combinations in King where all vechicles are BEVs?"
        response = await client.ask(question)
        
        print("\n" + "="*50)
        print(f"Question: {question}")
        print(f"Final Answer: {response}")
        print("="*50)

    finally:
        await client.cleanup()

if __name__ == "__main__":
    asyncio.run(main())