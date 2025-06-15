# MCP SQL Agent

An intelligent SQL query system that uses a multi-step AI pipeline to answer natural language questions about electric vehicle data. Built using MCP (Message Channel Protocol) and Anthropic's Claude API.

## Overview

This project implements a question-answering system that:
1. Takes natural language questions about electric vehicle data
2. Processes them through an AI pipeline
3. Generates and executes SQL queries
4. Returns human-readable answers

## Architecture

The system consists of two main components:

### Client (`client.py`)
- Manages the connection to the MCP server
- Orchestrates the multi-step AI pipeline
- Handles error recovery and retries
- Provides a clean interface for asking questions

### Server (`server.py`)
Implements 6 core tools:
1. `ner_generator_dynamic` - Extracts entities from questions
2. `create_sql` - Generates SQL queries
3. `validator_sql_agent` - Validates and corrects SQL syntax
4. `run_sqlite_query` - Executes queries against SQLite database
5. `handle_error_agent` - Fixes failed queries
6. `generate_final_answer` - Creates human-readable responses

## Prerequisites

- Python 3.x
- Anthropic API key
- SQLite database with electric vehicle data

## Installation

1. Clone the repository:
```sh
git clone https://github.com/yourusername/mcp-agents.git
cd mcp-agents
```

2. Install dependencies:
```sh
pip install mcp-core anthropic python-dotenv nest-asyncio
```

3. Set up environment variables:
```sh
echo "ANTHROPIC_API_KEY=your_api_key_here" > .env
```

## Usage

1. Start the server:
```sh
python server.py
```

2. Run the client:
```sh
python client.py
```

3. Example questions you can ask:
- "How many vehicles are there in King county?"
- "What is the maximum base MSRP in King county?"
- "Find the top 3 most expensive vehicles by Base MSRP in each city in King county?"

## Data Structure

The project expects:
- A SQLite database at `data/electric_vehicle_data.db`
- A data dictionary CSV at `data/data_dictionary.csv`
- Tables organized by county containing electric vehicle information

## License

See the [LICENSE](LICENSE) file for details.


