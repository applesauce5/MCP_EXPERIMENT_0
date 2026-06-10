# databricks_mcp_server.py
import asyncio
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from databricks import sql
import json
import os

import keyring


# Initialize the MCP server
server = Server("databricks-finance-server")

# Databricks connection config
DB_CONFIG = {
    "server_hostname": keyring.get_password("DATABRICKS_HOST", "HOST"),
    "http_path": keyring.get_password("DATABRICKS_HTTP_PATH", "PATH"),
    "access_token": keyring.get_password("DATABRICKS_TOKEN", "TOKEN"),
}

# ---- TOOL 1: Get job run status ----
@server.list_tools()
async def list_tools():
    """Tell Claude what tools are available"""
    return [
        Tool(
            name="get_job_status",
            description="Get the status of a Databricks job by name. Returns latest run result.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_name": {
                        "type": "string",
                        "description": "The name of the Databricks job"
                    },
                    "hours_back": {
                        "type": "integer",
                        "description": "How many hours back to look. Default 24.",
                        "default": 24
                    }
                },
                "required": ["job_name"]
            }
        ),
        Tool(
            name="get_failed_jobs",
            description="Get all failed Databricks jobs in the last N hours.",
            inputSchema={
                "type": "object",
                "properties": {
                    "hours_back": {
                        "type": "integer",
                        "description": "How many hours back to look. Default 24.",
                        "default": 24
                    }
                }
            }
        )
    ]

# ---- TOOL EXECUTION ----
@server.call_tool()
async def call_tool(name: str, arguments: dict):
    """Execute the tool Claude requested"""

    if name == "get_job_status":
        job_name = arguments["job_name"]
        hours_back = arguments.get("hours_back", 24)

        with sql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        job_name,
                        run_id,
                        result_state,
                        start_time,
                        end_time,
                        DATEDIFF(minute, start_time, end_time) as duration_mins
                    FROM system.lakeflow.job_runs
                    WHERE job_name ILIKE '%{job_name}%'
                    AND start_time >= current_timestamp() - interval {hours_back} hours
                    ORDER BY start_time DESC
                    LIMIT 1
                """)
                row = cursor.fetchone()

        if not row:
            result = f"No runs found for job '{job_name}' in the last {hours_back} hours."
        else:
            result = json.dumps({
                "job_name": row[0],
                "run_id": row[1],
                "status": row[2],
                "start_time": str(row[3]),
                "end_time": str(row[4]),
                "duration_minutes": row[5]
            }, indent=2)

        return [TextContent(type="text", text=result)]

    elif name == "get_failed_jobs":
        hours_back = arguments.get("hours_back", 24)

        with sql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        job_name,
                        run_id,
                        result_state,
                        start_time
                    FROM system.lakeflow.job_runs
                    WHERE result_state = 'FAILED'
                    AND start_time >= current_timestamp() - interval {hours_back} hours
                    ORDER BY start_time DESC
                """)
                rows = cursor.fetchall()

        if not rows:
            result = f"No failed jobs in the last {hours_back} hours. All clear."
        else:
            failed = [
                {"job_name": r[0], "run_id": r[1], "status": r[2], "start_time": str(r[3])}
                for r in rows
            ]
            result = json.dumps(failed, indent=2)

        return [TextContent(type="text", text=result)]

# ---- RUN THE SERVER ----
async def main():
    async with stdio_server() as streams:
        await server.run(
            streams[0],
            streams[1],
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())