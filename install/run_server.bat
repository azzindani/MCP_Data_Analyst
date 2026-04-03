@echo off
set REPO_DIR=%USERPROFILE%\.mcp_servers\MCP_Data_Analyst
set SERVER_DIR=%REPO_DIR%\servers\data_basic

if not exist "%REPO_DIR%" (
    git clone https://github.com/azzindani/MCP_Data_Analyst.git "%REPO_DIR%"
) else (
    cd /d "%REPO_DIR%" && git pull --quiet 2>nul
)

cd /d "%SERVER_DIR%"
uv run python server.py
