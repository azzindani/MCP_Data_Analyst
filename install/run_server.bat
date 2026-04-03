@echo off
setlocal EnableDelayedExpansion

set REPO_DIR=%USERPROFILE%\.mcp_servers\MCP_Data_Analyst
set SERVER_DIR=%REPO_DIR%\servers\data_basic

rem Check for git
where git >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    exit /b 1
)

rem Check for uv
where uv >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    exit /b 1
)

rem Clone or update repo
if not exist "%REPO_DIR%" (
    git clone https://github.com/azzindani/MCP_Data_Analyst.git "%REPO_DIR%" 2>nul
    if !ERRORLEVEL! NEQ 0 (
        exit /b 1
    )
) else (
    cd /d "%REPO_DIR%"
    git pull --quiet 2>nul
)

cd /d "%SERVER_DIR%"
uv run python server.py
