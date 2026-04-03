@echo off
setlocal EnableDelayedExpansion

set REPO_DIR=%USERPROFILE%\.mcp_servers\MCP_Data_Analyst
set SERVER_DIR=%REPO_DIR%\servers\data_basic
set LOG_FILE=%TEMP%\mcp_data_analyst.log

echo [%date% %time%] Starting MCP Data Analyst server >> "%LOG_FILE%"

rem Check for git
where git >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: git is not installed or not in PATH >> "%LOG_FILE%"
    echo ERROR: git is not installed or not in PATH
    exit /b 1
)

rem Check for uv
where uv >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: uv is not installed or not in PATH >> "%LOG_FILE%"
    echo ERROR: uv is not installed or not in PATH
    exit /b 1
)

rem Clone or update repo
if not exist "%REPO_DIR%" (
    echo Cloning repository... >> "%LOG_FILE%"
    git clone https://github.com/azzindani/MCP_Data_Analyst.git "%REPO_DIR%" >> "%LOG_FILE%" 2>&1
    if !ERRORLEVEL! NEQ 0 (
        echo ERROR: Failed to clone repository >> "%LOG_FILE%"
        exit /b 1
    )
) else (
    cd /d "%REPO_DIR%" >> "%LOG_FILE%" 2>&1
    git pull --quiet >> "%LOG_FILE%" 2>&1
)

cd /d "%SERVER_DIR%" >> "%LOG_FILE%" 2>&1
echo [%date% %time%] Starting server from %SERVER_DIR% >> "%LOG_FILE%"

rem Run the MCP server
uv run python server.py >> "%LOG_FILE%" 2>&1
