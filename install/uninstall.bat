@echo off
set REPO_DIR=%USERPROFILE%\.mcp_servers\MCP_Data_Analyst

if exist "%REPO_DIR%" (
    echo Removing MCP Data Analyst...
    rmdir /s /q "%REPO_DIR%"
    echo Done. All files removed.
) else (
    echo Nothing to remove. MCP Data Analyst is not installed.
)
pause