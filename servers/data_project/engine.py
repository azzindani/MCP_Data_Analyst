"""Redirect — data_project is merged into data_workspace. Use data_workspace instead."""

from servers.data_workspace.engine import (  # noqa: F401
    create_workspace,
    list_workspace_files,
    open_workspace,
    register_workspace_file,
    run_workspace_pipeline,
    save_workspace_pipeline,
)
