"""Redirect — data_project is merged into data_workspace. Use data_workspace instead."""

from servers.data_workspace.server import main, mcp  # noqa: F401

if __name__ == "__main__":
    main()
