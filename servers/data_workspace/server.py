"""Redirect — data_workspace is now merged into data_project/server.py."""

from servers.data_project.server import main, mcp  # noqa: F401

if __name__ == "__main__":
    main()
