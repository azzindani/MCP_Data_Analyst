"""Redirect — data_workspace is merged into data_project. Use data_project instead."""

from servers.data_project.server import main, mcp  # noqa: F401

if __name__ == "__main__":
    main()
