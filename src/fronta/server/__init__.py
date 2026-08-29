"""REST + MCP + dashboard server. `create_app(settings)` builds the FastAPI application."""

from fronta.server.app import create_app, serve

__all__ = ["create_app", "serve"]
