# Fronta

Fronta is a distributed task queue for asyncio tasks and sandboxed processes.

It is primarily a Python library intended to be embedded in applications. V1 also ships a server
with REST and MCP interfaces and a web dashboard, plus worker and db CLIs. See `SPEC.md`.

## Development

```bash
uv sync --all-extras
uv run fronta --version
make checkall
```

Fronta is licensed under the MIT License.
