.PHONY: check checkall fix

# Fast gate — lint, format, types, architecture, deps. Also runs as the git
# pre-commit hook. Never modifies files — `--locked` keeps uv from rewriting
# uv.lock as a side effect; if it fails, update the lockfile explicitly.
check:
	uv run --locked pre-commit run --all-files

# Full gate — required before handoff. CI runs exactly this.
checkall: check
	uv run --locked pytest
	uv run --locked pip-audit

# The only write path: apply ruff autofixes and formatting, then re-run `check`.
fix:
	uv run --locked ruff check --fix .
	uv run --locked ruff format .
	$(MAKE) check
