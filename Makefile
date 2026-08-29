.PHONY: check test audit checkall fix

# Fast gate — lint, format, types, architecture, deps. Also runs as the git
# pre-commit hook. Never modifies files — `--locked` keeps uv from rewriting
# uv.lock as a side effect; if it fails, update the lockfile explicitly.
check:
	uv run --locked pre-commit run --all-files

# The suite and the audit on their own. CI's lowest-bounds leg runs `check test` without the
# audit: the audit is about the versions we ship (the lock); the floors that leg installs are
# compatibility statements, and old versions carry known advisories by definition.
test:
	uv run --locked pytest

audit:
	uv run --locked pip-audit

# Full gate — required before handoff. CI runs exactly this.
checkall: check test audit

# The only write path: apply ruff autofixes and formatting, then re-run `check`.
fix:
	uv run --locked ruff check --fix .
	uv run --locked ruff format .
	$(MAKE) check
