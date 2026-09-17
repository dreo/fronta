from click.testing import CliRunner

from fronta import __version__
from fronta.cli import main


def test_cli_reports_version() -> None:
    result = CliRunner().invoke(main, ["--version"], prog_name="fronta")

    assert result.exit_code == 0
    assert result.output == f"fronta, version {__version__}\n"


def test_db_init_reports_invalid_settings_without_a_traceback(monkeypatch):
    monkeypatch.setenv("FRONTA_CONCURRENCY", "invalid")
    result = CliRunner().invoke(main, ["db", "init", "--dsn", "postgresql:///unused"])
    assert result.exit_code == 1
    assert "invalid settings" in result.output
    assert "Traceback" not in result.output


def test_db_sql_includes_additive_backfill_once():
    result = CliRunner().invoke(main, ["db", "sql"])
    assert result.exit_code == 0
    assert (
        result.output.count(
            "ALTER TABLE fronta.subscriptions ADD COLUMN IF NOT EXISTS backfill jsonb;"
        )
        == 1
    )
