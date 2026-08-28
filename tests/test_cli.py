from click.testing import CliRunner

from fronta import __version__
from fronta.cli import main


def test_cli_reports_version() -> None:
    result = CliRunner().invoke(main, ["--version"], prog_name="fronta")

    assert result.exit_code == 0
    assert result.output == f"fronta, version {__version__}\n"
