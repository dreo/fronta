import click


@click.command()
@click.version_option(package_name="fronta")
def main() -> None:
    """Expose Fronta's operational command-line interface."""
