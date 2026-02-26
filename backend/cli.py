"""CLI entry point for the universal spatial ingestor.

Usage:
  python -m backend.cli spine       # Build master spine
  python -m backend.cli ingest ...  # Run universal ingest
  python -m backend.cli assemble    # Assemble master blockgroup
"""

import click


@click.group()
def cli():
    """Universal Spatial Ingestor CLI."""
    pass


@cli.command()
@click.pass_context
def spine(ctx):
    """Build the master spine crosswalk."""
    from backend.scripts.build_master_spine import main
    ctx.invoke(main)


@cli.command()
@click.pass_context
def ingest(ctx):
    """Run universal ingest for a data source."""
    from backend.scripts.universal_ingest import main
    ctx.invoke(main)


@cli.command()
@click.pass_context
def assemble(ctx):
    """Assemble master blockgroup table."""
    from backend.scripts.assemble_master_blockgroup import main
    ctx.invoke(main)


if __name__ == "__main__":
    cli()
