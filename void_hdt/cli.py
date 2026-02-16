"""Command-line interface for void-hdt."""

import resource
import sys
from pathlib import Path

import click
from rdflib_hdt import HDTDocument

from void_hdt.partitions import PartitionAnalyzer
from void_hdt.void_generator import VOIDGenerator


def _get_rss_gb() -> float:
    """Get peak RSS in GB."""
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return usage / (1024**3)  # macOS: bytes
    return usage / (1024**2)  # Linux: KB


@click.command()
@click.argument("hdt_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "-o",
    "--output",
    type=click.Path(path_type=Path),
    required=True,
    help="Output file path for VOID description (Turtle format)",
)
@click.option(
    "--dataset-uri",
    default="http://example.org/dataset",
    help="URI for the dataset being described",
)
@click.option(
    "--use-blank-nodes",
    is_flag=True,
    default=False,
    help="Use blank nodes for partition nodes instead of URI references",
)
@click.option(
    "--cache-size",
    type=int,
    default=2_000_000,
    show_default=True,
    help="Max entries in the type-lookup cache (trades memory for speed)",
)
def main(
    hdt_file: Path,
    output: Path,
    dataset_uri: str,
    use_blank_nodes: bool,
    cache_size: int,
) -> None:
    """Generate VOID vocabulary descriptions from HDT files.

    Processes an HDT file to extract dataset statistics, class partitions,
    and property partitions, outputting the results as a VOID vocabulary
    description in Turtle format.

    HDT_FILE: Path to the input HDT file
    """
    click.echo(f"Processing HDT file: {hdt_file}")

    try:
        # Initialize components
        analyzer = PartitionAnalyzer()
        void_gen = VOIDGenerator(dataset_uri=dataset_uri, use_blank_nodes=use_blank_nodes)

        # Open HDT file
        document = HDTDocument(str(hdt_file))

        # Get statistics from HDT index (O(1) - no iteration needed)
        click.echo("Reading dataset statistics from HDT index...")
        click.echo(f"  Triples: {document.total_triples}")
        click.echo(f"  Distinct subjects: {document.nb_subjects}")
        click.echo(f"  Distinct predicates: {document.nb_predicates}")
        click.echo(f"  Distinct objects: {document.nb_objects}")

        # Analyze class and property partitions (two passes through data)
        click.echo(f"Peak RSS before analysis: {_get_rss_gb():.1f} GB")
        click.echo("Analyzing class partitions...")

        def _progress(msg: str) -> None:
            click.echo(f"{msg}  [RSS: {_get_rss_gb():.1f} GB]")

        analyzer.analyze(document, cache_size=cache_size, progress_fn=_progress)

        class_count = len(analyzer.class_partitions)
        click.echo(f"  Found {class_count} classes")

        # Generate VOID description
        click.echo("Generating VOID description...")
        void_gen.add_dataset_statistics(document)
        void_gen.add_dataset_property_partitions(analyzer)
        void_gen.add_class_partitions(analyzer)

        # Save output
        click.echo(f"Writing output to: {output}")
        void_gen.save(str(output), format="turtle")

        click.echo("Done!")

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
