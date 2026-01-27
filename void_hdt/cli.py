"""Command-line interface for void-hdt."""

import sys
from pathlib import Path

import click

from void_hdt.hdt_reader import HDTReader
from void_hdt.partitions import PartitionAnalyzer
from void_hdt.statistics import DatasetStatistics
from void_hdt.void_generator import VOIDGenerator


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
def main(hdt_file: Path, output: Path, dataset_uri: str) -> None:
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
        void_gen = VOIDGenerator(dataset_uri=dataset_uri)

        # Open HDT file
        with HDTReader(str(hdt_file)) as reader:
            # Get statistics from HDT index (O(1) - no iteration needed)
            click.echo("Reading dataset statistics from HDT index...")
            stats = DatasetStatistics.from_reader(reader)

            click.echo(f"  Triples: {stats.triple_count}")
            click.echo(f"  Distinct subjects: {stats.distinct_subjects}")
            click.echo(f"  Distinct predicates: {stats.distinct_predicates}")
            click.echo(f"  Distinct objects: {stats.distinct_objects}")

            # Analyze class and property partitions (two passes through data)
            click.echo("Analyzing class partitions...")
            analyzer.analyze(reader)

            class_count = len(analyzer.class_partitions)
            click.echo(f"  Found {class_count} classes")

        # Generate VOID description
        click.echo("Generating VOID description...")
        void_gen.add_dataset_statistics(stats)
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
