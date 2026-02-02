# void-hdt

A Python tool for efficiently processing RDF HDT files to generate VOID (Vocabulary of Interlinked Datasets) descriptions.

## Overview

void-hdt analyzes HDT files and produces comprehensive metadata about RDF datasets using the [VOID vocabulary](https://www.w3.org/TR/void/). It leverages the efficiency of the HDT format and uses iterator-based processing to handle large datasets.

## Features

- **Dataset Statistics**: Total triples, distinct subjects, predicates, and objects
- **Dataset Property Partitions**: Triple counts per property across all triples
- **Class Partitions**: Identifies classes (via `rdf:type`) and counts instances
- **Property Partitions**: For each class, documents property usage and triple counts
- **Object Class Partitions**: Breakdown of object classes for each property partition
- **Efficient Processing**: Iterator-based design for memory-efficient handling of large files
- **Turtle Output**: Generates VOID descriptions in Turtle format

## Installation

### Using Docker (recommended for quick usage)

Build the Docker image:

```bash
docker build -t void-hdt .
```

### Using uv (for development)

```bash
uv sync
```

### Using pip

```bash
pip install -e .
```

## Usage

### Docker

Process an HDT file using Docker:

```bash
docker run --rm -v /path/to/data:/data void-hdt /data/input.hdt -o /data/output.ttl
```

Or with custom dataset URI:

```bash
docker run --rm -v /path/to/data:/data void-hdt \
  /data/input.hdt \
  -o /data/output.ttl \
  --dataset-uri http://example.org/mydata
```

### Local Installation

### Command Line

```bash
void-hdt input.hdt -o output.ttl
```

### Options

- `HDT_FILE`: Path to the input HDT file (required)
- `-o, --output PATH`: Output file path for VOID description (required)
- `--dataset-uri URI`: URI for the dataset being described (default: `http://example.org/dataset`)
- `--use-blank-nodes`: Use blank nodes for partition nodes instead of URI references (optional)

### Example

```bash
void-hdt data/mydata.hdt -o void-description.ttl --dataset-uri http://example.org/mydata
```

## Output Format

The tool generates a VOID description in Turtle format that includes:

- Dataset-level statistics (triples, distinct subjects/predicates/objects)
- Dataset-level property partitions showing triple counts per property
- Class partitions with entity and triple counts
- Property partitions within each class partition showing triple counts per property
- Object class partitions showing the breakdown of object types for each property

Example output structure:

```turtle
@prefix void: <http://rdfs.org/ns/void#> .
@prefix voidext: <http://ldf.fi/void-ext#> .

<http://example.org/dataset> a void:Dataset ;
    void:triples 1000000 ;
    void:distinctSubjects 50000 ;
    void:properties 25 ;
    void:distinctObjects 75000 ;
    void:propertyPartition <http://example.org/dataset/property/abc123...> ;
    void:classPartition <http://example.org/dataset/class/def456...> .

# Dataset-level property partition
<http://example.org/dataset/property/abc123...> a void:Dataset ;
    void:property <http://example.org/name> ;
    void:triples 50000 .

# Class partition
<http://example.org/dataset/class/def456...> a void:Dataset ;
    void:class <http://example.org/Person> ;
    void:entities 10000 ;
    void:triples 30000 ;
    void:propertyPartition <http://example.org/dataset/class/def456.../property/789abc...> .

# Property partition within class
<http://example.org/dataset/class/def456.../property/789abc...> a void:Dataset ;
    void:property <http://example.org/worksFor> ;
    void:triples 8000 ;
    voidext:objectClassPartition <.../target/xyz789...> .

# Object class partition
<.../target/xyz789...> a void:Dataset ;
    void:class <http://example.org/Company> ;
    void:triples 8000 .
```

Note: Partition URIs use MD5 hashes of the original IRIs to ensure syntactically valid URIs. The original IRIs are preserved via `void:class` and `void:property` predicates.

## Development

### Requirements

- Python 3.12+
- uv for dependency management
- rdflib-hdt for HDT file access
- rdflib for VOID vocabulary generation
- click for CLI
- ty for type checking
- ruff for formatting and linting

### Type Checking

```bash
uvx ty check
```

### Formatting

```bash
uv run ruff format
```

### Linting

```bash
uv run ruff check
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
