# void-hdt

A Python tool for efficiently processing RDF HDT files to generate VOID (Vocabulary of Interlinked Datasets) descriptions.

## Overview

void-hdt analyzes HDT files and produces comprehensive metadata about RDF datasets using the [VOID vocabulary](https://www.w3.org/TR/void/). It leverages the efficiency of the HDT format and uses iterator-based processing to handle large datasets.

## Features

- **Dataset Statistics**: Total triples, distinct subjects, predicates, and objects
- **Class Partitions**: Identifies classes (via `rdf:type`) and counts instances
- **Property Partitions**: For each class, documents property usage and triple counts
- **Efficient Processing**: Iterator-based design for memory-efficient handling of large files
- **Turtle Output**: Generates clean, readable VOID descriptions in Turtle format

## Installation

### Using uv (recommended)

```bash
uv sync
```

### Using pip

```bash
pip install -e .
```

## Usage

### Command Line

```bash
void-hdt input.hdt -o output.ttl
```

### Options

- `HDT_FILE`: Path to the input HDT file (required)
- `-o, --output PATH`: Output file path for VOID description (required)
- `--dataset-uri URI`: URI for the dataset being described (default: `http://example.org/dataset`)

### Example

```bash
void-hdt data/mydata.hdt -o void-description.ttl --dataset-uri http://example.org/mydata
```

## Output Format

The tool generates a VOID description in Turtle format that includes:

- Dataset-level statistics (triples, distinct subjects/predicates/objects)
- Class partitions with entity counts
- Property partitions within each class partition showing triple counts per property

Example output structure:

```turtle
@prefix void: <http://rdfs.org/ns/void#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<http://example.org/dataset> a void:Dataset ;
    void:triples 1000000 ;
    void:distinctSubjects 50000 ;
    void:properties 25 ;
    void:distinctObjects 75000 ;
    void:classPartition <http://example.org/dataset/class/Person> .

<http://example.org/dataset/class/Person> a void:Dataset ;
    void:class <Person> ;
    void:entities 10000 ;
    void:propertyPartition <http://example.org/dataset/class/Person/property/name> .

<http://example.org/dataset/class/Person/property/name> a void:Dataset ;
    void:property <name> ;
    void:triples 10000 .
```

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
