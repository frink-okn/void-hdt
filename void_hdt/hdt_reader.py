"""HDT file reader with iterator-based access."""

from collections.abc import Iterator

from rdflib import BNode, Literal, URIRef
from rdflib_hdt import HDTDocument

# Type alias for RDF terms
type RDFTerm = URIRef | Literal | BNode
type Triple = tuple[RDFTerm, RDFTerm, RDFTerm]
# HDT pattern terms don't include BNode
type PatternTerm = URIRef | Literal


class HDTReader:
    """Iterator-based HDT file reader."""

    def __init__(self, hdt_path: str) -> None:
        """Initialize HDT reader.

        Args:
            hdt_path: Path to the HDT file
        """
        self.hdt_path = hdt_path
        self.document = HDTDocument(hdt_path)

    def iter_triples(
        self,
        subject: PatternTerm | None = None,
        predicate: PatternTerm | None = None,
        obj: PatternTerm | None = None,
    ) -> Iterator[Triple]:
        """Iterate over triples matching the given pattern.

        Args:
            subject: Subject filter (None for any). Note: BNode not supported in patterns.
            predicate: Predicate filter (None for any). Note: BNode not supported in patterns.
            obj: Object filter (None for any). Note: BNode not supported in patterns.

        Yields:
            Matching triples as (subject, predicate, object) tuples
        """
        # HDTDocument.search() expects a tuple, None means "any"
        pattern = (subject, predicate, obj)

        # search() returns (iterator, cardinality)
        triples, _ = self.document.search(pattern)
        yield from triples

    @property
    def nb_triples(self) -> int:
        """Get total number of triples from HDT index."""
        return self.document.total_triples

    @property
    def nb_subjects(self) -> int:
        """Get number of distinct subjects from HDT index."""
        return self.document.nb_subjects

    @property
    def nb_predicates(self) -> int:
        """Get number of distinct predicates from HDT index."""
        return self.document.nb_predicates

    @property
    def nb_objects(self) -> int:
        """Get number of distinct objects from HDT index."""
        return self.document.nb_objects

    def close(self) -> None:
        """Close the HDT document."""
        # HDTDocument doesn't have a close method, Python will handle cleanup
        pass

    def __enter__(self) -> "HDTReader":
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()
