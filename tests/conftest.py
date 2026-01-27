"""Pytest fixtures and mock HDTReader for testing."""

from collections.abc import Iterator

import pytest
from rdflib import BNode, Graph, Literal, URIRef

type RDFTerm = URIRef | Literal | BNode
type Triple = tuple[RDFTerm, RDFTerm, RDFTerm]
type PatternTerm = URIRef | Literal


class MockHDTReader:
    """Mock HDTReader that works with in-memory RDF graphs.

    This allows testing the VOID processing logic without actual HDT files.
    """

    def __init__(self, graph: Graph) -> None:
        """Initialize mock reader from an RDF graph.

        Args:
            graph: RDFLib graph containing triples to process
        """
        self.graph = graph
        self._triples: list[Triple] = [(s, p, o) for s, p, o in graph]
        self._subjects: set[RDFTerm] = set()
        self._predicates: set[RDFTerm] = set()
        self._objects: set[RDFTerm] = set()

        for s, p, o in self._triples:
            self._subjects.add(s)
            self._predicates.add(p)
            self._objects.add(o)

    def iter_triples(
        self,
        subject: PatternTerm | None = None,
        predicate: PatternTerm | None = None,
        obj: PatternTerm | None = None,
    ) -> Iterator[Triple]:
        """Iterate over triples matching the given pattern.

        Args:
            subject: Subject filter (None for any)
            predicate: Predicate filter (None for any)
            obj: Object filter (None for any)

        Yields:
            Matching triples as (subject, predicate, object) tuples
        """
        for s, p, o in self._triples:
            if subject is not None and s != subject:
                continue
            if predicate is not None and p != predicate:
                continue
            if obj is not None and o != obj:
                continue
            yield (s, p, o)

    @property
    def nb_triples(self) -> int:
        """Get total number of triples."""
        return len(self._triples)

    @property
    def nb_subjects(self) -> int:
        """Get number of distinct subjects."""
        return len(self._subjects)

    @property
    def nb_predicates(self) -> int:
        """Get number of distinct predicates."""
        return len(self._predicates)

    @property
    def nb_objects(self) -> int:
        """Get number of distinct objects."""
        return len(self._objects)

    def close(self) -> None:
        """Close the mock reader (no-op)."""
        pass

    def __enter__(self) -> "MockHDTReader":
        """Context manager entry."""
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()


@pytest.fixture
def ex() -> str:
    """Example namespace prefix."""
    return "http://example.org/"


@pytest.fixture
def create_graph() -> type[Graph]:
    """Factory for creating RDF graphs."""
    return Graph


@pytest.fixture
def create_reader():
    """Factory for creating mock HDT readers from graphs."""

    def _create(graph: Graph) -> MockHDTReader:
        return MockHDTReader(graph)

    return _create
