"""Pytest fixtures and mock HDTDocument for testing."""

from collections.abc import Iterator

import pytest
from rdflib import BNode, Graph, Literal, URIRef

type RDFTerm = URIRef | Literal | BNode
type Triple = tuple[RDFTerm, RDFTerm, RDFTerm]
type PatternTerm = URIRef | Literal


class MockHDTDocument:
    """Mock HDTDocument that works with in-memory RDF graphs.

    This allows testing the VOID processing logic without actual HDT files.
    Mimics the interface of rdflib_hdt.HDTDocument.
    """

    def __init__(self, graph: Graph) -> None:
        """Initialize mock document from an RDF graph.

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

    def search(
        self, pattern: tuple[PatternTerm | None, PatternTerm | None, PatternTerm | None]
    ) -> tuple[Iterator[Triple], int]:
        """Search for triples matching the given pattern.

        Args:
            pattern: Tuple of (subject, predicate, object) filters (None for any)

        Returns:
            Tuple of (iterator over matching triples, count of matches)
        """
        subject, predicate, obj = pattern
        matches: list[Triple] = []
        for s, p, o in self._triples:
            if subject is not None and s != subject:
                continue
            if predicate is not None and p != predicate:
                continue
            if obj is not None and o != obj:
                continue
            matches.append((s, p, o))
        return iter(matches), len(matches)

    @property
    def total_triples(self) -> int:
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


@pytest.fixture
def ex() -> str:
    """Example namespace prefix."""
    return "http://example.org/"


@pytest.fixture
def create_graph() -> type[Graph]:
    """Factory for creating RDF graphs."""
    return Graph


@pytest.fixture
def create_document():
    """Factory for creating mock HDT documents from graphs."""

    def _create(graph: Graph) -> MockHDTDocument:
        return MockHDTDocument(graph)

    return _create
