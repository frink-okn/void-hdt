"""Pytest fixtures and mock HDTDocument for testing."""

from collections.abc import Iterator
from typing import cast

import pytest
from rdflib import BNode, Graph, Literal, URIRef

type RDFTerm = URIRef | Literal | BNode
type Triple = tuple[RDFTerm, RDFTerm, RDFTerm]
type TripleID = tuple[int, int, int]
type PatternTerm = URIRef | Literal


class MockHDTDocument:
    """Mock HDTDocument that works with in-memory RDF graphs.

    This allows testing the VOID processing logic without actual HDT files.
    Mimics the interface of rdflib_hdt.HDTDocument, including ID-based access.

    ID assignment follows HDT dictionary structure:
    - Shared terms (both subject and object): IDs 1..S in both spaces
    - Subject-only terms: IDs S+1.. in subject space
    - Object-only terms: IDs S+1.. in object space
    - Predicates: separate ID space 1..P
    """

    def __init__(self, graph: Graph) -> None:
        """Initialize mock document from an RDF graph.

        Args:
            graph: RDFLib graph containing triples to process
        """
        self.graph = graph
        self._triples: list[Triple] = [cast(Triple, (s, p, o)) for s, p, o in graph]
        self._subjects: set[RDFTerm] = set()
        self._predicates: set[RDFTerm] = set()
        self._objects: set[RDFTerm] = set()

        for s, p, o in self._triples:
            self._subjects.add(s)
            self._predicates.add(p)
            self._objects.add(o)

        # HDT dictionary structure
        # Explicit annotations needed because set operations and sorted()
        # widen the element type to include Buffer (URIRef inherits str
        # which supports buffer protocol in Python 3.12+).
        shared: set[RDFTerm] = self._subjects & self._objects
        subject_only: set[RDFTerm] = self._subjects - shared
        object_only: set[RDFTerm] = self._objects - shared
        self._nb_shared = len(shared)

        shared_sorted: list[RDFTerm] = sorted(shared, key=str)  # type: ignore[assignment]
        subj_only_sorted: list[RDFTerm] = sorted(subject_only, key=str)  # type: ignore[assignment]
        obj_only_sorted: list[RDFTerm] = sorted(object_only, key=str)  # type: ignore[assignment]
        pred_sorted: list[RDFTerm] = sorted(self._predicates, key=str)  # type: ignore[assignment]

        # Subject ID space: shared (1..S), then subject-only (S+1..)
        self._subject_to_id: dict[RDFTerm, int] = {}
        self._id_to_subject: dict[int, RDFTerm] = {}
        for i, t in enumerate(shared_sorted, 1):
            self._subject_to_id[t] = i
            self._id_to_subject[i] = t
        for i, t in enumerate(subj_only_sorted, self._nb_shared + 1):
            self._subject_to_id[t] = i
            self._id_to_subject[i] = t

        # Object ID space: shared (1..S), then object-only (S+1..)
        self._object_to_id: dict[RDFTerm, int] = {}
        self._id_to_object: dict[int, RDFTerm] = {}
        for i, t in enumerate(shared_sorted, 1):
            self._object_to_id[t] = i
            self._id_to_object[i] = t
        for i, t in enumerate(obj_only_sorted, self._nb_shared + 1):
            self._object_to_id[t] = i
            self._id_to_object[i] = t

        # Predicate ID space: 1..P
        self._predicate_to_id: dict[RDFTerm, int] = {}
        self._id_to_predicate: dict[int, RDFTerm] = {}
        for i, t in enumerate(pred_sorted, 1):
            self._predicate_to_id[t] = i
            self._id_to_predicate[i] = t

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

    def search_ids(
        self,
        query: tuple[int | None, int | None, int | None],
        limit: int = 0,
        offset: int = 0,
    ) -> tuple[Iterator[TripleID], int]:
        """Search for triples matching the given ID pattern.

        Use 0 or None for wildcards.
        """
        s_id = query[0] or 0
        p_id = query[1] or 0
        o_id = query[2] or 0

        # Convert non-zero IDs to terms for matching
        s_filter = self._id_to_subject.get(s_id) if s_id else None
        p_filter = self._id_to_predicate.get(p_id) if p_id else None
        o_filter = self._id_to_object.get(o_id) if o_id else None

        # Non-zero ID not found in dictionary → no matches
        if (
            (s_id and s_filter is None)
            or (p_id and p_filter is None)
            or (o_id and o_filter is None)
        ):
            return iter([]), 0

        matches: list[TripleID] = []
        for s, p, o in self._triples:
            if s_filter is not None and s != s_filter:
                continue
            if p_filter is not None and p != p_filter:
                continue
            if o_filter is not None and o != o_filter:
                continue
            matches.append(
                (
                    self._subject_to_id[s],
                    self._predicate_to_id[p],
                    self._object_to_id[o],
                )
            )
        return iter(matches), len(matches)

    def term_to_id(self, term: RDFTerm, kind: int) -> int:
        """Convert an rdflib term to its HDT integer ID.

        Args:
            term: The rdflib term
            kind: 0=subject, 1=predicate, 2=object

        Returns:
            Integer ID, or 0 if not found
        """
        if kind == 0:
            return self._subject_to_id.get(term, 0)
        if kind == 1:
            return self._predicate_to_id.get(term, 0)
        if kind == 2:
            return self._object_to_id.get(term, 0)
        return 0

    def id_to_term(self, term_id: int, kind: int) -> RDFTerm:
        """Convert an HDT integer ID to its rdflib term.

        Args:
            term_id: The integer ID
            kind: 0=subject, 1=predicate, 2=object
        """
        if kind == 0:
            return self._id_to_subject[term_id]
        if kind == 1:
            return self._id_to_predicate[term_id]
        if kind == 2:
            return self._id_to_object[term_id]
        msg = f"Invalid kind: {kind}"
        raise ValueError(msg)

    @property
    def nb_shared(self) -> int:
        """Get number of shared subject-object terms."""
        return self._nb_shared

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
