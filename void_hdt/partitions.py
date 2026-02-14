"""Class and property partition analysis for VOID."""

from collections import defaultdict
from collections.abc import Callable, Iterator

from rdflib import RDF, BNode, Literal, URIRef
from rdflib_hdt import HDTDocument

type RDFTerm = URIRef | Literal | BNode

# Reusable constants to avoid allocating new frozensets per triple
_EMPTY_FROZENSET: frozenset[RDFTerm | None] = frozenset()
_UNTYPED_TARGET: frozenset[RDFTerm | None] = frozenset({None})


class PropertyPartition:
    """Represents a property partition with target class breakdowns."""

    def __init__(self, predicate: RDFTerm) -> None:
        """Initialize property partition.

        Args:
            predicate: The property/predicate URI
        """
        self.predicate = predicate
        self.total_count: int = 0
        # Target class -> count of triples with objects of that class
        # None key represents literals or untyped URIs
        self.target_class_counts: dict[RDFTerm | None, int] = defaultdict(int)

    def add_triple(self, target_classes: frozenset[RDFTerm | None]) -> None:
        """Record a triple using this property.

        Increments total_count by 1 (for the single triple), and increments
        target_class_counts for each class the object belongs to.

        Args:
            target_classes: Classes the object belongs to, or {None} for literals/untyped
        """
        self.total_count += 1
        for target_class in target_classes:
            self.target_class_counts[target_class] += 1

    def iter_target_classes(self) -> Iterator[tuple[RDFTerm | None, int]]:
        """Iterate over target class counts.

        Yields:
            Tuples of (target_class, count)
        """
        yield from self.target_class_counts.items()


class ClassPartition:
    """Represents a class partition with its property partitions."""

    def __init__(self, class_uri: RDFTerm) -> None:
        """Initialize class partition.

        Args:
            class_uri: URI of the class
        """
        self.class_uri = class_uri
        self.instance_count: int = 0
        # Property -> PropertyPartition with target class breakdowns
        self.property_partitions: dict[RDFTerm, PropertyPartition] = {}

    def add_triple(self, predicate: RDFTerm, target_classes: frozenset[RDFTerm | None]) -> None:
        """Record a triple for instances of this class.

        Args:
            predicate: The property/predicate being used
            target_classes: Classes the object belongs to, or {None} for literals/untyped
        """
        if predicate not in self.property_partitions:
            self.property_partitions[predicate] = PropertyPartition(predicate)
        self.property_partitions[predicate].add_triple(target_classes)

    @property
    def triple_count(self) -> int:
        """Get total triple count for this class partition.

        Returns:
            Sum of all property partition triple counts
        """
        return sum(pp.total_count for pp in self.property_partitions.values())

    def iter_property_partitions(self) -> Iterator[PropertyPartition]:
        """Iterate over property partitions.

        Yields:
            PropertyPartition objects
        """
        yield from self.property_partitions.values()


class PartitionAnalyzer:
    """Analyze class and property partitions in an RDF dataset."""

    def __init__(self) -> None:
        """Initialize partition analyzer."""
        self.class_partitions: dict[RDFTerm, ClassPartition] = {}
        # Dataset-level property counts (all triples, regardless of typing)
        self.dataset_property_counts: dict[RDFTerm, int] = defaultdict(int)

    def analyze(
        self,
        document: HDTDocument,
        cache_size: int = 2_000_000,
        progress_fn: Callable[[str], None] | None = None,
    ) -> None:
        """Analyze partitions from an HDT document.

        This is a two-pass process using ID-based iteration to avoid
        decompressing the HDT string dictionary:
        1. First pass: identify all classes and count instances via rdf:type
        2. Second pass: count property usage for each class, with target class breakdown

        Only ~100 class IDs and ~100 predicate IDs are converted to rdflib
        terms; the millions of subject/object terms are never decompressed.

        Subject lookups use a single-entry cache (exploiting HDT's SPO
        ordering), and object lookups use a bounded dict cleared in bulk
        to avoid pymalloc arena fragmentation. Object-only terms (ID >
        nb_shared) are skipped entirely since they can never be subjects
        and thus have no rdf:type triples.

        Args:
            document: HDT document to analyze
            cache_size: Maximum entries in the type-lookup cache (default 2M)
            progress_fn: Optional callback for progress reporting, receives a message string
        """

        def _log(msg: str) -> None:
            if progress_fn:
                progress_fn(msg)

        # Get rdf:type predicate ID (0 if not in dataset)
        type_pred_id: int = document.term_to_id(RDF.type, 1)
        nb_shared: int = document.nb_shared

        class_id_to_term: dict[int, RDFTerm] = {}

        _log("Pass 1: counting instances per class...")

        if type_pred_id != 0:
            # First pass: count instances per class using ID-based iteration
            type_triples, type_count = document.search_ids((0, type_pred_id, 0))  # type: ignore[arg-type]
            _log(f"  rdf:type triples to process: {type_count:,}")

            class_id_counts: dict[int, int] = defaultdict(int)
            for i, (_s_id, _p_id, class_id) in enumerate(type_triples):
                class_id_counts[class_id] += 1
                if progress_fn and i % 10_000_000 == 0 and i > 0:
                    _log(f"  Pass 1: {i:,} type triples processed")

            # Convert class IDs to rdflib terms (~100 conversions)
            for class_id, count in class_id_counts.items():
                term = document.id_to_term(class_id, 2)
                if isinstance(term, URIRef):
                    class_id_to_term[class_id] = term
                    self.class_partitions[term] = ClassPartition(term)
                    self.class_partitions[term].instance_count = count

            _log(f"  Found {len(self.class_partitions)} classes")
        else:
            _log("  No rdf:type predicate found in dataset")

        search_count = 0

        def _search_types_by_id(term_id: int) -> frozenset[RDFTerm | None]:
            """Look up rdf:type values for a term ID via HDT's SPO index."""
            nonlocal search_count
            search_count += 1
            triples, count = document.search_ids((term_id, type_pred_id, 0))  # type: ignore[arg-type]
            if count == 0:
                return _EMPTY_FROZENSET
            return frozenset(
                class_id_to_term[o_id] for _, _, o_id in triples if o_id in class_id_to_term
            )

        # Subject cache: single-entry, exploits HDT's SPO ordering.
        # Consecutive triples share the same subject, so caching just
        # the last lookup gives near-100% hit rate with O(1) memory.
        prev_subject_id: int = 0
        prev_subject_types: frozenset[RDFTerm | None] = _EMPTY_FROZENSET

        # Object cache: bounded dict, cleared in bulk when full.
        # Unlike lru_cache (which evicts one-by-one, fragmenting pymalloc
        # arenas), bulk clearing lets the allocator free entire arenas.
        obj_type_cache: dict[int, frozenset[RDFTerm | None]] = {}
        cache_clears = 0

        # Predicate ID → rdflib term cache (small, ~100 entries)
        pred_id_to_term: dict[int, RDFTerm] = {}

        _log("Pass 2: counting property usage per class...")

        # Second pass: count property usage for each class with target class tracking
        all_triples, total_count = document.search_ids((0, 0, 0))  # type: ignore[arg-type]
        _log(f"  Total triples to process: {total_count:,}")
        for i, (s_id, p_id, o_id) in enumerate(all_triples):
            # Convert predicate ID to term (cached, ~100 unique predicates)
            predicate = pred_id_to_term.get(p_id)
            if predicate is None:
                predicate = document.id_to_term(p_id, 1)
                pred_id_to_term[p_id] = predicate

            # Count all properties at dataset level
            self.dataset_property_counts[predicate] += 1

            # Look up subject types (single-entry cache, SPO-ordered)
            if s_id != prev_subject_id:
                if type_pred_id != 0:
                    prev_subject_types = _search_types_by_id(s_id)
                else:
                    prev_subject_types = _EMPTY_FROZENSET
                prev_subject_id = s_id

            if not prev_subject_types:
                continue

            # Look up object types
            if o_id > nb_shared:
                # Object-only term: can never be a subject, so no types
                target_classes = _UNTYPED_TARGET
            else:
                obj_classes = obj_type_cache.get(o_id)
                if obj_classes is None:
                    obj_classes = _search_types_by_id(o_id)
                    if len(obj_type_cache) >= cache_size:
                        obj_type_cache.clear()
                        cache_clears += 1
                    obj_type_cache[o_id] = obj_classes
                target_classes = obj_classes if obj_classes else _UNTYPED_TARGET

            # Record triple for each class the subject belongs to
            for class_uri in prev_subject_types:
                if class_uri in self.class_partitions:
                    self.class_partitions[class_uri].add_triple(predicate, target_classes)

            if progress_fn and i % 10_000_000 == 0 and i > 0:
                _log(
                    f"  Pass 2: {i:,}/{total_count:,} triples | "
                    f"obj_cache: {len(obj_type_cache):,} | "
                    f"searches: {search_count:,} | "
                    f"cache_clears: {cache_clears}"
                )

        _log(f"  Done. Total searches: {search_count:,}, cache clears: {cache_clears}")

    def iter_partitions(self) -> Iterator[ClassPartition]:
        """Iterate over all class partitions.

        Yields:
            ClassPartition objects
        """
        yield from self.class_partitions.values()

    def iter_dataset_properties(self) -> Iterator[tuple[RDFTerm, int]]:
        """Iterate over dataset-level property counts.

        Yields:
            Tuples of (predicate, count)
        """
        yield from self.dataset_property_counts.items()
