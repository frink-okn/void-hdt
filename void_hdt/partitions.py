"""Class and property partition analysis for VOID."""

from collections import defaultdict
from collections.abc import Callable, Iterator

from rdflib import RDF, BNode, Literal, URIRef
from rdflib_hdt import HDTDocument

type RDFTerm = URIRef | Literal | BNode

# Sentinel ID for literals or untyped URIs (0 is never a valid HDT ID)
UNTYPED_TARGET_ID = 0

# Reusable constants to avoid allocating new frozensets per triple
_EMPTY_FROZENSET: frozenset[int] = frozenset()
_UNTYPED_TARGET: frozenset[int] = frozenset({UNTYPED_TARGET_ID})


class PropertyPartition:
    """Represents a property partition with target class breakdowns."""

    def __init__(self, predicate_id: int) -> None:
        """Initialize property partition.

        Args:
            predicate_id: The HDT integer ID for this property/predicate
        """
        self.predicate_id = predicate_id
        self.total_count: int = 0
        # Target class ID -> count of triples with objects of that class
        # UNTYPED_TARGET_ID (0) represents literals or untyped URIs
        self.target_class_counts: dict[int, int] = defaultdict(int)

    def add_triple(self, target_class_ids: frozenset[int]) -> None:
        """Record a triple using this property.

        Increments total_count by 1 (for the single triple), and increments
        target_class_counts for each class the object belongs to.

        Args:
            target_class_ids: Class IDs the object belongs to, or {0} for literals/untyped
        """
        self.total_count += 1
        for class_id in target_class_ids:
            self.target_class_counts[class_id] += 1

    def iter_target_classes(
        self, class_id_to_term: dict[int, RDFTerm]
    ) -> Iterator[tuple[RDFTerm | None, int]]:
        """Iterate over target class counts, resolving IDs to terms.

        Args:
            class_id_to_term: Mapping from class IDs to RDFLib terms

        Yields:
            Tuples of (target_class_term_or_None, count)
        """
        for class_id, count in self.target_class_counts.items():
            if class_id == UNTYPED_TARGET_ID:
                yield (None, count)
            else:
                yield (class_id_to_term[class_id], count)


class ClassPartition:
    """Represents a class partition with its property partitions."""

    def __init__(self, class_id: int) -> None:
        """Initialize class partition.

        Args:
            class_id: HDT integer ID for this class
        """
        self.class_id = class_id
        self.instance_count: int = 0
        # Predicate ID -> PropertyPartition with target class breakdowns
        self.property_partitions: dict[int, PropertyPartition] = {}

    def add_triple(self, pred_id: int, target_class_ids: frozenset[int]) -> None:
        """Record a triple for instances of this class.

        Args:
            pred_id: The HDT integer ID of the predicate
            target_class_ids: Class IDs the object belongs to, or {0} for literals/untyped
        """
        if pred_id not in self.property_partitions:
            self.property_partitions[pred_id] = PropertyPartition(pred_id)
        self.property_partitions[pred_id].add_triple(target_class_ids)

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
    """Analyze class and property partitions in an RDF dataset.

    All internal data structures use HDT integer IDs for efficiency.
    ID-to-term mappings are stored for resolution during serialization.
    """

    def __init__(self) -> None:
        """Initialize partition analyzer."""
        self.class_partitions: dict[int, ClassPartition] = {}
        # Dataset-level property counts (all triples, regardless of typing)
        self.dataset_property_counts: dict[int, int] = defaultdict(int)
        # ID → term mappings for serialization
        self.class_id_to_term: dict[int, RDFTerm] = {}
        self.pred_id_to_term: dict[int, RDFTerm] = {}
        # Reverse mappings for convenience lookups (term → ID)
        self._term_to_class_id: dict[RDFTerm, int] = {}
        self._term_to_pred_id: dict[RDFTerm, int] = {}

    def class_partition_for(self, term: RDFTerm) -> ClassPartition | None:
        """Look up a class partition by its RDFLib term.

        Args:
            term: The class URI

        Returns:
            The ClassPartition, or None if not found
        """
        class_id = self._term_to_class_id.get(term)
        if class_id is None:
            return None
        return self.class_partitions.get(class_id)

    def property_partition_for(
        self, class_term: RDFTerm, pred_term: RDFTerm
    ) -> PropertyPartition | None:
        """Look up a property partition by class and predicate terms.

        Args:
            class_term: The class URI
            pred_term: The predicate URI

        Returns:
            The PropertyPartition, or None if not found
        """
        cp = self.class_partition_for(class_term)
        if cp is None:
            return None
        pred_id = self._term_to_pred_id.get(pred_term)
        if pred_id is None:
            return None
        return cp.property_partitions.get(pred_id)

    def has_class_partition(self, term: RDFTerm) -> bool:
        """Check if a class partition exists for the given term.

        Args:
            term: The class URI

        Returns:
            True if a partition exists
        """
        class_id = self._term_to_class_id.get(term)
        return class_id is not None and class_id in self.class_partitions

    def has_property_partition(self, class_term: RDFTerm, pred_term: RDFTerm) -> bool:
        """Check if a property partition exists for the given class and predicate.

        Args:
            class_term: The class URI
            pred_term: The predicate URI

        Returns:
            True if the property partition exists
        """
        return self.property_partition_for(class_term, pred_term) is not None

    def analyze(
        self,
        document: HDTDocument,
        cache_size: int = 2_000_000,
        progress_fn: Callable[[str], None] | None = None,
    ) -> None:
        """Analyze partitions from an HDT document.

        This is a two-pass process using ID-based iteration to avoid
        decompressing the HDT string dictionary:
        1. Pass 1: Identify all classes and count instances via rdf:type
        2. Pass 2: Single sequential scan of all triples — counts dataset-level
           property usage AND per-class property/target-class breakdowns

        Only class IDs and predicate IDs are converted to rdflib terms;
        the millions of subject/object terms are never decompressed.

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
        nb_subjects: int = document.nb_subjects

        # Bitmap tracking which subject IDs have rdf:type triples.
        # Allows O(1) skip of untyped subjects/objects in Pass 2,
        # eliminating millions of fruitless search_ids calls (~20 MB for 160M subjects).
        typed_bitmap = bytearray(nb_subjects // 8 + 1)

        # --- Pass 1: Count instances per class ---
        _log("Pass 1: counting instances per class...")

        if type_pred_id != 0:
            # First pass: count instances per class using ID-based iteration
            type_triples, type_count = document.search_ids((0, type_pred_id, 0))  # type: ignore[arg-type]
            _log(f"  rdf:type triples to process: {type_count:,}")

            class_id_counts: dict[int, int] = defaultdict(int)
            for i, (s_id, _p_id, class_id) in enumerate(type_triples):
                class_id_counts[class_id] += 1
                typed_bitmap[s_id >> 3] |= 1 << (s_id & 7)
                if progress_fn and i % 10_000_000 == 0 and i > 0:
                    _log(f"  Pass 1: {i:,} type triples processed")

            # Convert class IDs to rdflib terms (~100 conversions)
            for class_id, count in class_id_counts.items():
                term = document.id_to_term(class_id, 2)
                if isinstance(term, URIRef):
                    self.class_id_to_term[class_id] = term
                    self._term_to_class_id[term] = class_id
                    self.class_partitions[class_id] = ClassPartition(class_id)
                    self.class_partitions[class_id].instance_count = count

            typed_count = sum(b.bit_count() for b in typed_bitmap)
            _log(f"  Found {len(self.class_partitions)} classes, {typed_count:,} typed subjects")
        else:
            _log("  No rdf:type predicate found in dataset")

        search_count = 0

        def _search_types_by_id(term_id: int) -> frozenset[int]:
            """Look up rdf:type values for a term ID via HDT's SPO index."""
            nonlocal search_count
            search_count += 1
            triples, count = document.search_ids((term_id, type_pred_id, 0))  # type: ignore[arg-type]
            if count == 0:
                return _EMPTY_FROZENSET
            return frozenset(o_id for _, _, o_id in triples if o_id in self.class_id_to_term)

        # Object cache: bounded dict, cleared in bulk when full.
        # Unlike lru_cache (which evicts one-by-one, fragmenting pymalloc
        # arenas), bulk clearing lets the allocator free entire arenas.
        obj_type_cache: dict[int, frozenset[int]] = {}
        cache_clears = 0

        _log("Pass 2: counting properties and class partitions...")

        # --- Pass 2: Single sequential scan of all triples ---
        # Counts dataset-level property usage (all triples) and per-class
        # property/target-class breakdowns (typed subjects only).
        # Subject cache: single-entry, exploits HDT's SPO ordering.
        # Consecutive triples share the same subject, so caching just
        # the last lookup gives near-100% hit rate with O(1) memory.
        prev_subject_id: int = 0
        prev_subject_types: frozenset[int] = _EMPTY_FROZENSET

        # Predicate ID → rdflib term cache (small, ~100 entries)
        pred_id_to_term_local: dict[int, RDFTerm] = {}

        all_triples, total_count = document.search_ids((0, 0, 0))  # type: ignore[arg-type]
        _log(f"  Total triples to process: {total_count:,}")
        for i, (s_id, p_id, o_id) in enumerate(all_triples):
            # Convert predicate ID to term (cached, ~100 unique predicates)
            if p_id not in pred_id_to_term_local:
                term = document.id_to_term(p_id, 1)
                pred_id_to_term_local[p_id] = term
                self.pred_id_to_term[p_id] = term
                self._term_to_pred_id[term] = p_id

            # Count all properties at dataset level
            self.dataset_property_counts[p_id] += 1

            # Look up subject types (single-entry cache, SPO-ordered)
            # Bitmap check eliminates search_ids calls for untyped subjects
            if s_id != prev_subject_id:
                if typed_bitmap[s_id >> 3] & (1 << (s_id & 7)):
                    prev_subject_types = _search_types_by_id(s_id)
                else:
                    prev_subject_types = _EMPTY_FROZENSET
                prev_subject_id = s_id

            if not prev_subject_types:
                continue

            # Look up object types
            # Bitmap check skips untyped objects; nb_shared check skips object-only terms
            if o_id > nb_shared or not (typed_bitmap[o_id >> 3] & (1 << (o_id & 7))):
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
            for class_id in prev_subject_types:
                self.class_partitions[class_id].add_triple(p_id, target_classes)

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
        """Iterate over dataset-level property counts, resolving IDs to terms.

        Yields:
            Tuples of (predicate_term, count)
        """
        for pred_id, count in self.dataset_property_counts.items():
            yield (self.pred_id_to_term[pred_id], count)
