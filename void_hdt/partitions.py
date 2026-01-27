"""Class and property partition analysis for VOID."""

from collections import defaultdict
from collections.abc import Iterator

from rdflib import RDF, BNode, Literal, URIRef
from rdflib_hdt import HDTDocument

type RDFTerm = URIRef | Literal | BNode


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

    def add_triple(self, target_classes: set[RDFTerm] | set[None]) -> None:
        """Record a triple using this property.

        Increments total_count by 1 (for the single triple), and increments
        target_class_counts for each class the object belongs to.

        Args:
            target_classes: Set of classes the object belongs to, or {None} for literals/untyped
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

    def add_triple(self, predicate: RDFTerm, target_classes: set[RDFTerm] | set[None]) -> None:
        """Record a triple for instances of this class.

        Args:
            predicate: The property/predicate being used
            target_classes: Set of classes the object belongs to, or {None} for literals/untyped
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
        # Map instances to their classes
        self.instance_classes: dict[RDFTerm, set[RDFTerm]] = defaultdict(set)
        # Dataset-level property counts (all triples, regardless of typing)
        self.dataset_property_counts: dict[RDFTerm, int] = defaultdict(int)

    def analyze(self, document: HDTDocument) -> None:
        """Analyze partitions from an HDT document.

        This is a two-pass process:
        1. First pass: identify all instances and their classes via rdf:type
        2. Second pass: count property usage for each class, with target class breakdown

        Args:
            document: HDT document to analyze
        """
        # First pass: collect all rdf:type statements
        type_triples, _ = document.search((None, RDF.type, None))
        for triple in type_triples:
            subject, _, obj = triple
            # obj is the class, subject is the instance
            if isinstance(obj, URIRef):  # Classes should be URIs
                self._add_instance_to_class(subject, obj)

        # Second pass: count property usage for each class with target class tracking
        all_triples, _ = document.search((None, None, None))
        for triple in all_triples:
            subject, predicate, obj = triple

            # Count all properties at dataset level
            self.dataset_property_counts[predicate] += 1

            # Only process class partitions if subject is a typed instance
            if subject not in self.instance_classes:
                continue

            # Determine target class(es) for the object
            # If object is typed, use its classes; otherwise None (literal or untyped URI)
            if obj in self.instance_classes:
                target_classes: set[RDFTerm] | set[None] = self.instance_classes[obj]
            else:
                # Literal or untyped URI - use None as target class
                target_classes = {None}

            # Record triple for each class the subject belongs to
            for class_uri in self.instance_classes[subject]:
                if class_uri in self.class_partitions:
                    self.class_partitions[class_uri].add_triple(predicate, target_classes)

    def _add_instance_to_class(self, instance: RDFTerm, class_uri: RDFTerm) -> None:
        """Add an instance to a class partition.

        Args:
            instance: The instance URI
            class_uri: The class URI
        """
        # Check if we've already seen this instance-class pair
        already_seen = class_uri in self.instance_classes[instance]

        # Track which classes this instance belongs to
        self.instance_classes[instance].add(class_uri)

        # Only increment count if this is the first time seeing this pair
        if not already_seen:
            # Create partition if it doesn't exist
            if class_uri not in self.class_partitions:
                self.class_partitions[class_uri] = ClassPartition(class_uri)

            # Increment instance count
            self.class_partitions[class_uri].instance_count += 1

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
