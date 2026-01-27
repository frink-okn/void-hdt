"""Generate VOID vocabulary descriptions."""

import hashlib

from rdflib import RDF, RDFS, Graph, Literal, Namespace, URIRef
from rdflib.namespace import VOID, XSD
from rdflib_hdt import HDTDocument

from void_hdt.partitions import PartitionAnalyzer

# VOID extension namespace (from ldf.fi)
VOIDEXT = Namespace("http://ldf.fi/void-ext#")


class VOIDGenerator:
    """Generate VOID descriptions for RDF datasets."""

    def __init__(self, dataset_uri: str = "http://example.org/dataset") -> None:
        """Initialize VOID generator.

        Args:
            dataset_uri: URI for the dataset being described
        """
        self.dataset_uri = URIRef(dataset_uri)
        self.graph = Graph()
        self._bind_namespaces()

    def _bind_namespaces(self) -> None:
        """Bind common namespaces for cleaner output."""
        self.graph.bind("void", VOID)
        self.graph.bind("voidext", VOIDEXT)
        self.graph.bind("rdf", RDF)
        self.graph.bind("rdfs", RDFS)
        self.graph.bind("xsd", XSD)

    @staticmethod
    def _hash_iri(iri: str) -> str:
        """Compute MD5 hash of an IRI for use in partition URIs.

        Args:
            iri: The IRI to hash

        Returns:
            MD5 hash as a hexadecimal string
        """
        return hashlib.md5(iri.encode("utf-8")).hexdigest()

    def add_dataset_statistics(self, document: HDTDocument) -> None:
        """Add dataset-level statistics to the VOID description.

        Args:
            document: HDT document to get statistics from (O(1) via HDT index)
        """
        # Declare this is a VOID Dataset
        self.graph.add((self.dataset_uri, RDF.type, VOID.Dataset))

        # Add triple count
        self.graph.add(
            (self.dataset_uri, VOID.triples, Literal(document.total_triples, datatype=XSD.integer))
        )

        # Add distinct counts
        self.graph.add(
            (
                self.dataset_uri,
                VOID.distinctSubjects,
                Literal(document.nb_subjects, datatype=XSD.integer),
            )
        )
        self.graph.add(
            (
                self.dataset_uri,
                VOID.properties,
                Literal(document.nb_predicates, datatype=XSD.integer),
            )
        )
        self.graph.add(
            (
                self.dataset_uri,
                VOID.distinctObjects,
                Literal(document.nb_objects, datatype=XSD.integer),
            )
        )

    def add_dataset_property_partitions(self, analyzer: PartitionAnalyzer) -> None:
        """Add dataset-level property partitions to the VOID description.

        These are property partitions directly on the dataset, showing triple
        counts per property across all triples (regardless of subject typing).

        Args:
            analyzer: Partition analyzer with dataset property counts
        """
        for predicate, count in analyzer.iter_dataset_properties():
            # Create property partition URI using MD5 hash of the predicate URI
            predicate_hash = self._hash_iri(str(predicate))
            prop_partition_uri = URIRef(f"{self.dataset_uri}/property/{predicate_hash}")

            # Declare property partition
            self.graph.add((prop_partition_uri, RDF.type, VOID.Dataset))
            self.graph.add((self.dataset_uri, VOID.propertyPartition, prop_partition_uri))

            # Link to the property
            self.graph.add((prop_partition_uri, VOID.property, predicate))

            # Add triple count
            self.graph.add(
                (
                    prop_partition_uri,
                    VOID.triples,
                    Literal(count, datatype=XSD.integer),
                )
            )

    def add_class_partitions(self, analyzer: PartitionAnalyzer) -> None:
        """Add class partition information to the VOID description.

        Args:
            analyzer: Partition analyzer with class and property data
        """
        for partition in analyzer.iter_partitions():
            # Create a URI for this partition using MD5 hash of the class URI
            class_hash = self._hash_iri(str(partition.class_uri))
            partition_uri = URIRef(f"{self.dataset_uri}/class/{class_hash}")

            # Declare it as a class partition
            self.graph.add((partition_uri, RDF.type, VOID.Dataset))
            self.graph.add((self.dataset_uri, VOID.classPartition, partition_uri))

            # Link to the class
            self.graph.add((partition_uri, VOID["class"], partition.class_uri))

            # Add entity count (number of instances)
            self.graph.add(
                (
                    partition_uri,
                    VOID.entities,
                    Literal(partition.instance_count, datatype=XSD.integer),
                )
            )

            # Add triple count for this class partition
            self.graph.add(
                (
                    partition_uri,
                    VOID.triples,
                    Literal(partition.triple_count, datatype=XSD.integer),
                )
            )

            # Add property partitions
            for prop_partition in partition.iter_property_partitions():
                predicate = prop_partition.predicate

                # Create property partition URI using MD5 hash of the predicate URI
                predicate_hash = self._hash_iri(str(predicate))
                prop_partition_uri = URIRef(f"{partition_uri}/property/{predicate_hash}")

                # Declare property partition
                self.graph.add((prop_partition_uri, RDF.type, VOID.Dataset))
                self.graph.add((partition_uri, VOID.propertyPartition, prop_partition_uri))

                # Link to the property
                self.graph.add((prop_partition_uri, VOID.property, predicate))

                # Add total triple count for this property
                self.graph.add(
                    (
                        prop_partition_uri,
                        VOID.triples,
                        Literal(prop_partition.total_count, datatype=XSD.integer),
                    )
                )

                # Add target class partitions
                for target_class, count in prop_partition.iter_target_classes():
                    if target_class is None:
                        # Literals or untyped URIs - use special hash
                        target_hash = self._hash_iri("__untyped__")
                        target_partition_uri = URIRef(f"{prop_partition_uri}/target/{target_hash}")

                        # Declare target partition (no void:class for untyped)
                        self.graph.add((target_partition_uri, RDF.type, VOID.Dataset))
                        self.graph.add(
                            (prop_partition_uri, VOIDEXT.objectClassPartition, target_partition_uri)
                        )
                    else:
                        # Typed target class
                        target_hash = self._hash_iri(str(target_class))
                        target_partition_uri = URIRef(f"{prop_partition_uri}/target/{target_hash}")

                        # Declare target partition with class link
                        self.graph.add((target_partition_uri, RDF.type, VOID.Dataset))
                        self.graph.add(
                            (prop_partition_uri, VOIDEXT.objectClassPartition, target_partition_uri)
                        )
                        self.graph.add((target_partition_uri, VOID["class"], target_class))

                    # Add triple count for this target class
                    self.graph.add(
                        (
                            target_partition_uri,
                            VOID.triples,
                            Literal(count, datatype=XSD.integer),
                        )
                    )

    def serialize(self, format: str = "turtle") -> str:
        """Serialize the VOID description.

        Args:
            format: RDF serialization format (default: turtle)

        Returns:
            Serialized RDF as a string
        """
        return self.graph.serialize(format=format)

    def save(self, output_path: str, format: str = "turtle") -> None:
        """Save the VOID description to a file.

        Args:
            output_path: Path to save the file
            format: RDF serialization format (default: turtle)
        """
        self.graph.serialize(destination=output_path, format=format)
