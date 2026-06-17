"""Generate VOID vocabulary descriptions."""

import hashlib
from itertools import batched

from rdflib import RDF, RDFS, BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import VOID, XSD
from rdflib_hdt import HDTDocument

from void_hdt.partitions import PartitionAnalyzer

# VOID extension namespace (from ldf.fi)
VOIDEXT = Namespace("http://ldf.fi/void-ext#")


class VOIDGenerator:
    """Generate VOID descriptions for RDF datasets."""

    def __init__(
        self, dataset_uri: str = "http://example.org/dataset", use_blank_nodes: bool = False
    ) -> None:
        """Initialize VOID generator.

        Args:
            dataset_uri: URI for the dataset being described
            use_blank_nodes: Use blank nodes for partition nodes instead of URI references
        """
        self.dataset_uri = URIRef(dataset_uri)
        self.use_blank_nodes = use_blank_nodes
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

    def _create_partition_node(self, uri_path: str) -> URIRef | BNode:
        """Create a partition node as either a blank node or URI reference.

        Args:
            uri_path: The URI path to use if creating a URIRef

        Returns:
            Either a BNode (if use_blank_nodes is True) or URIRef
        """
        if self.use_blank_nodes:
            return BNode()
        else:
            return URIRef(uri_path)

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
            prop_partition_uri = self._create_partition_node(
                f"{self.dataset_uri}/property/{predicate_hash}"
            )

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

        Resolves integer IDs to RDFLib terms using the analyzer's mappings.

        Args:
            analyzer: Partition analyzer with class and property data
        """
        class_id_to_term = analyzer.class_id_to_term
        pred_id_to_term = analyzer.pred_id_to_term

        for partition in analyzer.iter_partitions():
            class_uri = class_id_to_term[partition.class_id]

            # Create a URI for this partition using MD5 hash of the class URI
            class_hash = self._hash_iri(str(class_uri))
            partition_uri = self._create_partition_node(f"{self.dataset_uri}/class/{class_hash}")

            # Declare it as a class partition
            self.graph.add((partition_uri, RDF.type, VOID.Dataset))
            self.graph.add((self.dataset_uri, VOID.classPartition, partition_uri))

            # Link to the class
            self.graph.add((partition_uri, VOID["class"], class_uri))

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
                predicate = pred_id_to_term[prop_partition.predicate_id]

                # Create property partition URI using MD5 hash of the predicate URI
                predicate_hash = self._hash_iri(str(predicate))
                prop_partition_uri = self._create_partition_node(
                    f"{partition_uri}/property/{predicate_hash}"
                )

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
                for target_class, count in prop_partition.iter_target_classes(class_id_to_term):
                    if target_class is None:
                        # Literals or untyped URIs - use special hash
                        target_hash = self._hash_iri("__untyped__")
                        target_partition_uri = self._create_partition_node(
                            f"{prop_partition_uri}/target/{target_hash}"
                        )

                        # Declare target partition (no void:class for untyped)
                        self.graph.add((target_partition_uri, RDF.type, VOID.Dataset))
                        self.graph.add(
                            (prop_partition_uri, VOIDEXT.objectClassPartition, target_partition_uri)
                        )
                    else:
                        # Typed target class
                        target_hash = self._hash_iri(str(target_class))
                        target_partition_uri = self._create_partition_node(
                            f"{prop_partition_uri}/target/{target_hash}"
                        )

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

    def add_labels_descriptions(self, analyzer: PartitionAnalyzer, document: HDTDocument) -> None:
        """Retrieves labels and descriptions for entities.

        First the graph itself is queried for these things,
        and then the federated endpoint is re-queried for them.

        Args:
            document: HDT of the graph to be checked first for labels.
        """
        from rdflib import Namespace
        from rdflib.namespace import DC, DCTERMS, PROV, RDFS, SDO, SKOS
        SDOH = Namespace("http://schema.org/")

        entities_to_check = set()
        class_id_to_term = analyzer.class_id_to_term
        pred_id_to_term = analyzer.pred_id_to_term
        for partition in analyzer.iter_partitions():
            class_uri = class_id_to_term[partition.class_id]
            entities_to_check.add('<' + str(class_uri) + '>')
            for prop_partition in partition.iter_property_partitions():
                predicate = pred_id_to_term[prop_partition.predicate_id]
                entities_to_check.add('<' + str(predicate) + '>')
                for target_class, count in prop_partition.iter_target_classes(class_id_to_term):
                    if target_class:
                        entities_to_check.add('<' + str(target_class) + '>')

        for pred, pred_options in [
            [RDFS.label, [RDFS.label, SDO.name, SDOH.name, DCTERMS.title, DC.title,],],
            [SKOS.definition, [DCTERMS.description, DC.description, SKOS.definition, SDO.description, SDOH.description, PROV.definition, RDFS.comment,],],
        ]:
            for pred_to_try in pred_options:
                triples, cardinality = document.search((None, pred_to_try, None))
                for s, p, o in triples:
                    if s in entities_to_check:
                        self.graph.add((s, pred, o))

            for batch in batched(list(entities_to_check), 50):
                target_query = f"""
SELECT ?s ?p ?o
WHERE {{
    SERVICE <https://frink.apps.renci.org/federation/sparql> {{
        values ?s {{ {" ".join(batch)} }}
        values ?p {{ {" ".join([('<' + str(uri) + '>') for uri in pred_options])} }}
        ?s ?p ?o .
    }}
}}
"""
                qres = self.graph.query(target_query)
                for row in qres:
                    self.graph.add((getattr(row, 's'), getattr(row, 'p'), getattr(row, 'o')))

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
