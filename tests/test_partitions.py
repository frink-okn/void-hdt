"""Tests for partition analysis and VOID generation.

These tests create RDF graphs in memory, use a mock HDTDocument to process them,
and verify that the VOID output correctly represents the data without
double-counting in various corner cases.
"""

from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.namespace import VOID

from void_hdt.partitions import PartitionAnalyzer
from void_hdt.void_generator import VOIDEXT, VOIDGenerator

EX = Namespace("http://example.org/")
DATASET_URI = "http://example.org/dataset"


class TestBasicPartitions:
    """Test basic VOID generation scenarios."""

    def test_single_class_single_instance(self, create_document):
        """Test simplest case: one class, one instance, one property."""
        g = Graph()
        g.add((EX.instance1, RDF.type, EX.ClassA))
        g.add((EX.instance1, EX.name, Literal("Test")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        # Should have one class partition
        assert len(analyzer.class_partitions) == 1
        assert analyzer.has_class_partition(EX.ClassA)

        partition = analyzer.class_partition_for(EX.ClassA)
        assert partition is not None
        assert partition.instance_count == 1
        # Two triples: rdf:type and ex:name
        assert partition.triple_count == 2

    def test_single_class_multiple_instances(self, create_document):
        """Test one class with multiple instances."""
        g = Graph()
        g.add((EX.instance1, RDF.type, EX.ClassA))
        g.add((EX.instance1, EX.name, Literal("Test1")))
        g.add((EX.instance2, RDF.type, EX.ClassA))
        g.add((EX.instance2, EX.name, Literal("Test2")))
        g.add((EX.instance3, RDF.type, EX.ClassA))
        g.add((EX.instance3, EX.name, Literal("Test3")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        assert len(analyzer.class_partitions) == 1
        partition = analyzer.class_partition_for(EX.ClassA)
        assert partition is not None
        assert partition.instance_count == 3
        # 6 triples: 3 rdf:type + 3 ex:name
        assert partition.triple_count == 6

    def test_multiple_classes_separate_instances(self, create_document):
        """Test multiple classes with distinct instances (no overlap)."""
        g = Graph()
        g.add((EX.personA, RDF.type, EX.Person))
        g.add((EX.personA, EX.name, Literal("Alice")))
        g.add((EX.companyX, RDF.type, EX.Company))
        g.add((EX.companyX, EX.name, Literal("Acme")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        assert len(analyzer.class_partitions) == 2
        assert analyzer.has_class_partition(EX.Person)
        assert analyzer.has_class_partition(EX.Company)

        person_partition = analyzer.class_partition_for(EX.Person)
        assert person_partition is not None
        assert person_partition.instance_count == 1
        assert person_partition.triple_count == 2

        company_partition = analyzer.class_partition_for(EX.Company)
        assert company_partition is not None
        assert company_partition.instance_count == 1
        assert company_partition.triple_count == 2

    def test_untyped_subject_not_counted(self, create_document):
        """Test that untyped subjects are not included in class partitions."""
        g = Graph()
        g.add((EX.typed, RDF.type, EX.ClassA))
        g.add((EX.typed, EX.name, Literal("Typed")))
        # Untyped subject - no rdf:type triple
        g.add((EX.untyped, EX.name, Literal("Untyped")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        assert len(analyzer.class_partitions) == 1
        partition = analyzer.class_partition_for(EX.ClassA)
        assert partition is not None
        assert partition.instance_count == 1
        # Only 2 triples from the typed subject
        assert partition.triple_count == 2


class TestSubjectWithMultipleTypes:
    """Test that triples are correctly counted when subjects have multiple types.

    When a subject has multiple rdf:type assertions, each triple involving that
    subject should be counted once per class partition it belongs to.
    """

    def test_subject_with_two_types_triple_counted_in_both(self, create_document):
        """A subject with two types should have its triples counted in both class partitions."""
        g = Graph()
        # Instance with two types
        g.add((EX.instance1, RDF.type, EX.ClassA))
        g.add((EX.instance1, RDF.type, EX.ClassB))
        g.add((EX.instance1, EX.name, Literal("Multi-typed")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        assert len(analyzer.class_partitions) == 2

        # Each class partition should count 3 triples:
        # - 2 rdf:type triples + 1 ex:name triple
        partition_a = analyzer.class_partition_for(EX.ClassA)
        assert partition_a is not None
        assert partition_a.instance_count == 1
        assert partition_a.triple_count == 3

        partition_b = analyzer.class_partition_for(EX.ClassB)
        assert partition_b is not None
        assert partition_b.instance_count == 1
        assert partition_b.triple_count == 3

    def test_subject_with_multiple_types_instance_count_correct(self, create_document):
        """Instance count should not double-count an instance with multiple types."""
        g = Graph()
        # One instance with three types
        g.add((EX.instance1, RDF.type, EX.ClassA))
        g.add((EX.instance1, RDF.type, EX.ClassB))
        g.add((EX.instance1, RDF.type, EX.ClassC))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        # Each class should have exactly 1 instance
        for class_uri in [EX.ClassA, EX.ClassB, EX.ClassC]:
            partition = analyzer.class_partition_for(class_uri)
            assert partition is not None
            assert partition.instance_count == 1

    def test_mixed_single_and_multi_typed_instances(self, create_document):
        """Mix of single-typed and multi-typed instances."""
        g = Graph()
        # Instance with two types
        g.add((EX.multi, RDF.type, EX.ClassA))
        g.add((EX.multi, RDF.type, EX.ClassB))
        g.add((EX.multi, EX.prop, Literal("value")))

        # Single-typed instance in ClassA
        g.add((EX.singleA, RDF.type, EX.ClassA))
        g.add((EX.singleA, EX.prop, Literal("single")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition_a = analyzer.class_partition_for(EX.ClassA)
        assert partition_a is not None
        assert partition_a.instance_count == 2  # multi + singleA

        partition_b = analyzer.class_partition_for(EX.ClassB)
        assert partition_b is not None
        assert partition_b.instance_count == 1  # only multi

    def test_duplicate_type_assertion_not_double_counted(self, create_document):
        """Duplicate rdf:type assertions should not inflate instance counts."""
        g = Graph()
        # Due to RDF semantics, duplicate triples are deduplicated in a Graph
        # But let's verify the analyzer handles this correctly
        g.add((EX.instance1, RDF.type, EX.ClassA))
        g.add((EX.instance1, EX.name, Literal("Test")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.ClassA)
        assert partition is not None
        assert partition.instance_count == 1


class TestSubjectWithMultiplePredicates:
    """Test that different predicates are correctly tracked in property partitions."""

    def test_single_instance_multiple_predicates(self, create_document):
        """Instance with multiple different predicates."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.person, EX.name, Literal("Alice")))
        g.add((EX.person, EX.age, Literal(30)))
        g.add((EX.person, EX.email, Literal("alice@example.org")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.Person)
        assert partition is not None
        assert partition.instance_count == 1
        # 4 triples: rdf:type + name + age + email
        assert partition.triple_count == 4

        # Verify property partitions
        assert len(partition.property_partitions) == 4
        assert analyzer.has_property_partition(EX.Person, RDF.type)
        assert analyzer.has_property_partition(EX.Person, EX.name)
        assert analyzer.has_property_partition(EX.Person, EX.age)
        assert analyzer.has_property_partition(EX.Person, EX.email)

        # Each property should have count of 1
        for prop_partition in partition.iter_property_partitions():
            assert prop_partition.total_count == 1

    def test_multiple_instances_same_predicates(self, create_document):
        """Multiple instances using the same predicates."""
        g = Graph()
        for i in range(5):
            instance = URIRef(f"http://example.org/person{i}")
            g.add((instance, RDF.type, EX.Person))
            g.add((instance, EX.name, Literal(f"Person {i}")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.Person)
        assert partition is not None
        assert partition.instance_count == 5
        assert partition.triple_count == 10  # 5 * 2 predicates

        # Property partition for ex:name should have count 5
        name_partition = analyzer.property_partition_for(EX.Person, EX.name)
        assert name_partition is not None
        assert name_partition.total_count == 5

    def test_instance_with_repeated_predicate_different_values(self, create_document):
        """Instance with same predicate used multiple times (different values)."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.person, EX.email, Literal("alice@work.org")))
        g.add((EX.person, EX.email, Literal("alice@home.org")))
        g.add((EX.person, EX.email, Literal("alice@mobile.org")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.Person)
        assert partition is not None
        # 4 triples: 1 rdf:type + 3 ex:email
        assert partition.triple_count == 4

        email_partition = analyzer.property_partition_for(EX.Person, EX.email)
        assert email_partition is not None
        assert email_partition.total_count == 3


class TestObjectWithMultipleTypes:
    """Test target class partitions when objects have multiple types."""

    def test_object_with_single_type(self, create_document):
        """Object with single type should have one target class partition."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.company, RDF.type, EX.Company))
        g.add((EX.person, EX.worksFor, EX.company))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        works_for_partition = analyzer.property_partition_for(EX.Person, EX.worksFor)
        assert works_for_partition is not None

        # Should have target class partition for Company
        target_classes = dict(works_for_partition.iter_target_classes(analyzer.class_id_to_term))
        assert EX.Company in target_classes
        assert target_classes[EX.Company] == 1

    def test_object_with_multiple_types_creates_multiple_target_partitions(self, create_document):
        """Object with multiple types should create target class entries for each type."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        # Object with two types
        g.add((EX.org, RDF.type, EX.Company))
        g.add((EX.org, RDF.type, EX.Organization))
        g.add((EX.person, EX.worksFor, EX.org))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        works_for_partition = analyzer.property_partition_for(EX.Person, EX.worksFor)
        assert works_for_partition is not None

        target_classes = dict(works_for_partition.iter_target_classes(analyzer.class_id_to_term))
        # Both Company and Organization should be target classes
        assert EX.Company in target_classes
        assert EX.Organization in target_classes
        # Each should have count of 1 (from the single triple)
        assert target_classes[EX.Company] == 1
        assert target_classes[EX.Organization] == 1

        # Total count should still be 1 (one triple)
        assert works_for_partition.total_count == 1

    def test_multiple_triples_to_multi_typed_objects(self, create_document):
        """Multiple triples pointing to objects with multiple types."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))

        # Two organizations, each with two types
        g.add((EX.org1, RDF.type, EX.Company))
        g.add((EX.org1, RDF.type, EX.LegalEntity))
        g.add((EX.org2, RDF.type, EX.Company))
        g.add((EX.org2, RDF.type, EX.LegalEntity))

        g.add((EX.person, EX.worksFor, EX.org1))
        g.add((EX.person, EX.worksFor, EX.org2))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        works_for_partition = analyzer.property_partition_for(EX.Person, EX.worksFor)
        assert works_for_partition is not None

        # Total count is 2 (two worksFor triples)
        assert works_for_partition.total_count == 2

        target_classes = dict(works_for_partition.iter_target_classes(analyzer.class_id_to_term))
        # Each target class should have count of 2
        assert target_classes[EX.Company] == 2
        assert target_classes[EX.LegalEntity] == 2


class TestMixedTypedAndUntypedObjects:
    """Test handling of literals and untyped URIs as objects."""

    def test_literal_object_uses_untyped_target(self, create_document):
        """Literal objects should use None as target class (untyped)."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.person, EX.name, Literal("Alice")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        name_partition = analyzer.property_partition_for(EX.Person, EX.name)
        assert name_partition is not None

        target_classes = dict(name_partition.iter_target_classes(analyzer.class_id_to_term))
        # Literal should have None as target class
        assert None in target_classes
        assert target_classes[None] == 1

    def test_untyped_uri_object_uses_untyped_target(self, create_document):
        """URI objects without rdf:type should use None as target class."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        # Reference to an untyped URI
        g.add((EX.person, EX.knows, EX.untypedEntity))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        knows_partition = analyzer.property_partition_for(EX.Person, EX.knows)
        assert knows_partition is not None

        target_classes = dict(knows_partition.iter_target_classes(analyzer.class_id_to_term))
        # Untyped URI should have None as target class
        assert None in target_classes
        assert len(target_classes) == 1

    def test_mixed_typed_and_literal_objects(self, create_document):
        """Mix of typed URIs and literal objects for the same predicate."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.company, RDF.type, EX.Company))

        # worksFor can point to either a Company or a literal (string)
        g.add((EX.person, EX.worksFor, EX.company))
        g.add((EX.person, EX.worksFor, Literal("Self-employed")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        works_for_partition = analyzer.property_partition_for(EX.Person, EX.worksFor)
        assert works_for_partition is not None

        assert works_for_partition.total_count == 2

        target_classes = dict(works_for_partition.iter_target_classes(analyzer.class_id_to_term))
        assert EX.Company in target_classes
        assert target_classes[EX.Company] == 1
        assert None in target_classes
        assert target_classes[None] == 1


class TestDocumentStatistics:
    """Test dataset-level statistics from HDTDocument."""

    def test_basic_statistics(self, create_document):
        """Test basic statistics from mock document."""
        g = Graph()
        g.add((EX.a, EX.p1, EX.b))
        g.add((EX.a, EX.p2, EX.c))
        g.add((EX.b, EX.p1, EX.c))

        doc = create_document(g)

        assert doc.total_triples == 3
        assert doc.nb_subjects == 2  # a, b
        assert doc.nb_predicates == 2  # p1, p2
        assert doc.nb_objects == 2  # b, c

    def test_statistics_with_literals(self, create_document):
        """Test statistics count literals as distinct objects."""
        g = Graph()
        g.add((EX.a, EX.name, Literal("Alice")))
        g.add((EX.b, EX.name, Literal("Bob")))
        g.add((EX.a, EX.age, Literal(30)))

        doc = create_document(g)

        assert doc.total_triples == 3
        assert doc.nb_subjects == 2
        assert doc.nb_predicates == 2
        assert doc.nb_objects == 3  # "Alice", "Bob", 30


class TestDatasetPropertyPartitions:
    """Test dataset-level property partition analysis."""

    def test_basic_dataset_property_counts(self, create_document):
        """Test basic property counting at dataset level."""
        g = Graph()
        g.add((EX.a, EX.p1, EX.b))
        g.add((EX.a, EX.p2, EX.c))
        g.add((EX.b, EX.p1, EX.c))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        # Should have counts for both predicates
        counts = dict(analyzer.iter_dataset_properties())
        assert EX.p1 in counts
        assert EX.p2 in counts
        assert counts[EX.p1] == 2
        assert counts[EX.p2] == 1

    def test_dataset_properties_include_untyped_subjects(self, create_document):
        """Dataset property counts should include ALL triples, not just typed subjects."""
        g = Graph()
        # Typed subject
        g.add((EX.typed, RDF.type, EX.ClassA))
        g.add((EX.typed, EX.name, Literal("Typed")))
        # Untyped subject - should still be counted at dataset level
        g.add((EX.untyped, EX.name, Literal("Untyped")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        counts = dict(analyzer.iter_dataset_properties())
        # Dataset level should count both name triples
        assert counts[EX.name] == 2
        # rdf:type should also be counted
        assert counts[RDF.type] == 1

        # But class partition should only count the typed subject's triples
        partition = analyzer.class_partition_for(EX.ClassA)
        assert partition is not None
        assert partition.triple_count == 2  # type + name for typed only

    def test_dataset_properties_with_multiple_predicates(self, create_document):
        """Test multiple predicates are tracked correctly."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.person, EX.name, Literal("Alice")))
        g.add((EX.person, EX.age, Literal(30)))
        g.add((EX.person, EX.email, Literal("alice@example.org")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        counts = dict(analyzer.iter_dataset_properties())
        assert len(counts) == 4  # rdf:type, name, age, email
        assert counts[RDF.type] == 1
        assert counts[EX.name] == 1
        assert counts[EX.age] == 1
        assert counts[EX.email] == 1

    def test_void_dataset_property_partition_output(self, create_document):
        """Test VOID output includes dataset-level property partitions."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.person, EX.name, Literal("Alice")))
        g.add((EX.untyped, EX.name, Literal("Bob")))  # Untyped subject

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        generator = VOIDGenerator(DATASET_URI)
        generator.add_dataset_property_partitions(analyzer)

        result = Graph()
        result.parse(data=generator.serialize(), format="turtle")

        # Find dataset-level property partitions
        dataset_uri = URIRef(DATASET_URI)
        prop_partitions = list(result.objects(dataset_uri, VOID.propertyPartition))
        assert len(prop_partitions) == 2  # rdf:type and ex:name

        # Verify one partition is for ex:name with count 2
        for pp in prop_partitions:
            prop = list(result.objects(pp, VOID.property))
            if EX.name in prop:
                triples = list(result.objects(pp, VOID.triples))
                assert len(triples) == 1
                assert int(str(triples[0])) == 2  # Both typed and untyped


class TestVOIDGeneration:
    """Test VOID RDF output generation."""

    def test_basic_void_output(self, create_document):
        """Test basic VOID generation produces valid RDF."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.person, EX.name, Literal("Alice")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        generator = VOIDGenerator(DATASET_URI)
        generator.add_dataset_statistics(reader)
        generator.add_class_partitions(analyzer)

        # Serialize and parse to verify valid RDF
        ttl = generator.serialize(format="turtle")
        assert ttl is not None

        # Parse the output
        result = Graph()
        result.parse(data=ttl, format="turtle")

        # Verify dataset URI exists
        dataset_uri = URIRef(DATASET_URI)
        assert (dataset_uri, RDF.type, VOID.Dataset) in result

        # Verify triple count
        triples = list(result.objects(dataset_uri, VOID.triples))
        assert len(triples) == 1
        assert int(str(triples[0])) == 2

    def test_void_class_partition_output(self, create_document):
        """Test VOID class partition output."""
        g = Graph()
        g.add((EX.person1, RDF.type, EX.Person))
        g.add((EX.person2, RDF.type, EX.Person))
        g.add((EX.person1, EX.name, Literal("Alice")))
        g.add((EX.person2, EX.name, Literal("Bob")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        generator = VOIDGenerator(DATASET_URI)
        generator.add_class_partitions(analyzer)

        result = Graph()
        result.parse(data=generator.serialize(), format="turtle")

        # Find the class partition for Person
        dataset_uri = URIRef(DATASET_URI)
        class_partitions = list(result.objects(dataset_uri, VOID.classPartition))
        assert len(class_partitions) == 1

        cp = class_partitions[0]
        # Verify it links to the correct class
        classes = list(result.objects(cp, VOID["class"]))
        assert EX.Person in classes

        # Verify entity count
        entities = list(result.objects(cp, VOID.entities))
        assert len(entities) == 1
        assert int(str(entities[0])) == 2

    def test_void_target_class_partitions(self, create_document):
        """Test VOID target class partition output."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.company, RDF.type, EX.Company))
        g.add((EX.person, EX.worksFor, EX.company))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        generator = VOIDGenerator(DATASET_URI)
        generator.add_class_partitions(analyzer)

        result = Graph()
        result.parse(data=generator.serialize(), format="turtle")

        # Find target class partitions (now uses void:class for the target class)
        target_partitions = list(result.subjects(VOID["class"], EX.Company))
        # Should have 2: one class partition for Company, one target class partition
        assert len(target_partitions) == 2

    def test_void_untyped_target_has_no_class(self, create_document):
        """Test that untyped targets don't have void:class predicate."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.person, EX.name, Literal("Alice")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        generator = VOIDGenerator(DATASET_URI)
        generator.add_class_partitions(analyzer)

        result = Graph()
        result.parse(data=generator.serialize(), format="turtle")

        # Find target class partitions (should have one for untyped)
        all_target_partitions = list(result.subjects(RDF.type, VOID.Dataset))
        # Check that some partitions don't have void:class
        has_untyped = False
        for partition in all_target_partitions:
            partition_classes = list(result.objects(partition, VOID["class"]))
            # Check if this is a target partition (linked via objectClassPartition)
            is_target = len(list(result.subjects(VOIDEXT.objectClassPartition, partition))) > 0
            if is_target and len(partition_classes) == 0:
                has_untyped = True
                break

        assert has_untyped, "Should have an untyped target class partition"


class TestCountAccuracy:
    """Test that counts are exact and consistent across all levels.

    These tests verify that dataset property counts, class partition triple
    counts, and property partition counts are all mutually consistent and
    match what manual iteration would produce.
    """

    def test_dataset_property_counts_are_exact(self, create_document):
        """Dataset property counts must equal actual triple counts per predicate."""
        g = Graph()
        # Mix of typed and untyped subjects with various predicates
        g.add((EX.typed1, RDF.type, EX.ClassA))
        g.add((EX.typed1, EX.p1, Literal("a")))
        g.add((EX.typed1, EX.p2, Literal("b")))
        g.add((EX.typed2, RDF.type, EX.ClassA))
        g.add((EX.typed2, EX.p1, Literal("c")))
        g.add((EX.untyped1, EX.p1, Literal("d")))
        g.add((EX.untyped1, EX.p2, Literal("e")))
        g.add((EX.untyped2, EX.p3, Literal("f")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        counts = dict(analyzer.iter_dataset_properties())
        # Manually verify: count ALL triples per predicate
        assert counts[RDF.type] == 2  # 2 rdf:type triples
        assert counts[EX.p1] == 3  # typed1, typed2, untyped1
        assert counts[EX.p2] == 2  # typed1, untyped1
        assert counts[EX.p3] == 1  # untyped2

        # Total across all predicates must equal total triples
        total_from_counts = sum(counts.values())
        assert total_from_counts == reader.total_triples

    def test_class_partition_triples_sum_to_property_partitions(self, create_document):
        """Class partition triple_count must equal sum of its property partition counts."""
        g = Graph()
        g.add((EX.a, RDF.type, EX.ClassA))
        g.add((EX.a, EX.p1, Literal("x")))
        g.add((EX.a, EX.p2, Literal("y")))
        g.add((EX.a, EX.p2, Literal("z")))
        g.add((EX.b, RDF.type, EX.ClassA))
        g.add((EX.b, EX.p1, Literal("w")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.ClassA)
        assert partition is not None

        # triple_count should be the sum of all property partitions
        prop_sum = sum(pp.total_count for pp in partition.iter_property_partitions())
        assert partition.triple_count == prop_sum
        # Manually: rdf:type(2) + p1(2) + p2(2) = 6
        assert partition.triple_count == 6

    def test_target_class_counts_consistent_with_total(self, create_document):
        """Property partition total_count >= max target class count."""
        g = Graph()
        g.add((EX.person1, RDF.type, EX.Person))
        g.add((EX.person2, RDF.type, EX.Person))
        g.add((EX.company, RDF.type, EX.Company))
        g.add((EX.org, RDF.type, EX.Organization))
        g.add((EX.person1, EX.worksFor, EX.company))
        g.add((EX.person2, EX.worksFor, EX.org))
        g.add((EX.person1, EX.name, Literal("Alice")))
        g.add((EX.person2, EX.name, Literal("Bob")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        works_for = analyzer.property_partition_for(EX.Person, EX.worksFor)
        assert works_for is not None
        assert works_for.total_count == 2

        targets = dict(works_for.iter_target_classes(analyzer.class_id_to_term))
        assert targets[EX.Company] == 1
        assert targets[EX.Organization] == 1

        name_pp = analyzer.property_partition_for(EX.Person, EX.name)
        assert name_pp is not None
        assert name_pp.total_count == 2
        name_targets = dict(name_pp.iter_target_classes(analyzer.class_id_to_term))
        # Literals are untyped
        assert None in name_targets
        assert name_targets[None] == 2

    def test_multi_class_triple_counts_independent(self, create_document):
        """When subject has multiple types, each class partition counts independently."""
        g = Graph()
        # Subject with 2 types
        g.add((EX.item, RDF.type, EX.ClassA))
        g.add((EX.item, RDF.type, EX.ClassB))
        g.add((EX.item, EX.p1, Literal("val1")))
        g.add((EX.item, EX.p2, Literal("val2")))

        # Another subject with only ClassA
        g.add((EX.other, RDF.type, EX.ClassA))
        g.add((EX.other, EX.p1, Literal("val3")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        pa = analyzer.class_partition_for(EX.ClassA)
        pb = analyzer.class_partition_for(EX.ClassB)
        assert pa is not None
        assert pb is not None

        # ClassA: item has 4 triples (2 type + p1 + p2), other has 2 (type + p1) = 6
        assert pa.triple_count == 6
        assert pa.instance_count == 2

        # ClassB: item has 4 triples (2 type + p1 + p2) = 4
        assert pb.triple_count == 4
        assert pb.instance_count == 1

        # Verify property partition counts within ClassA
        p1_a = analyzer.property_partition_for(EX.ClassA, EX.p1)
        assert p1_a is not None
        assert p1_a.total_count == 2  # item + other

        p1_b = analyzer.property_partition_for(EX.ClassB, EX.p1)
        assert p1_b is not None
        assert p1_b.total_count == 1  # only item


class TestEdgeCases:
    """Test edge cases and potential pitfalls."""

    def test_empty_graph(self, create_document):
        """Test handling of empty graph."""
        g = Graph()
        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        assert len(analyzer.class_partitions) == 0

    def test_only_type_triples(self, create_document):
        """Test graph with only rdf:type triples."""
        g = Graph()
        g.add((EX.a, RDF.type, EX.ClassA))
        g.add((EX.b, RDF.type, EX.ClassA))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.ClassA)
        assert partition is not None
        assert partition.instance_count == 2
        # Only rdf:type triples counted
        assert partition.triple_count == 2

    def test_self_referential_triple(self, create_document):
        """Test instance referencing itself."""
        g = Graph()
        g.add((EX.person, RDF.type, EX.Person))
        g.add((EX.person, EX.knows, EX.person))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.Person)
        assert partition is not None
        assert partition.instance_count == 1
        assert partition.triple_count == 2

        # Self-reference should have Person as target class
        knows_partition = analyzer.property_partition_for(EX.Person, EX.knows)
        assert knows_partition is not None
        target_classes = dict(knows_partition.iter_target_classes(analyzer.class_id_to_term))
        assert EX.Person in target_classes

    def test_circular_references(self, create_document):
        """Test instances referencing each other."""
        g = Graph()
        g.add((EX.person1, RDF.type, EX.Person))
        g.add((EX.person2, RDF.type, EX.Person))
        g.add((EX.person1, EX.knows, EX.person2))
        g.add((EX.person2, EX.knows, EX.person1))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.Person)
        assert partition is not None
        assert partition.instance_count == 2
        # 2 type triples + 2 knows triples
        assert partition.triple_count == 4

    def test_many_types_same_instance(self, create_document):
        """Test instance with many types doesn't cause issues."""
        g = Graph()
        # Instance with 10 types
        for i in range(10):
            g.add((EX.instance, RDF.type, URIRef(f"http://example.org/Class{i}")))
        g.add((EX.instance, EX.name, Literal("Multi")))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        # Should have 10 class partitions
        assert len(analyzer.class_partitions) == 10

        # Each partition should have 1 instance and 11 triples (10 types + 1 name)
        for partition in analyzer.iter_partitions():
            assert partition.instance_count == 1
            assert partition.triple_count == 11

    def test_large_number_of_instances(self, create_document):
        """Test with a larger number of instances."""
        g = Graph()
        num_instances = 100
        for i in range(num_instances):
            instance = URIRef(f"http://example.org/instance{i}")
            g.add((instance, RDF.type, EX.ClassA))
            g.add((instance, EX.value, Literal(i)))

        reader = create_document(g)
        analyzer = PartitionAnalyzer()
        analyzer.analyze(reader)

        partition = analyzer.class_partition_for(EX.ClassA)
        assert partition is not None
        assert partition.instance_count == num_instances
        assert partition.triple_count == num_instances * 2
