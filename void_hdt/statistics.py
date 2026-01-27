"""Statistics collection for RDF datasets."""

from void_hdt.hdt_reader import HDTReader


class DatasetStatistics:
    """Dataset statistics retrieved from HDT index.

    HDT files store cardinality information directly in their index,
    so we can retrieve counts in O(1) without iterating through triples.
    """

    def __init__(
        self,
        triple_count: int,
        distinct_subjects: int,
        distinct_predicates: int,
        distinct_objects: int,
    ) -> None:
        """Initialize statistics with values.

        Args:
            triple_count: Total number of triples
            distinct_subjects: Number of distinct subjects
            distinct_predicates: Number of distinct predicates
            distinct_objects: Number of distinct objects
        """
        self.triple_count = triple_count
        self.distinct_subjects = distinct_subjects
        self.distinct_predicates = distinct_predicates
        self.distinct_objects = distinct_objects

    @classmethod
    def from_reader(cls, reader: HDTReader) -> "DatasetStatistics":
        """Create statistics from an HDT reader using native HDT index.

        This is O(1) - HDT stores these counts in its header/index.

        Args:
            reader: HDT reader to get statistics from

        Returns:
            DatasetStatistics populated from HDT index
        """
        return cls(
            triple_count=reader.nb_triples,
            distinct_subjects=reader.nb_subjects,
            distinct_predicates=reader.nb_predicates,
            distinct_objects=reader.nb_objects,
        )
