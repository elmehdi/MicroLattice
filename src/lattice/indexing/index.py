"""
Indexing functionality for Lattice using succinct data structures.
"""
from typing import Dict, List, Any, Tuple, Optional, Set, Union
import json
from .succinct import BitVector, WaveletTree

class FieldIndex:
    """Index for a single field in a collection."""

    def __init__(self, field_name: str, field_type: str):
        """
        Initialize a field index.

        Args:
            field_name: Name of the field
            field_type: Type of the field (string, int, etc.)
        """
        self.field_name = field_name
        self.field_type = field_type
        self.values = []  # List of unique values
        self.value_map = {}  # Maps values to their index in the values list
        self.record_map = []  # Maps value indices to record indices

        # Succinct data structures
        self.wavelet_tree = None

    def add_record(self, record_idx: int, value: Any):
        """
        Add a record to the index.

        Args:
            record_idx: Index of the record
            value: Value of the field in this record
        """
        # Convert value to a hashable type if needed
        if isinstance(value, dict):
            value = json.dumps(value, sort_keys=True)
        elif isinstance(value, list):
            value = tuple(value)

        # Add value to the value map if it's not already there
        if value not in self.value_map:
            self.value_map[value] = len(self.values)
            self.values.append(value)

        value_idx = self.value_map[value]

        # Extend record_map if needed
        while len(self.record_map) <= value_idx:
            self.record_map.append([])

        # Add record index to the appropriate value index
        self.record_map[value_idx].append(record_idx)

    def build_index(self):
        """Build the succinct data structures for this index."""
        # TODO: Implement wavelet tree construction for efficient querying
        pass

    def find_records(self, value: Any) -> List[int]:
        """
        Find records with the given value.

        Args:
            value: Value to search for

        Returns:
            List[int]: List of record indices matching the value
        """
        # Convert value to a hashable type if needed
        if isinstance(value, dict):
            value = json.dumps(value, sort_keys=True)
        elif isinstance(value, list):
            value = tuple(value)

        # Look up the value in the value map
        if value not in self.value_map:
            return []

        value_idx = self.value_map[value]
        return self.record_map[value_idx]

    def find_records_range(self, start_value: Any, end_value: Any) -> List[int]:
        """
        Find records with values in the given range.

        Args:
            start_value: Start of the range (inclusive)
            end_value: End of the range (inclusive)

        Returns:
            List[int]: List of record indices matching the range
        """
        result = []

        # Simple implementation for now
        for value, value_idx in self.value_map.items():
            if start_value <= value <= end_value:
                result.extend(self.record_map[value_idx])

        return result

    def find_records_regex(self, pattern: str) -> List[int]:
        """
        Find records with values matching a regex pattern.
        Only applicable to string fields.

        Args:
            pattern: Regular expression pattern

        Returns:
            List[int]: List of record indices matching the pattern
        """
        import re

        result = []

        # Compile the regex pattern
        regex = re.compile(pattern)

        # Check each value
        for value, value_idx in self.value_map.items():
            # Only apply regex to string values
            if isinstance(value, str) and regex.search(value):
                result.extend(self.record_map[value_idx])

        return result

    def find_records_prefix(self, prefix: str) -> List[int]:
        """
        Find records with string values starting with the given prefix.
        Only applicable to string fields.

        Args:
            prefix: String prefix to match

        Returns:
            List[int]: List of record indices matching the prefix
        """
        result = []

        # Check each value
        for value, value_idx in self.value_map.items():
            # Only apply prefix matching to string values
            if isinstance(value, str) and value.startswith(prefix):
                result.extend(self.record_map[value_idx])

        return result


class CollectionIndex:
    """Index for a collection of records."""

    def __init__(self, collection_name: str, schema: Dict[str, str]):
        """
        Initialize a collection index.

        Args:
            collection_name: Name of the collection
            schema: Dictionary mapping field names to field types
        """
        self.collection_name = collection_name
        self.schema = schema
        self.field_indices = {}

        # Create field indices
        for field_name, field_type in schema.items():
            self.field_indices[field_name] = FieldIndex(field_name, field_type)

    def add_record(self, record_idx: int, record: Dict[str, Any]):
        """
        Add a record to the index.

        Args:
            record_idx: Index of the record
            record: Record data as a dictionary
        """
        for field_name, value in record.items():
            if field_name in self.field_indices:
                self.field_indices[field_name].add_record(record_idx, value)

    def build_index(self):
        """Build the indices for all fields."""
        for field_index in self.field_indices.values():
            field_index.build_index()

    def query(self, conditions: Dict[str, Any]) -> List[int]:
        """
        Query the index with the given conditions.

        Args:
            conditions: Dictionary mapping field names to values or conditions

        Returns:
            List[int]: List of record indices matching all conditions
        """
        if not conditions:
            return []

        result_sets = []

        for field_name, condition in conditions.items():
            if field_name not in self.field_indices:
                continue

            field_index = self.field_indices[field_name]

            # Handle different types of conditions
            if isinstance(condition, dict):
                # Complex condition
                if 'range' in condition:
                    # Range query
                    start_value = condition['range'][0]
                    end_value = condition['range'][1]
                    matches = field_index.find_records_range(start_value, end_value)
                elif 'regex' in condition:
                    # Regex query
                    pattern = condition['regex']
                    matches = field_index.find_records_regex(pattern)
                elif 'prefix' in condition:
                    # Prefix query
                    prefix = condition['prefix']
                    matches = field_index.find_records_prefix(prefix)
                elif 'in' in condition:
                    # In query (value must be in a list)
                    values = condition['in']
                    matches = []
                    for value in values:
                        matches.extend(field_index.find_records(value))
                elif 'not' in condition:
                    # Not query (negation)
                    value = condition['not']
                    # Get all record indices
                    all_indices = set()
                    for value_idx in range(len(field_index.record_map)):
                        all_indices.update(field_index.record_map[value_idx])

                    # Get indices matching the value
                    matching_indices = set(field_index.find_records(value))

                    # Return indices not matching the value
                    matches = list(all_indices - matching_indices)
                else:
                    # Unsupported condition type
                    continue
            else:
                # Simple equality condition
                matches = field_index.find_records(condition)

            result_sets.append(set(matches))

        if not result_sets:
            return []

        # Intersect all result sets
        final_result = result_sets[0]
        for result_set in result_sets[1:]:
            final_result &= result_set

        return sorted(list(final_result))

    def query_or(self, conditions: Dict[str, Any]) -> List[int]:
        """
        Query the index with the given conditions using OR logic.

        Args:
            conditions: Dictionary mapping field names to values or conditions

        Returns:
            List[int]: List of record indices matching any of the conditions
        """
        if not conditions:
            return []

        result_set = set()

        for field_name, condition in conditions.items():
            if field_name not in self.field_indices:
                continue

            field_index = self.field_indices[field_name]

            # Handle different types of conditions (same as in query method)
            if isinstance(condition, dict):
                # Complex condition
                if 'range' in condition:
                    # Range query
                    start_value = condition['range'][0]
                    end_value = condition['range'][1]
                    matches = field_index.find_records_range(start_value, end_value)
                elif 'regex' in condition:
                    # Regex query
                    pattern = condition['regex']
                    matches = field_index.find_records_regex(pattern)
                elif 'prefix' in condition:
                    # Prefix query
                    prefix = condition['prefix']
                    matches = field_index.find_records_prefix(prefix)
                elif 'in' in condition:
                    # In query (value must be in a list)
                    values = condition['in']
                    matches = []
                    for value in values:
                        matches.extend(field_index.find_records(value))
                elif 'not' in condition:
                    # Not query (negation)
                    value = condition['not']
                    # Get all record indices
                    all_indices = set()
                    for value_idx in range(len(field_index.record_map)):
                        all_indices.update(field_index.record_map[value_idx])

                    # Get indices matching the value
                    matching_indices = set(field_index.find_records(value))

                    # Return indices not matching the value
                    matches = list(all_indices - matching_indices)
                else:
                    # Unsupported condition type
                    continue
            else:
                # Simple equality condition
                matches = field_index.find_records(condition)

            # Union with the result set
            result_set.update(matches)

        return sorted(list(result_set))
