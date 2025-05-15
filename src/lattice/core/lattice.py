"""
Core Lattice database functionality.
"""
import os
import uuid
import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
import json

from ..serialization.serializer import Serializer
from ..indexing.index import CollectionIndex
from ..compression.compressor import Compressor
from .schema_evolution import SchemaEvolution
from .change_tracker import ChangeTracker

class LatticeDB:
    """Main Lattice database class."""

    def __init__(self, name: str = "lattice_db"):
        """
        Initialize a new Lattice database.

        Args:
            name: Name of the database
        """
        self.name = name
        self.version = "0.1.0"
        self.collections = {}  # name -> Collection
        self.serializer = Serializer()
        self.compressor = Compressor()
        self.schema_evolution = SchemaEvolution()
        self.change_tracker = ChangeTracker()

        # Metadata
        self.metadata = {
            "name": name,
            "version": self.version,
            "created_at": datetime.datetime.now().isoformat(),
            "updated_at": datetime.datetime.now().isoformat(),
            "size": 0,
            "collections": [],
            "schema_versions": {}  # collection_name -> [schema_versions]
        }

    def create_collection(self, name: str, schema: Dict[str, str]) -> bool:
        """
        Create a new collection with the given schema.

        Args:
            name: Name of the collection
            schema: Dictionary mapping field names to field types

        Returns:
            bool: True if collection was created successfully
        """
        if name in self.collections:
            print(f"Collection '{name}' already exists")
            return False

        # Create the collection with a reference to this database
        self.collections[name] = Collection(name, schema, self)
        self.metadata["collections"].append(name)

        # Store the initial schema version
        schema_version = {
            "version": 1,
            "schema": schema,
            "created_at": datetime.datetime.now().isoformat()
        }

        if name not in self.metadata["schema_versions"]:
            self.metadata["schema_versions"][name] = []

        self.metadata["schema_versions"][name].append(schema_version)

        # Track the schema creation
        self.change_tracker.track_schema_update(name, {}, schema, {
            "version": 1,
            "added_fields": [{"name": field_name, "type": field_type} for field_name, field_type in schema.items()],
            "removed_fields": [],
            "changed_types": []
        })

        return True

    def get_collection(self, name: str) -> Optional['Collection']:
        """
        Get a collection by name.

        Args:
            name: Name of the collection

        Returns:
            Optional[Collection]: The collection, or None if it doesn't exist
        """
        return self.collections.get(name)

    def update_collection_schema(self, name: str, new_schema: Dict[str, str], migrate_data: bool = True) -> Dict[str, Any]:
        """
        Update the schema of a collection.

        Args:
            name: Name of the collection
            new_schema: New schema for the collection
            migrate_data: Whether to migrate existing data to the new schema

        Returns:
            Dict[str, Any]: Migration information
        """
        if name not in self.collections:
            print(f"Collection '{name}' does not exist")
            return {"success": False, "error": "Collection does not exist"}

        collection = self.collections[name]
        old_schema = collection.schema

        # Evolve the schema
        evolved_schema, migration_info = self.schema_evolution.evolve_schema(old_schema, new_schema)

        if not migration_info["compatible"]:
            print(f"Schema evolution is not compatible: {migration_info}")
            return {"success": False, "error": "Incompatible schema evolution", "migration_info": migration_info}

        # Update the schema version
        current_version = len(self.metadata["schema_versions"].get(name, []))
        schema_version = {
            "version": current_version + 1,
            "schema": evolved_schema,
            "created_at": datetime.datetime.now().isoformat(),
            "migration_info": migration_info
        }

        if name not in self.metadata["schema_versions"]:
            self.metadata["schema_versions"][name] = []

        self.metadata["schema_versions"][name].append(schema_version)

        # Migrate the data if requested
        if migrate_data:
            # Create a new collection with the evolved schema
            new_collection = Collection(name, evolved_schema)

            # Migrate each record
            for record in collection.records:
                migrated_record = self.schema_evolution.migrate_record(record, old_schema, evolved_schema)
                new_collection.insert(migrated_record)

            # Replace the old collection
            self.collections[name] = new_collection
        else:
            # Just update the schema
            collection.schema = evolved_schema

        # Update the metadata
        self.metadata["updated_at"] = datetime.datetime.now().isoformat()

        return {"success": True, "migration_info": migration_info}

    def get_collection_schema_history(self, name: str) -> List[Dict[str, Any]]:
        """
        Get the schema history of a collection.

        Args:
            name: Name of the collection

        Returns:
            List[Dict[str, Any]]: Schema history
        """
        if name not in self.metadata["schema_versions"]:
            return []

        return self.metadata["schema_versions"][name]

    def drop_collection(self, name: str) -> bool:
        """
        Drop a collection.

        Args:
            name: Name of the collection

        Returns:
            bool: True if collection was dropped successfully
        """
        if name not in self.collections:
            print(f"Collection '{name}' does not exist")
            return False

        del self.collections[name]
        self.metadata["collections"].remove(name)

        # Remove schema versions
        if name in self.metadata["schema_versions"]:
            del self.metadata["schema_versions"][name]

        return True

    def save(self, file_path: str) -> bool:
        """
        Save the database to a file.

        Args:
            file_path: Path to save the database to

        Returns:
            bool: True if the database was saved successfully
        """
        try:
            # Serialize the database
            serialized_data = self._serialize()

            # Compress the serialized data
            compressed_data = self.compressor.compress(serialized_data)

            # Write to file
            with open(file_path, 'wb') as f:
                f.write(compressed_data)

            return True
        except Exception as e:
            print(f"Error saving database: {e}")
            return False

    def load(self, file_path: str) -> bool:
        """
        Load the database from a file.

        Args:
            file_path: Path to load the database from

        Returns:
            bool: True if the database was loaded successfully
        """
        try:
            # Read the compressed data
            with open(file_path, 'rb') as f:
                compressed_data = f.read()

            # Decompress the data
            serialized_data = self.compressor.decompress(compressed_data)

            # Deserialize the data
            self._deserialize(serialized_data)

            return True
        except Exception as e:
            print(f"Error loading database: {e}")
            return False

    def _serialize(self) -> bytes:
        """
        Serialize the database to bytes.

        Returns:
            bytes: Serialized database
        """
        # Convert collections to the format expected by the serializer
        collections_dict = {}
        for name, collection in self.collections.items():
            collections_dict[name] = collection.to_dict()

        # Use the FlatBuffers serializer if available, otherwise use JSON
        if hasattr(self.serializer, 'FLATBUFFERS_AVAILABLE') and self.serializer.FLATBUFFERS_AVAILABLE:
            return self.serializer.serialize_database(self.name, self.version, collections_dict)
        else:
            # Fallback to JSON serialization
            db_dict = {
                "name": self.name,
                "version": self.version,
                "metadata": self.metadata,
                "collections": collections_dict,
                "changes": self.change_tracker.changes,
                "last_sync_timestamp": self.change_tracker.last_sync_timestamp
            }
            return json.dumps(db_dict).encode('utf-8')

    def _deserialize(self, data: bytes):
        """
        Deserialize the database from bytes.

        Args:
            data: Serialized database
        """
        # Try to determine if this is JSON or FlatBuffers data
        try:
            # Check if it starts with a JSON object
            if data[:1] == b'{':
                # Assume it's JSON
                db_dict = json.loads(data.decode('utf-8'))
            else:
                # Assume it's FlatBuffers
                if hasattr(self.serializer, 'FLATBUFFERS_AVAILABLE') and self.serializer.FLATBUFFERS_AVAILABLE:
                    db_dict = self.serializer.deserialize_database(data)
                else:
                    # Fallback to JSON if FlatBuffers is not available
                    db_dict = json.loads(data.decode('utf-8'))

            self.name = db_dict["name"]
            self.version = db_dict["version"]

            # Load metadata if available
            if "metadata" in db_dict:
                self.metadata = db_dict["metadata"]

            self.collections = {}

            for name, collection_dict in db_dict["collections"].items():
                schema = collection_dict["schema"]
                collection = Collection(name, schema)
                collection.from_dict(collection_dict)
                self.collections[name] = collection

            # Load change tracking data if available
            if "changes" in db_dict:
                self.change_tracker.changes = db_dict["changes"]

            if "last_sync_timestamp" in db_dict:
                self.change_tracker.last_sync_timestamp = db_dict["last_sync_timestamp"]
        except Exception as e:
            print(f"Error deserializing database: {e}")
            raise

    def get_changes_since_last_sync(self) -> List[Dict[str, Any]]:
        """
        Get changes since the last synchronization.

        Returns:
            List[Dict[str, Any]]: List of changes
        """
        return self.change_tracker.get_changes_since_last_sync()

    def apply_changes(self, changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Apply changes from a remote source.

        Args:
            changes: List of changes to apply

        Returns:
            Dict[str, Any]: Result of applying changes
        """
        conflicts = self.change_tracker.apply_remote_changes(changes)

        # Apply non-conflicting changes
        applied_changes = []
        failed_changes = []

        for change in changes:
            # Skip changes that conflict with local changes
            if any(c["remote_change"]["id"] == change["id"] for c in conflicts):
                continue

            try:
                if change["operation"] == "insert":
                    collection = self.get_collection(change["collection"])
                    if collection:
                        collection.insert(change["data"])
                        applied_changes.append(change["id"])

                elif change["operation"] == "update":
                    collection = self.get_collection(change["collection"])
                    if collection:
                        collection.update({"_id": change["record_id"]}, change["data"])
                        applied_changes.append(change["id"])

                elif change["operation"] == "delete":
                    collection = self.get_collection(change["collection"])
                    if collection:
                        collection.delete({"_id": change["record_id"]})
                        applied_changes.append(change["id"])

                elif change["operation"] == "schema_update":
                    result = self.update_collection_schema(
                        change["collection"],
                        change["new_schema"]
                    )
                    if result["success"]:
                        applied_changes.append(change["id"])
                    else:
                        failed_changes.append({
                            "change_id": change["id"],
                            "error": "Schema update failed",
                            "details": result
                        })
            except Exception as e:
                failed_changes.append({
                    "change_id": change["id"],
                    "error": str(e)
                })

        # Mark changes as synced
        if changes:
            latest_timestamp = max(change["timestamp"] for change in changes)
            self.change_tracker.mark_synced(latest_timestamp)

        return {
            "applied_changes": applied_changes,
            "failed_changes": failed_changes,
            "conflicts": conflicts
        }

    def mark_synced(self, timestamp: Optional[float] = None):
        """
        Mark changes as synchronized up to the specified timestamp.

        Args:
            timestamp: Timestamp to mark as synced (default: current time)
        """
        self.change_tracker.mark_synced(timestamp)


class Collection:
    """A collection of records with a defined schema."""

    def __init__(self, name: str, schema: Dict[str, str], db=None):
        """
        Initialize a new collection.

        Args:
            name: Name of the collection
            schema: Dictionary mapping field names to field types
            db: Reference to the parent database (optional)
        """
        self.name = name
        self.schema = schema
        self.records = []
        self.index = CollectionIndex(name, schema)
        self.db = db  # Reference to the parent database for change tracking

    def insert(self, record: Dict[str, Any]) -> str:
        """
        Insert a record into the collection.

        Args:
            record: Record data as a dictionary

        Returns:
            str: ID of the inserted record
        """
        # Validate record against schema
        for field_name, field_type in self.schema.items():
            if field_name not in record:
                print(f"Missing required field '{field_name}'")
                return None

            # TODO: Add type validation

        # Generate a record ID if not provided
        if "_id" not in record:
            record["_id"] = str(uuid.uuid4())

        # Add the record
        record_idx = len(self.records)
        self.records.append(record)

        # Update the index
        self.index.add_record(record_idx, record)

        # Track the change if we have a reference to the parent database
        if self.db and hasattr(self.db, 'change_tracker'):
            self.db.change_tracker.track_insert(self.name, record["_id"], record)

        return record["_id"]

    def find(self, query: Dict[str, Any] = None, query_type: str = "and") -> List[Dict[str, Any]]:
        """
        Find records matching the query.

        Args:
            query: Query conditions
            query_type: Type of query ("and" or "or")

        Returns:
            List[Dict[str, Any]]: List of matching records
        """
        if query is None:
            return self.records.copy()

        # Use the index to find matching records
        if query_type.lower() == "or":
            record_indices = self.index.query_or(query)
        else:
            record_indices = self.index.query(query)

        # Return the matching records
        return [self.records[idx] for idx in record_indices]

    def find_one(self, query: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        Find a single record matching the query.

        Args:
            query: Query conditions

        Returns:
            Optional[Dict[str, Any]]: Matching record, or None if not found
        """
        results = self.find(query)
        return results[0] if results else None

    def find_by_id(self, record_id: str) -> Optional[Dict[str, Any]]:
        """
        Find a record by its ID.

        Args:
            record_id: ID of the record to find

        Returns:
            Optional[Dict[str, Any]]: Matching record, or None if not found
        """
        return self.find_one({"_id": record_id})

    def update(self, query: Dict[str, Any], update: Dict[str, Any]) -> int:
        """
        Update records matching the query.

        Args:
            query: Query conditions
            update: Update operations

        Returns:
            int: Number of records updated
        """
        # Find matching records
        record_indices = self.index.query(query)

        # Update the records
        for idx in record_indices:
            # Store the old record for change tracking
            old_record = self.records[idx].copy() if self.db else None

            # Update the record
            for field_name, value in update.items():
                if field_name in self.schema:
                    self.records[idx][field_name] = value

            # Track the change
            if self.db and hasattr(self.db, 'change_tracker'):
                self.db.change_tracker.track_update(
                    self.name,
                    self.records[idx]["_id"],
                    update,
                    old_record
                )

        # Rebuild the index
        self._rebuild_index()

        return len(record_indices)

    def delete(self, query: Dict[str, Any]) -> int:
        """
        Delete records matching the query.

        Args:
            query: Query conditions

        Returns:
            int: Number of records deleted
        """
        # Find matching records
        record_indices = self.index.query(query)

        # Sort indices in descending order to avoid shifting issues
        record_indices.sort(reverse=True)

        # Delete the records
        deleted_records = []
        for idx in record_indices:
            # Store the record for change tracking
            if self.db:
                deleted_records.append(self.records[idx].copy())

            # Delete the record
            del self.records[idx]

        # Track the changes
        if self.db and hasattr(self.db, 'change_tracker'):
            for record in deleted_records:
                self.db.change_tracker.track_delete(
                    self.name,
                    record["_id"],
                    record
                )

        # Rebuild the index
        self._rebuild_index()

        return len(record_indices)

    def _rebuild_index(self):
        """Rebuild the collection index."""
        self.index = CollectionIndex(self.name, self.schema)

        for idx, record in enumerate(self.records):
            self.index.add_record(idx, record)

        self.index.build_index()

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the collection to a dictionary.

        Returns:
            Dict[str, Any]: Dictionary representation of the collection
        """
        return {
            "name": self.name,
            "schema": self.schema,
            "records": self.records
        }

    def from_dict(self, data: Dict[str, Any]):
        """
        Load the collection from a dictionary.

        Args:
            data: Dictionary representation of the collection
        """
        self.name = data["name"]
        self.schema = data["schema"]
        self.records = data["records"]
        self._rebuild_index()
