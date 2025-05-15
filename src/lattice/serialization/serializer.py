"""
FlatBuffers serialization for Lattice.
"""
import os
import sys
import json
import flatbuffers
from typing import Dict, List, Any, Union, Optional, Tuple
from datetime import datetime

# Import the generated FlatBuffers code
# Note: This will be available after running the generate_flatbuffers.py script
try:
    # Add the generated directory to the Python path
    generated_dir = os.path.join(os.path.dirname(__file__), "generated")
    if generated_dir not in sys.path:
        sys.path.append(generated_dir)

    # Import the generated modules
    from Lattice import Database, Collection, Record, Schema, Field
    from Lattice import DataValue, Int, Double, String, Bool, Timestamp, Bytes, Array, Object, KeyValue
    FLATBUFFERS_AVAILABLE = True
except ImportError:
    print("Warning: FlatBuffers generated code not found. Using JSON serialization as fallback.")
    FLATBUFFERS_AVAILABLE = False


class SchemaManager:
    """Manages FlatBuffers schemas for Lattice."""

    def __init__(self, schema_dir: str = "schemas"):
        self.schema_dir = schema_dir
        self.compiled_schemas = {}
        self.schemas = {}  # name -> Schema object

    def load_schema(self, schema_name: str) -> Optional[Dict]:
        """
        Load a schema definition from a JSON file.

        Args:
            schema_name: Name of the schema

        Returns:
            Optional[Dict]: Schema definition, or None if not found
        """
        schema_path = os.path.join(self.schema_dir, f"{schema_name}.json")
        if not os.path.exists(schema_path):
            return None

        with open(schema_path, 'r') as f:
            return json.load(f)

    def save_schema(self, schema_name: str, schema_def: Dict) -> bool:
        """
        Save a schema definition to a JSON file.

        Args:
            schema_name: Name of the schema
            schema_def: Schema definition

        Returns:
            bool: True if the schema was saved successfully
        """
        os.makedirs(self.schema_dir, exist_ok=True)
        schema_path = os.path.join(self.schema_dir, f"{schema_name}.json")

        try:
            with open(schema_path, 'w') as f:
                json.dump(schema_def, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving schema: {e}")
            return False

    def register_schema(self, name: str, fields: Dict[str, str]) -> str:
        """
        Register a new schema.

        Args:
            name: Name of the schema
            fields: Dictionary mapping field names to field types

        Returns:
            str: Schema ID
        """
        schema_id = name  # For simplicity, use the name as the ID

        # Create the schema definition
        schema_def = {
            "name": name,
            "fields": [{"name": field_name, "type": field_type} for field_name, field_type in fields.items()]
        }

        # Save the schema
        self.save_schema(schema_id, schema_def)

        # Store the schema in memory
        self.schemas[schema_id] = schema_def

        return schema_id


class Serializer:
    """Handles serialization and deserialization using FlatBuffers."""

    def __init__(self):
        self.FLATBUFFERS_AVAILABLE = FLATBUFFERS_AVAILABLE
        if FLATBUFFERS_AVAILABLE:
            self.builder = flatbuffers.Builder(0)
        self.schema_manager = SchemaManager()

    def reset_builder(self):
        """Reset the FlatBuffers builder."""
        self.builder = flatbuffers.Builder(0)

    def _create_string(self, value: str) -> int:
        """
        Create a string in the FlatBuffers builder.

        Args:
            value: String value

        Returns:
            int: Offset to the string
        """
        return self.builder.CreateString(value)

    def _create_bytes(self, value: bytes) -> int:
        """
        Create a byte array in the FlatBuffers builder.

        Args:
            value: Bytes value

        Returns:
            int: Offset to the byte array
        """
        return self.builder.CreateByteVector(value)

    def serialize_value(self, value: Any) -> Tuple[int, int]:
        """
        Serialize a Python value to its FlatBuffers representation.

        Args:
            value: The Python value to serialize

        Returns:
            Tuple[int, int]: (Offset to the serialized value, DataValue type)
        """
        # Handle different types
        if value is None:
            # For None, we'll use a special type or return a default value
            # For now, let's use an empty string
            String.Start(self.builder)
            String.AddValue(self.builder, self._create_string(""))
            string_offset = String.End(self.builder)
            return string_offset, DataValue.String

        elif isinstance(value, bool):
            Bool.Start(self.builder)
            Bool.AddValue(self.builder, value)
            bool_offset = Bool.End(self.builder)
            return bool_offset, DataValue.Bool

        elif isinstance(value, int):
            Int.Start(self.builder)
            Int.AddValue(self.builder, value)
            int_offset = Int.End(self.builder)
            return int_offset, DataValue.Int

        elif isinstance(value, float):
            Double.Start(self.builder)
            Double.AddValue(self.builder, value)
            double_offset = Double.End(self.builder)
            return double_offset, DataValue.Double

        elif isinstance(value, str):
            String.Start(self.builder)
            String.AddValue(self.builder, self._create_string(value))
            string_offset = String.End(self.builder)
            return string_offset, DataValue.String

        elif isinstance(value, bytes):
            Bytes.Start(self.builder)
            Bytes.AddValue(self.builder, self._create_bytes(value))
            bytes_offset = Bytes.End(self.builder)
            return bytes_offset, DataValue.Bytes

        elif isinstance(value, datetime):
            # Convert datetime to timestamp (milliseconds since epoch)
            timestamp_ms = int(value.timestamp() * 1000)
            Timestamp.Start(self.builder)
            Timestamp.AddValue(self.builder, timestamp_ms)
            timestamp_offset = Timestamp.End(self.builder)
            return timestamp_offset, DataValue.Timestamp

        elif isinstance(value, list):
            # Serialize each item in the list
            value_offsets = []
            for item in value:
                item_offset, item_type = self.serialize_value(item)

                # Create a DataValue for this item
                DataValue.Start(self.builder)
                DataValue.AddType(self.builder, item_type)
                DataValue.AddValue(self.builder, item_offset)
                data_value_offset = DataValue.End(self.builder)

                value_offsets.append(data_value_offset)

            # Create the vector of values
            Array.StartValuesVector(self.builder, len(value_offsets))
            for offset in reversed(value_offsets):
                self.builder.PrependUOffsetTRelative(offset)
            values_vector = self.builder.EndVector()

            # Create the Array
            Array.Start(self.builder)
            Array.AddValues(self.builder, values_vector)
            array_offset = Array.End(self.builder)

            return array_offset, DataValue.Array

        elif isinstance(value, dict):
            # Serialize each key-value pair in the dict
            kv_offsets = []
            for key, val in value.items():
                # Serialize the value
                val_offset, val_type = self.serialize_value(val)

                # Create a DataValue for this value
                DataValue.Start(self.builder)
                DataValue.AddType(self.builder, val_type)
                DataValue.AddValue(self.builder, val_offset)
                data_value_offset = DataValue.End(self.builder)

                # Create the key string
                key_offset = self._create_string(str(key))

                # Create the KeyValue
                KeyValue.Start(self.builder)
                KeyValue.AddKey(self.builder, key_offset)
                KeyValue.AddValue(self.builder, data_value_offset)
                kv_offset = KeyValue.End(self.builder)

                kv_offsets.append(kv_offset)

            # Create the vector of key-value pairs
            Object.StartFieldsVector(self.builder, len(kv_offsets))
            for offset in reversed(kv_offsets):
                self.builder.PrependUOffsetTRelative(offset)
            fields_vector = self.builder.EndVector()

            # Create the Object
            Object.Start(self.builder)
            Object.AddFields(self.builder, fields_vector)
            object_offset = Object.End(self.builder)

            return object_offset, DataValue.Object

        else:
            # For unsupported types, convert to string
            string_value = str(value)
            String.Start(self.builder)
            String.AddValue(self.builder, self._create_string(string_value))
            string_offset = String.End(self.builder)
            return string_offset, DataValue.String

    def serialize_record(self, record_id: str, schema_id: str, values: Dict[str, Any]) -> bytes:
        """
        Serialize a record using FlatBuffers.

        Args:
            record_id: Unique identifier for the record
            schema_id: ID of the schema this record follows
            values: Dictionary of field values

        Returns:
            bytes: Serialized record as bytes
        """
        self.reset_builder()

        # Create the record ID and schema ID strings
        record_id_offset = self._create_string(record_id)
        schema_id_offset = self._create_string(schema_id)

        # Serialize the values
        value_offsets = []
        for field_name, field_value in values.items():
            value_offset, value_type = self.serialize_value(field_value)

            # Create a DataValue for this field
            DataValue.Start(self.builder)
            DataValue.AddType(self.builder, value_type)
            DataValue.AddValue(self.builder, value_offset)
            data_value_offset = DataValue.End(self.builder)

            value_offsets.append(data_value_offset)

        # Create the vector of values
        Record.StartValuesVector(self.builder, len(value_offsets))
        for offset in reversed(value_offsets):
            self.builder.PrependUOffsetTRelative(offset)
        values_vector = self.builder.EndVector()

        # Create the Record
        Record.Start(self.builder)
        Record.AddId(self.builder, record_id_offset)
        Record.AddSchemaId(self.builder, schema_id_offset)
        Record.AddValues(self.builder, values_vector)
        record_offset = Record.End(self.builder)

        # Finish the buffer
        self.builder.Finish(record_offset)

        # Get the serialized data
        return self.builder.Output()

    def serialize_collection(self, name: str, schema: Dict[str, str], records: List[Dict[str, Any]]) -> bytes:
        """
        Serialize a collection using FlatBuffers.

        Args:
            name: Name of the collection
            schema: Dictionary mapping field names to field types
            records: List of records in the collection

        Returns:
            bytes: Serialized collection as bytes
        """
        self.reset_builder()

        # Create the collection name string
        name_offset = self._create_string(name)

        # Create the schema
        field_offsets = []
        for field_name, field_type in schema.items():
            # Create the field name and type strings
            field_name_offset = self._create_string(field_name)
            field_type_offset = self._create_string(field_type)

            # Create the Field
            Field.Start(self.builder)
            Field.AddName(self.builder, field_name_offset)
            Field.AddType(self.builder, field_type_offset)
            field_offset = Field.End(self.builder)

            field_offsets.append(field_offset)

        # Create the vector of fields
        Schema.StartFieldsVector(self.builder, len(field_offsets))
        for offset in reversed(field_offsets):
            self.builder.PrependUOffsetTRelative(offset)
        fields_vector = self.builder.EndVector()

        # Create the Schema
        schema_name_offset = self._create_string(name + "_schema")
        Schema.Start(self.builder)
        Schema.AddName(self.builder, schema_name_offset)
        Schema.AddFields(self.builder, fields_vector)
        schema_offset = Schema.End(self.builder)

        # Serialize the records
        record_offsets = []
        for record in records:
            # Generate a record ID if not provided
            if "_id" not in record:
                record_id = str(len(record_offsets))
            else:
                record_id = record["_id"]

            # Serialize the record values
            value_offsets = []
            for field_name, field_value in record.items():
                if field_name != "_id":  # Skip the ID field
                    value_offset, value_type = self.serialize_value(field_value)

                    # Create a DataValue for this field
                    DataValue.Start(self.builder)
                    DataValue.AddType(self.builder, value_type)
                    DataValue.AddValue(self.builder, value_offset)
                    data_value_offset = DataValue.End(self.builder)

                    value_offsets.append(data_value_offset)

            # Create the vector of values
            Record.StartValuesVector(self.builder, len(value_offsets))
            for offset in reversed(value_offsets):
                self.builder.PrependUOffsetTRelative(offset)
            values_vector = self.builder.EndVector()

            # Create the Record
            record_id_offset = self._create_string(record_id)
            schema_id_offset = self._create_string(name + "_schema")
            Record.Start(self.builder)
            Record.AddId(self.builder, record_id_offset)
            Record.AddSchemaId(self.builder, schema_id_offset)
            Record.AddValues(self.builder, values_vector)
            record_offset = Record.End(self.builder)

            record_offsets.append(record_offset)

        # Create the vector of records
        Collection.StartRecordsVector(self.builder, len(record_offsets))
        for offset in reversed(record_offsets):
            self.builder.PrependUOffsetTRelative(offset)
        records_vector = self.builder.EndVector()

        # Create the Collection
        Collection.Start(self.builder)
        Collection.AddName(self.builder, name_offset)
        Collection.AddSchema(self.builder, schema_offset)
        Collection.AddRecords(self.builder, records_vector)
        collection_offset = Collection.End(self.builder)

        # Finish the buffer
        self.builder.Finish(collection_offset)

        # Get the serialized data
        return self.builder.Output()

    def serialize_database(self, name: str, version: str, collections: Dict[str, Dict]) -> bytes:
        """
        Serialize a database using FlatBuffers.

        Args:
            name: Name of the database
            version: Version of the database
            collections: Dictionary mapping collection names to collection data

        Returns:
            bytes: Serialized database as bytes
        """
        self.reset_builder()

        # Create the database name and version strings
        name_offset = self._create_string(name)
        version_offset = self._create_string(version)

        # Serialize the collections
        collection_offsets = []
        for collection_name, collection_data in collections.items():
            # Create the collection name string
            collection_name_offset = self._create_string(collection_name)

            # Create the schema
            schema = collection_data["schema"]
            field_offsets = []
            for field_name, field_type in schema.items():
                # Create the field name and type strings
                field_name_offset = self._create_string(field_name)
                field_type_offset = self._create_string(field_type)

                # Create the Field
                Field.Start(self.builder)
                Field.AddName(self.builder, field_name_offset)
                Field.AddType(self.builder, field_type_offset)
                field_offset = Field.End(self.builder)

                field_offsets.append(field_offset)

            # Create the vector of fields
            Schema.StartFieldsVector(self.builder, len(field_offsets))
            for offset in reversed(field_offsets):
                self.builder.PrependUOffsetTRelative(offset)
            fields_vector = self.builder.EndVector()

            # Create the Schema
            schema_name_offset = self._create_string(collection_name + "_schema")
            Schema.Start(self.builder)
            Schema.AddName(self.builder, schema_name_offset)
            Schema.AddFields(self.builder, fields_vector)
            schema_offset = Schema.End(self.builder)

            # Serialize the records
            records = collection_data["records"]
            record_offsets = []
            for record in records:
                # Generate a record ID if not provided
                if "_id" not in record:
                    record_id = str(len(record_offsets))
                else:
                    record_id = record["_id"]

                # Serialize the record values
                value_offsets = []
                for field_name, field_value in record.items():
                    if field_name != "_id":  # Skip the ID field
                        value_offset, value_type = self.serialize_value(field_value)

                        # Create a DataValue for this field
                        DataValue.Start(self.builder)
                        DataValue.AddType(self.builder, value_type)
                        DataValue.AddValue(self.builder, value_offset)
                        data_value_offset = DataValue.End(self.builder)

                        value_offsets.append(data_value_offset)

                # Create the vector of values
                Record.StartValuesVector(self.builder, len(value_offsets))
                for offset in reversed(value_offsets):
                    self.builder.PrependUOffsetTRelative(offset)
                values_vector = self.builder.EndVector()

                # Create the Record
                record_id_offset = self._create_string(record_id)
                schema_id_offset = self._create_string(collection_name + "_schema")
                Record.Start(self.builder)
                Record.AddId(self.builder, record_id_offset)
                Record.AddSchemaId(self.builder, schema_id_offset)
                Record.AddValues(self.builder, values_vector)
                record_offset = Record.End(self.builder)

                record_offsets.append(record_offset)

            # Create the vector of records
            Collection.StartRecordsVector(self.builder, len(record_offsets))
            for offset in reversed(record_offsets):
                self.builder.PrependUOffsetTRelative(offset)
            records_vector = self.builder.EndVector()

            # Create the Collection
            Collection.Start(self.builder)
            Collection.AddName(self.builder, collection_name_offset)
            Collection.AddSchema(self.builder, schema_offset)
            Collection.AddRecords(self.builder, records_vector)
            collection_offset = Collection.End(self.builder)

            collection_offsets.append(collection_offset)

        # Create the vector of collections
        Database.StartCollectionsVector(self.builder, len(collection_offsets))
        for offset in reversed(collection_offsets):
            self.builder.PrependUOffsetTRelative(offset)
        collections_vector = self.builder.EndVector()

        # Create the Database
        Database.Start(self.builder)
        Database.AddName(self.builder, name_offset)
        Database.AddVersion(self.builder, version_offset)
        Database.AddCollections(self.builder, collections_vector)
        database_offset = Database.End(self.builder)

        # Finish the buffer
        self.builder.Finish(database_offset)

        # Get the serialized data
        return self.builder.Output()

    def _deserialize_value(self, data_value) -> Any:
        """
        Deserialize a DataValue to its Python representation.

        Args:
            data_value: FlatBuffers DataValue object

        Returns:
            Any: Deserialized Python value
        """
        if data_value is None:
            return None

        value_type = data_value.Type()

        if value_type == DataValue.Int:
            # Deserialize Int
            int_value = Int.Int()
            int_value.Init(data_value.Value().Bytes, data_value.Value().Pos)
            return int_value.Value()

        elif value_type == DataValue.Double:
            # Deserialize Double
            double_value = Double.Double()
            double_value.Init(data_value.Value().Bytes, data_value.Value().Pos)
            return double_value.Value()

        elif value_type == DataValue.String:
            # Deserialize String
            string_value = String.String()
            string_value.Init(data_value.Value().Bytes, data_value.Value().Pos)
            return string_value.Value().decode('utf-8') if string_value.Value() else ""

        elif value_type == DataValue.Bool:
            # Deserialize Bool
            bool_value = Bool.Bool()
            bool_value.Init(data_value.Value().Bytes, data_value.Value().Pos)
            return bool_value.Value()

        elif value_type == DataValue.Timestamp:
            # Deserialize Timestamp
            timestamp_value = Timestamp.Timestamp()
            timestamp_value.Init(data_value.Value().Bytes, data_value.Value().Pos)
            # Convert from milliseconds to datetime
            from datetime import datetime
            return datetime.fromtimestamp(timestamp_value.Value() / 1000.0)

        elif value_type == DataValue.Bytes:
            # Deserialize Bytes
            bytes_value = Bytes.Bytes()
            bytes_value.Init(data_value.Value().Bytes, data_value.Value().Pos)
            return bytes(bytes_value.ValueAsNumpy()) if bytes_value.ValueLength() > 0 else b''

        elif value_type == DataValue.Array:
            # Deserialize Array
            array_value = Array.Array()
            array_value.Init(data_value.Value().Bytes, data_value.Value().Pos)

            # Deserialize each item in the array
            result = []
            for i in range(array_value.ValuesLength()):
                item = array_value.Values(i)
                result.append(self._deserialize_value(item))

            return result

        elif value_type == DataValue.Object:
            # Deserialize Object
            object_value = Object.Object()
            object_value.Init(data_value.Value().Bytes, data_value.Value().Pos)

            # Deserialize each key-value pair in the object
            result = {}
            for i in range(object_value.FieldsLength()):
                kv = object_value.Fields(i)
                key = kv.Key().decode('utf-8') if kv.Key() else ""
                value = self._deserialize_value(kv.Value())
                result[key] = value

            return result

        else:
            # Unknown type
            return None

    def deserialize_record(self, buffer_data: bytes, schema_dict: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Deserialize a record from its binary representation.

        Args:
            buffer_data: Binary data to deserialize
            schema_dict: Optional schema dictionary mapping field indices to field names

        Returns:
            Dict[str, Any]: Deserialized record as a dictionary
        """
        # Parse the buffer
        record = Record.Record.GetRootAsRecord(buffer_data, 0)

        # Extract the record ID
        record_id = record.Id().decode('utf-8') if record.Id() else ""

        # Create the result dictionary
        result = {"_id": record_id}

        # Extract the values
        for i in range(record.ValuesLength()):
            data_value = record.Values(i)

            # Get the field name from the schema if available
            field_name = f"field_{i}"
            if schema_dict and i in schema_dict:
                field_name = schema_dict[i]

            # Deserialize the value
            value = self._deserialize_value(data_value)
            result[field_name] = value

        return result

    def deserialize_collection(self, buffer_data: bytes) -> Dict[str, Any]:
        """
        Deserialize a collection from its binary representation.

        Args:
            buffer_data: Binary data to deserialize

        Returns:
            Dict[str, Any]: Deserialized collection as a dictionary
        """
        # Parse the buffer
        collection = Collection.Collection.GetRootAsCollection(buffer_data, 0)

        # Extract the collection name
        collection_name = collection.Name().decode('utf-8') if collection.Name() else ""

        # Extract the schema
        schema = collection.Schema()
        schema_dict = {}
        field_index_to_name = {}

        for i in range(schema.FieldsLength()):
            field = schema.Fields(i)
            field_name = field.Name().decode('utf-8') if field.Name() else ""
            field_type = field.Type().decode('utf-8') if field.Type() else ""
            schema_dict[field_name] = field_type
            field_index_to_name[i] = field_name

        # Extract the records
        records = []
        for i in range(collection.RecordsLength()):
            record = collection.Records(i)
            record_id = record.Id().decode('utf-8') if record.Id() else ""

            # Create the record dictionary
            record_dict = {"_id": record_id}

            # Extract the values
            for j in range(record.ValuesLength()):
                data_value = record.Values(j)

                # Get the field name from the schema
                field_name = field_index_to_name.get(j, f"field_{j}")

                # Deserialize the value
                value = self._deserialize_value(data_value)
                record_dict[field_name] = value

            records.append(record_dict)

        # Create the result
        result = {
            "name": collection_name,
            "schema": schema_dict,
            "records": records
        }

        return result

    def deserialize_database(self, buffer_data: bytes) -> Dict[str, Any]:
        """
        Deserialize a database from its binary representation.

        Args:
            buffer_data: Binary data to deserialize

        Returns:
            Dict[str, Any]: Deserialized database as a dictionary
        """
        # Parse the buffer
        db = Database.Database.GetRootAsDatabase(buffer_data, 0)

        # Extract the database information
        result = {
            "name": db.Name().decode('utf-8') if db.Name() else "",
            "version": db.Version().decode('utf-8') if db.Version() else "",
            "collections": {}
        }

        # Extract the collections
        for i in range(db.CollectionsLength()):
            collection = db.Collections(i)
            collection_name = collection.Name().decode('utf-8') if collection.Name() else ""

            # Extract the schema
            schema = collection.Schema()
            schema_dict = {}
            field_index_to_name = {}

            for j in range(schema.FieldsLength()):
                field = schema.Fields(j)
                field_name = field.Name().decode('utf-8') if field.Name() else ""
                field_type = field.Type().decode('utf-8') if field.Type() else ""
                schema_dict[field_name] = field_type
                field_index_to_name[j] = field_name

            # Extract the records
            records = []
            for j in range(collection.RecordsLength()):
                record = collection.Records(j)
                record_id = record.Id().decode('utf-8') if record.Id() else ""

                # Create the record dictionary
                record_dict = {"_id": record_id}

                # Extract the values
                for k in range(record.ValuesLength()):
                    data_value = record.Values(k)

                    # Get the field name from the schema
                    field_name = field_index_to_name.get(k, f"field_{k}")

                    # Deserialize the value
                    value = self._deserialize_value(data_value)
                    record_dict[field_name] = value

                records.append(record_dict)

            # Add the collection to the result
            result["collections"][collection_name] = {
                "schema": schema_dict,
                "records": records
            }

        return result
