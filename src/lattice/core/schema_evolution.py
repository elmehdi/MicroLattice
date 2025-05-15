"""
Schema evolution functionality for Lattice.
"""
from typing import Dict, List, Any, Optional, Tuple
import json
import copy

class SchemaEvolution:
    """Handles schema evolution for Lattice databases."""
    
    def __init__(self):
        self.type_compatibility = {
            "int": ["int", "float", "string"],
            "float": ["float", "string"],
            "string": ["string"],
            "bool": ["bool", "string"],
            "array": ["array"],
            "object": ["object"]
        }
    
    def is_compatible(self, old_type: str, new_type: str) -> bool:
        """
        Check if a field type is compatible with a new type.
        
        Args:
            old_type: Original field type
            new_type: New field type
            
        Returns:
            bool: True if the types are compatible
        """
        if old_type == new_type:
            return True
        
        return new_type in self.type_compatibility.get(old_type, [])
    
    def evolve_schema(self, old_schema: Dict[str, str], new_schema: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, Any]]:
        """
        Evolve a schema to a new version.
        
        Args:
            old_schema: Original schema
            new_schema: New schema
            
        Returns:
            Tuple[Dict[str, str], Dict[str, Any]]: Evolved schema and migration info
        """
        # Start with a copy of the old schema
        evolved_schema = copy.deepcopy(old_schema)
        
        # Track migration information
        migration_info = {
            "added_fields": [],
            "removed_fields": [],
            "changed_types": [],
            "compatible": True
        }
        
        # Check for added fields
        for field_name, field_type in new_schema.items():
            if field_name not in old_schema:
                evolved_schema[field_name] = field_type
                migration_info["added_fields"].append({
                    "name": field_name,
                    "type": field_type
                })
        
        # Check for removed fields
        for field_name, field_type in old_schema.items():
            if field_name not in new_schema:
                migration_info["removed_fields"].append({
                    "name": field_name,
                    "type": field_type
                })
                # Keep the field in the evolved schema for backward compatibility
        
        # Check for type changes
        for field_name, field_type in old_schema.items():
            if field_name in new_schema and field_type != new_schema[field_name]:
                old_type = field_type
                new_type = new_schema[field_name]
                
                # Check if the type change is compatible
                if self.is_compatible(old_type, new_type):
                    evolved_schema[field_name] = new_type
                    migration_info["changed_types"].append({
                        "name": field_name,
                        "old_type": old_type,
                        "new_type": new_type,
                        "compatible": True
                    })
                else:
                    migration_info["changed_types"].append({
                        "name": field_name,
                        "old_type": old_type,
                        "new_type": new_type,
                        "compatible": False
                    })
                    migration_info["compatible"] = False
        
        return evolved_schema, migration_info
    
    def migrate_record(self, record: Dict[str, Any], old_schema: Dict[str, str], new_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Migrate a record from an old schema to a new schema.
        
        Args:
            record: Record to migrate
            old_schema: Original schema
            new_schema: New schema
            
        Returns:
            Dict[str, Any]: Migrated record
        """
        # Start with a copy of the record
        migrated_record = copy.deepcopy(record)
        
        # Handle added fields (set to default values)
        for field_name, field_type in new_schema.items():
            if field_name not in record:
                migrated_record[field_name] = self._get_default_value(field_type)
        
        # Handle type changes
        for field_name, field_type in old_schema.items():
            if field_name in new_schema and field_type != new_schema[field_name]:
                old_type = field_type
                new_type = new_schema[field_name]
                
                # Convert the value if needed
                if field_name in record:
                    migrated_record[field_name] = self._convert_value(record[field_name], old_type, new_type)
        
        return migrated_record
    
    def _get_default_value(self, field_type: str) -> Any:
        """
        Get a default value for a field type.
        
        Args:
            field_type: Field type
            
        Returns:
            Any: Default value for the field type
        """
        if field_type == "int":
            return 0
        elif field_type == "float":
            return 0.0
        elif field_type == "string":
            return ""
        elif field_type == "bool":
            return False
        elif field_type == "array":
            return []
        elif field_type == "object":
            return {}
        else:
            return None
    
    def _convert_value(self, value: Any, old_type: str, new_type: str) -> Any:
        """
        Convert a value from one type to another.
        
        Args:
            value: Value to convert
            old_type: Original type
            new_type: New type
            
        Returns:
            Any: Converted value
        """
        # If the value is None, return the default value for the new type
        if value is None:
            return self._get_default_value(new_type)
        
        # Handle specific type conversions
        if old_type == "int" and new_type == "float":
            return float(value)
        elif old_type == "int" and new_type == "string":
            return str(value)
        elif old_type == "float" and new_type == "string":
            return str(value)
        elif old_type == "bool" and new_type == "string":
            return str(value).lower()
        else:
            # For incompatible types, return the default value
            return self._get_default_value(new_type)
