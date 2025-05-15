"""
Change tracking functionality for Lattice.
"""
import time
import uuid
from typing import Dict, List, Any, Optional, Union

class ChangeTracker:
    """Tracks changes to a Lattice database for synchronization."""
    
    def __init__(self):
        """Initialize the change tracker."""
        self.changes = []
        self.last_sync_timestamp = 0
    
    def track_insert(self, collection_name: str, record_id: str, record: Dict[str, Any]):
        """
        Track an insert operation.
        
        Args:
            collection_name: Name of the collection
            record_id: ID of the inserted record
            record: The inserted record
        """
        self.changes.append({
            "id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "operation": "insert",
            "collection": collection_name,
            "record_id": record_id,
            "data": record
        })
    
    def track_update(self, collection_name: str, record_id: str, 
                    updates: Dict[str, Any], old_record: Optional[Dict[str, Any]] = None):
        """
        Track an update operation.
        
        Args:
            collection_name: Name of the collection
            record_id: ID of the updated record
            updates: The updates applied to the record
            old_record: The record before updates (optional)
        """
        change = {
            "id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "operation": "update",
            "collection": collection_name,
            "record_id": record_id,
            "data": updates
        }
        
        if old_record:
            change["old_data"] = old_record
        
        self.changes.append(change)
    
    def track_delete(self, collection_name: str, record_id: str, 
                    old_record: Optional[Dict[str, Any]] = None):
        """
        Track a delete operation.
        
        Args:
            collection_name: Name of the collection
            record_id: ID of the deleted record
            old_record: The deleted record (optional)
        """
        change = {
            "id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "operation": "delete",
            "collection": collection_name,
            "record_id": record_id
        }
        
        if old_record:
            change["old_data"] = old_record
        
        self.changes.append(change)
    
    def track_schema_update(self, collection_name: str, 
                           old_schema: Dict[str, str], 
                           new_schema: Dict[str, str],
                           migration_info: Dict[str, Any]):
        """
        Track a schema update operation.
        
        Args:
            collection_name: Name of the collection
            old_schema: The old schema
            new_schema: The new schema
            migration_info: Information about the migration
        """
        self.changes.append({
            "id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "operation": "schema_update",
            "collection": collection_name,
            "old_schema": old_schema,
            "new_schema": new_schema,
            "migration_info": migration_info
        })
    
    def get_changes_since(self, timestamp: float) -> List[Dict[str, Any]]:
        """
        Get changes since the specified timestamp.
        
        Args:
            timestamp: Timestamp to get changes since
            
        Returns:
            List[Dict[str, Any]]: List of changes
        """
        return [change for change in self.changes if change["timestamp"] > timestamp]
    
    def get_changes_since_last_sync(self) -> List[Dict[str, Any]]:
        """
        Get changes since the last synchronization.
        
        Returns:
            List[Dict[str, Any]]: List of changes
        """
        return self.get_changes_since(self.last_sync_timestamp)
    
    def mark_synced(self, timestamp: Optional[float] = None):
        """
        Mark changes as synchronized up to the specified timestamp.
        
        Args:
            timestamp: Timestamp to mark as synced (default: current time)
        """
        if timestamp is None:
            timestamp = time.time()
        
        self.last_sync_timestamp = timestamp
        
        # Remove changes older than the sync timestamp
        self.changes = [change for change in self.changes 
                       if change["timestamp"] > self.last_sync_timestamp]
    
    def apply_remote_changes(self, remote_changes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply remote changes and detect conflicts.
        
        Args:
            remote_changes: Changes from the remote server
            
        Returns:
            List[Dict[str, Any]]: List of conflicts
        """
        conflicts = []
        
        # Group local changes by record ID for conflict detection
        local_changes_by_record = {}
        for change in self.changes:
            record_key = f"{change['collection']}:{change['record_id']}"
            if record_key not in local_changes_by_record:
                local_changes_by_record[record_key] = []
            local_changes_by_record[record_key].append(change)
        
        # Process remote changes and detect conflicts
        for remote_change in remote_changes:
            record_key = f"{remote_change['collection']}:{remote_change['record_id']}"
            
            # Check for conflicts with local changes
            if record_key in local_changes_by_record:
                local_changes = local_changes_by_record[record_key]
                
                # Find the latest local change for this record
                latest_local_change = max(local_changes, key=lambda c: c["timestamp"])
                
                # If the local change is newer than the remote change, we have a conflict
                if latest_local_change["timestamp"] > remote_change["timestamp"]:
                    conflicts.append({
                        "record_key": record_key,
                        "local_change": latest_local_change,
                        "remote_change": remote_change
                    })
        
        return conflicts
