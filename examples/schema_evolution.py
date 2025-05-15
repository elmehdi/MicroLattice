"""
Example demonstrating schema evolution in Lattice.
"""
import os
import sys
import json
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lattice.core.lattice import LatticeDB

def print_schema_history(db, collection_name):
    """Print the schema history of a collection."""
    schema_history = db.get_collection_schema_history(collection_name)
    print(f"\nSchema history for collection '{collection_name}':")
    for version in schema_history:
        print(f"  Version {version['version']} - Created at: {version['created_at']}")
        print(f"  Schema: {json.dumps(version['schema'], indent=2)}")
        if "migration_info" in version:
            migration_info = version["migration_info"]
            print(f"  Migration info:")
            if migration_info["added_fields"]:
                print(f"    Added fields: {migration_info['added_fields']}")
            if migration_info["removed_fields"]:
                print(f"    Removed fields: {migration_info['removed_fields']}")
            if migration_info["changed_types"]:
                print(f"    Changed types: {migration_info['changed_types']}")
        print()

def main():
    # Create a new Lattice database
    db = LatticeDB("users_db")
    
    # Create a users collection with initial schema
    initial_schema = {
        "id": "int",
        "username": "string",
        "email": "string",
        "age": "int",
        "active": "bool"
    }
    
    db.create_collection("users", initial_schema)
    
    # Get the users collection
    users_collection = db.get_collection("users")
    
    # Insert some initial data
    print("Inserting initial data...")
    users = [
        {"id": 1, "username": "user1", "email": "user1@example.com", "age": 25, "active": True},
        {"id": 2, "username": "user2", "email": "user2@example.com", "age": 30, "active": False},
        {"id": 3, "username": "user3", "email": "user3@example.com", "age": 35, "active": True}
    ]
    
    for user in users:
        users_collection.insert(user)
    
    print(f"Inserted {len(users)} users")
    
    # Print the schema history
    print_schema_history(db, "users")
    
    # Evolve the schema - Add new fields
    print("\nEvolving schema - Adding new fields...")
    new_schema_1 = {
        "id": "int",
        "username": "string",
        "email": "string",
        "age": "int",
        "active": "bool",
        "created_at": "string",
        "profile": "object"
    }
    
    result = db.update_collection_schema("users", new_schema_1)
    print(f"Schema evolution result: {result['success']}")
    
    # Print the schema history
    print_schema_history(db, "users")
    
    # Check the migrated data
    users_collection = db.get_collection("users")
    all_users = users_collection.find()
    
    print("\nMigrated data:")
    for user in all_users:
        print(f"User {user['id']}: {user}")
    
    # Insert a user with the new schema
    print("\nInserting a user with the new schema...")
    new_user = {
        "id": 4,
        "username": "user4",
        "email": "user4@example.com",
        "age": 40,
        "active": True,
        "created_at": datetime.now().isoformat(),
        "profile": {
            "bio": "A new user with the updated schema",
            "location": "New York"
        }
    }
    
    users_collection.insert(new_user)
    
    # Evolve the schema again - Change field types
    print("\nEvolving schema again - Changing field types...")
    new_schema_2 = {
        "id": "int",
        "username": "string",
        "email": "string",
        "age": "float",  # Changed from int to float
        "active": "bool",
        "created_at": "string",
        "profile": "object",
        "tags": "array"  # Added new field
    }
    
    result = db.update_collection_schema("users", new_schema_2)
    print(f"Schema evolution result: {result['success']}")
    
    # Print the schema history
    print_schema_history(db, "users")
    
    # Check the migrated data
    users_collection = db.get_collection("users")
    all_users = users_collection.find()
    
    print("\nMigrated data after second evolution:")
    for user in all_users:
        print(f"User {user['id']}: {user}")
    
    # Try an incompatible schema evolution
    print("\nTrying an incompatible schema evolution...")
    incompatible_schema = {
        "id": "int",
        "username": "string",
        "email": "string",
        "age": "string",  # Changed from float to string (compatible)
        "active": "int",  # Changed from bool to int (incompatible)
        "created_at": "string",
        "profile": "object",
        "tags": "array"
    }
    
    result = db.update_collection_schema("users", incompatible_schema)
    print(f"Schema evolution result: {result['success']}")
    if not result['success']:
        print(f"Error: {result.get('error')}")
        if 'migration_info' in result:
            print(f"Migration info: {result['migration_info']}")
    
    # Save the database to a file
    print("\nSaving database to file...")
    db.save("users_db.lattice")
    
    # Get the file size
    file_size = os.path.getsize("users_db.lattice")
    print(f"Database file size: {file_size / 1024:.2f} KB")
    
    print("\nExample completed successfully!")

if __name__ == "__main__":
    main()
