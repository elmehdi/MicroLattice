"""
Example demonstrating synchronization in Lattice.
"""
import os
import sys
import json
import time
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lattice.core.lattice import LatticeDB

def simulate_server():
    """Simulate a server with a master database."""
    # Create a server-side database
    server_db = LatticeDB("server_db")
    
    # Create a users collection
    user_schema = {
        "id": "int",
        "username": "string",
        "email": "string",
        "age": "int",
        "active": "bool",
        "last_login": "string"
    }
    
    server_db.create_collection("users", user_schema)
    
    # Add some initial data
    users_collection = server_db.get_collection("users")
    users_collection.insert({
        "id": 1,
        "username": "admin",
        "email": "admin@example.com",
        "age": 35,
        "active": True,
        "last_login": datetime.now().isoformat()
    })
    
    users_collection.insert({
        "id": 2,
        "username": "user1",
        "email": "user1@example.com",
        "age": 28,
        "active": True,
        "last_login": datetime.now().isoformat()
    })
    
    # Save the initial database state
    server_db.save("server_db.lattice")
    
    return server_db

def simulate_client(server_db):
    """Simulate a client with a local database."""
    print("\n=== Client: Initial Download ===")
    
    # Client downloads the database
    client_db = LatticeDB()
    client_db.load("server_db.lattice")
    
    # Make some local changes
    print("\n=== Client: Making Local Changes ===")
    users_collection = client_db.get_collection("users")
    
    # Update an existing user
    users_collection.update({"id": 1}, {"last_login": datetime.now().isoformat()})
    print("Updated user 1's last login time")
    
    # Add a new user
    users_collection.insert({
        "id": 3,
        "username": "user2",
        "email": "user2@example.com",
        "age": 42,
        "active": True,
        "last_login": datetime.now().isoformat()
    })
    print("Added new user with ID 3")
    
    # Save the client database
    client_db.save("client_db.lattice")
    
    # Get changes since last sync
    changes = client_db.get_changes_since_last_sync()
    print(f"\nChanges to sync: {len(changes)}")
    for change in changes:
        print(f"  - {change['operation']} on {change['collection']} (ID: {change['record_id']})")
    
    return client_db, changes

def simulate_server_processing_changes(server_db, client_changes):
    """Simulate the server processing client changes."""
    print("\n=== Server: Processing Client Changes ===")
    
    # In a real implementation, this would be done through an API
    # Here we'll directly apply the changes to the server database
    for change in client_changes:
        if change["operation"] == "insert":
            collection = server_db.get_collection(change["collection"])
            collection.insert(change["data"])
            print(f"Inserted new record in {change['collection']} with ID {change['record_id']}")
        
        elif change["operation"] == "update":
            collection = server_db.get_collection(change["collection"])
            collection.update({"_id": change["record_id"]}, change["data"])
            print(f"Updated record in {change['collection']} with ID {change['record_id']}")
    
    # Make a server-side change
    users_collection = server_db.get_collection("users")
    users_collection.update({"id": 2}, {"active": False})
    print("Server updated user 2's active status to False")
    
    # Save the updated server database
    server_db.save("server_db_updated.lattice")
    
    # Get server changes to send back to client
    server_changes = server_db.get_changes_since_last_sync()
    print(f"\nServer changes to send to client: {len(server_changes)}")
    for change in server_changes:
        print(f"  - {change['operation']} on {change['collection']} (ID: {change['record_id']})")
    
    return server_changes

def simulate_client_receiving_server_changes(client_db, server_changes):
    """Simulate the client receiving and applying server changes."""
    print("\n=== Client: Receiving Server Changes ===")
    
    # Apply server changes to client database
    result = client_db.apply_changes(server_changes)
    
    print(f"Applied {len(result['applied_changes'])} changes from server")
    if result["conflicts"]:
        print(f"Detected {len(result['conflicts'])} conflicts")
        for conflict in result["conflicts"]:
            print(f"  - Conflict on {conflict['record_key']}")
    
    # Save the updated client database
    client_db.save("client_db_updated.lattice")
    
    # Verify the final state
    users_collection = client_db.get_collection("users")
    all_users = users_collection.find()
    
    print("\n=== Final Client Database State ===")
    for user in all_users:
        print(f"User {user['id']}: {user['username']} (Active: {user['active']})")
    
    # Mark as synced
    client_db.mark_synced()
    
    # Verify no more changes to sync
    changes = client_db.get_changes_since_last_sync()
    print(f"\nRemaining changes to sync: {len(changes)}")

def main():
    print("=== Lattice Synchronization Example ===")
    
    # Simulate server with initial data
    server_db = simulate_server()
    
    # Simulate client downloading and making changes
    client_db, client_changes = simulate_client(server_db)
    
    # Simulate server processing client changes
    server_changes = simulate_server_processing_changes(server_db, client_changes)
    
    # Simulate client receiving server changes
    simulate_client_receiving_server_changes(client_db, server_changes)
    
    print("\nSynchronization example completed successfully!")
    
    # Clean up
    for file in ["server_db.lattice", "client_db.lattice", 
                "server_db_updated.lattice", "client_db_updated.lattice"]:
        if os.path.exists(file):
            os.remove(file)

if __name__ == "__main__":
    main()
