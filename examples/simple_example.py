"""
Simple example demonstrating the use of Lattice.
"""
import os
import sys
import json
import random
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lattice.core.lattice import LatticeDB

def generate_sample_data(num_records=1000):
    """Generate sample user data."""
    users = []
    
    for i in range(num_records):
        user = {
            "id": i,
            "username": f"user_{i}",
            "email": f"user_{i}@example.com",
            "age": random.randint(18, 80),
            "active": random.choice([True, False]),
            "created_at": datetime.now().isoformat(),
            "preferences": {
                "theme": random.choice(["light", "dark", "system"]),
                "notifications": random.choice([True, False]),
                "language": random.choice(["en", "fr", "es", "de", "ja"])
            }
        }
        users.append(user)
    
    return users

def main():
    # Create a new Lattice database
    db = LatticeDB("example_db")
    
    # Create a users collection
    user_schema = {
        "id": "int",
        "username": "string",
        "email": "string",
        "age": "int",
        "active": "bool",
        "created_at": "string",
        "preferences": "object"
    }
    
    db.create_collection("users", user_schema)
    
    # Get the users collection
    users_collection = db.get_collection("users")
    
    # Generate and insert sample data
    print("Generating sample data...")
    sample_users = generate_sample_data(1000)
    
    print("Inserting records...")
    for user in sample_users:
        users_collection.insert(user)
    
    print(f"Inserted {len(sample_users)} records")
    
    # Perform some queries
    print("\nQuerying data:")
    
    # Find users with age > 60
    elderly_users = users_collection.find({"age": {"range": [60, 80]}})
    print(f"Users over 60: {len(elderly_users)}")
    
    # Find active users with dark theme
    active_dark_users = [
        user for user in users_collection.find({"active": True})
        if user["preferences"]["theme"] == "dark"
    ]
    print(f"Active users with dark theme: {len(active_dark_users)}")
    
    # Save the database to a file
    print("\nSaving database to file...")
    db.save("example_db.lattice")
    
    # Get the file size
    file_size = os.path.getsize("example_db.lattice")
    print(f"Database file size: {file_size / 1024:.2f} KB")
    
    # Load the database from the file
    print("\nLoading database from file...")
    new_db = LatticeDB()
    new_db.load("example_db.lattice")
    
    # Verify the data
    new_users_collection = new_db.get_collection("users")
    all_users = new_users_collection.find()
    print(f"Loaded {len(all_users)} records")
    
    # Verify a specific query
    elderly_users_new = new_users_collection.find({"age": {"range": [60, 80]}})
    print(f"Users over 60 (after loading): {len(elderly_users_new)}")
    
    print("\nExample completed successfully!")

if __name__ == "__main__":
    main()
