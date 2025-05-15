"""
Tests for the Lattice database.
"""
import os
import sys
import unittest
import tempfile
import random
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lattice.core.lattice import LatticeDB

class TestLatticeDB(unittest.TestCase):
    """Test cases for the LatticeDB class."""
    
    def setUp(self):
        """Set up a test database."""
        self.db = LatticeDB("test_db")
        
        # Create a test collection
        self.user_schema = {
            "id": "int",
            "username": "string",
            "email": "string",
            "age": "int",
            "active": "bool"
        }
        
        self.db.create_collection("users", self.user_schema)
        
        # Add some test data
        self.users_collection = self.db.get_collection("users")
        
        self.test_users = [
            {"id": 1, "username": "user1", "email": "user1@example.com", "age": 25, "active": True},
            {"id": 2, "username": "user2", "email": "user2@example.com", "age": 30, "active": False},
            {"id": 3, "username": "user3", "email": "user3@example.com", "age": 35, "active": True},
            {"id": 4, "username": "user4", "email": "user4@example.com", "age": 40, "active": False},
            {"id": 5, "username": "user5", "email": "user5@example.com", "age": 45, "active": True}
        ]
        
        for user in self.test_users:
            self.users_collection.insert(user)
    
    def test_collection_creation(self):
        """Test creating a collection."""
        # Create a new collection
        result = self.db.create_collection("posts", {"id": "int", "title": "string", "content": "string"})
        self.assertTrue(result)
        
        # Try to create a collection with the same name
        result = self.db.create_collection("posts", {"id": "int", "title": "string"})
        self.assertFalse(result)
        
        # Check that the collection exists
        posts_collection = self.db.get_collection("posts")
        self.assertIsNotNone(posts_collection)
    
    def test_insert_and_find(self):
        """Test inserting and finding records."""
        # Check that all test users were inserted
        all_users = self.users_collection.find()
        self.assertEqual(len(all_users), len(self.test_users))
        
        # Find a specific user
        user2 = self.users_collection.find({"id": 2})
        self.assertEqual(len(user2), 1)
        self.assertEqual(user2[0]["username"], "user2")
        
        # Find active users
        active_users = self.users_collection.find({"active": True})
        self.assertEqual(len(active_users), 3)
    
    def test_update(self):
        """Test updating records."""
        # Update user2's active status
        updated_count = self.users_collection.update({"id": 2}, {"active": True})
        self.assertEqual(updated_count, 1)
        
        # Check that the update was applied
        user2 = self.users_collection.find({"id": 2})
        self.assertEqual(len(user2), 1)
        self.assertTrue(user2[0]["active"])
        
        # Update multiple users
        updated_count = self.users_collection.update({"active": True}, {"age": 50})
        self.assertEqual(updated_count, 4)  # Now 4 active users after the previous update
        
        # Check that the updates were applied
        age_50_users = self.users_collection.find({"age": 50})
        self.assertEqual(len(age_50_users), 4)
    
    def test_delete(self):
        """Test deleting records."""
        # Delete user3
        deleted_count = self.users_collection.delete({"id": 3})
        self.assertEqual(deleted_count, 1)
        
        # Check that user3 was deleted
        all_users = self.users_collection.find()
        self.assertEqual(len(all_users), len(self.test_users) - 1)
        
        user3 = self.users_collection.find({"id": 3})
        self.assertEqual(len(user3), 0)
        
        # Delete multiple users
        deleted_count = self.users_collection.delete({"active": True})
        self.assertEqual(deleted_count, 2)  # 2 remaining active users after deleting user3
        
        # Check that the active users were deleted
        active_users = self.users_collection.find({"active": True})
        self.assertEqual(len(active_users), 0)
    
    def test_save_and_load(self):
        """Test saving and loading the database."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Save the database
            result = self.db.save(temp_path)
            self.assertTrue(result)
            
            # Create a new database and load the saved data
            new_db = LatticeDB()
            result = new_db.load(temp_path)
            self.assertTrue(result)
            
            # Check that the collections were loaded
            self.assertIn("users", new_db.collections)
            
            # Check that the data was loaded
            users_collection = new_db.get_collection("users")
            all_users = users_collection.find()
            self.assertEqual(len(all_users), len(self.test_users))
            
            # Check a specific query
            active_users = users_collection.find({"active": True})
            self.assertEqual(len(active_users), 3)
        finally:
            # Clean up
            os.unlink(temp_path)
    
    def test_drop_collection(self):
        """Test dropping a collection."""
        # Drop the users collection
        result = self.db.drop_collection("users")
        self.assertTrue(result)
        
        # Check that the collection was dropped
        users_collection = self.db.get_collection("users")
        self.assertIsNone(users_collection)
        
        # Try to drop a non-existent collection
        result = self.db.drop_collection("non_existent")
        self.assertFalse(result)

if __name__ == "__main__":
    unittest.main()
