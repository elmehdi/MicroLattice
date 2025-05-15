"""
Example demonstrating advanced query capabilities in Lattice.
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
    """Generate sample product data."""
    products = []

    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Toys", "Food", "Beauty"]
    conditions = ["New", "Used", "Refurbished", "Like New", "Good", "Fair", "Poor"]

    for i in range(num_records):
        product = {
            "id": i,
            "name": f"Product {i}",
            "description": f"This is a description for product {i}. It's a great product!",
            "price": round(random.uniform(1.0, 1000.0), 2),
            "category": random.choice(categories),
            "tags": [random.choice(categories).lower() for _ in range(random.randint(1, 3))],
            "in_stock": random.choice([True, False]),
            "condition": random.choice(conditions),
            "rating": round(random.uniform(1.0, 5.0), 1),
            "created_at": datetime.now().isoformat(),
            "metadata": {
                "weight": round(random.uniform(0.1, 10.0), 2),
                "dimensions": {
                    "width": round(random.uniform(1.0, 50.0), 2),
                    "height": round(random.uniform(1.0, 50.0), 2),
                    "depth": round(random.uniform(1.0, 50.0), 2)
                }
            }
        }
        products.append(product)

    return products

def main():
    # Create a new Lattice database
    db = LatticeDB("products_db")

    # Create a products collection
    product_schema = {
        "id": "int",
        "name": "string",
        "description": "string",
        "price": "float",
        "category": "string",
        "tags": "array",
        "in_stock": "bool",
        "condition": "string",
        "rating": "float",
        "created_at": "string",
        "metadata": "object"
    }

    db.create_collection("products", product_schema)

    # Get the products collection
    products_collection = db.get_collection("products")

    # Generate and insert sample data
    print("Generating sample data...")
    sample_products = generate_sample_data(1000)

    print("Inserting records...")
    for product in sample_products:
        products_collection.insert(product)

    print(f"Inserted {len(sample_products)} records")

    # Perform some advanced queries
    print("\nPerforming advanced queries:")

    # 1. Range query - Find products with price between 100 and 200
    price_range_query = {"price": {"range": [100, 200]}}
    price_range_results = products_collection.find(price_range_query)
    print(f"Products with price between 100 and 200: {len(price_range_results)}")

    # 2. Regex query - Find products with "great" in the description
    regex_query = {"description": {"regex": "great"}}
    regex_results = products_collection.find(regex_query)
    print(f"Products with 'great' in the description: {len(regex_results)}")

    # 3. Prefix query - Find products with names starting with "Product 1"
    prefix_query = {"name": {"prefix": "Product 1"}}
    prefix_results = products_collection.find(prefix_query)
    print(f"Products with names starting with 'Product 1': {len(prefix_results)}")

    # 4. In query - Find products in specific categories
    in_query = {"category": {"in": ["Electronics", "Books"]}}
    in_results = products_collection.find(in_query)
    print(f"Products in Electronics or Books categories: {len(in_results)}")

    # 5. Not query - Find products not in "New" condition
    not_query = {"condition": {"not": "New"}}
    not_results = products_collection.find(not_query)
    print(f"Products not in 'New' condition: {len(not_results)}")

    # 6. Combined query - Find in-stock Electronics with price > 500
    combined_query = {
        "category": "Electronics",
        "in_stock": True,
        "price": {"range": [500, 1000]}
    }
    combined_results = products_collection.find(combined_query)
    print(f"In-stock Electronics with price > 500: {len(combined_results)}")

    # 7. OR query - Find products that are either Electronics or have a rating > 4.5
    or_query = {
        "category": "Electronics",
        "rating": {"range": [4.5, 5.0]}
    }
    or_results = products_collection.find(or_query, query_type="or")
    print(f"Products that are either Electronics or have rating > 4.5: {len(or_results)}")

    # 8. Find one - Get a single product
    single_product = products_collection.find_one({"id": 42})
    if single_product:
        print(f"\nFound product with ID 42: {single_product['name']}")

    # Save the database to a file
    print("\nSaving database to file...")
    db.save("products_db.lattice")

    # Get the file size
    file_size = os.path.getsize("products_db.lattice")
    print(f"Database file size: {file_size / 1024:.2f} KB")

    print("\nExample completed successfully!")

if __name__ == "__main__":
    main()
