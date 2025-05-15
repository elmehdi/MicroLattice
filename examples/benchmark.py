"""
Benchmark script for Lattice.

This script compares Lattice with other storage methods:
1. JSON (plain)
2. JSON (compressed with Zstandard)
3. SQLite
4. Lattice

Metrics measured:
1. File size
2. Write time
3. Read time
4. Query time
"""
import os
import sys
import json
import time
import sqlite3
import random
import zstandard as zstd
from datetime import datetime

# Add the src directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.lattice.core.lattice import LatticeDB

def generate_sample_data(num_records=10000):
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

def benchmark_json(data, filename="benchmark_json.json"):
    """Benchmark JSON storage."""
    # Write
    start_time = time.time()
    with open(filename, 'w') as f:
        json.dump(data, f)
    write_time = time.time() - start_time
    
    # File size
    file_size = os.path.getsize(filename)
    
    # Read
    start_time = time.time()
    with open(filename, 'r') as f:
        loaded_data = json.load(f)
    read_time = time.time() - start_time
    
    # Query (find users over 60)
    start_time = time.time()
    result = [user for user in loaded_data if user["age"] > 60]
    query_time = time.time() - start_time
    
    return {
        "format": "JSON",
        "file_size": file_size,
        "write_time": write_time,
        "read_time": read_time,
        "query_time": query_time,
        "result_count": len(result)
    }

def benchmark_json_compressed(data, filename="benchmark_json_compressed.json.zst"):
    """Benchmark compressed JSON storage."""
    # Write
    start_time = time.time()
    json_data = json.dumps(data).encode('utf-8')
    compressor = zstd.ZstdCompressor(level=19)
    compressed_data = compressor.compress(json_data)
    with open(filename, 'wb') as f:
        f.write(compressed_data)
    write_time = time.time() - start_time
    
    # File size
    file_size = os.path.getsize(filename)
    
    # Read
    start_time = time.time()
    with open(filename, 'rb') as f:
        compressed_data = f.read()
    decompressor = zstd.ZstdDecompressor()
    json_data = decompressor.decompress(compressed_data)
    loaded_data = json.loads(json_data)
    read_time = time.time() - start_time
    
    # Query (find users over 60)
    start_time = time.time()
    result = [user for user in loaded_data if user["age"] > 60]
    query_time = time.time() - start_time
    
    return {
        "format": "JSON+Zstd",
        "file_size": file_size,
        "write_time": write_time,
        "read_time": read_time,
        "query_time": query_time,
        "result_count": len(result)
    }

def benchmark_sqlite(data, filename="benchmark_sqlite.db"):
    """Benchmark SQLite storage."""
    # Write
    start_time = time.time()
    conn = sqlite3.connect(filename)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            email TEXT,
            age INTEGER,
            active INTEGER,
            created_at TEXT,
            preferences TEXT
        )
    ''')
    
    for user in data:
        preferences = json.dumps(user["preferences"])
        c.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                user["id"],
                user["username"],
                user["email"],
                user["age"],
                1 if user["active"] else 0,
                user["created_at"],
                preferences
            )
        )
    
    conn.commit()
    write_time = time.time() - start_time
    
    # File size
    file_size = os.path.getsize(filename)
    
    # Read
    start_time = time.time()
    c.execute("SELECT * FROM users")
    loaded_data = c.fetchall()
    read_time = time.time() - start_time
    
    # Query (find users over 60)
    start_time = time.time()
    c.execute("SELECT * FROM users WHERE age > 60")
    result = c.fetchall()
    query_time = time.time() - start_time
    
    conn.close()
    
    return {
        "format": "SQLite",
        "file_size": file_size,
        "write_time": write_time,
        "read_time": read_time,
        "query_time": query_time,
        "result_count": len(result)
    }

def benchmark_lattice(data, filename="benchmark_lattice.lattice"):
    """Benchmark Lattice storage."""
    # Write
    start_time = time.time()
    db = LatticeDB("benchmark_db")
    
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
    users_collection = db.get_collection("users")
    
    for user in data:
        users_collection.insert(user)
    
    db.save(filename)
    write_time = time.time() - start_time
    
    # File size
    file_size = os.path.getsize(filename)
    
    # Read
    start_time = time.time()
    new_db = LatticeDB()
    new_db.load(filename)
    read_time = time.time() - start_time
    
    # Query (find users over 60)
    start_time = time.time()
    users_collection = new_db.get_collection("users")
    result = users_collection.find({"age": {"range": [61, 100]}})
    query_time = time.time() - start_time
    
    return {
        "format": "Lattice",
        "file_size": file_size,
        "write_time": write_time,
        "read_time": read_time,
        "query_time": query_time,
        "result_count": len(result)
    }

def format_results(results):
    """Format benchmark results as a table."""
    headers = ["Format", "File Size (KB)", "Write Time (s)", "Read Time (s)", "Query Time (s)", "Result Count"]
    
    # Calculate column widths
    col_widths = [max(len(header), max(len(str(result[key])) for result in results))
                 for header, key in zip(headers, ["format", "file_size", "write_time", "read_time", "query_time", "result_count"])]
    
    # Format header
    header_row = " | ".join(header.ljust(width) for header, width in zip(headers, col_widths))
    separator = "-+-".join("-" * width for width in col_widths)
    
    # Format rows
    rows = []
    for result in results:
        row = [
            result["format"],
            f"{result['file_size'] / 1024:.2f}",
            f"{result['write_time']:.4f}",
            f"{result['read_time']:.4f}",
            f"{result['query_time']:.4f}",
            str(result["result_count"])
        ]
        rows.append(" | ".join(cell.ljust(width) for cell, width in zip(row, col_widths)))
    
    # Combine all parts
    table = "\n".join([header_row, separator] + rows)
    return table

def main():
    # Generate sample data
    print("Generating sample data...")
    num_records = 10000
    data = generate_sample_data(num_records)
    print(f"Generated {num_records} records")
    
    # Run benchmarks
    print("\nRunning benchmarks...")
    results = []
    
    print("Benchmarking JSON...")
    results.append(benchmark_json(data))
    
    print("Benchmarking JSON+Zstd...")
    results.append(benchmark_json_compressed(data))
    
    print("Benchmarking SQLite...")
    results.append(benchmark_sqlite(data))
    
    print("Benchmarking Lattice...")
    results.append(benchmark_lattice(data))
    
    # Print results
    print("\nBenchmark Results:")
    print(format_results(results))
    
    # Clean up
    print("\nCleaning up...")
    for filename in ["benchmark_json.json", "benchmark_json_compressed.json.zst", 
                     "benchmark_sqlite.db", "benchmark_lattice.lattice"]:
        if os.path.exists(filename):
            os.remove(filename)
    
    print("\nBenchmark completed successfully!")

if __name__ == "__main__":
    main()
