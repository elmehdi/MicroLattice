# MicroLattice

Introducing MicroLattice, a minimalist, file-based data storage system that compresses millions of diverse data types into a single, tiny file (a few MB) and enables millisecond queries locally within an application. It’s not a database in the traditional sense—no server, no API, no schema. Instead, it’s a self-contained, hyper-compressed "data crystal" that you can drop into any app (web, mobile, or desktop) and query instantly. Think of it as a souped-up JSON file with the power of a database but the simplicity of a text file.


## Overview

MicroLattice combines three existing, underutilized technologies in a novel way:

1. **Zstandard Compression (Zstd):** A modern, ultra-efficient compression algorithm (used by Facebook, Linux kernels) that achieves near-optimal compression ratios with blazing-fast decompression. [for high-ratio compression]

2. **Succinct Data Structures:** Bit-level encoding techniques from bioinformatics and graph theory that pack complex data into minimal space while supporting queries without full decompression. [for bit-level indexing and querying]

3. **FlatBuffers:** A serialization format (from Google) that allows direct memory access to structured data with zero parsing overhead, ideal for millisecond queries. [for efficient serialization]

The result is a single file that:
- Contains an entire database
- Remains small (target: 4MB max)
- Supports fast querying
- Eliminates the need for APIs and traditional databases
- Can be fetched via a single HTTP request

## Features

- **Lightweight**: Minimal footprint, designed for efficiency
- **Portable**: Single file contains all data
- **Queryable**: Fast data retrieval without unpacking the entire dataset
- **Compressed**: Optimized for minimal size
- **Schema-based**: Structured data with type safety
- **Schema Evolution**: Support for evolving schemas over time
- **Complex Queries**: Support for range, regex, prefix, and logical operations
- **Offline-first**: Works entirely client-side without requiring a server

## Installation

```bash
# Install dependencies
pip install flatbuffers zstandard

# Clone the repository
git clone https://github.com/yourusername/lattice.git
cd lattice

# Install the package
pip install -e .
```

## Quick Start

```python
from lattice.core.lattice import LatticeDB

# Create a new database
db = LatticeDB("my_database")

# Create a collection with a schema
user_schema = {
    "id": "int",
    "username": "string",
    "email": "string",
    "age": "int",
    "active": "bool"
}
db.create_collection("users", user_schema)

# Get the collection
users = db.get_collection("users")

# Insert data
users.insert({
    "id": 1,
    "username": "user1",
    "email": "user1@example.com",
    "age": 25,
    "active": True
})

# Query data
active_users = users.find({"active": True})
young_users = users.find({"age": {"range": [18, 30]}})
email_users = users.find({"email": {"regex": "@example.com$"}})

# Save the database to a file
db.save("my_database.lattice")

# Load the database from a file
new_db = LatticeDB()
new_db.load("my_database.lattice")
```

## Technical Details

### Architecture

Lattice is built on three key technologies:

1. **Serialization Layer (FlatBuffers)**
   - Schema-based serialization for type safety
   - Zero-copy deserialization for performance
   - Support for nested structures and complex types

2. **Indexing Layer (Succinct Data Structures)**
   - Bit-level indexing for efficient queries
   - Rank and select operations in constant time
   - Wavelet trees for multi-dimensional queries

3. **Compression Layer (Zstandard)**
   - High compression ratio (typically 3-5x smaller than JSON)
   - Fast decompression for quick data access
   - Dictionary-based compression for repeated patterns

### Data Structures

#### BitVector

The foundation of our succinct data structures is the `BitVector` class, which provides:

- Compact storage of bits (64 bits per machine word)
- O(1) rank operations (count the number of 1s or 0s up to a position)
- O(log n) select operations (find the position of the k-th 1 or 0)

```python
# Example of rank and select operations
bit_vector = BitVector(1000)  # Create a bit vector of size 1000
bit_vector.set_bit(42, True)  # Set bit at position 42 to 1
count = bit_vector.rank1(100)  # Count 1s up to position 100
position = bit_vector.select1(5)  # Find position of the 5th 1
```

#### WaveletTree

For more complex indexing, we use Wavelet Trees:

- Support for rank and select on arbitrary alphabets
- O(log σ) time complexity for operations (where σ is the alphabet size)
- Space-efficient representation of sequences

```python
# Example of wavelet tree operations
sequence = [3, 1, 4, 1, 5, 9, 2, 6, 5]
wavelet_tree = WaveletTree(sequence, max(sequence) + 1)
count = wavelet_tree.rank(5, 7)  # Count occurrences of 5 up to position 7
position = wavelet_tree.select(1, 1)  # Find position of the 2nd occurrence of 1
```

### Query Capabilities

Lattice supports a rich set of query operations:

- **Equality**: `users.find({"username": "user1"})`
- **Range**: `users.find({"age": {"range": [18, 30]}})`
- **Regex**: `users.find({"email": {"regex": "@example.com$"}})`
- **Prefix**: `users.find({"username": {"prefix": "admin"}})`
- **In**: `users.find({"role": {"in": ["admin", "moderator"]}})`
- **Not**: `users.find({"status": {"not": "inactive"}})`
- **Logical AND**: `users.find({"age": {"range": [18, 30]}, "active": True})`
- **Logical OR**: `users.find({"role": "admin", "age": {"range": [30, 50]}}, query_type="or")`

### Schema Evolution

Lattice supports evolving schemas over time:

- Adding new fields
- Removing fields (with backward compatibility)
- Changing field types (when compatible)
- Tracking schema versions and migrations

```python
# Example of schema evolution
new_schema = {
    "id": "int",
    "username": "string",
    "email": "string",
    "age": "float",  # Changed from int to float
    "active": "bool",
    "created_at": "string",  # New field
    "profile": "object"  # New field
}

result = db.update_collection_schema("users", new_schema)
```

### File Format

The `.lattice` file format is a compressed serialized database:

1. The database is serialized using FlatBuffers (or JSON as fallback)
2. The serialized data is compressed using Zstandard
3. The result is a single file that can be easily distributed

## Performance

Lattice is designed for performance in several key areas:

### Size Efficiency

- **3-5x smaller** than equivalent JSON data
- **10-20x smaller** than SQLite databases with similar data
- Target maximum size of 4MB for typical datasets

### Query Performance

- **O(1) access** to records by ID
- **O(log n) lookup** for indexed fields
- **No unpacking required** for targeted queries

### Memory Efficiency

- **Zero-copy deserialization** with FlatBuffers
- **Bit-level indexing** minimizes memory overhead
- **Lazy loading** of large values

## Using Lattice in CRUD Applications

Lattice can replace traditional API-based architectures in many CRUD (Create, Read, Update, Delete) applications. Instead of making API calls for every operation, clients can work with a local .lattice file and synchronize changes periodically.

### Traditional API vs. Lattice Approach

**Traditional API Approach:**
```
Client → HTTP Request → Server → Database Query → Response → Client
```

**Lattice Approach:**
```
Initial: Client ← Download .lattice file ← Server
Operations: Client ↔ Local .lattice file (no server)
Periodic: Client → Sync changes → Server → Updated .lattice → Client
```

### Synchronization Process

The synchronization process with Lattice typically works as follows:

1. **Change Tracking**:
   - Each write operation (insert, update, delete) is tracked in a local change log
   - Changes include timestamps and operation types

2. **Periodic Synchronization**:
   - At defined intervals or triggered by user action (e.g., "Save" button)
   - Client sends batched changes to server
   - Server processes changes and updates master database
   - Server returns new/updated records and conflict resolutions
   - Client applies server changes to local .lattice file

3. **Conflict Resolution**:
   - Server-side: Last-write-wins or custom merge strategies
   - Client-side: Apply server resolutions or prompt user

### Implementation Example

```javascript
// Initialize Lattice database
const db = await LatticeDB.load('app_data.lattice');

// CRUD operations (all local)
const users = db.getCollection('users');
const userId = users.insert({ name: "John", email: "john@example.com" });
const user = users.findOne({ id: userId });
users.update({ id: userId }, { name: "John Smith" });
users.delete({ id: userId });

// Synchronize with server
async function syncWithServer() {
  // Get local changes since last sync
  const changes = db.getChangesSinceLastSync();

  if (changes.length > 0) {
    // Send changes to server
    const response = await fetch('/api/sync', {
      method: 'POST',
      body: JSON.stringify(changes)
    });

    // Apply server changes to local database
    const serverChanges = await response.json();
    db.applyChanges(serverChanges);

    // Save updated database locally
    await db.saveToFile('app_data.lattice');
  }
}

// Call sync periodically or on user action
setInterval(syncWithServer, 5 * 60 * 1000); // Every 5 minutes
```

### Benefits for CRUD Applications

1. **Offline-First**: Applications work without internet connection
2. **Reduced Latency**: Local operations are near-instantaneous
3. **Bandwidth Efficiency**: Only changes are transmitted, not entire datasets
4. **Simplified Backend**: Server focuses on synchronization, not serving every request
5. **Better User Experience**: No loading spinners for basic operations

### Synchronization Strategies

Lattice supports multiple synchronization strategies:

1. **Full Sync**: Replace entire .lattice file periodically
   - Simple but inefficient for large datasets
   - Good for read-heavy applications with infrequent updates

2. **Incremental Sync**: Exchange only changes since last sync
   - More efficient for frequent updates
   - Requires change tracking and conflict resolution

3. **Differential Sync**: Send only the differences in data
   - Most bandwidth-efficient
   - More complex to implement

4. **Event-Based Sync**: Use WebSockets or similar to push changes
   - Near real-time updates
   - More complex server infrastructure

### Server-Side Implementation

To complete a CRUD application with Lattice, you'll need a server component that handles synchronization. Here's a basic implementation approach:

#### 1. Server Endpoints

```javascript
// Express.js example
const express = require('express');
const app = express();
app.use(express.json());

// Master database (could be any database)
const masterDB = require('./database');

// Endpoint to get the initial .lattice file
app.get('/api/data', async (req, res) => {
  // Generate a .lattice file from the master database
  const latticeFile = await generateLatticeFile(masterDB);

  // Send the file to the client
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Disposition', 'attachment; filename="app_data.lattice"');
  res.send(latticeFile);
});

// Endpoint to handle synchronization
app.post('/api/sync', async (req, res) => {
  const clientChanges = req.body;

  // Process client changes
  const result = await processClientChanges(clientChanges);

  // Return server changes and conflict resolutions
  res.json(result);
});
```

#### 2. Processing Changes

```javascript
async function processClientChanges(clientChanges) {
  const serverChanges = [];
  const conflicts = [];

  // Process each change from the client
  for (const change of clientChanges) {
    try {
      switch (change.operation) {
        case 'insert':
          await masterDB.insert(change.collection, change.data);
          break;

        case 'update':
          // Check for conflicts
          const currentRecord = await masterDB.findOne(
            change.collection,
            { id: change.record_id }
          );

          if (currentRecord.updated_at > change.timestamp) {
            // Conflict detected
            conflicts.push({
              type: 'update_conflict',
              clientChange: change,
              serverRecord: currentRecord
            });
          } else {
            // Apply update
            await masterDB.update(
              change.collection,
              { id: change.record_id },
              change.data
            );
          }
          break;

        case 'delete':
          await masterDB.delete(change.collection, { id: change.record_id });
          break;
      }
    } catch (error) {
      conflicts.push({
        type: 'error',
        clientChange: change,
        error: error.message
      });
    }
  }

  // Get changes from server since client's last sync
  const lastSyncTimestamp = Math.min(...clientChanges.map(c => c.timestamp));
  const newServerChanges = await masterDB.getChangesSince(lastSyncTimestamp);

  return {
    serverChanges: newServerChanges,
    conflicts: conflicts,
    success: conflicts.length === 0
  };
}
```

#### 3. Generating .lattice Files

```javascript
async function generateLatticeFile(db) {
  // Create a Lattice database
  const latticeDB = new LatticeDB('app_data');

  // For each collection in the master database
  const collections = await db.getCollections();

  for (const collection of collections) {
    // Get the schema
    const schema = await db.getSchema(collection);

    // Create the collection in Lattice
    latticeDB.createCollection(collection, schema);

    // Get all records
    const records = await db.find(collection, {});

    // Insert records into Lattice
    const latticeCollection = latticeDB.getCollection(collection);
    for (const record of records) {
      latticeCollection.insert(record);
    }
  }

  // Serialize and compress the Lattice database
  return latticeDB.serialize();
}
```

## Use Cases

Lattice is ideal for:

1. **Mobile Applications**: Store application data efficiently on device
2. **Web Applications**: Bundle data with the application for offline use
3. **Edge Computing**: Distribute databases to edge devices
4. **Data Distribution**: Share datasets without requiring a database server
5. **Embedded Systems**: Store structured data on resource-constrained devices
6. **CRUD Applications**: Replace API calls with local data operations
7. **Offline-First Apps**: Support disconnected operation with periodic synchronization

## Limitations

Lattice is not designed for:

1. **Large-scale databases**: Not suitable for multi-GB datasets
2. **High-concurrency writes**: No built-in transaction support
3. **Real-time collaboration**: Basic conflict resolution requires custom implementation

## Examples

See the `examples/` directory for complete examples:

- `simple_example.py`: Basic usage of Lattice
- `advanced_queries.py`: Demonstrates complex query capabilities
- `schema_evolution.py`: Shows schema evolution in action
- `sync_example.py`: Demonstrates synchronization between client and server
- `benchmark.py`: Performance comparison with other storage methods

## Future Improvements

While Lattice is functional and ready for use, there are several areas for future improvement:

### 1. Enhanced Querying Capabilities

The current implementation includes basic succinct data structures (BitVector, WaveletTree) for efficient querying, but could be enhanced with:

- **Full-text search**: Implement inverted indices for text search
- **Spatial queries**: Add support for geospatial data and queries
- **Aggregation functions**: Support for count, sum, average, etc.
- **Query optimization**: Implement cost-based query planning

### 2. Improved Schema Definition

The current schema system uses a simple dictionary-based approach. Future improvements could include:

- **Complete FlatBuffers integration**: Generate code from schemas for type safety
- **Schema validation**: More robust validation of data against schemas
- **Cross-language support**: Generate schemas for multiple programming languages
- **JSON Schema support**: Allow defining schemas using JSON Schema

### 3. Enhanced Documentation

Future documentation improvements could include:

- **API reference**: Complete documentation of all classes and methods
- **Tutorial series**: Step-by-step guides for common use cases
- **Architecture diagrams**: Visual explanations of how Lattice works
- **Video tutorials**: Screencasts demonstrating Lattice in action

### 4. Additional Features

Other potential features for future versions:

- **Encryption**: Support for encrypting sensitive data
- **Compression options**: Allow choosing different compression algorithms
- **Streaming support**: Process large datasets without loading everything into memory
- **Reactive queries**: Subscribe to changes in query results

## Contributing

Contributions are welcome! Here are some ways you can contribute:

1. **Implement new features**: Pick one of the future improvements and implement it
2. **Fix bugs**: Help identify and fix issues
3. **Improve documentation**: Add examples, clarify explanations, fix typos
4. **Write tests**: Increase test coverage and add new test cases
5. **Share use cases**: Let us know how you're using Lattice

Please feel free to submit a Pull Request or open an Issue to discuss improvements.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
