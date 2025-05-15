import zstandard as zstd
import json
import sys

def decompress_lattice_file(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()
    
    decompressor = zstd.ZstdDecompressor()
    decompressed = decompressor.decompress(data)
    
    # Try to parse as JSON
    try:
        json_data = json.loads(decompressed)
        print(json.dumps(json_data, indent=2))
    except json.JSONDecodeError:
        print("Not valid JSON data. Raw decompressed data:")
        print(decompressed[:1000])  # Print first 1000 bytes
        print("...")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python decompress.py <lattice_file>")
        sys.exit(1)
    
    decompress_lattice_file(sys.argv[1])
