#!/usr/bin/env python3
"""
Script to generate Python code from FlatBuffers schema.
"""
import os
import subprocess
import sys
import argparse

def ensure_dir(directory):
    """Ensure that a directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def generate_flatbuffers_code(schema_dir, output_dir):
    """
    Generate Python code from FlatBuffers schema.
    
    Args:
        schema_dir: Directory containing the schema files
        output_dir: Directory to output the generated code
    
    Returns:
        bool: True if generation was successful
    """
    # Ensure the output directory exists
    ensure_dir(output_dir)
    
    # Find all schema files
    schema_files = [f for f in os.listdir(schema_dir) if f.endswith('.fbs')]
    
    if not schema_files:
        print(f"No schema files found in {schema_dir}")
        return False
    
    # Generate code for each schema file
    for schema_file in schema_files:
        schema_path = os.path.join(schema_dir, schema_file)
        print(f"Generating code for {schema_file}...")
        
        try:
            # Run the flatc compiler
            result = subprocess.run([
                "flatc", "--python", "-o", output_dir, schema_path
            ], check=True)
            
            if result.returncode != 0:
                print(f"Error generating code for {schema_file}")
                return False
            
            print(f"Successfully generated code for {schema_file}")
        except subprocess.CalledProcessError as e:
            print(f"Error running flatc: {e}")
            return False
        except FileNotFoundError:
            print("flatc compiler not found. Please install FlatBuffers.")
            print("You can install it using:")
            print("  - On macOS: brew install flatbuffers")
            print("  - On Ubuntu: apt-get install flatbuffers-compiler")
            print("  - On Windows: Download from https://github.com/google/flatbuffers/releases")
            return False
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Generate Python code from FlatBuffers schema.')
    parser.add_argument('--schema-dir', default='schemas', help='Directory containing schema files')
    parser.add_argument('--output-dir', default='src/lattice/serialization/generated', help='Output directory for generated code')
    
    args = parser.parse_args()
    
    success = generate_flatbuffers_code(args.schema_dir, args.output_dir)
    
    if success:
        print("Code generation completed successfully.")
        return 0
    else:
        print("Code generation failed.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
