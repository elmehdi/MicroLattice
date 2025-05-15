"""
Compression functionality for Lattice using Zstandard.
"""
import zstandard as zstd
from typing import Dict, Any, Optional, Union, Tuple

class Compressor:
    """Handles compression and decompression using Zstandard."""
    
    def __init__(self, compression_level: int = 19):
        """
        Initialize the compressor.
        
        Args:
            compression_level: Zstandard compression level (1-22, higher = better compression but slower)
        """
        self.compression_level = compression_level
        self.compressor = zstd.ZstdCompressor(level=compression_level)
        self.decompressor = zstd.ZstdDecompressor()
        
        # Dictionary training parameters
        self.dict_size = 1024 * 1024  # 1MB dictionary size
        self.trained_dict = None
    
    def train_dictionary(self, samples: list) -> bool:
        """
        Train a compression dictionary from sample data.
        
        Args:
            samples: List of byte strings to use for training
            
        Returns:
            bool: True if dictionary training was successful
        """
        try:
            self.trained_dict = zstd.train_dictionary(self.dict_size, samples)
            # Update compressor and decompressor to use the trained dictionary
            self.compressor = zstd.ZstdCompressor(level=self.compression_level, dict_data=self.trained_dict)
            self.decompressor = zstd.ZstdDecompressor(dict_data=self.trained_dict)
            return True
        except Exception as e:
            print(f"Error training dictionary: {e}")
            return False
    
    def compress(self, data: bytes) -> bytes:
        """
        Compress data using Zstandard.
        
        Args:
            data: Bytes to compress
            
        Returns:
            bytes: Compressed data
        """
        return self.compressor.compress(data)
    
    def decompress(self, compressed_data: bytes) -> bytes:
        """
        Decompress data using Zstandard.
        
        Args:
            compressed_data: Compressed bytes
            
        Returns:
            bytes: Decompressed data
        """
        return self.decompressor.decompress(compressed_data)
    
    def get_compression_ratio(self, original_data: bytes, compressed_data: bytes) -> float:
        """
        Calculate the compression ratio.
        
        Args:
            original_data: Original uncompressed data
            compressed_data: Compressed data
            
        Returns:
            float: Compression ratio (original size / compressed size)
        """
        original_size = len(original_data)
        compressed_size = len(compressed_data)
        
        if compressed_size == 0:
            return 0.0
        
        return original_size / compressed_size
    
    def compress_with_metadata(self, data: bytes, metadata: Dict[str, Any] = None) -> bytes:
        """
        Compress data with optional metadata.
        
        Args:
            data: Bytes to compress
            metadata: Optional metadata to include with the compressed data
            
        Returns:
            bytes: Compressed data with metadata
        """
        # TODO: Implement metadata inclusion in the compressed stream
        return self.compress(data)
    
    def decompress_with_metadata(self, compressed_data: bytes) -> Tuple[bytes, Optional[Dict[str, Any]]]:
        """
        Decompress data and extract metadata.
        
        Args:
            compressed_data: Compressed bytes with metadata
            
        Returns:
            Tuple[bytes, Optional[Dict[str, Any]]]: Decompressed data and metadata
        """
        # TODO: Implement metadata extraction from the compressed stream
        return self.decompress(compressed_data), None
