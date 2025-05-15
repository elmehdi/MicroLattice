"""
Succinct data structures for bit-level indexing in Lattice.
"""
from typing import List, Dict, Any, Tuple, Optional
import array
import math

class BitVector:
    """A compact bit vector implementation."""

    def __init__(self, size: int = 0):
        """
        Initialize a bit vector of the given size.

        Args:
            size: Number of bits in the vector
        """
        self.size = size
        # Use an array of unsigned longs to store bits
        self.bits = array.array('L', [0] * ((size + 63) // 64))

        # For rank/select operations
        self.block_size = 512  # Block size in bits
        self.superblock_size = 512 * 64  # Superblock size in bits

        # These will be initialized when build_index is called
        self.rank_samples = None
        self.select_samples = None

    def set_bit(self, pos: int, value: bool = True):
        """
        Set the bit at position pos to value.

        Args:
            pos: Position in the bit vector (0-indexed)
            value: True for 1, False for 0
        """
        if pos < 0 or pos >= self.size:
            raise IndexError(f"Position {pos} out of range")

        block_idx = pos // 64
        bit_idx = pos % 64

        if value:
            self.bits[block_idx] |= (1 << bit_idx)
        else:
            self.bits[block_idx] &= ~(1 << bit_idx)

    def get_bit(self, pos: int) -> bool:
        """
        Get the bit at position pos.

        Args:
            pos: Position in the bit vector (0-indexed)

        Returns:
            bool: True if the bit is 1, False if 0
        """
        if pos < 0 or pos >= self.size:
            raise IndexError(f"Position {pos} out of range")

        block_idx = pos // 64
        bit_idx = pos % 64

        return bool(self.bits[block_idx] & (1 << bit_idx))

    def build_index(self):
        """Build auxiliary data structures for rank and select operations."""
        # Initialize rank samples
        num_blocks = (self.size + self.block_size - 1) // self.block_size
        num_superblocks = (self.size + self.superblock_size - 1) // self.superblock_size

        self.rank_samples = {
            'superblocks': array.array('L', [0] * num_superblocks),
            'blocks': array.array('H', [0] * num_blocks)  # Using unsigned short for blocks
        }

        # Build rank samples
        rank_count = 0
        for i in range(self.size):
            if i % self.superblock_size == 0:
                self.rank_samples['superblocks'][i // self.superblock_size] = rank_count

            if i % self.block_size == 0:
                # Store the relative rank within the superblock
                superblock_base = (i // self.superblock_size) * self.superblock_size
                relative_rank = rank_count - self.rank_samples['superblocks'][i // self.superblock_size]
                self.rank_samples['blocks'][i // self.block_size] = relative_rank

            if self.get_bit(i):
                rank_count += 1

        # Build select samples
        # We'll sample every 64th 1-bit and 0-bit
        sample_rate = 64

        # Count the total number of 1s and 0s
        total_ones = self.rank1(self.size - 1)
        total_zeros = self.size - total_ones

        # Initialize select samples
        num_samples_1 = (total_ones + sample_rate - 1) // sample_rate
        num_samples_0 = (total_zeros + sample_rate - 1) // sample_rate

        self.select_samples = {
            'ones': array.array('L', [0] * num_samples_1),
            'zeros': array.array('L', [0] * num_samples_0)
        }

        # Build select samples for 1s
        ones_count = 0
        for i in range(self.size):
            if self.get_bit(i):
                if ones_count % sample_rate == 0:
                    sample_idx = ones_count // sample_rate
                    if sample_idx < num_samples_1:
                        self.select_samples['ones'][sample_idx] = i
                ones_count += 1

        # Build select samples for 0s
        zeros_count = 0
        for i in range(self.size):
            if not self.get_bit(i):
                if zeros_count % sample_rate == 0:
                    sample_idx = zeros_count // sample_rate
                    if sample_idx < num_samples_0:
                        self.select_samples['zeros'][sample_idx] = i
                zeros_count += 1

    def rank1(self, pos: int) -> int:
        """
        Count the number of 1s up to position pos (inclusive).

        Args:
            pos: Position in the bit vector (0-indexed)

        Returns:
            int: Number of 1s up to position pos
        """
        if pos < 0 or pos >= self.size:
            raise IndexError(f"Position {pos} out of range")

        # Get the superblock and block counts
        superblock_idx = pos // self.superblock_size
        block_idx = pos // self.block_size

        # Start with the superblock count
        count = self.rank_samples['superblocks'][superblock_idx]

        # Add the block count (relative to the superblock)
        count += self.rank_samples['blocks'][block_idx]

        # Count the remaining bits linearly
        block_start = block_idx * self.block_size
        for i in range(block_start, pos + 1):
            if i < self.size and self.get_bit(i):
                count += 1

        return count

    def rank0(self, pos: int) -> int:
        """
        Count the number of 0s up to position pos (inclusive).

        Args:
            pos: Position in the bit vector (0-indexed)

        Returns:
            int: Number of 0s up to position pos
        """
        return pos + 1 - self.rank1(pos)

    def select1(self, rank: int) -> int:
        """
        Find the position of the rank-th 1 (0-indexed).

        Args:
            rank: The rank of the 1 to find (0-indexed)

        Returns:
            int: Position of the rank-th 1
        """
        # Simple binary search implementation
        # This could be optimized with select samples
        left, right = 0, self.size - 1

        while left <= right:
            mid = (left + right) // 2
            mid_rank = self.rank1(mid)

            if mid_rank == rank + 1 and (mid == 0 or self.rank1(mid - 1) == rank):
                return mid
            elif mid_rank <= rank:
                left = mid + 1
            else:
                right = mid - 1

        raise ValueError(f"No bit with rank {rank} found")

    def select0(self, rank: int) -> int:
        """
        Find the position of the rank-th 0 (0-indexed).

        Args:
            rank: The rank of the 0 to find (0-indexed)

        Returns:
            int: Position of the rank-th 0
        """
        # Simple binary search implementation
        left, right = 0, self.size - 1

        while left <= right:
            mid = (left + right) // 2
            mid_rank = self.rank0(mid)

            if mid_rank == rank + 1 and (mid == 0 or self.rank0(mid - 1) == rank):
                return mid
            elif mid_rank <= rank:
                left = mid + 1
            else:
                right = mid - 1

        raise ValueError(f"No bit with rank {rank} found")


class WaveletTree:
    """
    A wavelet tree implementation for efficient string operations.
    This allows for rank/select queries on arbitrary alphabets.
    """

    def __init__(self, sequence: List[int], alphabet_size: int):
        """
        Initialize a wavelet tree for the given sequence.

        Args:
            sequence: List of integers representing the sequence
            alphabet_size: Size of the alphabet (max value + 1)
        """
        self.sequence = sequence
        self.alphabet_size = alphabet_size
        self.length = len(sequence)

        # Build the tree
        self.root = self._build_tree(sequence, 0, alphabet_size - 1)

    def _build_tree(self, sequence: List[int], alpha_min: int, alpha_max: int):
        """
        Recursively build the wavelet tree.

        Args:
            sequence: The sequence at this node
            alpha_min: Minimum alphabet value at this node
            alpha_max: Maximum alphabet value at this node

        Returns:
            dict: Node representation
        """
        # Base case: leaf node (only one symbol in the alphabet range)
        if alpha_min == alpha_max:
            return {
                'is_leaf': True,
                'value': alpha_min,
                'length': len(sequence)
            }

        # Internal node
        alpha_mid = (alpha_min + alpha_max) // 2

        # Create bit vector for this level
        bv = BitVector(len(sequence))
        left_sequence = []
        right_sequence = []

        # Partition the sequence
        for i, symbol in enumerate(sequence):
            if symbol <= alpha_mid:
                bv.set_bit(i, False)  # 0 for left child
                left_sequence.append(symbol)
            else:
                bv.set_bit(i, True)   # 1 for right child
                right_sequence.append(symbol)

        # Build index for rank/select operations
        bv.build_index()

        # Recursively build left and right subtrees
        left_child = self._build_tree(left_sequence, alpha_min, alpha_mid)
        right_child = self._build_tree(right_sequence, alpha_mid + 1, alpha_max)

        return {
            'is_leaf': False,
            'bitvector': bv,
            'left': left_child,
            'right': right_child,
            'alpha_min': alpha_min,
            'alpha_max': alpha_max,
            'alpha_mid': alpha_mid
        }

    def access(self, pos: int) -> int:
        """
        Access the symbol at position pos in the sequence.

        Args:
            pos: Position in the sequence (0-indexed)

        Returns:
            int: Symbol at position pos
        """
        if pos < 0 or pos >= self.length:
            raise IndexError(f"Position {pos} out of range")

        # Start at the root
        node = self.root

        # Traverse the tree until we reach a leaf
        while not node['is_leaf']:
            # Check the bit at position pos
            bit = node['bitvector'].get_bit(pos)

            if bit:  # 1 means go right
                # Update position for the right child
                pos = node['bitvector'].rank1(pos) - 1
                node = node['right']
            else:  # 0 means go left
                # Update position for the left child
                pos = node['bitvector'].rank0(pos) - 1
                node = node['left']

        # Return the value at the leaf
        return node['value']

    def rank(self, symbol: int, pos: int) -> int:
        """
        Count the number of occurrences of symbol up to position pos (inclusive).

        Args:
            symbol: Symbol to count
            pos: Position in the sequence (0-indexed)

        Returns:
            int: Number of occurrences of symbol up to position pos
        """
        if pos < 0 or pos >= self.length:
            raise IndexError(f"Position {pos} out of range")

        if symbol < 0 or symbol >= self.alphabet_size:
            raise ValueError(f"Symbol {symbol} out of range")

        # Start at the root
        node = self.root

        # Traverse the tree until we reach a leaf or the symbol is out of range
        while not node['is_leaf']:
            if symbol <= node['alpha_mid']:
                # Symbol is in the left subtree
                if pos < 0:
                    return 0

                # Count 0s up to position pos
                pos = node['bitvector'].rank0(pos) - 1
                node = node['left']
            else:
                # Symbol is in the right subtree
                if pos < 0:
                    return 0

                # Count 1s up to position pos
                pos = node['bitvector'].rank1(pos) - 1
                node = node['right']

        # If we reached a leaf with the target symbol, return the position + 1
        if node['value'] == symbol:
            return pos + 1
        else:
            return 0

    def select(self, symbol: int, rank: int) -> int:
        """
        Find the position of the rank-th occurrence of symbol (0-indexed).

        Args:
            symbol: Symbol to find
            rank: The rank of the occurrence to find (0-indexed)

        Returns:
            int: Position of the rank-th occurrence of symbol
        """
        if rank < 0:
            raise ValueError(f"Rank {rank} out of range")

        if symbol < 0 or symbol >= self.alphabet_size:
            raise ValueError(f"Symbol {symbol} out of range")

        # Start at the leaf node for the symbol
        node = self.root
        pos = rank

        # Find the leaf node for the symbol
        path = []
        while not node['is_leaf']:
            if symbol <= node['alpha_mid']:
                # Symbol is in the left subtree
                path.append(0)  # 0 means we went left
                node = node['left']
            else:
                # Symbol is in the right subtree
                path.append(1)  # 1 means we went right
                node = node['right']

        # Check if we found the right symbol
        if node['value'] != symbol:
            raise ValueError(f"Symbol {symbol} not found")

        # If the rank is out of range, raise an error
        if rank >= node['length']:
            raise ValueError(f"Rank {rank} out of range for symbol {symbol}")

        # Traverse back up the tree
        for i in range(len(path) - 1, -1, -1):
            parent = self.root

            # Navigate to the parent node
            for j in range(i):
                if path[j] == 0:
                    parent = parent['left']
                else:
                    parent = parent['right']

            # Calculate the position in the parent
            if path[i] == 0:
                # We went left, so use select0
                pos = parent['bitvector'].select0(pos)
            else:
                # We went right, so use select1
                pos = parent['bitvector'].select1(pos)

        return pos
