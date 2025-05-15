from setuptools import setup, find_packages

setup(
    name="lattice-db",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "flatbuffers",
        "zstandard",
        # We'll need to find or implement a Python library for succinct data structures
    ],
    author="Mehdi",
    description="A lightweight, efficient, and queryable file-based database system",
    keywords="database, flatbuffers, succinct, compression",
    python_requires=">=3.7",
)
