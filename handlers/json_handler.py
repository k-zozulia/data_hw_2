"""
JSON Handler - read, write, filter, aggregate JSON files
"""

import json
from typing import List, Dict, Any, Iterator
from pathlib import Path

from .base_handler import BaseFileHandler


class JSONHandler(BaseFileHandler):
    """Handler for JSON files"""

    def read(self, filepath: Path = None) -> List[Dict[str, Any]]:
        """Read entire JSON file"""

        path = filepath if filepath else self.filepath

        if not path or not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Ensure data is a list
        if isinstance(data, dict):
            data = [data]

        return data

    def write(self, data: List[Dict[str, Any]], filepath: Path = None) -> None:
        """Write data to JSON file"""

        path = filepath if filepath else self.filepath

        if not data:
            print("⚠ No data to write")
            return

        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def read_chunks(
        self, filepath: Path = None, chunk_size: int = 1000
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Read JSON file in chunks

        Note: JSON doesn't support native streaming, so we load the entire file
        and then yield chunks from memory.
        """
        path = filepath if filepath else self.filepath

        if not path or not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        # Load entire file
        data = self.read(path)

        # Yield chunks
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]

    def read_streaming_jsonl(
        self, filepath: Path = None, chunk_size: int = 1000
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Read JSONL (JSON Lines) file in true streaming mode
        Each line is a separate JSON object
        """
        path = filepath if filepath else self.filepath

        if not path or not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        chunk = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        chunk.append(record)

                        if len(chunk) >= chunk_size:
                            yield chunk
                            chunk = []
                    except json.JSONDecodeError:
                        print(f" Skipping invalid JSON line: {line[:50]}...")

        # Yield remaining records
        if chunk:
            yield chunk

    def write_jsonl(self, data: List[Dict[str, Any]], filepath: Path = None) -> None:
        """
        Write data to JSONL (JSON Lines) format
        Each record on a separate line
        """
        path = filepath if filepath else self.filepath

        if not data:
            print("⚠ No data to write")
            return

        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            for record in data:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    """Test JSON handler"""
    print("=" * 80)
    print("JSON HANDLER - TESTING")
    print("=" * 80)

    # Create test data
    test_data = [
        {
            "id": 1,
            "name": "Product A",
            "price": 100.50,
            "stock": 50,
            "tags": ["electronics", "popular"],
        },
        {
            "id": 2,
            "name": "Product B",
            "price": 200.75,
            "stock": 30,
            "tags": ["electronics"],
        },
        {
            "id": 3,
            "name": "Product C",
            "price": 50.25,
            "stock": 100,
            "tags": ["home", "cheap"],
        },
        {
            "id": 4,
            "name": "Product D",
            "price": 150.00,
            "stock": 20,
            "tags": ["popular"],
        },
    ]

    handler = JSONHandler()

    # Test write (regular JSON)
    print("\n→ Writing test data to JSON...")
    test_file = Path("data/test/test_products.json")
    handler.write(test_data, test_file)
    print(f"  ✓ Written to {test_file}")
    print(f"  File size: {handler.get_file_size_mb(test_file):.4f} MB")

    # Test write (JSONL)
    print("\n→ Writing test data to JSONL...")
    test_jsonl = Path("data/test/test_products.jsonl")
    handler.write_jsonl(test_data, test_jsonl)
    print(f"  ✓ Written to {test_jsonl}")

    # Test read
    print("\n→ Reading JSON...")
    data = handler.read(test_file)
    print(f"  ✓ Read {len(data)} records")
    print(f"  First record: {data[0]}")

    # Test filter
    print("\n→ Filtering (price > 100)...")
    filtered = handler.filter(data, lambda x: x["price"] > 100)
    print(f"  ✓ Found {len(filtered)} records")
    for item in filtered:
        print(f"    • {item['name']}: ${item['price']}")

    # Test aggregations
    print("\n→ Aggregations on 'stock' column...")
    print(f"  • Sum:   {handler.aggregate(data, 'stock', 'sum')}")
    print(f"  • Count: {handler.aggregate(data, 'stock', 'count')}")
    print(f"  • Min:   {handler.aggregate(data, 'stock', 'min')}")
    print(f"  • Max:   {handler.aggregate(data, 'stock', 'max')}")
    print(f"  • Avg:   {handler.aggregate(data, 'stock', 'avg'):.2f}")

    # Test sorting
    print("\n→ Sorting by price (DESC)...")
    sorted_data = handler.sort(data, "price", reverse=True)
    for item in sorted_data:
        print(f"  • {item['name']}: ${item['price']}")

    # Test streaming (regular JSON chunks)
    print("\n→ Streaming JSON (chunk_size=2)...")
    chunk_count = 0
    for chunk in handler.read_chunks(test_file, chunk_size=2):
        chunk_count += 1
        print(f"  Chunk {chunk_count}: {len(chunk)} records")

    # Test streaming (JSONL)
    print("\n→ Streaming JSONL (chunk_size=2)...")
    chunk_count = 0
    for chunk in handler.read_streaming_jsonl(test_jsonl, chunk_size=2):
        chunk_count += 1
        print(f"  Chunk {chunk_count}: {len(chunk)} records")

    print("\n✅ JSON HANDLER TEST COMPLETE!")


if __name__ == "__main__":
    main()
