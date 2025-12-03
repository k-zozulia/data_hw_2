"""
CSV Handler - read, write, filter, aggregate CSV files
"""

import csv
from typing import List, Dict, Any, Iterator
from pathlib import Path

from .base_handler import BaseFileHandler


class CSVHandler(BaseFileHandler):
    """Handler for CSV files"""

    def read(self, file_path: Path = None) -> List[Dict[str, Any]]:
        """Read entire CSV file"""

        if not file_path or not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        data = []

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                converted_row = self._convert_types(row)
                data.append(converted_row)

        return data

    def write(self, data: List[Dict[str, Any]], filepath: Path = None) -> None:
        """Write data to CSV file"""

        path = filepath if filepath else self.filepath

        if not data:
            print("No data to write")
            return

        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w", newline="", encoding="utf-8") as f:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    def read_chunks(
        self, filepath: Path = None, chunk_size: int = 1000
    ) -> Iterator[List[Dict[str, Any]]]:
        """Read CSV file in chunks (streaming)"""

        path = filepath if filepath else self.filepath

        if not path or not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            chunk = []
            for row in reader:
                converted_row = self._convert_types(row)
                chunk.append(converted_row)

                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []

            if chunk:
                yield chunk

    def _convert_types(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Convert string values to appropriate types"""
        converted = {}

        for key, value in row.items():
            if value == "" or value is None:
                converted[key] = None
                continue

            # Try to convert to int
            try:
                converted[key] = int(value)
                continue
            except (ValueError, TypeError):
                pass

            # Try to convert to float
            try:
                converted[key] = float(value)
                continue
            except (ValueError, TypeError):
                pass

            # Keep as string
            converted[key] = value

        return converted


def main():
    """Test CSV handler"""
    print("=" * 80)
    print("CSV HANDLER - TESTING")
    print("=" * 80)

    # Create test data
    test_data = [
        {"id": 1, "name": "Product A", "price": 100.50, "stock": 50},
        {"id": 2, "name": "Product B", "price": 200.75, "stock": 30},
        {"id": 3, "name": "Product C", "price": 50.25, "stock": 100},
        {"id": 4, "name": "Product D", "price": 150.00, "stock": 20},
    ]

    handler = CSVHandler()

    # Test write
    print("\n→ Writing test data to CSV...")
    test_file = Path("data/test/test_products.csv")
    handler.write(test_data, test_file)
    print(f"  ✓ Written to {test_file}")

    # Test read
    print("\n→ Reading CSV...")
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
    print("\n→ Aggregations on 'price' column...")
    print(f"  • Sum:   ${handler.aggregate(data, 'price', 'sum'):.2f}")
    print(f"  • Count: {handler.aggregate(data, 'price', 'count')}")
    print(f"  • Min:   ${handler.aggregate(data, 'price', 'min'):.2f}")
    print(f"  • Max:   ${handler.aggregate(data, 'price', 'max'):.2f}")
    print(f"  • Avg:   ${handler.aggregate(data, 'price', 'avg'):.2f}")

    # Test sorting
    print("\n→ Sorting by price (DESC)...")
    sorted_data = handler.sort(data, "price", reverse=True)
    for item in sorted_data:
        print(f"  • {item['name']}: ${item['price']}")

    # Test streaming (chunks)
    print("\n→ Streaming (chunk_size=2)...")
    chunk_count = 0
    for chunk in handler.read_chunks(test_file, chunk_size=2):
        chunk_count += 1
        print(f"  Chunk {chunk_count}: {len(chunk)} records")

    print("\n✅ CSV HANDLER TEST COMPLETE!")


if __name__ == "__main__":
    main()
