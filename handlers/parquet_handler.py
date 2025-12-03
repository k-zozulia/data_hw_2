"""
Parquet Handler - read, write, filter, aggregate Parquet files

"""

from typing import List, Dict, Any, Iterator
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from configs.config import TEST_DIR
from .base_handler import BaseFileHandler


class ParquetHandler(BaseFileHandler):
    """Handler for Parquet files"""

    def read(self, filepath: Path = None) -> List[Dict[str, Any]]:
        """Read entire Parquet file"""

        path = filepath if filepath else self.filepath

        if not path or not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        table = pq.read_table(path)
        data = table.to_pylist()

        return data

    def write(
        self,
        data: List[Dict[str, Any]],
        filepath: Path = None,
        compression: str = "snappy",
    ) -> None:
        """
        Write data to Parquet file

        Compression options: 'snappy', 'gzip', 'brotli', 'lz4', 'zstd', None
        """

        path = filepath if filepath else self.filepath

        if not data:
            print(" No data to write")
            return

        path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(data)
        pq.write_table(table, path, compression=compression)

    def read_chunks(
        self, filepath: Path = None, chunk_size: int = 1000
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Read Parquet file in chunks using ParquetFile.
        This is more memory-efficient for large files
        """

        path = filepath if filepath else self.filepath

        if not path or not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        parquet_file = pq.ParquetFile(path)

        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            chunk_data = batch.to_pylist()
            yield chunk_data

    def read_with_filter(
        self, filepath: Path = None, filters: List = None
    ) -> List[Dict[str, Any]]:
        """
        Read Parquet with predicate pushdown (efficient filtering)

        Example filters:
            [('price', '>', 100), ('stock', '>', 0)]
        """

        path = filepath if filepath else self.filepath

        if not path or not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        table = pq.read_table(path, filters=filters)

        return table.to_pylist()

    def get_metadata(self, filepath: Path = None) -> Dict[str, Any]:
        """Get Parquet file metadata"""

        path = filepath if filepath else self.filepath

        if not path or not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata

        return {
            "num_rows": metadata.num_rows,
            "num_columns": metadata.num_columns,
            "num_row_groups": metadata.num_row_groups,
            "serialized_size": metadata.serialized_size,
            "schema": str(parquet_file.schema),
        }


def main():
    """Test Parquet handler"""

    print("=" * 80)
    print("PARQUET HANDLER - TESTING")
    print("=" * 80)

    # Create test data
    test_data = [
        {
            "id": 1,
            "name": "Product A",
            "price": 100.5,
            "stock": 50,
            "active": True,
            "category": "Electronics",
        },
        {
            "id": 2,
            "name": "Product B",
            "price": 200.75,
            "stock": 30,
            "active": True,
            "category": "Electronics",
        },
        {
            "id": 3,
            "name": "Product C",
            "price": 50.25,
            "stock": 100,
            "active": False,
            "category": "Home",
        },
        {
            "id": 4,
            "name": "Product D",
            "price": 150.0,
            "stock": 20,
            "active": True,
            "category": "Electronics",
        },
        {
            "id": 5,
            "name": "Product E",
            "price": 75.5,
            "stock": 60,
            "active": True,
            "category": "Home",
        },
    ]

    handler = ParquetHandler()

    # Test write with different compressions
    compressions = ["snappy", "gzip", "brotli", None]

    print("\n→ Writing test data with different compressions...")
    for comp in compressions:
        test_file = Path(f"{TEST_DIR}/test_products_{comp or 'none'}.parquet")
        handler.write(test_data, test_file, compression=comp)
        size_mb = handler.get_file_size_mb(test_file)
        print(f"  ✓ {comp or 'none':10s}: {size_mb:.4f} MB")

    # Use snappy for further tests
    test_file = Path(TEST_DIR / "test_products_snappy.parquet")

    # Test read
    print("\n→ Reading Parquet...")
    data = handler.read(test_file)
    print(f"  ✓ Read {len(data)} records")
    print(f"  First record: {data[0]}")

    # Test metadata
    print("\n→ Reading metadata...")
    metadata = handler.get_metadata(test_file)
    print(f"  • Rows:       {metadata['num_rows']}")
    print(f"  • Columns:    {metadata['num_columns']}")
    print(f"  • Row groups: {metadata['num_row_groups']}")
    print(f"  • Size:       {metadata['serialized_size']} bytes")

    # Test filter
    print("\n→ Filtering (price > 100)...")
    filtered = handler.filter(data, lambda x: x["price"] > 100)
    print(f"  ✓ Found {len(filtered)} records")
    for item in filtered:
        print(f"    • {item['name']}: ${item['price']}")

    # Test predicate pushdown (efficient filtering)
    print("\n→ Predicate pushdown (price > 100 AND stock > 20)...")
    filtered_efficient = handler.read_with_filter(
        test_file, filters=[("price", ">", 100), ("stock", ">", 20)]
    )
    print(f"  ✓ Found {len(filtered_efficient)} records")
    for item in filtered_efficient:
        print(f"    • {item['name']}: ${item['price']}, stock: {item['stock']}")

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

    # Test streaming
    print("\n→ Streaming (chunk_size=2)...")
    chunk_count = 0
    for chunk in handler.read_chunks(test_file, chunk_size=2):
        chunk_count += 1
        print(f"  Chunk {chunk_count}: {len(chunk)} records")

    print("\n✅ PARQUET HANDLER TEST COMPLETE!")


if __name__ == "__main__":
    main()
