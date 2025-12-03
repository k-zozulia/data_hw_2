"""
Benchmark all file formats - compare performance and file sizes
"""

import time
from pathlib import Path
from typing import List, Dict, Any
from tabulate import tabulate

from handlers.csv_handler import CSVHandler
from handlers.json_handler import JSONHandler
from handlers.avro_handler import AvroHandler
from handlers.parquet_handler import ParquetHandler
from generate.test_data_generator import TestDataGenerator


class FormatBenchmark:
    """Compare performance of different file formats"""

    def __init__(self, data_dir: str = "data/test/"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.results = {}

    def benchmark_csv(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Benchmark CSV format"""
        print("\n→ Benchmarking CSV...")

        handler = CSVHandler()
        filepath = self.data_dir / "benchmark_users.csv"

        # Write
        start = time.time()
        handler.write(data, filepath)
        write_time = time.time() - start

        # Read
        start = time.time()
        read_data = handler.read(filepath)
        read_time = time.time() - start

        # File size
        size_mb = handler.get_file_size_mb(filepath)

        # Rows and columns
        rows = handler.count_rows(read_data)
        cols = handler.count_columns(read_data)

        print(
            f"  ✓ CSV: {size_mb:.2f} MB, write: {write_time:.3f}s, read: {read_time:.3f}s"
        )

        return {
            "format": "CSV",
            "size_mb": round(size_mb, 2),
            "rows": rows,
            "columns": cols,
            "write_time": round(write_time, 3),
            "read_time": round(read_time, 3),
        }

    def benchmark_json(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Benchmark JSON format"""
        print("\n→ Benchmarking JSON...")

        handler = JSONHandler()
        filepath = self.data_dir / "benchmark_users.json"

        # Write
        start = time.time()
        handler.write(data, filepath)
        write_time = time.time() - start

        # Read
        start = time.time()
        read_data = handler.read(filepath)
        read_time = time.time() - start

        # File size
        size_mb = handler.get_file_size_mb(filepath)

        # Rows and columns
        rows = handler.count_rows(read_data)
        cols = handler.count_columns(read_data)

        print(
            f"  ✓ JSON: {size_mb:.2f} MB, write: {write_time:.3f}s, read: {read_time:.3f}s"
        )

        return {
            "format": "JSON",
            "size_mb": round(size_mb, 2),
            "rows": rows,
            "columns": cols,
            "write_time": round(write_time, 3),
            "read_time": round(read_time, 3),
        }

    def benchmark_avro(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Benchmark Avro format"""

        print("\n→ Benchmarking Avro...")

        # Define schema based on first record
        if data:
            handler = AvroHandler()
            schema = handler.infer_schema(data[0], "BenchmarkUser")
        else:
            return None

        filepath = self.data_dir / "benchmark_users.avro"

        # Write
        start = time.time()
        handler.write(data, filepath, schema)
        write_time = time.time() - start

        # Read
        start = time.time()
        read_data = handler.read(filepath)
        read_time = time.time() - start

        # File size
        size_mb = handler.get_file_size_mb(filepath)

        # Rows and columns
        rows = handler.count_rows(read_data)
        cols = handler.count_columns(read_data)

        print(
            f"  ✓ Avro: {size_mb:.2f} MB, write: {write_time:.3f}s, read: {read_time:.3f}s"
        )

        return {
            "format": "Avro",
            "size_mb": round(size_mb, 2),
            "rows": rows,
            "columns": cols,
            "write_time": round(write_time, 3),
            "read_time": round(read_time, 3),
        }

    def benchmark_parquet(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Benchmark Parquet format"""

        print("\n→ Benchmarking Parquet...")

        handler = ParquetHandler()
        filepath = self.data_dir / "benchmark_users.parquet"

        # Write
        start = time.time()
        handler.write(data, filepath, compression="snappy")
        write_time = time.time() - start

        # Read
        start = time.time()
        read_data = handler.read(filepath)
        read_time = time.time() - start

        # File size
        size_mb = handler.get_file_size_mb(filepath)

        # Rows and columns
        rows = handler.count_rows(read_data)
        cols = handler.count_columns(read_data)

        print(
            f"  ✓ Parquet: {size_mb:.2f} MB, write: {write_time:.3f}s, read: {read_time:.3f}s"
        )

        return {
            "format": "Parquet",
            "size_mb": round(size_mb, 2),
            "rows": rows,
            "columns": cols,
            "write_time": round(write_time, 3),
            "read_time": round(read_time, 3),
        }

    def get_test_data(self) -> List[Dict[str, Any]]:
        """Get test data"""

        handler = JSONHandler()
        generator = TestDataGenerator()

        test_path = self.data_dir / "test_users.json"

        if not test_path.exists():
            generator.generate_test_users()

        return handler.read(test_path)

    def run_all_benchmarks(self, num_records: int = 10000) -> None:
        """Run benchmarks for all formats"""

        print("\n" + "=" * 80)
        print("FILE FORMAT BENCHMARK")
        print("=" * 80)

        # Get test data
        test_data = self.get_test_data()

        # Run benchmarks
        results = []

        csv_result = self.benchmark_csv(test_data)
        if csv_result:
            results.append(csv_result)

        json_result = self.benchmark_json(test_data)
        if json_result:
            results.append(json_result)

        avro_result = self.benchmark_avro(test_data)
        if avro_result:
            results.append(avro_result)

        parquet_result = self.benchmark_parquet(test_data)
        if parquet_result:
            results.append(parquet_result)

        # Print results table
        self.print_results_table(results)

        # Store results
        self.results = results

    def print_results_table(self, results: List[Dict[str, Any]]) -> None:
        """Print comparison table"""

        print("\n" + "=" * 80)
        print("PERFORMANCE COMPARISON")
        print("=" * 80)

        if not results:
            print("⚠ No results to display")
            return

        # Prepare table data
        table_data = []
        for r in results:
            table_data.append(
                [
                    r["format"],
                    r["size_mb"],
                    r["rows"],
                    r["columns"],
                    r["read_time"],
                    r["write_time"],
                ]
            )

        headers = [
            "Format",
            "Size (MB)",
            "Rows",
            "Columns",
            "Read time (s)",
            "Write time (s)",
        ]
        print("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))

        # Find best performers
        print("\n" + "=" * 80)
        print("BEST PERFORMERS")
        print("=" * 80)

        smallest = min(results, key=lambda x: x["size_mb"])
        fastest_read = min(results, key=lambda x: x["read_time"])
        fastest_write = min(results, key=lambda x: x["write_time"])

        print(
            f"\n🏆 Smallest file:     {smallest['format']} ({smallest['size_mb']} MB)"
        )
        print(
            f"🏆 Fastest read:      {fastest_read['format']} ({fastest_read['read_time']}s)"
        )
        print(
            f"🏆 Fastest write:     {fastest_write['format']} ({fastest_write['write_time']}s)"
        )

        print("\n" + "=" * 80)


def main():
    """Run file format benchmarks"""

    benchmark = FormatBenchmark()

    # Run with 10k records
    benchmark.run_all_benchmarks(num_records=10000)

    print("\n✅ FILE FORMAT BENCHMARK COMPLETE!")


if __name__ == "__main__":
    main()
