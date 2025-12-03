"""
Benchmark Script - Compare performance across PostgreSQL, MongoDB, and Redis
"""

import time
import random
import json
import copy
from pathlib import Path
from typing import Dict, List
from tabulate import tabulate

import psycopg2
from pymongo import MongoClient
import redis

from generate.test_data_generator import TestDataGenerator


class DatabaseBenchmark:
    """Run performance benchmarks across all storage systems"""

    def __init__(self):
        # Database configurations
        self.pg_config = {
            "host": "localhost",
            "database": "dummyjson_db",
            "user": "etl_user",
            "password": "etl_password",
            "port": 5432,
        }

        self.mongo_config = {
            "host": "localhost",
            "port": 27017,
            "database": "dummyjson_db",
            "user": "etl_user",
            "password": "etl_password",
        }

        self.redis_config = {"host": "localhost", "port": 6379, "db": 0}

        # Connections
        self.pg_conn = None
        self.pg_cursor = None
        self.mongo_client = None
        self.mongo_db = None
        self.redis_client = None

        # Results storage
        self.results = {}

    def connect_all(self) -> None:
        """Connect to all databases"""
        print("\n→ Connecting to databases...")

        # PostgreSQL
        self.pg_conn = psycopg2.connect(**self.pg_config)
        self.pg_cursor = self.pg_conn.cursor()
        print("  ✓ PostgreSQL connected")

        # MongoDB
        connection_string = (
            f"mongodb://{self.mongo_config['user']}:"
            f"{self.mongo_config['password']}@"
            f"{self.mongo_config['host']}:"
            f"{self.mongo_config['port']}/"
        )
        self.mongo_client = MongoClient(
            connection_string, serverSelectionTimeoutMS=5000
        )
        self.mongo_db = self.mongo_client[self.mongo_config["database"]]
        print("  ✓ MongoDB connected")

        # Redis
        self.redis_client = redis.Redis(
            host=self.redis_config["host"],
            port=self.redis_config["port"],
            db=self.redis_config["db"],
            decode_responses=True,
        )
        self.redis_client.ping()
        print("  ✓ Redis connected")

    def disconnect_all(self) -> None:
        """Disconnect from all databases"""
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
        if self.mongo_client:
            self.mongo_client.close()
        if self.redis_client:
            self.redis_client.close()
        print("\n✓ All connections closed")

    def benchmark_insert_10k(self) -> None:
        """Test inserting 10,000 records"""

        print("\n" + "=" * 80)
        print("BENCHMARK 1: INSERT 10,000 RECORDS")
        print("=" * 80)

        # Generate test data if not exists
        test_file = Path("data/test/test_simple_records.json")
        if not test_file.exists():
            print("\n→ Generating test data...")
            generator = TestDataGenerator()
            generator.generate_simple_test_table_data(target_count=10000)

        with open(test_file, "r") as f:
            test_data = json.load(f)

        print(f"\n→ Loaded {len(test_data)} test records")

        # PostgreSQL - single inserts
        pg_single_time = self._benchmark_pg_insert_single(test_data[:1000])

        # PostgreSQL - batch inserts
        pg_batch_time = self._benchmark_pg_insert_batch(test_data)

        # MongoDB
        mongo_time = self._benchmark_mongo_insert(test_data)

        # Redis (for comparison, though it's a cache)
        redis_time = self._benchmark_redis_insert(test_data)

        self.results["insert_10k"] = {
            "PostgreSQL (single)": pg_single_time,
            "PostgreSQL (batch)": pg_batch_time,
            "MongoDB": mongo_time,
            "Redis": redis_time,
        }

    def _benchmark_pg_insert_single(self, data: List[Dict]) -> float:
        """PostgreSQL single insert"""
        print("\n→ PostgreSQL (single inserts)...")

        # Create temp table
        self.pg_cursor.execute(
            """
            DROP TABLE IF EXISTS test_benchmark;
            CREATE TABLE test_benchmark (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                description TEXT,
                value DECIMAL(10, 2),
                status VARCHAR(50),
                category VARCHAR(10),
                created_at TIMESTAMP,
                is_active BOOLEAN,
                priority INTEGER
            );
        """
        )

        start = time.time()
        for record in data:
            self.pg_cursor.execute(
                """
                INSERT INTO test_benchmark 
                (id, name, description, value, status, category, created_at, is_active, priority)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    record["id"],
                    record["name"],
                    record["description"],
                    record["value"],
                    record["status"],
                    record["category"],
                    record["created_at"],
                    record["is_active"],
                    record["priority"],
                ),
            )
        self.pg_conn.commit()
        elapsed = time.time() - start

        print(f"  ✓ Inserted {len(data)} records in {elapsed:.3f}s")
        print(f"  ⚡ {len(data) / elapsed:.0f} records/sec")

        return elapsed

    def _benchmark_pg_insert_batch(self, data: List[Dict]) -> float:
        """PostgreSQL batch insert"""
        print("\n→ PostgreSQL (batch inserts)...")

        from psycopg2.extras import execute_values

        self.pg_cursor.execute("TRUNCATE test_benchmark;")

        start = time.time()

        values = [
            (
                r["id"],
                r["name"],
                r["description"],
                r["value"],
                r["status"],
                r["category"],
                r["created_at"],
                r["is_active"],
                r["priority"],
            )
            for r in data
        ]

        execute_values(
            self.pg_cursor,
            """
            INSERT INTO test_benchmark 
            (id, name, description, value, status, category, created_at, is_active, priority)
            VALUES %s
            """,
            values,
            page_size=1000,
        )
        self.pg_conn.commit()
        elapsed = time.time() - start

        print(f"  ✓ Inserted {len(data)} records in {elapsed:.3f}s")
        print(f"  ⚡ {len(data) / elapsed:.0f} records/sec")

        # Cleanup
        self.pg_cursor.execute("DROP TABLE test_benchmark;")
        self.pg_conn.commit()

        return elapsed

    def _benchmark_mongo_insert(self, data: List[Dict]) -> float:
        """MongoDB batch insert"""
        print("\n→ MongoDB (batch inserts)...")

        collection = self.mongo_db["test_benchmark"]
        collection.drop()

        start = time.time()
        mongo_data = copy.deepcopy(data)
        collection.insert_many(mongo_data, ordered=False)
        elapsed = time.time() - start

        print(f"  ✓ Inserted {len(data)} documents in {elapsed:.3f}s")
        print(f"  ⚡ {len(data) / elapsed:.0f} documents/sec")

        # Cleanup
        collection.drop()

        return elapsed

    def _benchmark_redis_insert(self, data: List[Dict]) -> float:
        """Redis batch insert (pipeline)"""
        print("\n→ Redis (batch pipeline)...")

        # Clear test keys
        for key in self.redis_client.keys("test:*"):
            self.redis_client.delete(key)

        start = time.time()
        pipe = self.redis_client.pipeline()
        for record in data:
            key = f"test:{record['id']}"
            pipe.setex(key, 3600, json.dumps(record))
        pipe.execute()
        elapsed = time.time() - start

        print(f"  ✓ Cached {len(data)} items in {elapsed:.3f}s")
        print(f"  ⚡ {len(data) / elapsed:.0f} items/sec")

        # Cleanup
        for key in self.redis_client.keys("test:*"):
            self.redis_client.delete(key)

        return elapsed

    def benchmark_read_single(self, iterations: int = 1000) -> None:
        """Test reading a single item by ID"""
        print("\n" + "=" * 80)
        print(f"BENCHMARK 2: READ SINGLE ITEM ({iterations} iterations)")
        print("=" * 80)

        # PostgreSQL
        pg_time = self._benchmark_pg_read_single(iterations)

        # MongoDB
        mongo_time = self._benchmark_mongo_read_single(iterations)

        # Redis
        redis_time = self._benchmark_redis_read_single(iterations)

        self.results["read_single"] = {
            "PostgreSQL": pg_time,
            "MongoDB": mongo_time,
            "Redis": redis_time,
        }

    def _benchmark_pg_read_single(self, iterations: int) -> float:
        """PostgreSQL single read"""
        print("\n→ PostgreSQL (SELECT by ID)...")

        start = time.time()
        for _ in range(iterations):
            user_id = random.randint(1, 100)
            self.pg_cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            self.pg_cursor.fetchone()
        elapsed = time.time() - start

        avg_ms = (elapsed / iterations) * 1000
        print(f"  ✓ {iterations} reads in {elapsed:.3f}s")
        print(f"  ⚡ Average: {avg_ms:.2f}ms per query")

        return elapsed

    def _benchmark_mongo_read_single(self, iterations: int) -> float:
        """MongoDB single read"""
        print("\n→ MongoDB (find_one by _id)...")

        collection = self.mongo_db["users"]

        start = time.time()
        for _ in range(iterations):
            user_id = random.randint(1, 100)
            collection.find_one({"_id": user_id})
        elapsed = time.time() - start

        avg_ms = (elapsed / iterations) * 1000
        print(f"  ✓ {iterations} reads in {elapsed:.3f}s")
        print(f"  ⚡ Average: {avg_ms:.2f}ms per query")

        return elapsed

    def _benchmark_redis_read_single(self, iterations: int) -> float:
        """Redis single read"""
        print("\n→ Redis (GET by key)...")

        start = time.time()
        for _ in range(iterations):
            user_id = random.randint(1, 100)
            self.redis_client.get(f"user:{user_id}")
        elapsed = time.time() - start

        avg_ms = (elapsed / iterations) * 1000
        print(f"  ✓ {iterations} reads in {elapsed:.3f}s")
        print(f"  ⚡ Average: {avg_ms:.2f}ms per query")

        return elapsed

    def benchmark_read_filtered(self, iterations: int = 100) -> None:
        """Test reading filtered data"""
        print("\n" + "=" * 80)
        print(f"BENCHMARK 3: READ FILTERED SET ({iterations} iterations)")
        print("=" * 80)

        # PostgreSQL
        pg_time = self._benchmark_pg_read_filtered(iterations)

        # MongoDB
        mongo_time = self._benchmark_mongo_read_filtered(iterations)

        self.results["read_filtered"] = {
            "PostgreSQL": pg_time,
            "MongoDB": mongo_time,
            "Redis": "N/A (cache, not for complex queries)",
        }

    def _benchmark_pg_read_filtered(self, iterations: int) -> float:
        """PostgreSQL filtered query"""
        print("\n→ PostgreSQL (WHERE clause + JOIN)...")

        start = time.time()
        for _ in range(iterations):
            self.pg_cursor.execute(
                """
                SELECT p.*, c.name as category_name
                FROM products p
                JOIN categories c ON p.category_id = c.id
                WHERE p.price > 50 AND p.stock > 10
                LIMIT 20
            """
            )
            self.pg_cursor.fetchall()
        elapsed = time.time() - start

        avg_ms = (elapsed / iterations) * 1000
        print(f"  ✓ {iterations} queries in {elapsed:.3f}s")
        print(f"  ⚡ Average: {avg_ms:.2f}ms per query")

        return elapsed

    def _benchmark_mongo_read_filtered(self, iterations: int) -> float:
        """MongoDB filtered query"""
        print("\n→ MongoDB (find with filter)...")

        collection = self.mongo_db["products"]

        start = time.time()
        for _ in range(iterations):
            list(
                collection.find({"price": {"$gt": 50}, "stock": {"$gt": 10}}).limit(20)
            )
        elapsed = time.time() - start

        avg_ms = (elapsed / iterations) * 1000
        print(f"  ✓ {iterations} queries in {elapsed:.3f}s")
        print(f"  ⚡ Average: {avg_ms:.2f}ms per query")

        return elapsed

    def print_results_table(self) -> None:
        """Print final comparison table"""
        print("\n" + "=" * 80)
        print("PERFORMANCE COMPARISON TABLE")
        print("=" * 80)

        # Prepare data for table
        table_data = []

        # Insert 10k
        if "insert_10k" in self.results:
            insert_data = self.results["insert_10k"]
            table_data.append(
                [
                    "Insert 10k records",
                    f"{insert_data.get('PostgreSQL (single)', 'N/A')}",
                    f"{insert_data.get('PostgreSQL (batch)', 'N/A')}",
                    f"{insert_data.get('MongoDB', 'N/A')}",
                    f"{insert_data.get('Redis', 'N/A')}",
                ]
            )

        # Read single
        if "read_single" in self.results:
            read_data = self.results["read_single"]
            pg_ms = (read_data.get("PostgreSQL", 0) / 1000) * 1000
            mongo_ms = (read_data.get("MongoDB", 0) / 1000) * 1000
            redis_ms = (read_data.get("Redis", 0) / 1000) * 1000

            table_data.append(
                [
                    "Read single item (avg)",
                    f"{pg_ms:.2f} ms",
                    "—",
                    f"{mongo_ms:.2f} ms",
                    f"{redis_ms:.2f} ms",
                ]
            )

        # Read filtered
        if "read_filtered" in self.results:
            filtered_data = self.results["read_filtered"]
            pg_ms = (filtered_data.get("PostgreSQL", 0) / 100) * 1000
            mongo_ms = (filtered_data.get("MongoDB", 0) / 100) * 1000

            table_data.append(
                [
                    "Read filtered set (avg)",
                    f"{pg_ms:.2f} ms",
                    "—",
                    f"{mongo_ms:.2f} ms",
                    "N/A",
                ]
            )

        # Print table
        headers = ["Operation", "PostgreSQL", "PG (batch)", "MongoDB", "Redis"]
        print("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))

        print("\n" + "=" * 80)

    def run_all_benchmarks(self) -> None:
        """Run all benchmarks"""
        print("\n")
        print("╔" + "=" * 78 + "╗")
        print("║" + " " * 20 + "PERFORMANCE BENCHMARK - ALL TESTS" + " " * 25 + "║")
        print("╚" + "=" * 78 + "╝")

        self.connect_all()

        try:
            self.benchmark_insert_10k()
            self.benchmark_read_single(iterations=1000)
            self.benchmark_read_filtered(iterations=100)

            self.print_results_table()

            print("\n✅ All benchmarks completed!")

        finally:
            self.disconnect_all()


def main():
    """Main benchmark runner"""

    try:
        runner = DatabaseBenchmark()
        runner.run_all_benchmarks()

        print("\n" + "=" * 80)
        print("🎉 Benchmark complete!")
        print("=" * 80 + "\n")

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    main()
