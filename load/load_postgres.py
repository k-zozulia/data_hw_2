"""
Load module - loading data into PostgreSQL
"""

import json
import time
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from typing import List, Dict, Any


class PostgresDataLoader:
    """Loads normalized data into PostgreSQL"""

    def __init__(
        self, db_config: Dict[str, str], data_dir: str = "data/processed"
    ) -> None:
        self.db_config = db_config
        self.data_dir = Path(data_dir)
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                host=self.db_config["host"],
                port=self.db_config.get("port", 5432),
            )
            self.cursor = self.conn.cursor()
            print("✓ Connected to PostgreSQL")

        except Exception as e:
            print(f"✗ Database connection error: {e}")
            raise

    def disconnect(self) -> None:
        """Close connection to PostgreSQL database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

        print("✓ Connection closed")

    def create_schema(self) -> None:
        """Create tables schema from SQL file"""

        print("\n→ Creating sql schema...")

        sql_file = Path("sql/create_tables_3nf.sql")
        if not sql_file.exists():
            print(f"✗ SQL file not found: {sql_file}")
            return

        with open(sql_file, "r") as f:
            sql = f.read()

        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("  ✓ SQL schema created successfully!")
        except Exception as e:
            self.conn.rollback()
            print(f"  ✗ Error creating tables: {e}")
            raise

    def load_all_data(self) -> Dict[str, float]:
        """Load all data and measure execution time"""

        print("Load all data...")

        timing_results = {}

        # Order matters – parent tables first
        load_order = [
            "addresses",
            "banks",
            "categories",
            "companies",
            "users",
            "products",
            "product_tags",
            "product_images",
            "reviews",
            "orders",
            "order_items",
        ]

        for table_name in load_order:
            print(f"\n→ Loading {table_name}...")
            data = self._load_json(f"{table_name}.json")

            if not data:
                print(f"  ⚠ No data found for {table_name}")
                continue

            if len(data) <= 10:
                insert_time = self._insert_one_by_one(table_name, data)
                method = "one-by-one"
            else:
                insert_time = self._insert_batch(table_name, data)
                method = "batch"

            timing_results[table_name] = {
                "records": len(data),
                "time": insert_time,
                "method": method,
            }

            print(f"  ✓ {len(data)} records in {insert_time:.3f}s ({method})")

        return timing_results

    def _insert_one_by_one(self, table_name: str, data: List[Dict]) -> float:
        """Insert records one at a time (single INSERT calls)"""

        if not data:
            return 0.0

        start_time = time.time()
        try:
            for record in data:
                columns = list(record.keys())
                values = [record[col] for col in columns]

                placeholders = ", ".join(["%s"] * len(columns))
                columns_str = ", ".join(columns)

                query = (
                    f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                )

                self.cursor.execute(query, values)

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            print(f"  ✗ Insert error in {table_name}: {e}")
            raise

        return time.time() - start_time

    def _insert_batch(
        self, table_name: str, data: List[Dict], batch_size: int = 100
    ) -> float:
        """Batch insert using execute_values (many rows at once)"""

        if not data:
            return 0.0

        start_time = time.time()

        try:
            columns = list(data[0].keys())
            columns_str = ", ".join(columns)

            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                values = [[record[col] for col in columns] for record in batch]

                query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
                execute_values(self.cursor, query, values)

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            print(f"  ✗ Batch insert error in {table_name}: {e}")
            raise

        return time.time() - start_time

    def compare_insert_methods(self, table_name: str = "test_comparison") -> None:
        """Compare speed of INSERT methods"""

        print("Compare insert methods...")

        data = self._load_json("product_tags.json")
        if not data or len(data) < 100:
            print("⚠ Not enough data for comparison")
            return

        test_data = data[:1000]

        print(f"\nTesting {len(test_data)} records...")

        self.cursor.execute(
            f"""
                    CREATE TEMP TABLE {table_name} (
                        id INTEGER,
                        product_id INTEGER,
                        tag VARCHAR(100)
                    )
                """
        )
        print("\n→ Method 1: Single-row INSERT")
        self.cursor.execute(f"TRUNCATE {table_name}")
        time_one = self._insert_one_by_one(table_name, test_data)
        print(f"  ⏱ Time: {time_one:.3f}s")

        print("\n→ Method 2: Batch INSERT (100 rows)")
        self.cursor.execute(f"TRUNCATE {table_name}")
        time_batch_100 = self._insert_batch(table_name, test_data, batch_size=100)
        print(f"  ⏱ Time: {time_batch_100:.3f}s")

        print("\n→ Method 3: Batch INSERT (1000 rows)")
        self.cursor.execute(f"TRUNCATE {table_name}")
        time_batch_1000 = self._insert_batch(table_name, test_data, batch_size=1000)
        print(f"  ⏱ Time: {time_batch_1000:.3f}s")

        print(f"One-by-one:       {time_one:.3f}s (baseline)")
        print(
            f"Batch 100:        {time_batch_100:.3f}s ({time_one / time_batch_100:.1f}x faster)"
        )
        print(
            f"Batch 1000:       {time_batch_1000:.3f}s ({time_one / time_batch_1000:.1f}x faster)"
        )

        self.cursor.execute(f"DROP TABLE {table_name}")
        self.conn.commit()

    def _load_json(self, filename: str) -> List[Dict]:
        """Load JSON file"""
        file_path = self.data_dir / filename

        if not file_path.exists():
            print(f"  ⚠ File not found: {file_path}")
            return []

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_stats(self) -> None:
        """Print table statistics"""

        print("Database statistics...")

        tables = [
            "addresses",
            "banks",
            "companies",
            "categories",
            "users",
            "products",
            "product_tags",
            "product_images",
            "reviews",
            "orders",
            "order_items",
        ]

        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.cursor.fetchone()[0]
            print(f"  • {table:20s}: {count:6d} records")


def main():
    """Main function for testing"""

    print("=" * 80)
    print("ETL LOAD MODULE - TESTING")
    print("=" * 80)

    db_config = {
        "host": "localhost",
        "database": "dummyjson_db",
        "user": "etl_user",
        "password": "etl_password",
        "port": 5432,
    }

    loader = PostgresDataLoader(db_config, data_dir="data/processed")

    try:
        loader.connect()
        loader.create_schema()
        loader.compare_insert_methods()
        timing_results = loader.load_all_data()
        loader.get_stats()

        print("\n" + "=" * 80)
        print("⏱ LOADING TIME BY TABLE")
        print("=" * 80)
        for table, stats in timing_results.items():
            print(
                f"{table:20s}: {stats['records']:6d} records in {stats['time']:6.3f}s ({stats['method']})"
            )
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        raise

    finally:
        loader.disconnect()

    print("\n✅ LOAD MODULE COMPLETED!")


if __name__ == "__main__":
    main()
