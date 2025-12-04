"""
Export Fact Tables to Different Formats
Uses existing file handlers for Parquet and JSON export
"""

import psycopg2
from decimal import Decimal
from pathlib import Path
from typing import List, Dict, Any
from configs.config import DatabaseConfig, EXPORT_DIR
from handlers.json_handler import JSONHandler
from handlers.parquet_handler import ParquetHandler


class FactTableExporter:
    """Export fact tables from PostgreSQL to various formats"""

    def __init__(self, db_config: Dict[str, str], output_dir: str = EXPORT_DIR):
        self.db_config = db_config
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.conn = None
        self.cursor = None

        # Initialize handlers
        self.json_handler = JSONHandler()
        self.parquet_handler = ParquetHandler()

    def connect(self) -> None:
        """Connect to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ Connected to PostgreSQL")
        except Exception as e:
            print(f"✗ Connection error: {e}")
            raise

    def disconnect(self) -> None:
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Connection closed")

    def fetch_fact_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Fetch entire fact table as list of dicts"""
        print(f"\n→ Fetching {table_name}...")

        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)

        # Get column names
        columns = [desc[0] for desc in self.cursor.description]

        # Fetch all rows
        rows = self.cursor.fetchall()

        # Convert to list of dicts
        data = []
        for row in rows:
            record = {}
            for i, col_name in enumerate(columns):
                value = row[i]

                # Convert date/datetime
                if hasattr(value, "isoformat"):
                    value = value.isoformat()

                # Convert Decimal
                if isinstance(value, Decimal):
                    value = float(value)  # or str(value)

                record[col_name] = value
            data.append(record)

        print(f"  ✓ Fetched {len(data)} records")
        return data

    def export_fact_table(self) -> None:
        """Export star_fact_orders to JSON and Parquet"""

        # Fetch data
        data = self.fetch_fact_table("star_fact_orders")

        if not data:
            print("⚠ No data to export")
            return

        json_file = self.output_dir / "star_fact_orders.json"
        self.json_handler.write(data, json_file)
        json_size = self.json_handler.get_file_size_mb(json_file)

        parquet_file = self.output_dir / "star_fact_orders.parquet"
        self.parquet_handler.write(data, parquet_file, compression="snappy")
        parquet_size = self.parquet_handler.get_file_size_mb(parquet_file)


def main():
    """Main export function"""

    db_config = DatabaseConfig.postgres()

    exporter = FactTableExporter(db_config, output_dir=EXPORT_DIR)

    try:
        exporter.connect()
        exporter.export_fact_table()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

    finally:
        exporter.disconnect()

    print("\n🎉 Export complete!")


if __name__ == "__main__":
    main()
