"""
Load module - loading data into Star Schema
"""

import json
import time
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values


class StarSchemaLoader:
    """Loads data into Star Schema (Fact + Dimensions)"""

    def __init__(self, db_config: Dict[str, str], data_dir: str = "data/processed"):
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
            print("✓ Connected to PostgreSQL (Star Schema)")

        except Exception as e:
            print(f"✗ Database connection error: {e}")
            raise

    def disconnect(self) -> None:
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Connection closed")

    def create_schema(self) -> None:
        """Create Star Schema tables from SQL file"""
        print("\n→ Creating Star Schema tables...")

        sql_file = Path("sql/create_tables_star.sql")
        if not sql_file.exists():
            print(f"✗ SQL file not found: {sql_file}")
            return

        with open(sql_file, "r") as f:
            sql = f.read()

        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("  ✓ Star Schema tables created successfully!")
        except Exception as e:
            self.conn.rollback()
            print(f"  ✗ Error creating tables: {e}")
            raise

    def load_dim_users(self) -> Dict[str, Any]:
        """Load dimension: users"""
        print("\n→ Loading star_dim_users...")

        users = self._load_json("users.json")

        if not users:
            return {"records": 0, "time": 0}

        start_time = time.time()

        # Transform to dimension format
        dim_users = []
        for user in users:
            dim_users.append(
                {
                    "user_id": user["id"],
                    "username": user.get("username"),
                    "email": user.get("email"),
                    "first_name": user.get("first_name"),
                    "last_name": user.get("last_name"),
                    "full_name": f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
                    "age": user.get("age"),
                    "gender": user.get("gender"),
                    "phone": user.get("phone"),
                    "birth_date": user.get("birth_date"),
                    "blood_group": user.get("blood_group"),
                    "university": user.get("university"),
                    "role": user.get("role", "user"),
                }
            )

        # Batch insert
        columns = list(dim_users[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dim_users]

        query = f"INSERT INTO star_dim_users ({columns_str}) VALUES %s ON CONFLICT (user_id) DO NOTHING"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(dim_users)} users in {elapsed:.3f}s")

        return {"records": len(dim_users), "time": elapsed}

    def load_dim_products(self) -> Dict[str, Any]:
        """Load dimension: products"""
        print("\n→ Loading star_dim_products...")

        products = self._load_json("products.json")
        categories = {c["id"]: c for c in self._load_json("categories.json")}

        if not products:
            return {"records": 0, "time": 0}

        start_time = time.time()

        # Transform to dimension format
        dim_products = []
        for product in products:
            category_id = product.get("category_id")
            category_name = categories.get(category_id, {}).get("name", "Unknown")

            dim_products.append(
                {
                    "product_id": product["id"],
                    "title": product.get("title"),
                    "description": product.get("description"),
                    "category": category_name,
                    "brand": product.get("brand"),
                    "sku": product.get("sku"),
                    "price": product.get("price"),
                    "discount_percentage": product.get("discount_percentage"),
                    "rating": product.get("rating"),
                    "stock": product.get("stock"),
                    "weight": product.get("weight"),
                    "warranty_info": product.get("warranty_info"),
                    "availability_status": product.get("availability_status"),
                }
            )

        # Batch insert
        columns = list(dim_products[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dim_products]

        query = f"INSERT INTO star_dim_products ({columns_str}) VALUES %s ON CONFLICT (product_id) DO NOTHING"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(dim_products)} products in {elapsed:.3f}s")

        return {"records": len(dim_products), "time": elapsed}

    def load_dim_date(
        self, start_year: int = 2020, end_year: int = 2026
    ) -> Dict[str, Any]:
        """Generate and load date dimension"""
        print("\n→ Generating and loading star_dim_date...")

        start_time = time.time()

        dates = self._generate_date_dimension(start_year, end_year)

        # Batch insert
        columns = list(dates[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dates]

        query = f"INSERT INTO star_dim_date ({columns_str}) VALUES %s ON CONFLICT (full_date) DO NOTHING"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Generated and loaded {len(dates)} dates in {elapsed:.3f}s")

        return {"records": len(dates), "time": elapsed}

    def _generate_date_dimension(self, start_year: int, end_year: int) -> List[Dict]:
        """Generate date dimension with all date attributes"""

        dates = []
        current = datetime(start_year, 1, 1)
        date_id = 1

        # US holidays (simplified)
        holidays = {
            (1, 1): "New Year's Day",
            (7, 4): "Independence Day",
            (12, 25): "Christmas Day",
        }

        while current.year <= end_year:
            month_names = [
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            ]
            day_names = [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]

            is_holiday = (current.month, current.day) in holidays

            # Fiscal year: starts in October
            fiscal_year = current.year if current.month < 10 else current.year + 1
            fiscal_quarter = ((current.month - 10) % 12) // 3 + 1

            dates.append(
                {
                    "date_id": date_id,
                    "full_date": current.date(),
                    "year": current.year,
                    "quarter": (current.month - 1) // 3 + 1,
                    "month": current.month,
                    "month_name": month_names[current.month - 1],
                    "day": current.day,
                    "day_of_week": current.weekday() + 1,  # 1=Monday, 7=Sunday
                    "day_name": day_names[current.weekday()],
                    "week_of_year": current.isocalendar()[1],
                    "is_weekend": current.weekday() >= 5,
                    "is_holiday": is_holiday,
                    "fiscal_year": fiscal_year,
                    "fiscal_quarter": fiscal_quarter,
                }
            )

            current += timedelta(days=1)
            date_id += 1

        return dates

    def load_dim_location(self) -> Dict[str, Any]:
        """Load dimension: location"""
        print("\n→ Loading star_dim_location...")

        addresses = self._load_json("addresses.json")

        if not addresses:
            return {"records": 0, "time": 0}

        start_time = time.time()

        # Determine region based on state
        regions = {
            "Northeast": ["NY", "PA", "NJ", "MA", "CT", "RI", "VT", "NH", "ME"],
            "Southeast": [
                "FL",
                "GA",
                "SC",
                "NC",
                "VA",
                "WV",
                "KY",
                "TN",
                "AL",
                "MS",
                "AR",
                "LA",
            ],
            "Midwest": [
                "OH",
                "MI",
                "IN",
                "IL",
                "WI",
                "MN",
                "IA",
                "MO",
                "ND",
                "SD",
                "NE",
                "KS",
            ],
            "Southwest": ["TX", "OK", "NM", "AZ"],
            "West": ["CA", "NV", "UT", "CO", "WY", "MT", "ID", "WA", "OR", "AK", "HI"],
        }

        def get_region(state_code: str) -> str:
            for region, states in regions.items():
                if state_code in states:
                    return region
            return "Other"

        # Transform to dimension format
        dim_locations = []
        for addr in addresses:
            state_code = addr.get("state_code", "")
            dim_locations.append(
                {
                    "address_line": addr.get("address_line"),
                    "city": addr.get("city"),
                    "state": addr.get("state"),
                    "state_code": state_code,
                    "postal_code": addr.get("postal_code"),
                    "country": addr.get("country", "United States"),
                    "region": get_region(state_code),
                    "latitude": addr.get("latitude"),
                    "longitude": addr.get("longitude"),
                }
            )

        # Batch insert
        columns = list(dim_locations[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dim_locations]

        query = f"INSERT INTO star_dim_location ({columns_str}) VALUES %s RETURNING location_id"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(dim_locations)} locations in {elapsed:.3f}s")

        return {"records": len(dim_locations), "time": elapsed}

    def load_fact_orders(self) -> Dict[str, Any]:
        """Load fact table: orders"""
        print("\n→ Loading star_fact_orders...")

        orders = self._load_json("orders.json")
        order_items = self._load_json("order_items.json")
        users = self._load_json("users.json")
        addresses = self._load_json("addresses.json")

        if not orders or not order_items:
            return {"records": 0, "time": 0}

        # Create lookups
        user_address_map = {u["id"]: u.get("address_id") for u in users}
        address_location_map = self._get_address_location_mapping(addresses)

        # Group order items by order_id
        items_by_order = {}
        for item in order_items:
            order_id = item["order_id"]
            items_by_order.setdefault(order_id, []).append(item)

        start_time = time.time()

        # Build fact records
        fact_records = []
        for order in orders:
            order_id = order["id"]
            user_id = order.get("user_id")
            order_date = datetime.fromisoformat(order["order_date"]).date()

            # Get location
            address_id = user_address_map.get(user_id)
            location_id = address_location_map.get(address_id)

            # Get date_id
            date_id = self._get_date_id(order_date)

            # Process each item in the order
            items = items_by_order.get(order_id, [])

            for item in items:
                discount_pct = item.get("discount_percentage", 0) or 0
                unit_price = item.get("price", 0)
                quantity = item.get("quantity", 0)
                subtotal = unit_price * quantity
                discount_amount = subtotal * (discount_pct / 100)
                total = subtotal - discount_amount

                fact_records.append(
                    {
                        "order_id": order_id,
                        "user_id": user_id,
                        "product_id": item.get("product_id"),
                        "date_id": date_id,
                        "location_id": location_id,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "discount_percentage": discount_pct,
                        "discount_amount": round(discount_amount, 2),
                        "subtotal": round(subtotal, 2),
                        "total_amount": round(total, 2),
                        "order_status": order.get("status", "completed"),
                    }
                )

        # Batch insert
        columns = list(fact_records[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in fact_records]

        query = f"INSERT INTO star_fact_orders ({columns_str}) VALUES %s"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(fact_records)} fact records in {elapsed:.3f}s")

        return {"records": len(fact_records), "time": elapsed}

    def _get_date_id(self, date: datetime.date) -> int:
        """Get date_id for a given date"""
        self.cursor.execute(
            "SELECT date_id FROM star_dim_date WHERE full_date = %s", (date,)
        )
        result = self.cursor.fetchone()
        if result:
            return result[0]
        # Fallback: use YYYYMMDD as date_id
        return int(date.strftime("%Y%m%d"))

    def _get_address_location_mapping(self, addresses: List[Dict]) -> Dict[int, int]:
        """Map address_id to location_id from database"""
        mapping = {}

        for addr in addresses:
            # Query to find location_id by matching address attributes
            self.cursor.execute(
                """
                SELECT location_id FROM star_dim_location
                WHERE city = %s AND state_code = %s AND postal_code = %s
                LIMIT 1
            """,
                (addr.get("city"), addr.get("state_code"), addr.get("postal_code")),
            )

            result = self.cursor.fetchone()
            if result:
                mapping[addr["id"]] = result[0]

        return mapping

    def load_all_dimensions(self) -> Dict[str, Dict[str, Any]]:
        """Load all dimension tables"""
        print("\n" + "=" * 80)
        print("LOADING DIMENSION TABLES")
        print("=" * 80)

        results = {}

        results["dim_date"] = self.load_dim_date()
        results["dim_users"] = self.load_dim_users()
        results["dim_products"] = self.load_dim_products()
        results["dim_location"] = self.load_dim_location()

        return results

    def load_all(self) -> Dict[str, Dict[str, Any]]:
        """Load complete Star Schema"""
        print("\n")
        print("╔" + "=" * 78 + "╗")
        print("║" + " " * 25 + "STAR SCHEMA ETL" + " " * 38 + "║")
        print("╚" + "=" * 78 + "╝")

        results = {}

        # Load dimensions first
        dim_results = self.load_all_dimensions()
        results.update(dim_results)

        # Then load fact table
        print("\n" + "=" * 80)
        print("LOADING FACT TABLE")
        print("=" * 80)

        results["fact_orders"] = self.load_fact_orders()

        return results

    def get_stats(self) -> None:
        """Print Star Schema statistics"""
        print("\n" + "=" * 80)
        print("STAR SCHEMA STATISTICS")
        print("=" * 80)

        tables = [
            "star_dim_users",
            "star_dim_products",
            "star_dim_date",
            "star_dim_location",
            "star_fact_orders",
        ]

        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = self.cursor.fetchone()[0]
            print(f"  • {table:25s}: {count:8d} records")

    def _load_json(self, filename: str) -> List[Dict]:
        """Load JSON file"""
        file_path = self.data_dir / filename

        if not file_path.exists():
            print(f"  ⚠ File not found: {file_path}")
            return []

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)


def main():
    """Test Star Schema loader"""

    print("=" * 80)
    print("STAR SCHEMA LOADER - TESTING")
    print("=" * 80)

    db_config = {
        "host": "localhost",
        "database": "dummyjson_db",
        "user": "etl_user",
        "password": "etl_password",
        "port": 5432,
    }

    loader = StarSchemaLoader(db_config, data_dir="data/processed")

    try:
        loader.connect()
        loader.create_schema()

        # Load all data
        results = loader.load_all()

        # Print stats
        loader.get_stats()

        # Print timing summary
        print("\n" + "=" * 80)
        print("LOADING TIME SUMMARY")
        print("=" * 80)

        total_time = 0
        total_records = 0

        for table, stats in results.items():
            print(
                f"{table:20s}: {stats['records']:6d} records in {stats['time']:6.3f}s"
            )
            total_time += stats["time"]
            total_records += stats["records"]

        print("=" * 80)
        print(f"{'TOTAL':20s}: {total_records:6d} records in {total_time:6.3f}s")
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        raise

    finally:
        loader.disconnect()

    print("\n✅ STAR SCHEMA LOAD COMPLETE!")


if __name__ == "__main__":
    main()
