"""
Load module - loading data into Snowflake Schema (with snow_ prefix)
"""

import json
import time
from pathlib import Path
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
from configs.config import DatabaseConfig, PROCESSED_DIR


class SnowflakeSchemaLoader:
    """Loads data into Snowflake Schema (normalized dimensional model)"""

    def __init__(self, db_config: Dict[str, str], data_dir: str = PROCESSED_DIR):
        self.db_config = db_config
        self.data_dir = Path(data_dir)
        self.conn = None
        self.cursor = None

        # Lookup caches for sub-dimensions
        self.role_cache = {}
        self.category_cache = {}
        self.brand_cache = {}
        self.state_cache = {}
        self.city_cache = {}

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
            print("✓ Connected to PostgreSQL (Snowflake Schema)")

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
        """Create Snowflake Schema tables from SQL file"""
        print("\n→ Creating Snowflake Schema tables...")

        sql_file = Path("sql/create_tables_snowflake.sql")
        if not sql_file.exists():
            print(f"✗ SQL file not found: {sql_file}")
            return

        with open(sql_file, "r") as f:
            sql = f.read()

        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("  ✓ Snowflake Schema tables created successfully!")
        except Exception as e:
            self.conn.rollback()
            print(f"  ✗ Error creating tables: {e}")
            raise

    def load_dim_user_roles(self) -> Dict[str, Any]:
        """Load user roles sub-dimension"""
        print("\n→ Loading snow_dim_user_roles...")

        users = self._load_json("users.json")

        # Extract unique roles
        roles = set()
        for user in users:
            role = user.get("role", "user")
            if role:
                roles.add(role)

        start_time = time.time()

        # Insert roles
        for role in roles:
            description = {
                "user": "Regular user",
                "admin": "Administrator",
                "moderator": "Content moderator",
                "guest": "Guest user",
            }.get(role, "User role")

            self.cursor.execute(
                """
                INSERT INTO snow_dim_user_roles (role_name, role_description)
                VALUES (%s, %s)
                ON CONFLICT (role_name) DO NOTHING
                RETURNING role_id, role_name
            """,
                (role, description),
            )

            result = self.cursor.fetchone()
            if result:
                self.role_cache[result[1]] = result[0]

        # Build cache from existing records
        self.cursor.execute("SELECT role_id, role_name FROM snow_dim_user_roles")
        for role_id, role_name in self.cursor.fetchall():
            self.role_cache[role_name] = role_id

        self.conn.commit()
        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(roles)} roles in {elapsed:.3f}s")
        return {"records": len(roles), "time": elapsed}

    def load_dim_categories(self) -> Dict[str, Any]:
        """Load categories sub-dimension"""
        print("\n→ Loading snow_dim_categories...")

        categories = self._load_json("categories.json")

        start_time = time.time()

        dim_categories = []
        for cat in categories:
            dim_categories.append(
                {
                    "category_name": cat.get("name"),
                    "category_slug": cat.get("slug"),
                    "parent_category_id": None,
                }
            )

        columns = list(dim_categories[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dim_categories]

        query = f"""
            INSERT INTO snow_dim_categories ({columns_str}) 
            VALUES %s 
            ON CONFLICT (category_slug) DO NOTHING
            RETURNING category_id, category_slug
        """
        execute_values(self.cursor, query, values, page_size=1000, fetch=True)

        # Build cache
        self.cursor.execute(
            "SELECT category_id, category_slug FROM snow_dim_categories"
        )
        for cat_id, cat_slug in self.cursor.fetchall():
            self.category_cache[cat_slug] = cat_id

        self.conn.commit()
        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(dim_categories)} categories in {elapsed:.3f}s")
        return {"records": len(dim_categories), "time": elapsed}

    def load_dim_brands(self) -> Dict[str, Any]:
        """Load brands sub-dimension"""
        print("\n→ Loading snow_dim_brands...")

        products = self._load_json("products.json")

        brands = set()
        for product in products:
            brand = product.get("brand")
            if brand:
                brands.add(brand)

        start_time = time.time()

        dim_brands = []
        for brand in brands:
            dim_brands.append(
                {"brand_name": brand, "brand_country": None, "brand_website": None}
            )

        columns = list(dim_brands[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dim_brands]

        query = f"""
            INSERT INTO snow_dim_brands ({columns_str}) 
            VALUES %s 
            ON CONFLICT (brand_name) DO NOTHING
            RETURNING brand_id, brand_name
        """
        execute_values(self.cursor, query, values, page_size=1000, fetch=True)

        self.cursor.execute("SELECT brand_id, brand_name FROM snow_dim_brands")
        for brand_id, brand_name in self.cursor.fetchall():
            self.brand_cache[brand_name] = brand_id

        self.conn.commit()
        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(brands)} brands in {elapsed:.3f}s")
        return {"records": len(brands), "time": elapsed}

    def load_dim_states(self) -> Dict[str, Any]:
        """Load states sub-dimension (simplified without countries)"""
        print("\n→ Loading snow_dim_states...")

        addresses = self._load_json("addresses.json")

        # Extract unique states
        states = {}
        for addr in addresses:
            state_name = addr.get("state")
            state_code = addr.get("state_code")

            if state_code and state_name:
                states[state_code] = state_name

        start_time = time.time()

        # US regions mapping
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
            for region, codes in regions.items():
                if state_code in codes:
                    return region
            return "Other"

        for state_code, state_name in states.items():
            region = get_region(state_code)

            self.cursor.execute(
                """
                INSERT INTO snow_dim_states (state_name, state_code, region, country)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (state_code) DO NOTHING
                RETURNING state_id, state_code
            """,
                (state_name, state_code, region, "United States"),
            )

            result = self.cursor.fetchone()
            if result:
                self.state_cache[result[1]] = result[0]

        # Build cache from existing records
        self.cursor.execute("SELECT state_id, state_code FROM snow_dim_states")
        for state_id, state_code in self.cursor.fetchall():
            self.state_cache[state_code] = state_id

        self.conn.commit()
        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(states)} states in {elapsed:.3f}s")
        return {"records": len(states), "time": elapsed}

    def load_dim_cities(self) -> Dict[str, Any]:
        """Load cities sub-dimension"""
        print("\n→ Loading snow_dim_cities...")

        addresses = self._load_json("addresses.json")

        # Extract unique cities
        cities = {}
        for addr in addresses:
            city_name = addr.get("city")
            state_code = addr.get("state_code")

            if city_name and state_code:
                state_id = self.state_cache.get(state_code)
                if state_id:
                    cities[(city_name, state_id)] = city_name

        start_time = time.time()

        for (city_name, state_id), _ in cities.items():
            self.cursor.execute(
                """
                INSERT INTO snow_dim_cities (city_name, state_id, population, timezone)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (city_name, state_id) DO NOTHING
                RETURNING city_id, city_name, state_id
            """,
                (city_name, state_id, None, None),
            )

            result = self.cursor.fetchone()
            if result:
                self.city_cache[(result[1], result[2])] = result[0]

        # Build cache from existing records
        self.cursor.execute("SELECT city_id, city_name, state_id FROM snow_dim_cities")
        for city_id, city_name, state_id in self.cursor.fetchall():
            self.city_cache[(city_name, state_id)] = city_id

        self.conn.commit()
        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(cities)} cities in {elapsed:.3f}s")
        return {"records": len(cities), "time": elapsed}

    def load_dim_users(self) -> Dict[str, Any]:
        """Load users dimension"""
        print("\n→ Loading snow_dim_users...")

        users = self._load_json("users.json")
        addresses = {a["id"]: a for a in self._load_json("addresses.json")}

        start_time = time.time()

        dim_users = []
        for user in users:
            address_id = user.get("address_id")
            address = addresses.get(address_id, {})

            role_id = self.role_cache.get(user.get("role", "user"))

            city_name = address.get("city")
            state_code = address.get("state_code")
            state_id = self.state_cache.get(state_code)
            city_id = (
                self.city_cache.get((city_name, state_id))
                if city_name and state_id
                else None
            )

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
                    "role_id": role_id,
                    "city_id": city_id,
                    "postal_code": address.get("postal_code"),
                    "latitude": address.get("latitude"),
                    "longitude": address.get("longitude"),
                }
            )

        columns = list(dim_users[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dim_users]

        query = f"INSERT INTO snow_dim_users ({columns_str}) VALUES %s ON CONFLICT (user_id) DO NOTHING"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(dim_users)} users in {elapsed:.3f}s")
        return {"records": len(dim_users), "time": elapsed}

    def load_dim_products(self) -> Dict[str, Any]:
        """Load products dimension"""
        print("\n→ Loading snow_dim_products...")

        products = self._load_json("products.json")
        categories = {c["id"]: c for c in self._load_json("categories.json")}

        start_time = time.time()

        dim_products = []
        for product in products:
            category_id_old = product.get("category_id")
            category_slug = categories.get(category_id_old, {}).get("slug")
            category_id = self.category_cache.get(category_slug)

            brand_name = product.get("brand")
            brand_id = self.brand_cache.get(brand_name)

            dim_products.append(
                {
                    "product_id": product["id"],
                    "title": product.get("title"),
                    "description": product.get("description"),
                    "category_id": category_id,
                    "brand_id": brand_id,
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

        columns = list(dim_products[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dim_products]

        query = f"INSERT INTO snow_dim_products ({columns_str}) VALUES %s ON CONFLICT (product_id) DO NOTHING"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(dim_products)} products in {elapsed:.3f}s")
        return {"records": len(dim_products), "time": elapsed}

    def load_dim_date(
        self, start_year: int = 2020, end_year: int = 2026
    ) -> Dict[str, Any]:
        """Generate and load date dimension"""
        print("\n→ Generating and loading snow_dim_date...")

        start_time = time.time()

        dates = self._generate_date_dimension(start_year, end_year)

        columns = list(dates[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in dates]

        query = f"INSERT INTO snow_dim_date ({columns_str}) VALUES %s ON CONFLICT (full_date) DO NOTHING"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Generated and loaded {len(dates)} dates in {elapsed:.3f}s")
        return {"records": len(dates), "time": elapsed}

    def _generate_date_dimension(self, start_year: int, end_year: int) -> List[Dict]:
        """Generate date dimension"""
        dates = []
        current = datetime(start_year, 1, 1)
        date_id = 1

        holidays = {
            (1, 1): "New Year's Day",
            (7, 4): "Independence Day",
            (12, 25): "Christmas Day",
        }

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

        while current.year <= end_year:
            is_holiday = (current.month, current.day) in holidays
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
                    "day_of_week": current.weekday() + 1,
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

    def load_fact_orders(self) -> Dict[str, Any]:
        """Load fact table"""
        print("\n→ Loading snow_fact_orders...")

        orders = self._load_json("orders.json")
        order_items = self._load_json("order_items.json")

        if not orders or not order_items:
            return {"records": 0, "time": 0}

        items_by_order = {}
        for item in order_items:
            order_id = item["order_id"]
            items_by_order.setdefault(order_id, []).append(item)

        start_time = time.time()

        fact_records = []
        for order in orders:
            order_id = order["id"]
            user_id = order.get("user_id")
            order_date = datetime.fromisoformat(order["order_date"]).date()
            date_id = self._get_date_id(order_date)

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
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "discount_percentage": discount_pct,
                        "discount_amount": round(discount_amount, 2),
                        "subtotal": round(subtotal, 2),
                        "total_amount": round(total, 2),
                        "order_status": order.get("status", "completed"),
                    }
                )

        columns = list(fact_records[0].keys())
        columns_str = ", ".join(columns)
        values = [[record[col] for col in columns] for record in fact_records]

        query = f"INSERT INTO snow_fact_orders ({columns_str}) VALUES %s"
        execute_values(self.cursor, query, values, page_size=1000)
        self.conn.commit()

        elapsed = time.time() - start_time

        print(f"  ✓ Loaded {len(fact_records)} fact records in {elapsed:.3f}s")
        return {"records": len(fact_records), "time": elapsed}

    def _get_date_id(self, date: datetime.date) -> int:
        """Get date_id for a given date"""
        self.cursor.execute(
            "SELECT date_id FROM snow_dim_date WHERE full_date = %s", (date,)
        )
        result = self.cursor.fetchone()
        if result:
            return result[0]
        return int(date.strftime("%Y%m%d"))

    def load_all_subdimensions(self) -> Dict[str, Dict[str, Any]]:
        """Load all sub-dimension tables"""
        print("\n" + "=" * 80)
        print("LOADING SUB-DIMENSIONS")
        print("=" * 80)

        results = {}

        results["snow_dim_user_roles"] = self.load_dim_user_roles()
        results["snow_dim_states"] = self.load_dim_states()
        results["snow_dim_cities"] = self.load_dim_cities()
        results["snow_dim_categories"] = self.load_dim_categories()
        results["snow_dim_brands"] = self.load_dim_brands()

        return results

    def load_all_dimensions(self) -> Dict[str, Dict[str, Any]]:
        """Load all main dimension tables"""
        print("\n" + "=" * 80)
        print("LOADING MAIN DIMENSIONS")
        print("=" * 80)

        results = {}

        results["snow_dim_date"] = self.load_dim_date()
        results["snow_dim_users"] = self.load_dim_users()
        results["snow_dim_products"] = self.load_dim_products()

        return results

    def load_all(self) -> Dict[str, Dict[str, Any]]:
        """Load complete Snowflake Schema"""
        print("\n")
        print("╔" + "=" * 78 + "╗")
        print("║" + " " * 22 + "SNOWFLAKE SCHEMA ETL" + " " * 35 + "║")
        print("╚" + "=" * 78 + "╝")

        results = {}

        subdim_results = self.load_all_subdimensions()
        results.update(subdim_results)

        dim_results = self.load_all_dimensions()
        results.update(dim_results)

        print("\n" + "=" * 80)
        print("LOADING FACT TABLE")
        print("=" * 80)

        results["snow_fact_orders"] = self.load_fact_orders()

        return results

    def get_stats(self) -> None:
        """Print Snowflake Schema statistics"""
        print("\n" + "=" * 80)
        print("SNOWFLAKE SCHEMA STATISTICS")
        print("=" * 80)

        tables = [
            "snow_dim_user_roles",
            "snow_dim_categories",
            "snow_dim_brands",
            "snow_dim_states",
            "snow_dim_cities",
            "snow_dim_users",
            "snow_dim_products",
            "snow_dim_date",
            "snow_fact_orders",
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
    """Test Snowflake Schema loader"""

    print("=" * 80)
    print("SNOWFLAKE SCHEMA LOADER - TESTING")
    print("=" * 80)

    loader = SnowflakeSchemaLoader(DatabaseConfig.postgres(), data_dir=PROCESSED_DIR)

    try:
        loader.connect()
        loader.create_schema()

        results = loader.load_all()
        loader.get_stats()

        print("\n" + "=" * 80)
        print("LOADING TIME SUMMARY")
        print("=" * 80)

        total_time = 0
        total_records = 0

        for table, stats in results.items():
            print(
                f"{table:25s}: {stats['records']:6d} records in {stats['time']:6.3f}s"
            )
            total_time += stats["time"]
            total_records += stats["records"]

        print("=" * 80)
        print(f"{'TOTAL':25s}: {total_records:6d} records in {total_time:6.3f}s")
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        raise

    finally:
        loader.disconnect()

    print("\n✅ SNOWFLAKE SCHEMA LOAD COMPLETE!")


if __name__ == "__main__":
    main()
