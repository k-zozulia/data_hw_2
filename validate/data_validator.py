"""
Data validation module for ETL pipeline
Validates data integrity, referential integrity, and data quality
"""

import json
import psycopg2
from pathlib import Path
from typing import Dict, List, Any, Tuple
from collections import defaultdict
from configs.config import DatabaseConfig, PROCESSED_DIR
from utils.logger import setup_logger

logger = setup_logger(__name__, "validation.log")


class DataValidator:
    """Validates data quality and integrity"""

    def __init__(self, data_dir: str = PROCESSED_DIR):
        self.data_dir = Path(data_dir)
        self.errors = defaultdict(list)
        self.warnings = defaultdict(list)

    def validate_all_files(self) -> Dict[str, Any]:
        """Validate all normalized JSON files"""
        logger.info("\n" + "=" * 80)
        logger.info("DATA VALIDATION STARTED")
        logger.info("=" * 80)

        results = {
            "files_checked": 0,
            "total_records": 0,
            "errors": {},
            "warnings": {},
            "passed": True,
        }

        # Expected files
        files_to_check = [
            "users.json",
            "addresses.json",
            "banks.json",
            "companies.json",
            "categories.json",
            "products.json",
            "product_tags.json",
            "product_images.json",
            "reviews.json",
            "orders.json",
            "order_items.json",
        ]

        for filename in files_to_check:
            filepath = self.data_dir / filename

            if not filepath.exists():
                self.errors[filename].append(f"File not found: {filepath}")
                continue

            try:
                data = self._load_json(filepath)
                results["files_checked"] += 1
                results["total_records"] += len(data)

                self._validate_file(filename, data)

                logger.info(f"✓ {filename}: {len(data)} records validated")

            except Exception as e:
                self.errors[filename].append(f"Error reading file: {str(e)}")
                logger.error(f"✗ {filename}: {str(e)}")

        # Check referential integrity
        self._validate_referential_integrity()

        # Compile results
        results["errors"] = dict(self.errors)
        results["warnings"] = dict(self.warnings)
        results["passed"] = len(self.errors) == 0

        # Print summary
        self._print_validation_summary(results)

        return results

    def _validate_file(self, filename: str, data: List[Dict]) -> None:
        """Validate specific file"""

        if not data:
            self.warnings[filename].append("File is empty")
            return

        # Check for required fields based on table
        if filename == "users.json":
            self._validate_users(data)
        elif filename == "products.json":
            self._validate_products(data)
        elif filename == "orders.json":
            self._validate_orders(data)
        elif filename == "order_items.json":
            self._validate_order_items(data)
        elif filename == "addresses.json":
            self._validate_addresses(data)

    def _validate_users(self, users: List[Dict]) -> None:
        """Validate users data"""

        required_fields = ["id", "email", "username"]

        for idx, user in enumerate(users):
            # Check required fields
            for field in required_fields:
                if field not in user or user[field] is None:
                    self.errors["users.json"].append(
                        f"Record {idx}: Missing required field '{field}'"
                    )

            # Validate email format
            if "email" in user and user["email"]:
                if "@" not in user["email"]:
                    self.warnings["users.json"].append(
                        f"Record {idx}: Invalid email format: {user['email']}"
                    )

            # Check age range
            if "age" in user and user["age"]:
                if not (0 < user["age"] < 120):
                    self.warnings["users.json"].append(
                        f"Record {idx}: Suspicious age value: {user['age']}"
                    )

    def _validate_products(self, products: List[Dict]) -> None:
        """Validate products data"""

        for idx, product in enumerate(products):
            # Check required fields
            if "id" not in product:
                self.errors["products.json"].append(f"Record {idx}: Missing 'id'")

            if "title" not in product or not product["title"]:
                self.errors["products.json"].append(f"Record {idx}: Missing 'title'")

            # Validate price
            if "price" in product and product["price"]:
                if product["price"] < 0:
                    self.errors["products.json"].append(
                        f"Record {idx}: Negative price: {product['price']}"
                    )

            # Validate stock
            if "stock" in product and product["stock"]:
                if product["stock"] < 0:
                    self.warnings["products.json"].append(
                        f"Record {idx}: Negative stock: {product['stock']}"
                    )

            # Validate rating
            if "rating" in product and product["rating"]:
                if not (0 <= product["rating"] <= 5):
                    self.warnings["products.json"].append(
                        f"Record {idx}: Rating out of range: {product['rating']}"
                    )

    def _validate_orders(self, orders: List[Dict]) -> None:
        """Validate orders data"""
        for idx, order in enumerate(orders):
            # Check required fields
            if "id" not in order:
                self.errors["orders.json"].append(f"Record {idx}: Missing 'id'")

            if "user_id" not in order:
                self.errors["orders.json"].append(f"Record {idx}: Missing 'user_id'")

            # Validate total
            if "total" in order and order["total"]:
                if order["total"] < 0:
                    self.errors["orders.json"].append(
                        f"Record {idx}: Negative total: {order['total']}"
                    )

    def _validate_order_items(self, items: List[Dict]) -> None:
        """Validate order items"""
        for idx, item in enumerate(items):
            # Check required fields
            required = ["order_id", "product_id", "quantity"]
            for field in required:
                if field not in item:
                    self.errors["order_items.json"].append(
                        f"Record {idx}: Missing '{field}'"
                    )

            # Validate quantity
            if "quantity" in item and item["quantity"]:
                if item["quantity"] <= 0:
                    self.errors["order_items.json"].append(
                        f"Record {idx}: Invalid quantity: {item['quantity']}"
                    )

    def _validate_addresses(self, addresses: List[Dict]) -> None:
        """Validate addresses"""
        for idx, addr in enumerate(addresses):
            if "id" not in addr:
                self.errors["addresses.json"].append(f"Record {idx}: Missing 'id'")

            # Check for at least city or state
            if not addr.get("city") and not addr.get("state"):
                self.warnings["addresses.json"].append(
                    f"Record {idx}: Missing both city and state"
                )

    def _validate_referential_integrity(self) -> None:
        """Check foreign key relationships"""
        logger.info("\n→ Validating referential integrity...")

        try:
            # Load all data
            users = self._load_json(self.data_dir / "users.json")
            products = self._load_json(self.data_dir / "products.json")
            orders = self._load_json(self.data_dir / "orders.json")
            order_items = self._load_json(self.data_dir / "order_items.json")
            addresses = self._load_json(self.data_dir / "addresses.json")
            categories = self._load_json(self.data_dir / "categories.json")

            # Build ID sets
            user_ids = {u["id"] for u in users}
            product_ids = {p["id"] for p in products}
            order_ids = {o["id"] for o in orders}
            address_ids = {a["id"] for a in addresses}
            category_ids = {c["id"] for c in categories}

            # Check orders -> users
            for order in orders:
                if order.get("user_id") and order["user_id"] not in user_ids:
                    self.errors["referential_integrity"].append(
                        f"Order {order['id']}: user_id {order['user_id']} not found in users"
                    )

            # Check order_items -> orders
            for item in order_items:
                if item.get("order_id") and item["order_id"] not in order_ids:
                    self.errors["referential_integrity"].append(
                        f"Order item {item['id']}: order_id {item['order_id']} not found"
                    )

            # Check order_items -> products
            for item in order_items:
                if item.get("product_id") and item["product_id"] not in product_ids:
                    self.errors["referential_integrity"].append(
                        f"Order item {item['id']}: product_id {item['product_id']} not found"
                    )

            # Check users -> addresses
            for user in users:
                if user.get("address_id") and user["address_id"] not in address_ids:
                    self.errors["referential_integrity"].append(
                        f"User {user['id']}: address_id {user['address_id']} not found"
                    )

            # Check products -> categories
            for product in products:
                if (
                    product.get("category_id")
                    and product["category_id"] not in category_ids
                ):
                    self.errors["referential_integrity"].append(
                        f"Product {product['id']}: category_id {product['category_id']} not found"
                    )

            if not self.errors.get("referential_integrity"):
                logger.info("  ✓ All foreign keys are valid")
            else:
                logger.error(
                    f"  ✗ Found {len(self.errors['referential_integrity'])} FK violations"
                )

        except Exception as e:
            self.errors["referential_integrity"].append(f"Error checking FK: {str(e)}")
            logger.error(f"  ✗ Error checking referential integrity: {str(e)}")

    def _load_json(self, filepath: Path) -> List[Dict]:
        """Load JSON file"""
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def _print_validation_summary(self, results: Dict) -> None:
        """Print validation summary"""
        logger.info("\n" + "=" * 80)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 80)

        logger.info(f"Files checked: {results['files_checked']}")
        logger.info(f"Total records: {results['total_records']}")
        logger.info(f"Errors: {sum(len(v) for v in results['errors'].values())}")
        logger.info(f"Warnings: {sum(len(v) for v in results['warnings'].values())}")

        if results["errors"]:
            logger.error("\n❌ ERRORS FOUND:")
            for file, errors in results["errors"].items():
                logger.error(f"\n  {file}:")
                for error in errors[:5]:
                    logger.error(f"    • {error}")
                if len(errors) > 5:
                    logger.error(f"    ... and {len(errors) - 5} more")

        if results["warnings"]:
            logger.warning("\n⚠️  WARNINGS:")
            for file, warnings in results["warnings"].items():
                logger.warning(f"\n  {file}:")
                for warning in warnings[:3]:
                    logger.warning(f"    • {warning}")
                if len(warnings) > 3:
                    logger.warning(f"    ... and {len(warnings) - 3} more")

        if results["passed"]:
            logger.info("\n✅ ALL VALIDATIONS PASSED!")
        else:
            logger.error("\n❌ VALIDATION FAILED!")

        logger.info("=" * 80)


class DatabaseValidator:
    """Validate data in PostgreSQL database"""

    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        """Connect to database"""
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()

    def disconnect(self) -> None:
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def validate_database(self) -> Dict[str, Any]:
        """Validate database integrity"""
        logger.info("\n" + "=" * 80)
        logger.info("DATABASE VALIDATION")
        logger.info("=" * 80)

        results = {
            "tables_checked": 0,
            "total_records": 0,
            "fk_violations": 0,
            "null_violations": 0,
            "passed": True,
        }

        self.connect()

        try:
            # Check table counts
            tables = [
                "users",
                "products",
                "orders",
                "order_items",
                "addresses",
                "categories",
                "banks",
                "companies",
            ]

            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                logger.info(f"  • {table:20s}: {count:6d} records")
                results["tables_checked"] += 1
                results["total_records"] += count

            # Check for FK violations (PostgreSQL should prevent these, but let's verify)
            logger.info("\n→ Checking foreign key constraints...")

            # Check orders -> users
            self.cursor.execute(
                """
                SELECT COUNT(*) FROM orders o
                LEFT JOIN users u ON o.user_id = u.id
                WHERE o.user_id IS NOT NULL AND u.id IS NULL
            """
            )
            fk_violations = self.cursor.fetchone()[0]
            if fk_violations > 0:
                logger.error(f"  ✗ Found {fk_violations} orders with invalid user_id")
                results["fk_violations"] += fk_violations
            else:
                logger.info("  ✓ orders -> users: OK")

            # Check products -> categories
            self.cursor.execute(
                """
                SELECT COUNT(*) FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                WHERE p.category_id IS NOT NULL AND c.id IS NULL
            """
            )
            fk_violations = self.cursor.fetchone()[0]
            if fk_violations > 0:
                logger.error(
                    f"  ✗ Found {fk_violations} products with invalid category_id"
                )
                results["fk_violations"] += fk_violations
            else:
                logger.info("  ✓ products -> categories: OK")

            results["passed"] = (
                results["fk_violations"] == 0 and results["null_violations"] == 0
            )

            if results["passed"]:
                logger.info("\n✅ DATABASE VALIDATION PASSED!")
            else:
                logger.error("\n❌ DATABASE VALIDATION FAILED!")

        finally:
            self.disconnect()

        return results


def main():
    """Run all validations"""
    print("\n" + "=" * 80)
    print("ETL DATA VALIDATION")
    print("=" * 80)

    # Validate files
    file_validator = DataValidator(data_dir=PROCESSED_DIR)
    file_results = file_validator.validate_all_files()

    # Validate database
    db_validator = DatabaseValidator(DatabaseConfig.postgres())
    db_results = db_validator.validate_database()

    # Overall result
    if file_results["passed"] and db_results["passed"]:
        print("\n" + "=" * 80)
        print("✅ ALL VALIDATIONS PASSED!")
        print("=" * 80)
        return 0
    else:
        print("\n" + "=" * 80)
        print("❌ VALIDATION FAILED - CHECK LOGS FOR DETAILS")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    exit(main())
