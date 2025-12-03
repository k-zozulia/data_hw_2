"""
Test Data Generator - creates 10k records for performance testing
"""

import json
import random
import time
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta
from configs.config import PROCESSED_DIR, TEST_DIR


class TestDataGenerator:
    """Generate 10,000 test records by duplicating and modifying real data"""

    def __init__(
        self, source_dir: str = PROCESSED_DIR, output_dir: str = TEST_DIR
    ):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_all_test_data(self, target_count: int = 10000) -> Dict[str, int]:
        """Generate test data for all entities"""

        print(f" Generating {target_count} test records")

        results = {}

        # Generate test data for main entities
        print("\n→ Generating test users...")
        results["users"] = self.generate_test_users(target_count)

        print("\n→ Generating test products...")
        results["products"] = self.generate_test_products(target_count)

        print("\n→ Generating test orders...")
        results["orders"] = self.generate_test_orders(target_count)

        print(" Test data generation complete")

        return results

    def generate_test_users(self, target_count: int = 10000) -> int:
        """Generate 10k users by duplicating and modifying"""

        # Load original users
        source_file = self.source_dir / "users.json"
        with open(source_file, "r", encoding="utf-8") as f:
            original_users = json.load(f)

        print(f"  • Original users: {len(original_users)}")

        test_users = []
        user_id = 10000

        replications_needed = (target_count // len(original_users)) + 1

        for replication in range(replications_needed):
            for original_user in original_users:
                if len(test_users) >= target_count:
                    break

                test_user = original_user.copy()
                test_user["id"] = user_id

                test_user["username"] = (
                    f"{original_user.get('username', 'user')}_{user_id}"
                )
                test_user["email"] = f"test_user_{user_id}@example.com"
                test_user["phone"] = (
                    f"+1-555-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
                )
                test_user["ssn"] = (
                    f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
                )

                if test_user.get("age"):
                    test_user["age"] = random.randint(18, 75)

                test_users.append(test_user)
                user_id += 1

            if len(test_users) >= target_count:
                break

        output_file = self.output_dir / "test_users.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(test_users[:target_count], f, indent=2, ensure_ascii=False)

        print(f"  ✓ Generated {len(test_users[:target_count])} test users")
        print(f"  ✓ Saved to {output_file}")

        return len(test_users[:target_count])

    def generate_test_products(self, target_count: int = 10000) -> int:
        """Generate 10k products by duplicating and modifying"""

        source_file = self.source_dir / "products.json"
        with open(source_file, "r", encoding="utf-8") as f:
            original_products = json.load(f)

        print(f"  • Original products: {len(original_products)}")

        test_products = []
        product_id = 10000

        replications_needed = (target_count // len(original_products)) + 1

        for replication in range(replications_needed):
            for original_product in original_products:
                if len(test_products) >= target_count:
                    break

                test_product = original_product.copy()
                test_product["id"] = product_id

                test_product["title"] = (
                    f"{original_product.get('title', 'Product')} v{replication + 1}"
                )
                test_product["sku"] = f"SKU-{product_id:06d}"
                test_product["barcode"] = (
                    f"{random.randint(1000000000000, 9999999999999)}"
                )

                if test_product.get("price"):
                    base_price = original_product.get("price", 100)
                    test_product["price"] = round(
                        base_price * random.uniform(0.8, 1.2), 2
                    )

                test_product["stock"] = random.randint(0, 500)

                if test_product.get("rating"):
                    test_product["rating"] = round(random.uniform(3.0, 5.0), 2)

                test_products.append(test_product)
                product_id += 1

            if len(test_products) >= target_count:
                break

        output_file = self.output_dir / "test_products.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(test_products[:target_count], f, indent=2, ensure_ascii=False)

        print(f"  ✓ Generated {len(test_products[:target_count])} test products")
        print(f"  ✓ Saved to {output_file}")

        return len(test_products[:target_count])

    def generate_test_orders(self, target_count: int = 10000) -> int:
        """Generate 10k orders by duplicating and modifying"""

        source_file = self.source_dir / "orders.json"
        with open(source_file, "r", encoding="utf-8") as f:
            original_orders = json.load(f)

        print(f"  • Original orders: {len(original_orders)}")

        test_orders = []
        order_id = 10000

        replications_needed = (target_count // len(original_orders)) + 1

        for replication in range(replications_needed):
            for original_order in original_orders:
                if len(test_orders) >= target_count:
                    break

                test_order = original_order.copy()
                test_order["id"] = order_id

                test_order["user_id"] = random.randint(10000, 10000 + target_count)

                if test_order.get("total"):
                    base_total = original_order.get("total", 100)
                    test_order["total"] = round(
                        base_total * random.uniform(0.9, 1.1), 2
                    )
                    test_order["discounted_total"] = round(
                        test_order["total"] * 0.85, 2
                    )

                test_orders.append(test_order)
                order_id += 1

            if len(test_orders) >= target_count:
                break

        output_file = self.output_dir / "test_orders.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(test_orders[:target_count], f, indent=2, ensure_ascii=False)

        print(f"  ✓ Generated {len(test_orders[:target_count])} test orders")
        print(f"  ✓ Saved to {output_file}")

        return len(test_orders[:target_count])

    def generate_simple_test_table_data(self, target_count: int = 10000) -> int:
        """
        Generate simple flat test data for pure insert performance testing
        This is useful for comparing raw insert speed without FK constraints
        """

        print("\n→ Generating simple test table data...")

        test_records = []

        for i in range(1, target_count + 1):
            record = {
                "id": i,
                "name": f"Test Record {i}",
                "description": f"This is test record number {i} for performance benchmarking",
                "value": round(random.uniform(10.0, 1000.0), 2),
                "status": random.choice(["active", "inactive", "pending"]),
                "category": random.choice(["A", "B", "C", "D", "E"]),
                "created_at": (
                    datetime.now() - timedelta(days=random.randint(0, 365))
                ).isoformat(),
                "is_active": random.choice([True, False]),
                "priority": random.randint(1, 10),
            }
            test_records.append(record)

        output_file = self.output_dir / "test_simple_records.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(test_records, f, indent=2, ensure_ascii=False)

        print(f"  ✓ Generated {len(test_records)} simple test records")
        print(f"  ✓ Saved to {output_file}")

        return len(test_records)

    def print_summary(self):
        """Print summary of generated files"""

        print(" Generated all test records")

        test_files = [
            "test_users.json",
            "test_products.json",
            "test_orders.json",
            "test_simple_records.json",
        ]

        total_size = 0

        for filename in test_files:
            filepath = self.output_dir / filename
            if filepath.exists():
                size_mb = filepath.stat().st_size / (1024 * 1024)

                with open(filepath, "r") as f:
                    records = json.load(f)

                print(
                    f"  • {filename:30s} {len(records):6d} records  {size_mb:6.2f} MB"
                )
                total_size += size_mb

        print(f"  Total size: {total_size:.2f} MB")


def main():
    """Generate all test data"""

    generator = TestDataGenerator()

    results = generator.generate_all_test_data(target_count=10000)

    generator.generate_simple_test_table_data(target_count=10000)

    generator.print_summary()

    print("\n Test data generation complete!")


if __name__ == "__main__":
    main()
