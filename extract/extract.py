"""
Extract module - extracting data from the API and files
"""

import csv
import requests
import json
from pathlib import Path
from typing import List, Dict, Any
from configs.config import API_CONFIG, RAW_DIR


class DataExtractor:
    """Extract data from various sources"""

    def __init__(self, data_dir: str = RAW_DIR):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # API endpoints
        self.data_source = API_CONFIG['base_url']

    def extract_from_api(self, save_to_file: bool = True) -> Dict[str, Any]:
        """Extract data from DummyJSON API"""

        print("Extracting data from DummyJSON API...")

        data = {}

        # Products
        data["products"] = self._fetch_all_paginated("products")
        print(f" ✓ Extracted {len(data['products'])} products")

        # Users
        data["users"] = self._fetch_all_paginated("users")
        print(f" ✓ Extracted {len(data['users'])} users")

        # Carts
        data["carts"] = self._fetch_all_paginated("carts")
        print(f" ✓ Extracted {len(data['carts'])} carts")

        if save_to_file:
            self._save_api_data(data)

        return data

    def _fetch_all_paginated(self, entity: str, limit: int = 100) -> List[Dict]:
        """Fetch all data with pagination from DummyJSON"""

        items = []
        skip = 0

        while True:
            url = f"{self.data_source}/{entity}?limit={limit}&skip={skip}"
            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            batch = data.get(entity, [])
            items.extend(batch)

            if len(batch) < limit:
                break  # No more pages

            skip += limit

        return items

    def _save_api_data(self, data: Dict[str, Any]) -> None:
        """Saving API data to JSON and CSV files"""

        print("\nSaving API data to files...")

        # Saving JSON
        for key, value in data.items():
            json_file = self.data_dir / f"{key}.json"
            with open(json_file, "w", encoding="utf-8") as file:
                json.dump(value, file, ensure_ascii=False, indent=2)
                print(f" ✓ Saved {json_file}")

        # Save CSVs
        self._save_products_to_csv(data["products"])
        self._save_users_to_csv(data["users"])

    def _save_products_to_csv(self, products: List[Dict]) -> None:
        """Converts products from DummyJSON to CSV"""

        csv_file = self.data_dir / "products.csv"

        if not products:
            return

        flat_products = []

        for p in products:
            flat_p = {
                "id": p["id"],
                "title": p["title"],
                "description": p["description"],
                "category": p["category"],
                "price": p["price"],
                "discountPercentage": p.get("discountPercentage"),
                "rating": p.get("rating"),
                "stock": p.get("stock"),
                "brand": p.get("brand"),
                "sku": p.get("sku"),
                "weight": p.get("weight"),
                "thumbnail": p.get("thumbnail"),
            }
            flat_products.append(flat_p)

        fieldnames = flat_products[0].keys()

        with open(csv_file, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_products)

        print(f"\n ✓ Saved {len(flat_products)} products to CSV")

    def _save_users_to_csv(self, users: List[Dict]) -> None:
        """Converts users from DummyJSON to CSV"""

        csv_file = self.data_dir / "users.csv"

        if not users:
            return

        flat_users = []

        for u in users:
            addr = u.get("address", {})
            flat_u = {
                "id": u["id"],
                "firstName": u.get("firstName"),
                "lastName": u.get("lastName"),
                "maidenName": u.get("maidenName"),
                "age": u.get("age"),
                "gender": u.get("gender"),
                "email": u.get("email"),
                "phone": u.get("phone"),
                "username": u.get("username"),
                "password": u.get("password"),
                "birthDate": u.get("birthDate"),
                "image": u.get("image"),
                "address_city": addr.get("city"),
                "address_street": addr.get("address"),
                "address_state": addr.get("state"),
                "address_postalCode": addr.get("postalCode"),
            }
            flat_users.append(flat_u)

        fieldnames = flat_users[0].keys()

        with open(csv_file, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_users)

        print(f"\n ✓ Saved {len(flat_users)} users to CSV")

    def load_data_from_json(self, json_file: str) -> Any:
        """Load data from JSON"""
        file_path = self.data_dir / json_file
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def load_data_from_csv(self, csv_file: str) -> List[Dict]:
        """Load data from CSV"""

        file_path = self.data_dir / csv_file
        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            return list(reader)


def main():
    """Extract data from DummyJSON API"""

    print("=" * 80)
    print("Extracting data from API...")
    print("=" * 80)

    extractor = DataExtractor()

    try:
        data = extractor.extract_from_api()
        print("Data successfully extracted")

        print("\nReading data from CSV...")
        products_csv = extractor.load_data_from_csv("products.csv")
        print(f"  ✓ Read {len(products_csv)} products from CSV")

        users_csv = extractor.load_data_from_csv("users.csv")
        print(f"  ✓ Read {len(users_csv)} users from CSV")

        print("=" * 80)

    except Exception as ex:
        print(f"Error: {ex}")
        raise


if __name__ == "__main__":
    main()
