"""
Transform module - normalizes data to 3NF
"""

import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta
from configs.config import RAW_DIR, PROCESSED_DIR
import random


class DataNormalizer:
    """Normalizes DummyJSON data to 3NF"""

    def __init__(self, data_dir: str = RAW_DIR, output_dir: str = PROCESSED_DIR):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Counters for auto-generated IDs
        self.address_id = 1
        self.bank_id = 1
        self.company_id = 1
        self.category_id = 1
        self.review_id = 1
        self.product_tag_id = 1
        self.product_image_id = 1
        self.order_item_id = 1

        # Caches to avoid duplicates
        self.category_cache = {}
        self.address_cache = {}

    def normalize_all(self) -> Dict[str, List[Dict]]:
        """Normalize all data and return tables"""

        print("Normalizing all data...")

        # Load raw data
        users_raw = self._load_json("users.json")
        products_raw = self._load_json("products.json")
        carts_raw = self._load_json("carts.json")

        # Initialize tables
        tables = {
            "addresses": [],
            "banks": [],
            "companies": [],
            "categories": [],
            "users": [],
            "products": [],
            "product_tags": [],
            "product_images": [],
            "reviews": [],
            "orders": [],
            "order_items": [],
        }

        # Normalize users (includes addresses, banks, companies)
        print("\n→ Normalizing users...")
        for user in users_raw:
            normalized = self._normalize_user(user)
            if normalized["address"]:
                tables["addresses"].append(normalized["address"])
            if normalized["bank"]:
                tables["banks"].append(normalized["bank"])
            if normalized["company"]:
                tables["companies"].append(normalized["company"])
                if normalized["company_address"]:
                    tables["addresses"].append(normalized["company_address"])
            tables["users"].append(normalized["user"])

        print(f"  ✓ Users: {len(tables['users'])}")
        print(f"  ✓ Addresses: {len(tables['addresses'])}")
        print(f"  ✓ Banks: {len(tables['banks'])}")
        print(f"  ✓ Companies: {len(tables['companies'])}")

        # Normalize products (includes categories, tags, images, reviews)
        print("\n→ Normalizing products...")
        for product in products_raw:
            normalized = self._normalize_product(product)
            tables["products"].append(normalized["product"])
            tables["product_tags"].extend(normalized["tags"])
            tables["product_images"].extend(normalized["images"])
            tables["reviews"].extend(normalized["reviews"])

        tables["categories"] = list(self.category_cache.values())

        print(f"  ✓ Products: {len(tables['products'])}")
        print(f"  ✓ Categories: {len(tables['categories'])}")
        print(f"  ✓ Product tags: {len(tables['product_tags'])}")
        print(f"  ✓ Product images: {len(tables['product_images'])}")
        print(f"  ✓ Reviews: {len(tables['reviews'])}")

        # Normalize carts → orders
        print("\n→ Normalizing carts to orders...")
        for cart in carts_raw:
            normalized = self._normalize_cart(cart)  # renamed method
            tables["orders"].append(normalized["order"])
            tables["order_items"].extend(normalized["items"])

        print(f"  ✓ Orders: {len(tables['orders'])}")
        print(f"  ✓ Order items: {len(tables['order_items'])}")

        # Save normalized data
        self._save_normalized_data(tables)

        return tables

    def _normalize_user(self, user: Dict) -> Dict:
        """Normalize user and extract related entities"""

        # Extract address
        address = None
        address_id = None
        if user.get("address"):
            addr = user["address"]
            coords = addr.get("coordinates", {})
            address = {
                "id": self.address_id,
                "address_line": addr.get("address", ""),
                "city": addr.get("city", ""),
                "state": addr.get("state", ""),
                "state_code": addr.get("stateCode", ""),
                "postal_code": addr.get("postalCode", ""),
                "country": addr.get("country", "United States"),
                "latitude": coords.get("lat"),
                "longitude": coords.get("lng"),
            }
            address_id = self.address_id
            self.address_id += 1

        # Extract bank
        bank = None
        bank_id = None
        if user.get("bank"):
            b = user["bank"]
            bank = {
                "id": self.bank_id,
                "card_number": b.get("cardNumber"),
                "card_type": b.get("cardType"),
                "card_expire": b.get("cardExpire"),
                "currency": b.get("currency"),
                "iban": b.get("iban"),
            }
            bank_id = self.bank_id
            self.bank_id += 1

        # Extract company and company address
        company = None
        company_address = None
        company_id = None
        if user.get("company"):
            comp = user["company"]

            # Company address
            comp_addr_id = None
            if comp.get("address"):
                ca = comp["address"]
                coords = ca.get("coordinates", {})
                company_address = {
                    "id": self.address_id,
                    "address_line": ca.get("address", ""),
                    "city": ca.get("city", ""),
                    "state": ca.get("state", ""),
                    "state_code": ca.get("stateCode", ""),
                    "postal_code": ca.get("postalCode", ""),
                    "country": ca.get("country", "United States"),
                    "latitude": coords.get("lat"),
                    "longitude": coords.get("lng"),
                }
                comp_addr_id = self.address_id
                self.address_id += 1

            company = {
                "id": self.company_id,
                "name": comp.get("name", ""),
                "department": comp.get("department"),
                "title": comp.get("title"),
                "address_id": comp_addr_id,
            }
            company_id = self.company_id
            self.company_id += 1

        # Extract hair
        hair = user.get("hair", {})

        # Main user record
        user_normalized = {
            "id": user["id"],
            "first_name": user.get("firstName"),
            "last_name": user.get("lastName"),
            "maiden_name": user.get("maidenName"),
            "age": user.get("age"),
            "gender": user.get("gender"),
            "email": user.get("email"),
            "phone": user.get("phone"),
            "username": user.get("username"),
            "password": user.get("password"),
            "birth_date": user.get("birthDate"),
            "image_url": user.get("image"),
            "blood_group": user.get("bloodGroup"),
            "height": user.get("height"),
            "weight": user.get("weight"),
            "eye_color": user.get("eyeColor"),
            "hair_color": hair.get("color"),
            "hair_type": hair.get("type"),
            "ip_address": user.get("ip"),
            "mac_address": user.get("macAddress"),
            "user_agent": user.get("userAgent"),
            "university": user.get("university"),
            "ein": user.get("ein"),
            "ssn": user.get("ssn"),
            "role": user.get("role", "user"),
            "crypto_coin": user.get("crypto", {}).get("coin"),
            "crypto_wallet": user.get("crypto", {}).get("wallet"),
            "crypto_network": user.get("crypto", {}).get("network"),
            "bank_id": bank_id,
            "company_id": company_id,
            "address_id": address_id,
        }

        return {
            "user": user_normalized,
            "address": address,
            "bank": bank,
            "company": company,
            "company_address": company_address,
        }

    def _normalize_product(self, product: Dict) -> Dict:
        """Normalize product and extract related entities"""

        # Get or create category
        category_name = product.get("category", "uncategorized")
        category_slug = category_name.lower().replace(" ", "-")

        if category_slug not in self.category_cache:
            self.category_cache[category_slug] = {
                "id": self.category_id,
                "name": category_name,
                "slug": category_slug,
            }
            category_id = self.category_id
            self.category_id += 1
        else:
            category_id = self.category_cache[category_slug]["id"]

        # Dimensions
        dims = product.get("dimensions", {})

        # Meta
        meta = product.get("meta", {})

        # Main product
        product_normalized = {
            "id": product["id"],
            "title": product.get("title"),
            "description": product.get("description"),
            "category_id": category_id,
            "price": product.get("price"),
            "discount_percentage": product.get("discountPercentage"),
            "rating": product.get("rating"),
            "stock": product.get("stock"),
            "brand": product.get("brand"),
            "sku": product.get("sku"),
            "weight": product.get("weight"),
            "width": dims.get("width"),
            "height": dims.get("height"),
            "depth": dims.get("depth"),
            "warranty_info": product.get("warrantyInformation"),
            "shipping_info": product.get("shippingInformation"),
            "availability_status": product.get("availabilityStatus"),
            "return_policy": product.get("returnPolicy"),
            "minimum_order_quantity": product.get("minimumOrderQuantity"),
            "barcode": meta.get("barcode"),
            "qr_code_url": meta.get("qrCode"),
            "thumbnail_url": product.get("thumbnail"),
            "created_at": meta.get("createdAt"),
            "updated_at": meta.get("updatedAt"),
        }

        # Tags
        tags = []
        for tag in product.get("tags", []):
            tags.append(
                {"id": self.product_tag_id, "product_id": product["id"], "tag": tag}
            )
            self.product_tag_id += 1

        # Images
        images = []
        for idx, img_url in enumerate(product.get("images", [])):
            images.append(
                {
                    "id": self.product_image_id,
                    "product_id": product["id"],
                    "image_url": img_url,
                    "image_order": idx,
                }
            )
            self.product_image_id += 1

        # Reviews
        reviews = []
        for review in product.get("reviews", []):
            reviews.append(
                {
                    "id": self.review_id,
                    "product_id": product["id"],
                    "rating": review.get("rating"),
                    "comment": review.get("comment"),
                    "reviewer_name": review.get("reviewerName"),
                    "reviewer_email": review.get("reviewerEmail"),
                    "review_date": review.get("date"),
                }
            )
            self.review_id += 1

        return {
            "product": product_normalized,
            "tags": tags,
            "images": images,
            "reviews": reviews,
        }

    def _normalize_cart(self, cart: Dict) -> Dict:
        """Normalize cart → order and add timestamp"""

        order_date = self._generate_order_date(cart.get("userId"))

        order_normalized = {
            "id": cart["id"],
            "user_id": cart.get("userId"),
            "total": cart.get("total"),
            "order_date": order_date,
            "status": random.choice(["completed", "pending", "shipped", "delivered"]),
            "discounted_total": cart.get("discountedTotal"),
            "total_products": cart.get("totalProducts"),
            "total_quantity": cart.get("totalQuantity"),
        }

        # Order items
        items = []
        for product in cart.get("products", []):
            items.append(
                {
                    "id": self.order_item_id,
                    "order_id": cart["id"],
                    "product_id": product.get("id"),
                    "quantity": product.get("quantity"),
                    "price": product.get("price"),
                    "discount_percentage": product.get("discountPercentage"),
                    "discounted_total": product.get("discountedTotal"),
                    "total": product.get("total"),
                }
            )
            self.order_item_id += 1

        return {"order": order_normalized, "items": items}

    def _load_json(self, filename: str) -> List[Dict]:
        """Load json file"""

        with open(self.data_dir / filename, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_normalized_data(self, tables: Dict[str, List[Dict]]) -> None:
        """Save normalized tables to JSON files"""

        print("\n→ Saving normalized data...")

        for table_name, records in tables.items():
            output_file = self.output_dir / f"{table_name}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
            print(f"  ✓ {output_file} ({len(records)} records)")

        print(" Normalization complete")

    def _generate_order_date(self, user_id: int) -> str:
        """
        Generate realistic order date
        Distributed over last 12 months
        """
        days_ago = random.randint(0, 365)
        order_date = datetime.now() - timedelta(days=days_ago)

        # Add random time (business hours 9-21)
        hour = random.randint(9, 21)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        order_date = order_date.replace(hour=hour, minute=minute, second=second)

        return order_date.isoformat()


def main():
    """Test normalization"""
    normalizer = DataNormalizer(output_dir=PROCESSED_DIR, data_dir=RAW_DIR)
    tables = normalizer.normalize_all()

    # Print summary
    print("\n Summary:")
    for table, records in tables.items():
        print(f"  • {table}: {len(records)} records")


if __name__ == "__main__":
    main()
