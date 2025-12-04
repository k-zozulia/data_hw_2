"""
Load module – loading denormalized data into MongoDB
"""

import json
import time
from pathlib import Path
from typing import List, Dict, Any
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from configs.config import DatabaseConfig, PROCESSED_DIR


class MongoDataLoader:
    """Loads denormalized data into MongoDB"""

    def __init__(self, mongo_config: Dict[str, Any], data_dir: str = PROCESSED_DIR):
        self.mongo_config = mongo_config
        self.data_dir = Path(data_dir)
        self.client = None
        self.db = None

    def connect(self) -> None:
        """Connect to MongoDB"""

        try:
            connection_string = (
                f"mongodb://{self.mongo_config['user']}:"
                f"{self.mongo_config['password']}@"
                f"{self.mongo_config['host']}:"
                f"{self.mongo_config['port']}/"
            )

            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)

            # Connection check
            self.client.admin.command("ping")

            self.db = self.client[self.mongo_config["database"]]
            print("✓ Connected to MongoDB")

        except ConnectionFailure as e:
            print(f"✗ MongoDB connection error: {e}")
            raise

    def disconnect(self) -> None:
        """Close the connection"""

        if self.client:
            self.client.close()
        print("✓ MongoDB connection closed")

    def drop_collections(self) -> None:
        """Drop all collections"""
        print("\n→ Dropping old collections...")

        collections = ["users", "products", "orders"]
        for coll_name in collections:
            self.db[coll_name].drop()
            print(f"  ✓ {coll_name} dropped")

    def denormalize_and_load(self) -> Dict[str, Any]:
        """Denormalize and load all data"""

        timing_results = {}

        # 1. Load all normalized data
        print("\n→ Loading normalized data from JSON...")
        normalized_data = self._load_all_normalized_data()

        # 2. Create denormalized documents
        print("\n→ Creating denormalized documents...")

        # Users (with embedded address, bank, company)
        print("\n  • Denormalizing users...")
        users_docs = self._denormalize_users(normalized_data)
        print(f"    ✓ Created {len(users_docs)} user documents")

        # Products (with embedded category, tags, images, reviews)
        print("  • Denormalizing products...")
        products_docs = self._denormalize_products(normalized_data)
        print(f"    ✓ Created {len(products_docs)} product documents")

        # orders (with embedded user + items with product details)
        print("  • Denormalizing orders...")
        orders_docs = self._denormalize_orders(normalized_data)
        print(f"    ✓ Created {len(orders_docs)} order documents")

        # 3. Load into MongoDB
        print("\n→ Loading denormalized data into MongoDB...")

        timing_results["users"] = self._load_collection("users", users_docs)
        timing_results["products"] = self._load_collection("products", products_docs)
        timing_results["orders"] = self._load_collection("orders", orders_docs)

        return timing_results

    def _load_all_normalized_data(self) -> Dict[str, List[Dict]]:
        """Load all normalized JSON files"""

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

        data = {}
        for table in tables:
            data[table] = self._load_json(f"{table}.json")
            print(f"    ✓ {table}: {len(data[table])} records")

        return data

    def _load_json(self, filename: str) -> List[Dict]:
        """Load JSON file"""
        file_path = self.data_dir / filename

        if not file_path.exists():
            print(f"  ⚠ File not found: {file_path}")
            return []

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _denormalize_users(self, normalized_data: Dict) -> List[Dict]:
        """Denormalize users (embed address, bank, company)"""

        users = normalized_data["users"]
        addresses = {a["id"]: a for a in normalized_data["addresses"]}
        banks = {b["id"]: b for b in normalized_data["banks"]}
        companies = {c["id"]: c for c in normalized_data["companies"]}

        denormalized_users = []

        for user in users:
            user_doc = {
                "_id": user["id"],
                "first_name": user.get("first_name"),
                "last_name": user.get("last_name"),
                "maiden_name": user.get("maiden_name"),
                "age": user.get("age"),
                "gender": user.get("gender"),
                "email": user.get("email"),
                "phone": user.get("phone"),
                "username": user.get("username"),
                "password": user.get("password"),
                "birth_date": user.get("birth_date"),
                "image_url": user.get("image_url"),
                "blood_group": user.get("blood_group"),
                "height": user.get("height"),
                "weight": user.get("weight"),
                "eye_color": user.get("eye_color"),
                "hair": {
                    "color": user.get("hair_color"),
                    "type": user.get("hair_type"),
                },
                "ip_address": user.get("ip_address"),
                "mac_address": user.get("mac_address"),
                "user_agent": user.get("user_agent"),
                "university": user.get("university"),
                "ein": user.get("ein"),
                "ssn": user.get("ssn"),
                "role": user.get("role"),
                "crypto": {
                    "coin": user.get("crypto_coin"),
                    "wallet": user.get("crypto_wallet"),
                    "network": user.get("crypto_network"),
                },
            }

            # Embed address
            address_id = user.get("address_id")
            addr = addresses.get(address_id)
            if addr:
                user_doc["address"] = {
                    "address_line": addr.get("address_line"),
                    "city": addr.get("city"),
                    "state": addr.get("state"),
                    "state_code": addr.get("state_code"),
                    "postal_code": addr.get("postal_code"),
                    "country": addr.get("country"),
                    "coordinates": {
                        "lat": addr.get("latitude"),
                        "lng": addr.get("longitude"),
                    },
                }

            # Embed bank
            bank_id = user.get("bank_id")
            bank = banks.get(bank_id)
            if bank:
                user_doc["bank"] = {
                    "card_number": bank.get("card_number"),
                    "card_type": bank.get("card_type"),
                    "card_expire": bank.get("card_expire"),
                    "currency": bank.get("currency"),
                    "iban": bank.get("iban"),
                }

            # Embed company
            company_id = user.get("company_id")
            comp = companies.get(company_id)
            if comp:
                user_doc["company"] = {
                    "name": comp.get("name"),
                    "department": comp.get("department"),
                    "title": comp.get("title"),
                }

                comp_address_id = comp.get("address_id")
                comp_addr = addresses.get(comp_address_id)
                if comp_addr:
                    user_doc["company"]["address"] = {
                        "address_line": comp_addr.get("address_line"),
                        "city": comp_addr.get("city"),
                        "state": comp_addr.get("state"),
                        "state_code": comp_addr.get("state_code"),
                        "postal_code": comp_addr.get("postal_code"),
                        "country": comp_addr.get("country"),
                        "coordinates": {
                            "lat": comp_addr.get("latitude"),
                            "lng": comp_addr.get("longitude"),
                        },
                    }

            denormalized_users.append(user_doc)

        return denormalized_users

    def _denormalize_products(self, normalized_data: Dict) -> List[Dict]:
        """Denormalize products (embed category, tags, images, reviews)"""

        products = normalized_data.get("products", [])
        categories = {c.get("id"): c for c in normalized_data.get("categories", [])}

        # Group tags, images, reviews by product_id
        tags_by_product = {}
        for tag in normalized_data.get("product_tags", []):
            pid = tag.get("product_id")
            if pid is not None:
                tags_by_product.setdefault(pid, []).append(tag.get("tag"))

        images_by_product = {}
        for img in normalized_data.get("product_images", []):
            pid = img.get("product_id")
            if pid is not None:
                images_by_product.setdefault(pid, []).append(
                    {"url": img.get("image_url"), "order": img.get("image_order", 0)}
                )

        reviews_by_product = {}
        for review in normalized_data.get("reviews", []):
            pid = review.get("product_id")
            if pid is not None:
                reviews_by_product.setdefault(pid, []).append(
                    {
                        "rating": review.get("rating"),
                        "comment": review.get("comment"),
                        "reviewer_name": review.get("reviewer_name"),
                        "reviewer_email": review.get("reviewer_email"),
                        "date": review.get("review_date"),
                    }
                )

        denormalized_products = []

        for product in products:

            product_doc = {
                "_id": product["id"],
                "title": product.get("title"),
                "description": product.get("description"),
                "price": product.get("price"),
                "discount_percentage": product.get("discount_percentage"),
                "rating": product.get("rating"),
                "stock": product.get("stock"),
                "brand": product.get("brand"),
                "sku": product.get("sku"),
                "weight": product.get("weight"),
                "dimensions": {
                    "width": product.get("width"),
                    "height": product.get("height"),
                    "depth": product.get("depth"),
                },
                "warranty_info": product.get("warranty_info"),
                "shipping_info": product.get("shipping_info"),
                "availability_status": product.get("availability_status"),
                "return_policy": product.get("return_policy"),
                "minimum_order_quantity": product.get("minimum_order_quantity"),
                "meta": {
                    "barcode": product.get("barcode"),
                    "qr_code": product.get("qr_code_url"),
                    "created_at": product.get("created_at"),
                    "updated_at": product.get("updated_at"),
                },
                "thumbnail": product.get("thumbnail_url"),
            }

            # Embed category
            category_id = product.get("category_id")
            if category_id in categories:
                cat = categories[category_id]
                product_doc["category"] = {
                    "id": cat.get("id"),
                    "name": cat.get("name"),
                    "slug": cat.get("slug"),
                }

            # Embed tags
            product_doc["tags"] = tags_by_product.get(product.get("id"), [])

            # Embed images (sorted)
            images = images_by_product.get(product.get("id"), [])
            images.sort(key=lambda x: x.get("order", 0))
            product_doc["images"] = [img.get("url") for img in images]

            # Embed reviews
            product_doc["reviews"] = reviews_by_product.get(product.get("id"), [])

            denormalized_products.append(product_doc)

        return denormalized_products

    def _denormalize_orders(self, normalized_data: Dict) -> List[Dict]:
        """Denormalize orders (embed user info and items with full product details)"""

        orders = normalized_data.get("orders", [])
        users = {u.get("id"): u for u in normalized_data.get("users", [])}
        products = {p.get("id"): p for p in normalized_data.get("products", [])}
        categories = {c.get("id"): c for c in normalized_data.get("categories", [])}

        # Group order items by order_id
        items_by_order = {}
        for item in normalized_data.get("order_items", []):
            cid = item.get("order_id")
            if cid is not None:
                items_by_order.setdefault(cid, []).append(item)

        denormalized_orders = []

        for order in orders:

            order_doc = {
                "_id": order.get("id"),
                "order_date": order.get("order_date"),
                "status": order.get("status"),
                "total": order.get("total"),
                "discounted_total": order.get("discounted_total"),
                "total_products": order.get("total_products"),
                "total_quantity": order.get("total_quantity"),
            }

            # Embed user
            user_id = order.get("user_id")
            if user_id in users:
                user = users[user_id]
                order_doc["user"] = {
                    "id": user.get("id"),
                    "username": user.get("username"),
                    "first_name": user.get("first_name"),
                    "last_name": user.get("last_name"),
                    "email": user.get("email"),
                }

            # Items
            order_items = items_by_order.get(order.get("id"), [])
            order_doc["items"] = []

            for item in order_items:
                product_id = item.get("product_id")

                if product_id in products:
                    product = products[product_id]

                    category_name = None
                    cat_id = product.get("category_id")
                    if cat_id in categories:
                        category_name = categories[cat_id].get("name")

                    order_doc["items"].append(
                        {
                            "product_id": product.get("id"),
                            "title": product.get("title"),
                            "category": category_name,
                            "price": item.get("price"),
                            "quantity": item.get("quantity"),
                            "total": item.get("total"),
                            "discount_percentage": item.get("discount_percentage"),
                            "discounted_total": item.get("discounted_total"),
                            "thumbnail": product.get("thumbnail_url"),
                        }
                    )

            denormalized_orders.append(order_doc)

        return denormalized_orders

    def _load_collection(
        self, collection_name: str, documents: List[Dict]
    ) -> Dict[str, Any]:
        """Load documents into a collection and measure time"""

        if not documents:
            print(f"\n⚠ No data for {collection_name}")
            return {"records": 0, "time": 0.0}

        print(f"\n→ Loading {collection_name}...")

        collection = self.db[collection_name]

        start_time = time.time()

        try:
            result = collection.insert_many(documents, ordered=False)
            insert_time = time.time() - start_time

            print(
                f"  ✓ Inserted {len(result.inserted_ids)} documents in {insert_time:.3f}s"
            )

            return {"records": len(result.inserted_ids), "time": insert_time}

        except Exception as e:
            print(f"  ✗ Error loading {collection_name}: {e}")
            raise

    def create_indexes(self) -> None:
        """Create indexes to optimize queries"""

        print("\n→ Creating indexes...")

        # Users indexes
        print("\n→ Indexes for users...")
        self.db.users.create_index("email")
        self.db.users.create_index("username")
        print("  ✓ email, username")

        # Products indexes
        print("→ Indexes for products...")
        self.db.products.create_index("category.slug")
        self.db.products.create_index("brand")
        self.db.products.create_index("price")
        self.db.products.create_index("rating")
        print("  ✓ category.slug, brand, price, rating")

        # orders indexes
        print("→ Indexes for orders...")
        self.db.orders.create_index("user.id")
        print("  ✓ user.id")

        print("\n Indexes created!")

    def get_stats(self) -> None:
        """Print statistics for collections"""

        print("\n→ MongoDB stats")

        collections = ["users", "products", "orders"]

        for coll_name in collections:
            count = self.db[coll_name].count_documents({})

            stats = self.db.command("collStats", coll_name)
            size_mb = stats["size"] / (1024 * 1024)

            print(f"  • {coll_name:15s}: {count:6d} documents ({size_mb:.2f} MB)")


def main():
    """Main function for testing"""

    print("=" * 80)
    print("ETL LOAD MODULE - TESTING")
    print("=" * 80)

    loader = MongoDataLoader(DatabaseConfig.mongodb(), data_dir=PROCESSED_DIR)

    try:
        loader.connect()
        loader.drop_collections()
        timing_results = loader.denormalize_and_load()
        loader.create_indexes()
        loader.get_stats()

        print("\n" + "=" * 80)
        print("Loading time per collection...")
        print("=" * 80)
        for collection, stats in timing_results.items():
            print(
                f"{collection:15s}: {stats['records']:6d} documents for {stats['time']:6.3f}s"
            )

        total_time = sum(s["time"] for s in timing_results.values())
        total_docs = sum(s["records"] for s in timing_results.values())
        print("=" * 80)
        print(f"{'ALl':15s}: {total_docs:6d} documents for {total_time:6.3f}s")
        print("=" * 80)

    except Exception as ex:
        print(ex)
        raise

    print("Load mongodb complete!")


if __name__ == "__main__":
    main()
