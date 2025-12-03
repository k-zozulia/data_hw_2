"""
Load module - caching frequently accessed data in Redis
"""

import redis
import json
import time
from typing import Any, Dict, List, Optional, Literal
from pathlib import Path


EntityType = Literal["user", "product", "order"]


class RedisCache:
    """Cache frequently accessed data in Redis for fast retrieval"""

    def __init__(self, redis_config: Dict[str, Any], data_dir: str = "data/processed"):
        self.redis_config = redis_config
        self.data_dir = Path(data_dir)
        self.client = None

        # TTL settings (in seconds) for different entity types
        self.TTL_CONFIG = {
            "user": 3600,  # 1 hour
            "product": 86400,  # 24 hours
            "order": 1800,  # 30 minutes
        }

    def connect(self) -> None:
        """Connect to Redis"""
        try:
            self.client = redis.Redis(
                host=self.redis_config.get("host", "localhost"),
                port=self.redis_config.get("port", 6379),
                db=self.redis_config.get("db", 0),
                decode_responses=True,
                socket_timeout=5,
            )

            self.client.ping()
            print("✓ Connected to Redis")

        except redis.ConnectionError as e:
            print(f"✗ Redis connection error: {e}")
            raise

    def disconnect(self) -> None:
        """Close Redis connection"""
        if self.client:
            self.client.close()
            print("✓ Redis connection closed")

    def flush_all(self) -> None:
        """Clear all data from Redis (use with caution!)"""
        if self.client:
            self.client.flushdb()
            print("✓ Redis database flushed")

    def cache_item(self, entity_type: EntityType, item_id: int, data: Dict) -> bool:
        """Universal method to cache any entity type"""

        try:
            key = f"{entity_type}:{item_id}"
            value = json.dumps(data)
            ttl = self.TTL_CONFIG.get(entity_type, 3600)

            self.client.setex(name=key, time=ttl, value=value)
            return True

        except Exception as e:
            print(f"  ✗ Error caching {entity_type} {item_id}: {e}")
            return False

    def get_item(self, entity_type: EntityType, item_id: int) -> Optional[Dict]:
        """Universal method to retrieve any entity type from cache"""

        try:
            key = f"{entity_type}:{item_id}"
            value = self.client.get(key)

            if value:
                return json.loads(value)
            return None

        except Exception as e:
            print(f"  ✗ Error retrieving {entity_type} {item_id}: {e}")
            return None

    def cache_all_items(
        self, entity_type: EntityType, items: List[Dict]
    ) -> Dict[str, Any]:
        """Cache multiple items using Redis pipeline (batch mode)"""
        print(f"\n→ Caching {entity_type}s to Redis (batch mode)...")

        start_time = time.time()

        pipe = self.client.pipeline()
        ttl = self.TTL_CONFIG.get(entity_type, 3600)
        count = 0

        for item in items:
            item_id = item.get("id") or item.get("_id")
            if item_id:
                key = f"{entity_type}:{item_id}"
                value = json.dumps(item)
                pipe.setex(name=key, time=ttl, value=value)
                count += 1

        pipe.execute()
        elapsed = time.time() - start_time

        print(
            f"  ✓ Cached {count}/{len(items)} {entity_type}s in {elapsed:.3f}s (batch)"
        )

        return {"total": len(items), "cached": count, "time": elapsed}

    def delete_item(self, entity_type: EntityType, item_id: int) -> bool:
        """Delete specific item from cache"""
        key = f"{entity_type}:{item_id}"
        return bool(self.client.delete(key))

    def cache_user(self, user_id: int, user_data: Dict) -> bool:
        """Cache user profile (wrapper for cache_item)"""
        return self.cache_item("user", user_id, user_data)

    def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user from cache (wrapper for get_item)"""
        return self.get_item("user", user_id)

    def cache_product(self, product_id: int, product_data: Dict) -> bool:
        """Cache product (wrapper for cache_item)"""
        return self.cache_item("product", product_id, product_data)

    def get_product(self, product_id: int) -> Optional[Dict]:
        """Get product from cache (wrapper for get_item)"""
        return self.get_item("product", product_id)

    def cache_order(self, order_id: int, order_data: Dict) -> bool:
        """Cache order (wrapper for cache_item)"""
        return self.cache_item("order", order_id, order_data)

    def get_order(self, order_id: int) -> Optional[Dict]:
        """Get order from cache (wrapper for get_item)"""
        return self.get_item("order", order_id)

    def get_ttl(self, entity_type: EntityType, item_id: int) -> int:
        """Get remaining TTL for an item (in seconds)"""
        key = f"{entity_type}:{item_id}"
        return self.client.ttl(key)

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics"""
        info = self.client.info()

        # Count keys by pattern
        user_keys = len(self.client.keys("user:*"))
        product_keys = len(self.client.keys("product:*"))
        order_keys = len(self.client.keys("order:*"))

        return {
            "total_keys": info.get("db0", {}).get("keys", 0),
            "used_memory_mb": info.get("used_memory", 0) / (1024 * 1024),
            "users_cached": user_keys,
            "products_cached": product_keys,
            "orders_cached": order_keys,
            "uptime_seconds": info.get("uptime_in_seconds", 0),
        }

    def print_stats(self) -> None:
        """Print cache statistics"""

        print("Redis cache statistics:")

        stats = self.get_cache_stats()

        print(f"  • Total keys:       {stats['total_keys']}")
        print(f"  • Memory used:      {stats['used_memory_mb']:.2f} MB")
        print(f"  • Users cached:     {stats['users_cached']}")
        print(f"  • Products cached:  {stats['products_cached']}")
        print(f"  • Orders cached:     {stats['orders_cached']}")
        print(f"  • Uptime:           {stats['uptime_seconds']}s")

        print("=" * 80)

    def load_from_mongo_export(self) -> Dict[str, Any]:
        """Load denormalized data from MongoDB JSON exports"""

        print("Loading denormalized data from MongpDB to Redis")

        results = {}

        # Define entity types and their corresponding files
        entities = {
            "user": "users.json",
            "product": "products.json",
            "order": "orders.json",
        }

        for entity_type, filename in entities.items():
            filepath = self.data_dir / filename

            if filepath.exists():
                with open(filepath, "r", encoding="utf-8") as f:
                    items = json.load(f)
                    results[entity_type] = self.cache_all_items(entity_type, items)
            else:
                print(f"  ⚠ {filename} not found")

        return results


def main():
    """Test Redis caching module"""

    print("=" * 80)
    print("REDIS CACHE MODULE - TESTING")
    print("=" * 80)

    redis_config = {"host": "localhost", "port": 6379, "db": 0}

    cache = RedisCache(redis_config, data_dir="data/processed")

    try:
        cache.connect()
        cache.flush_all()

        # Test 1: Universal methods
        print("\n→ Test 1: Universal methods...")
        test_user = {
            "id": 1,
            "username": "test_user",
            "email": "test@example.com",
            "first_name": "John",
            "last_name": "Doe",
        }

        cache.cache_item("user", 1, test_user)
        retrieved = cache.get_item("user", 1)
        print(f"  ✓ Retrieved user: {retrieved.get('username')}")

        ttl = cache.get_ttl("user", 1)
        print(f"  ✓ TTL for user:1 = {ttl}s")

        # Test 2: Convenience wrappers (backward compatibility)
        print("\n→ Test 2: Convenience wrappers...")
        test_product = {"id": 5, "title": "Laptop", "price": 1200}
        cache.cache_product(5, test_product)
        retrieved_product = cache.get_product(5)
        print(f"  ✓ Retrieved product: {retrieved_product.get('title')}")

        # Test 3: Batch loading
        print("\n→ Test 3: Batch loading...")
        test_items = [{"id": i, "name": f"Item {i}"} for i in range(100)]
        result = cache.cache_all_items("product", test_items)
        print(f"  ✓ Batch time: {result['time']:.3f}s")

        # Test 4: Load from files
        print("\n→ Test 4: Loading from JSON files...")
        cache.flush_all()
        results = cache.load_from_mongo_export()

        # Print statistics
        cache.print_stats()

        if results:
            print("\n" + "=" * 80)
            print("TIMING RESULTS")
            print("=" * 80)
            for entity, stats in results.items():
                print(
                    f"  {entity:15s}: {stats['cached']:6d} records in {stats['time']:6.3f}s"
                )
            print("=" * 80)

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        raise

    finally:
        cache.disconnect()

    print("\n REDIS CACHE MODULE TEST COMPLETED!")


if __name__ == "__main__":
    main()
