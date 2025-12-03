"""
Main ETL Pipeline
Orchestrates Extract → Transform → Load process for all storage systems
"""

import sys
from pathlib import Path

from extract.extract import DataExtractor
from transform.transform import DataNormalizer
from load.load_postgres import PostgresDataLoader
from load.load_mongo import MongoDataLoader
from load.load_redis import RedisCache
from configs.config import DatabaseConfig, RAW_DIR, PROCESSED_DIR

def print_banner(text: str) -> None:
    """Print formatted banner"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)


def run_extract() -> bool:
    """Step 1: Extract data from API"""
    print_banner("STEP 1: EXTRACT DATA FROM API")

    try:
        extractor = DataExtractor(data_dir=RAW_DIR)
        data = extractor.extract_from_api(save_to_file=True)

        print("\n✅ Extraction complete!")
        print(f"  • Products: {len(data['products'])}")
        print(f"  • Users: {len(data['users'])}")
        print(f"  • Carts: {len(data['carts'])}")

        return True

    except Exception as e:
        print(f"\n❌ Extraction failed: {e}")
        return False


def run_transform() -> bool:
    """Step 2: Transform data to 3NF"""
    print_banner("STEP 2: TRANSFORM DATA TO 3NF")

    try:
        normalizer = DataNormalizer(data_dir=RAW_DIR, output_dir=PROCESSED_DIR)

        tables = normalizer.normalize_all()

        print("\n✅ Transformation complete!")
        print("\nNormalized tables:")
        for table_name, records in tables.items():
            print(f"  • {table_name:20s}: {len(records):5d} records")

        return True

    except Exception as e:
        print(f"\n❌ Transformation failed: {e}")
        return False


def run_load_postgres() -> bool:
    """Step 3: Load data into PostgreSQL (3NF)"""
    print_banner("STEP 3: LOAD DATA INTO POSTGRESQL (3NF)")

    try:
        loader = PostgresDataLoader(DatabaseConfig.postgres(), data_dir=PROCESSED_DIR)

        loader.connect()
        loader.create_schema()

        print("\n→ Loading data...")
        timing_results = loader.load_all_data()

        print("\n→ Database statistics:")
        loader.get_stats()

        print("\n Loading times:")
        for table, stats in timing_results.items():
            print(
                f"  • {table:20s}: {stats['records']:6d} records in {stats['time']:6.3f}s ({stats['method']})"
            )

        loader.disconnect()

        print("\n✅ PostgreSQL load complete!")
        return True

    except Exception as e:
        print(f"\n❌ PostgreSQL load failed: {e}")
        return False


def run_load_mongo() -> bool:
    """Step 4: Load data into MongoDB (denormalized)"""
    print_banner("STEP 4: LOAD DATA INTO MONGODB (DENORMALIZED)")

    try:
        loader = MongoDataLoader(DatabaseConfig.mongodb(), data_dir=PROCESSED_DIR)

        loader.connect()
        loader.drop_collections()

        timing_results = loader.denormalize_and_load()

        loader.create_indexes()
        loader.get_stats()

        print("\n Loading times:")
        total_time = 0
        total_docs = 0

        for collection, stats in timing_results.items():
            print(
                f"  • {collection:15s}: {stats['records']:6d} documents in {stats['time']:6.3f}s"
            )
            total_time += stats["time"]
            total_docs += stats["records"]

        print(f"  • {'TOTAL':15s}: {total_docs:6d} documents in {total_time:6.3f}s")

        loader.disconnect()

        print("\n✅ MongoDB load complete!")
        return True

    except Exception as e:
        print(f"\n❌ MongoDB load failed: {e}")
        return False


def run_load_redis() -> bool:
    """Step 5: Cache data in Redis"""
    print_banner("STEP 5: CACHE DATA IN REDIS")

    try:
        cache = RedisCache(DatabaseConfig.redis(), data_dir=PROCESSED_DIR)

        cache.connect()
        cache.flush_all()

        print("\n→ Loading data from files to Redis...")
        results = cache.load_from_mongo_export()

        cache.print_stats()

        print("\n Caching times:")
        for entity, stats in results.items():
            print(
                f"  • {entity:15s}: {stats['cached']:6d} records in {stats['time']:6.3f}s"
            )

        cache.disconnect()

        print("\n✅ Redis cache complete!")
        return True

    except Exception as e:
        print(f"\n❌ Redis cache failed: {e}")
        return False


def main():
    """Main ETL pipeline"""

    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 25 + "ETL PIPELINE - FULL RUN" + " " * 30 + "║")
    print("╚" + "=" * 78 + "╝")

    # Create data directories
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)

    steps = [
        ("Extract", run_extract),
        ("Transform", run_transform),
        ("Load PostgreSQL", run_load_postgres),
        ("Load MongoDB", run_load_mongo),
        ("Cache Redis", run_load_redis),
    ]

    results = {}

    for step_name, step_func in steps:
        success = step_func()
        results[step_name] = success

        if not success:
            print(f"\n⚠️  Pipeline stopped at step: {step_name}")
            print("Fix the error and run again.")
            sys.exit(1)

    # Final summary
    print_banner("ETL PIPELINE COMPLETE")

    print("\n✅ All steps completed successfully!")
    print("\nSteps executed:")
    for i, (step_name, _) in enumerate(steps, 1):
        status = "✅" if results[step_name] else "❌"
        print(f"  {i}. {status} {step_name}")

    print("\n" + "=" * 80)
    print("\n ETL Pipeline finished! Data is ready in:")
    print("  • PostgreSQL: dummyjson_db (normalized, 3NF)")
    print("  • MongoDB: dummyjson_db (denormalized documents)")
    print("  • Redis: cached frequently accessed data")
    print("\nRun benchmark.py to compare performance!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
