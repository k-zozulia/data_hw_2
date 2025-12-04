"""
Main ETL Pipeline with comprehensive logging
Orchestrates Extract → Transform → Load process for all storage systems
"""

import sys
import time
from pathlib import Path

from extract.extract import DataExtractor
from transform.transform import DataNormalizer
from load.load_postgres import PostgresDataLoader
from load.load_mongo import MongoDataLoader
from load.load_redis import RedisCache
from configs.config import DatabaseConfig, RAW_DIR, PROCESSED_DIR
from utils.logger import setup_pipeline_logger, log_performance, log_stats

# Setup logger
logger = setup_pipeline_logger()


def print_banner(text: str) -> None:
    """Print formatted banner"""
    banner = "=" * 80
    logger.info(f"\n{banner}")
    logger.info(f"  {text}")
    logger.info(banner)


def run_extract() -> bool:
    """Step 1: Extract data from API"""
    print_banner("STEP 1: EXTRACT DATA FROM API")
    start_time = time.time()

    try:
        extractor = DataExtractor(data_dir=RAW_DIR)
        data = extractor.extract_from_api(save_to_file=True)

        elapsed = time.time() - start_time
        total_records = sum(len(v) for v in data.values())

        log_performance(logger, "Extract", elapsed, total_records)

        stats = {
            "Products": len(data["products"]),
            "Users": len(data["users"]),
            "Carts": len(data["carts"]),
            "Total records": total_records,
        }
        log_stats(logger, "Extraction Summary", stats)

        logger.info("✅ Extraction complete!")
        return True

    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}", exc_info=True)
        return False


def run_transform() -> bool:
    """Step 2: Transform data to 3NF"""
    print_banner("STEP 2: TRANSFORM DATA TO 3NF")
    start_time = time.time()

    try:
        normalizer = DataNormalizer(data_dir=RAW_DIR, output_dir=PROCESSED_DIR)
        tables = normalizer.normalize_all()

        elapsed = time.time() - start_time
        total_records = sum(len(v) for v in tables.values())

        log_performance(logger, "Transform", elapsed, total_records)

        stats = {table: len(records) for table, records in tables.items()}
        log_stats(logger, "Normalized Tables", stats)

        logger.info("✅ Transformation complete!")
        return True

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True)
        return False


def run_load_postgres() -> bool:
    """Step 3: Load data into PostgreSQL (3NF)"""
    print_banner("STEP 3: LOAD DATA INTO POSTGRESQL (3NF)")
    start_time = time.time()

    try:
        loader = PostgresDataLoader(DatabaseConfig.postgres(), data_dir=PROCESSED_DIR)

        loader.connect()
        logger.info("Connected to PostgreSQL")

        loader.create_schema()
        logger.info("Schema created")

        timing_results = loader.load_all_data()

        loader.get_stats()
        loader.disconnect()

        elapsed = time.time() - start_time
        total_records = sum(s["records"] for s in timing_results.values())

        log_performance(logger, "Load PostgreSQL", elapsed, total_records)

        stats = {
            table: f"{s['records']} records in {s['time']:.3f}s ({s['method']})"
            for table, s in timing_results.items()
        }
        log_stats(logger, "PostgreSQL Loading Times", stats)

        logger.info("✅ PostgreSQL load complete!")
        return True

    except Exception as e:
        logger.error(f"❌ PostgreSQL load failed: {e}", exc_info=True)
        return False


def run_load_mongo() -> bool:
    """Step 4: Load data into MongoDB (denormalized)"""
    print_banner("STEP 4: LOAD DATA INTO MONGODB (DENORMALIZED)")
    start_time = time.time()

    try:
        loader = MongoDataLoader(DatabaseConfig.mongodb(), data_dir=PROCESSED_DIR)

        loader.connect()
        logger.info("Connected to MongoDB")

        loader.drop_collections()
        timing_results = loader.denormalize_and_load()

        loader.create_indexes()
        loader.get_stats()
        loader.disconnect()

        elapsed = time.time() - start_time
        total_docs = sum(s["records"] for s in timing_results.values())

        log_performance(logger, "Load MongoDB", elapsed, total_docs)

        stats = {
            coll: f"{s['records']} documents in {s['time']:.3f}s"
            for coll, s in timing_results.items()
        }
        log_stats(logger, "MongoDB Loading Times", stats)

        logger.info("✅ MongoDB load complete!")
        return True

    except Exception as e:
        logger.error(f"❌ MongoDB load failed: {e}", exc_info=True)
        return False


def run_load_redis() -> bool:
    """Step 5: Cache data in Redis"""
    print_banner("STEP 5: CACHE DATA IN REDIS")
    start_time = time.time()

    try:
        cache = RedisCache(DatabaseConfig.redis(), data_dir=PROCESSED_DIR)

        cache.connect()
        logger.info("Connected to Redis")

        cache.flush_all()
        results = cache.load_from_mongo_export()

        cache.print_stats()
        cache.disconnect()

        elapsed = time.time() - start_time
        total_cached = sum(s["cached"] for s in results.values())

        log_performance(logger, "Cache Redis", elapsed, total_cached)

        stats = {
            entity: f"{s['cached']} records in {s['time']:.3f}s"
            for entity, s in results.items()
        }
        log_stats(logger, "Redis Caching Times", stats)

        logger.info("✅ Redis cache complete!")
        return True

    except Exception as e:
        logger.error(f"❌ Redis cache failed: {e}", exc_info=True)
        return False


def main():
    """Main ETL pipeline"""

    pipeline_start = time.time()

    logger.info("\n")
    logger.info("╔" + "=" * 78 + "╗")
    logger.info("║" + " " * 25 + "ETL PIPELINE - FULL RUN" + " " * 30 + "║")
    logger.info("╚" + "=" * 78 + "╝")

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
            logger.error(f"\n⚠️  Pipeline stopped at step: {step_name}")
            logger.error("Fix the error and run again.")
            sys.exit(1)

    pipeline_elapsed = time.time() - pipeline_start

    # Final summary
    print_banner("ETL PIPELINE COMPLETE")

    logger.info("\n✅ All steps completed successfully!")
    logger.info(f"\n⏱  Total pipeline time: {pipeline_elapsed:.1f}s")

    logger.info("\nSteps executed:")
    for i, (step_name, _) in enumerate(steps, 1):
        status = "✅" if results[step_name] else "❌"
        logger.info(f"  {i}. {status} {step_name}")

    logger.info("\n" + "=" * 80)
    logger.info("\n🎉 ETL Pipeline finished! Data is ready in:")
    logger.info("  • PostgreSQL: dummyjson_db (normalized, 3NF)")
    logger.info("  • MongoDB: dummyjson_db (denormalized documents)")
    logger.info("  • Redis: cached frequently accessed data")
    logger.info("\nNext steps:")
    logger.info("  • Run benchmarks: python benchmark/benchmark_databases.py")
    logger.info("  • Validate data: python validate/data_validator.py")
    logger.info("  • Check logs: results/logs/")
    logger.info("=" * 80 + "\n")


if __name__ == "__main__":
    main()
