"""
Benchmark Script - Compare JOIN performance across 3NF, Star, and Snowflake schemas
"""

import time
import psycopg2
from typing import Dict
from tabulate import tabulate
from configs.config import DatabaseConfig


class SchemaBenchmark:
    """Compare query performance across different database schemas"""

    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.results = []

    def connect(self) -> None:
        """Connect to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("✓ Connected to PostgreSQL")
        except Exception as e:
            print(f"✗ Connection error: {e}")
            raise

    def disconnect(self) -> None:
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Connection closed")

    def execute_query(self, query: str, iterations: int = 5) -> float:
        """Execute query multiple times and return average time"""
        times = []

        for _ in range(iterations):
            start = time.time()
            try:
                self.cursor.execute(query)
                self.cursor.fetchall()  # Fetch results to ensure query completes
                elapsed = time.time() - start
                times.append(elapsed)
            except Exception as e:
                print(f"  ✗ Query failed: {e}")
                return -1

        # Return average time
        return sum(times) / len(times)

    def benchmark_revenue_by_product(self) -> None:
        """Benchmark: Revenue by Product query"""
        print("\n→ Benchmark 1: Revenue by Product")

        # 3NF Query
        query_3nf = """
        SELECT 
            p.id,
            p.title,
            c.name AS category,
            p.brand,
            COUNT(DISTINCT oi.order_id) AS total_orders,
            SUM(oi.quantity) AS total_quantity,
            SUM(oi.total) AS total_revenue
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        JOIN order_items oi ON p.id = oi.product_id
        GROUP BY p.id, p.title, c.name, p.brand
        ORDER BY total_revenue DESC
        LIMIT 50;
        """

        # Star Schema Query
        query_star = """
        SELECT 
            p.product_id,
            p.title,
            p.category,
            p.brand,
            COUNT(DISTINCT f.order_id) AS total_orders,
            SUM(f.quantity) AS total_quantity,
            SUM(f.total_amount) AS total_revenue
        FROM star_fact_orders f
        JOIN star_dim_products p ON f.product_id = p.product_id
        GROUP BY p.product_id, p.title, p.category, p.brand
        ORDER BY total_revenue DESC
        LIMIT 50;
        """

        # Snowflake Schema Query
        query_snowflake = """
        SELECT 
            p.product_id,
            p.title,
            c.category_name,
            b.brand_name,
            COUNT(DISTINCT f.order_id) AS total_orders,
            SUM(f.quantity) AS total_quantity,
            SUM(f.total_amount) AS total_revenue
        FROM snow_fact_orders f
        JOIN snow_dim_products p ON f.product_id = p.product_id
        LEFT JOIN snow_dim_categories c ON p.category_id = c.category_id
        LEFT JOIN snow_dim_brands b ON p.brand_id = b.brand_id
        GROUP BY p.product_id, p.title, c.category_name, b.brand_name
        ORDER BY total_revenue DESC
        LIMIT 50;
        """

        time_3nf = self.execute_query(query_3nf)
        time_star = self.execute_query(query_star)
        time_snowflake = self.execute_query(query_snowflake)

        self.results.append(
            {
                "query": "Revenue by Product",
                "3nf": time_3nf,
                "star": time_star,
                "snowflake": time_snowflake,
            }
        )

        print(f"  3NF:       {time_3nf:.4f}s")
        print(f"  Star:      {time_star:.4f}s")
        print(f"  Snowflake: {time_snowflake:.4f}s")

    def benchmark_top_users(self) -> None:
        """Benchmark: Top Users by spending"""
        print("\n→ Benchmark 2: Top Users by Spending")

        # 3NF Query
        query_3nf = """
        SELECT 
            u.id,
            u.username,
            CONCAT(u.first_name, ' ', u.last_name) AS full_name,
            COUNT(DISTINCT o.id) AS total_orders,
            SUM(o.total) AS lifetime_value
        FROM users u
        JOIN orders o ON u.id = o.user_id
        GROUP BY u.id, u.username, u.first_name, u.last_name
        ORDER BY lifetime_value DESC
        LIMIT 100;
        """

        # Star Schema Query
        query_star = """
        SELECT 
            u.user_id,
            u.username,
            u.full_name,
            COUNT(DISTINCT f.order_id) AS total_orders,
            SUM(f.total_amount) AS lifetime_value
        FROM star_fact_orders f
        JOIN star_dim_users u ON f.user_id = u.user_id
        GROUP BY u.user_id, u.username, u.full_name
        ORDER BY lifetime_value DESC
        LIMIT 100;
        """

        # Snowflake Schema Query
        query_snowflake = """
        SELECT 
            u.user_id,
            u.username,
            u.full_name,
            COUNT(DISTINCT f.order_id) AS total_orders,
            SUM(f.total_amount) AS lifetime_value
        FROM snow_fact_orders f
        JOIN snow_dim_users u ON f.user_id = u.user_id
        GROUP BY u.user_id, u.username, u.full_name
        ORDER BY lifetime_value DESC
        LIMIT 100;
        """

        time_3nf = self.execute_query(query_3nf)
        time_star = self.execute_query(query_star)
        time_snowflake = self.execute_query(query_snowflake)

        self.results.append(
            {
                "query": "Top Users",
                "3nf": time_3nf,
                "star": time_star,
                "snowflake": time_snowflake,
            }
        )

        print(f"  3NF:       {time_3nf:.4f}s")
        print(f"  Star:      {time_star:.4f}s")
        print(f"  Snowflake: {time_snowflake:.4f}s")

    def benchmark_monthly_revenue(self) -> None:
        """Benchmark: Monthly Revenue aggregation"""
        print("\n→ Benchmark 3: Monthly Revenue")

        # 3NF Query
        query_3nf = """
        SELECT 
            TO_CHAR(o.order_date, 'YYYY-MM') AS month,
            COUNT(DISTINCT o.id) AS orders_count,
            SUM(o.total) AS revenue
        FROM orders o
        GROUP BY TO_CHAR(o.order_date, 'YYYY-MM')
        ORDER BY month DESC;
        """

        # Star Schema Query
        query_star = """
        SELECT 
            d.year,
            d.month,
            d.month_name,
            COUNT(DISTINCT f.order_id) AS orders_count,
            SUM(f.total_amount) AS revenue
        FROM star_fact_orders f
        JOIN star_dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year DESC, d.month DESC;
        """

        # Snowflake Schema Query
        query_snowflake = """
        SELECT 
            d.year,
            d.month,
            d.month_name,
            COUNT(DISTINCT f.order_id) AS orders_count,
            SUM(f.total_amount) AS revenue
        FROM snow_fact_orders f
        JOIN snow_dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year DESC, d.month DESC;
        """

        time_3nf = self.execute_query(query_3nf)
        time_star = self.execute_query(query_star)
        time_snowflake = self.execute_query(query_snowflake)

        self.results.append(
            {
                "query": "Monthly Revenue",
                "3nf": time_3nf,
                "star": time_star,
                "snowflake": time_snowflake,
            }
        )

        print(f"  3NF:       {time_3nf:.4f}s")
        print(f"  Star:      {time_star:.4f}s")
        print(f"  Snowflake: {time_snowflake:.4f}s")

    def benchmark_complex_join(self) -> None:
        """Benchmark: Complex multi-table JOIN"""
        print("\n→ Benchmark 4: Complex Multi-Table JOIN")

        # 3NF Query (5-table JOIN)
        query_3nf = """
        SELECT 
            o.id AS order_id,
            u.username,
            p.title AS product_name,
            c.name AS category,
            a.city,
            oi.quantity,
            oi.total
        FROM orders o
        JOIN users u ON o.user_id = u.id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        LEFT JOIN categories c ON p.category_id = c.id
        LEFT JOIN addresses a ON u.address_id = a.id
        ORDER BY o.order_date DESC
        LIMIT 1000;
        """

        # Star Schema Query
        query_star = """
        SELECT 
            f.order_id,
            u.username,
            p.title AS product_name,
            p.category,
            l.city,
            f.quantity,
            f.total_amount
        FROM star_fact_orders f
        JOIN star_dim_users u ON f.user_id = u.user_id
        JOIN star_dim_products p ON f.product_id = p.product_id
        JOIN star_dim_date d ON f.date_id = d.date_id
        LEFT JOIN star_dim_location l ON f.location_id = l.location_id
        ORDER BY d.full_date DESC
        LIMIT 1000;
        """

        # Snowflake Schema Query (with sub-dimension navigation)
        query_snowflake = """
        SELECT 
            f.order_id,
            u.username,
            p.title AS product_name,
            c.category_name,
            ci.city_name,
            f.quantity,
            f.total_amount
        FROM snow_fact_orders f
        JOIN snow_dim_users u ON f.user_id = u.user_id
        JOIN snow_dim_products p ON f.product_id = p.product_id
        LEFT JOIN snow_dim_categories c ON p.category_id = c.category_id
        LEFT JOIN snow_dim_cities ci ON u.city_id = ci.city_id
        JOIN snow_dim_date d ON f.date_id = d.date_id
        ORDER BY d.full_date DESC
        LIMIT 1000;
        """

        time_3nf = self.execute_query(query_3nf)
        time_star = self.execute_query(query_star)
        time_snowflake = self.execute_query(query_snowflake)

        self.results.append(
            {
                "query": "Complex Multi-Table JOIN",
                "3nf": time_3nf,
                "star": time_star,
                "snowflake": time_snowflake,
            }
        )

        print(f"  3NF:       {time_3nf:.4f}s")
        print(f"  Star:      {time_star:.4f}s")
        print(f"  Snowflake: {time_snowflake:.4f}s")

    def benchmark_aggregation_heavy(self) -> None:
        """Benchmark: Aggregation-heavy query"""
        print("\n→ Benchmark 5: Heavy Aggregations")

        # 3NF Query
        query_3nf = """
        SELECT 
            COUNT(DISTINCT o.id) AS total_orders,
            COUNT(DISTINCT u.id) AS unique_customers,
            COUNT(DISTINCT p.id) AS unique_products,
            SUM(oi.quantity) AS total_items,
            SUM(o.total) AS total_revenue,
            AVG(o.total) AS avg_order_value,
            MIN(o.order_date) AS first_order,
            MAX(o.order_date) AS last_order
        FROM orders o
        JOIN users u ON o.user_id = u.id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id;
        """

        # Star Schema Query
        query_star = """
        SELECT 
            COUNT(DISTINCT f.order_id) AS total_orders,
            COUNT(DISTINCT f.user_id) AS unique_customers,
            COUNT(DISTINCT f.product_id) AS unique_products,
            SUM(f.quantity) AS total_items,
            SUM(f.total_amount) AS total_revenue,
            AVG(f.total_amount) AS avg_order_value,
            MIN(d.full_date) AS first_order,
            MAX(d.full_date) AS last_order
        FROM star_fact_orders f
        JOIN star_dim_date d ON f.date_id = d.date_id;
        """

        # Snowflake Schema Query
        query_snowflake = """
        SELECT 
            COUNT(DISTINCT f.order_id) AS total_orders,
            COUNT(DISTINCT f.user_id) AS unique_customers,
            COUNT(DISTINCT f.product_id) AS unique_products,
            SUM(f.quantity) AS total_items,
            SUM(f.total_amount) AS total_revenue,
            AVG(f.total_amount) AS avg_order_value,
            MIN(d.full_date) AS first_order,
            MAX(d.full_date) AS last_order
        FROM snow_fact_orders f
        JOIN snow_dim_date d ON f.date_id = d.date_id;
        """

        time_3nf = self.execute_query(query_3nf)
        time_star = self.execute_query(query_star)
        time_snowflake = self.execute_query(query_snowflake)

        self.results.append(
            {
                "query": "Heavy Aggregations",
                "3nf": time_3nf,
                "star": time_star,
                "snowflake": time_snowflake,
            }
        )

        print(f"  3NF:       {time_3nf:.4f}s")
        print(f"  Star:      {time_star:.4f}s")
        print(f"  Snowflake: {time_snowflake:.4f}s")

    def print_summary_table(self) -> None:
        """Print comparison table"""
        print("\n" + "=" * 100)
        print("SCHEMA PERFORMANCE COMPARISON")
        print("=" * 100)

        table_data = []
        for result in self.results:
            # Calculate relative performance
            times = [result["3nf"], result["star"], result["snowflake"]]
            min_time = min(times)

            table_data.append(
                [
                    result["query"],
                    f"{result['3nf']:.4f}s",
                    f"{result['star']:.4f}s",
                    f"{result['snowflake']:.4f}s",
                ]
            )

        headers = [
            "Query",
            "3NF Time",
            "Star Time",
            "Snowflake Time",
        ]

        print("\n" + tabulate(table_data, headers=headers, tablefmt="grid"))

        # Calculate averages
        avg_3nf = sum(r["3nf"] for r in self.results) / len(self.results)
        avg_star = sum(r["star"] for r in self.results) / len(self.results)
        avg_snowflake = sum(r["snowflake"] for r in self.results) / len(self.results)

        print("\n" + "=" * 100)
        print("AVERAGE QUERY TIME")
        print("=" * 100)
        print(f"  3NF:       {avg_3nf:.4f}s")
        print(f"  Star:      {avg_star:.4f}s")
        print(f"  Snowflake: {avg_snowflake:.4f}s")

        # Determine winner
        times_dict = {"3NF": avg_3nf, "Star": avg_star, "Snowflake": avg_snowflake}
        winner = min(times_dict, key=times_dict.get)

        print("\n" + "=" * 100)
        print(f"🏆 FASTEST SCHEMA: {winner}")
        print("=" * 100)

    def run_all_benchmarks(self) -> None:
        """Run all benchmarks"""
        print("\n")
        print("╔" + "=" * 98 + "╗")
        print("║" + " " * 30 + "SCHEMA PERFORMANCE BENCHMARK" + " " * 40 + "║")
        print("╚" + "=" * 98 + "╝")

        try:
            self.benchmark_revenue_by_product()
            self.benchmark_top_users()
            self.benchmark_monthly_revenue()
            self.benchmark_complex_join()
            self.benchmark_aggregation_heavy()

            self.print_summary_table()

        except Exception as e:
            print(f"\n❌ Benchmark failed: {e}")
            raise


def main():
    """Main benchmark runner"""

    db_config = DatabaseConfig.postgres()

    try:
        benchmark = SchemaBenchmark(db_config)
        benchmark.connect()
        benchmark.run_all_benchmarks()
        benchmark.disconnect()

        print("\n" + "=" * 100)
        print("🎉 Schema benchmark complete!")
        print("=" * 100 + "\n")

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    main()
