"""Spark job for processing sales data - No PostgreSQL needed."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from data_engineering.logger import configure_logging, get_logger
from data_engineering.spark.spark_session import get_spark_session, stop_spark_session
from data_engineering.spark.extractors import SparkCSVExtractor
from data_engineering.spark.transformers import SparkCleaningTransformer


def main() -> int:
    """Run sales processing with Spark - outputs to Parquet."""
    configure_logging()
    logger = get_logger("spark.job.sales")
    
    try:
        spark = get_spark_session("process_sales", memory="2g")
        
        # Create sample data if it doesn't exist
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        csv_file = data_dir / "sales.csv"
        
        if not csv_file.exists():
            csv_file.write_text("""sale_id,product_id,customer_id,sale_date,quantity,unit_price,total_amount
1,101,1001,2024-01-15,2,29.99,59.98
2,102,1002,2024-01-15,1,49.99,49.99
3,101,1003,2024-01-16,3,29.99,89.97
4,103,1001,2024-01-16,1,99.99,99.99
5,102,1004,2024-01-17,2,49.99,99.98""")
            logger.info(f"Created sample data: {csv_file}")
        
        # Extract
        logger.info("Extracting sales data")
        extractor = SparkCSVExtractor("sales", str(csv_file), header=True, infer_schema=True)
        df = extractor.extract()
        
        # Transform
        logger.info("Transforming data")
        transformer = SparkCleaningTransformer("clean", drop_duplicates=True, fill_null_strategy="drop")
        clean_df = transformer.transform(df)
        
        # Load to Parquet (no PostgreSQL needed)
        output_path = "data/output_sales"
        logger.info(f"Writing to Parquet: {output_path}")
        clean_df.write.mode("overwrite").parquet(output_path)
        
        rows = clean_df.count()
        logger.info(f"Job complete. Wrote {rows} rows to {output_path}/")
        
        # Show preview
        clean_df.show(5, truncate=False)
        
        return 0
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        stop_spark_session()


if __name__ == "__main__":
    sys.exit(main())