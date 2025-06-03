import time
from utils.logger import logger
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import main as load_cleaned_main
from config.config import CLEANING_CONFIG  # <-- updated import

# ---------- RUN ETL PIPELINE ----------
def run_etl():
    start_time = time.time()
    logger.info("🚀 Starting ETL pipeline...")

    try:
        # Step 1: Extract raw data
        logger.info("📥 Extracting data...")
        raw_dataframes = extract_data()

        if not raw_dataframes or not isinstance(raw_dataframes, dict):
            logger.error("❌ Data extraction failed or returned empty/invalid data.")
            return

        logger.info(f"✅ Extracted {len(raw_dataframes)} datasets. Proceeding to transformation.")

        # Step 2: Transform and clean data
        logger.info("🧹 Transforming data...")
        cleaned_dataframes = transform_data(raw_dataframes, CLEANING_CONFIG)

        if not cleaned_dataframes or not isinstance(cleaned_dataframes, dict):
            logger.error("❌ Transformation failed or returned empty/invalid data.")
            return

        # Rename tables to *_cleaned to avoid FK conflicts on load
        cleaned_dataframes = {
            f"{table_name}_cleaned": df
            for table_name, df in cleaned_dataframes.items()
        }

        logger.info(f"✅ Transformed {len(cleaned_dataframes)} datasets. Proceeding to loading.")

        # Step 3: Load cleaned data into PostgreSQL
        logger.info("📤 Loading cleaned data into PostgreSQL...")
        load_cleaned_main(cleaned_dataframes)  # <-- Direct call; no return value check

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"✅ ETL pipeline completed successfully in {elapsed} seconds.")

    except Exception as e:
        logger.exception(f"❌ ETL pipeline failed due to an unexpected error: {e}")

# ---------- MAIN ENTRY ----------
if __name__ == "__main__":
    run_etl()
