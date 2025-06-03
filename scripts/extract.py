import pandas as pd
from scripts.db_connection import get_engine
from utils.logger import logger

# ---------- CONFIGURABLE TABLES ----------
TABLES_TO_EXTRACT = [
    "insurance_customers",
    "insurance_claims",
    "insurance_feedback",
    "insurance_payments",
    "insurance_policies"
]

def is_valid_table_name(name):
    """Basic validation: table name must be alphanumeric or underscore."""
    import re
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name))

# ---------- EXTRACT FUNCTION ----------
def extract_data():
    """
    Extract specified tables from PostgreSQL into pandas DataFrames.

    Returns:
        dict: {table_name: DataFrame} if successful, else None.
    """
    try:
        engine = get_engine()
        if engine is None:
            logger.error("❌No database engine available. Extraction aborted.")
            return None

        extracted_data = {}
        failed_tables = []

        for table in TABLES_TO_EXTRACT:
            if not is_valid_table_name(table):
                logger.error(f"Invalid table name detected, skipping extraction: {table}")
                failed_tables.append(table)
                continue

            try:
                query = f"SELECT * FROM {table}"
                df = pd.read_sql_query(query, con=engine)
                extracted_data[table] = df
                logger.info(f"✅Extracted table: {table} with {len(df)} rows")
            except Exception as e:
                logger.error(f"❌Failed to extract table '{table}': {e}", exc_info=True)
                failed_tables.append(table)

        if extracted_data:
            logger.info("Data extraction completed for available tables.")
            if failed_tables:
                logger.warning(f"Extraction failed for tables: {failed_tables}")
            return extracted_data
        else:
            logger.warning("❌No tables were successfully extracted.")
            return None

    except Exception as e:
        logger.error("❌Unexpected error during data extraction.", exc_info=True)
        return None

# ---------- STANDALONE TEST ----------
if __name__ == "__main__":
    extracted = extract_data()
    if extracted:
        for table, df in extracted.items():
            print(f"{table}: {len(df)} rows extracted")
    else:
        print("❌Extraction failed.")
