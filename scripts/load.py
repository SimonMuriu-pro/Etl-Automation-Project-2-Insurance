import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from config.config import DB_CONFIG, SCHEMA_NAME
from utils.logger import logger
import traceback


# ---------- DB CONNECTION ----------
def get_engine():
    try:
        db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"]
        )
        engine = create_engine(db_url, pool_pre_ping=True)
        logger.info("✅ Database engine created.")
        return engine
    except Exception as e:
        logger.exception("❌ Failed to create database engine.")
        raise


# ---------- CREATE SCHEMA ----------
def create_schema(engine, schema_name):
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            logger.info(f"✅ Schema '{schema_name}' verified or created.")
    except Exception as e:
        logger.exception(f"❌ Failed to create schema '{schema_name}'.")
        raise


# ---------- LOAD TO POSTGRES ----------
def load_cleaned_data(cleaned_dataframes, engine, schema, if_exists="replace"):
    for table_name, df in cleaned_dataframes.items():
        if df.empty:
            logger.warning(f"❌ DataFrame '{table_name}' is empty. Skipping load.")
            continue

        try:
            logger.info(f"📥 Preparing to load table '{table_name}' with {len(df)} rows.")
            logger.debug(f"Dtypes for '{table_name}':\n{df.dtypes}")
            logger.debug(f"Sample rows from '{table_name}':\n{df.head(3).to_dict(orient='records')}")

            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    raise TypeError(
                        f"Column '{col}' in table '{table_name}' contains dict or list types, "
                        "which are not supported by SQLAlchemy."
                    )

            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,  # Replaces table every ETL run
                index=False
            )
            logger.info(f"✅ Loaded {len(df)} rows into {schema}.{table_name}")

        except Exception as e:
            logger.error(f"❌ Failed to load table '{table_name}'. Error: {e}")
            logger.debug(traceback.format_exc())
            raise


# ---------- MAIN ENTRY POINT ----------
def main(cleaned_dataframes):
    engine = None
    try:
        engine = get_engine()
        create_schema(engine, SCHEMA_NAME)
        load_cleaned_data(cleaned_dataframes, engine, SCHEMA_NAME)
        logger.info("✅ Data load complete.")
        return True
    except Exception as e:
        logger.error(f"❌ Data load failed: {e}")
        return False
    finally:
        if engine:
            try:
                engine.dispose()
                logger.info("✅ Database engine disposed.")
            except Exception:
                logger.warning("⚠️ Engine disposal failed or was not initialized.")
