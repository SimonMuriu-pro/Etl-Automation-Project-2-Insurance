import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv
from utils.logger import logger

# Load environment variables once
load_dotenv()

def get_engine(retries=3, backoff=2):
    """
    Create and return a SQLAlchemy engine using environment variables,
    with retries and basic validation.
    """
    required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {missing_vars}")

    db_config = {
        'drivername': 'postgresql+psycopg2',
        'username': os.getenv("DB_USER"),
        'password': os.getenv("DB_PASSWORD"),
        'host': os.getenv("DB_HOST"),
        'port': os.getenv("DB_PORT"),
        'database': os.getenv("DB_NAME"),
    }

    connection_url = URL.create(**db_config)
    engine = None

    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(
                connection_url,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=1800,
                connect_args={"sslmode": "prefer"}  # ✅ Works for local and cloud
            )
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Successfully connected to PostgreSQL.")
            return engine

        except Exception as e:
            logger.error(f"Attempt {attempt} - DB connection failed: {e}", exc_info=True)
            if attempt < retries:
                time.sleep(backoff ** attempt)
            else:
                raise

# Standalone test
if __name__ == "__main__":
    try:
        engine = get_engine()
        print("✅ Database connection established.")
    except Exception:
        print("❌ Database connection failed.")
