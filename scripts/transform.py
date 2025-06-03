import pandas as pd
from utils.logger import logger  # centralized logger


# --- Missing Values Handling ---

def drop_null_columns(df, columns_config, drop_threshold):
    null_ratios = df.isnull().mean()
    critical_cols = [col for col, conf in columns_config.items() if conf.get('critical', False)]
    drop_cols = null_ratios[(null_ratios > drop_threshold) & (~null_ratios.index.isin(critical_cols))].index.tolist()

    df = df.drop(columns=drop_cols)
    logger.info(f"✅ Dropped columns above threshold {drop_threshold}: {drop_cols}")
    return df


def drop_null_rows(df, drop_threshold, columns_config):
    critical_cols = [col for col, conf in columns_config.items() if conf.get('critical', False)]
    missing_critical_cols = [col for col in critical_cols if col not in df.columns]

    if missing_critical_cols:
        logger.error(f"❌ Critical columns missing from DataFrame: {missing_critical_cols}")
        raise ValueError(f"Critical columns missing from DataFrame: {missing_critical_cols}")

    # Drop rows missing any critical column
    critical_mask = df[critical_cols].notnull().all(axis=1)

    # Drop rows where overall null ratio exceeds threshold
    row_null_ratios = df.isnull().mean(axis=1)
    row_mask = row_null_ratios <= drop_threshold

    combined_mask = critical_mask & row_mask
    dropped_count = len(df) - combined_mask.sum()

    df = df.loc[combined_mask].copy()
    logger.info(f"✅ Dropped {dropped_count} rows missing critical columns or exceeding null threshold {drop_threshold}")
    return df


def impute_missing_values(df, columns_config):
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count == 0:
            continue

        col_conf = columns_config.get(col, {})
        strategy = col_conf.get('impute', 'default')

        if strategy == 'skip':
            logger.info(f"✅ Skipped imputation for critical column: {col}")
            continue

        if strategy == 'median':
            value = df[col].median() if not df[col].dropna().empty else 0
        elif strategy == 'mode':
            value = df[col].mode().iloc[0] if not df[col].mode().empty else 'Unknown'
        elif strategy == 'constant':
            value = col_conf.get('value', 'Unknown')
        elif strategy == 'default':
            if pd.api.types.is_numeric_dtype(df[col]):
                value = df[col].median() if not df[col].dropna().empty else 0
            else:
                value = 'Unknown'
        else:
            logger.error(f"❌ Unknown imputation strategy '{strategy}' for column '{col}'")
            raise ValueError(f"Unknown imputation strategy '{strategy}' for column '{col}'")

        df[col] = df[col].fillna(value)
        logger.info(f"✅ Imputed '{col}' with strategy '{strategy}', value: {value}")

    return df


# --- Data Type Standardization ---

def standardize_data_types(df, columns_config):
    for col, conf in columns_config.items():
        if col not in df.columns:
            continue

        dtype = conf.get('dtype')
        try:
            if dtype == 'datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif dtype == 'int':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif dtype == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif dtype == 'string':
                df[col] = df[col].astype(str).str.strip()
            logger.info(f"✅ Standardized '{col}' to {dtype}")
        except Exception as e:
            logger.error(f"❌ Error converting '{col}' to {dtype}: {e}", exc_info=True)

    return df


# --- Text Cleaning ---

def clean_text_fields(df, columns_config=None):
    text_cols = df.select_dtypes(include='object').columns

    for col in text_cols:
        if columns_config:
            dtype = columns_config.get(col, {}).get('dtype')
            if dtype == 'datetime':
                continue

        try:
            cleaned = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()
                .str.replace(r"[^a-z0-9\s\.,'-]", '', regex=True)
            )
            df[col] = cleaned
            logger.info(f"✅ Cleaned text column: {col}")
        except Exception as e:
            logger.error(f"❌ Error cleaning text in column '{col}': {e}", exc_info=True)

    return df


# --- Remove duplicates ---

def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    logger.info(f"✅ Removed {before - after} duplicate rows")
    return df


# --- Column Naming ---

def enforce_column_naming(df):
    try:
        original = df.columns.tolist()
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r'[^\w\s]', '', regex=True)
            .str.replace(r'\s+', '_', regex=True)
        )
        logger.info(f"✅ Standardized column names: {dict(zip(original, df.columns))}")
    except Exception as e:
        logger.error(f"❌ Error enforcing column naming: {e}", exc_info=True)
    return df


# --- Main Cleaning Pipeline ---

def clean_table(df, table_name, config):
    tables_config = config.get('tables', {})
    if table_name not in tables_config:
        logger.error(f"❌ No config found for table: {table_name}")
        raise ValueError(f"No config found for table: {table_name}")

    table_config = tables_config[table_name]
    columns_config = table_config.get('columns', {})

    try:
        df = enforce_column_naming(df)
        df = standardize_data_types(df, columns_config)
        df = drop_null_columns(df, columns_config, config.get('drop_column_threshold', 0.5))
        df = drop_null_rows(df, config.get('drop_row_threshold', 0.5), columns_config)
        df = impute_missing_values(df, columns_config)
        df = clean_text_fields(df, columns_config)
        df = remove_duplicates(df)
        logger.info(f"✅ Finished cleaning table: {table_name}")
    except Exception as e:
        logger.error(f"❌ Cleaning failed for table '{table_name}': {e}", exc_info=True)
        raise

    return df


def transform_data(raw_dataframes, config):
    cleaned_dataframes = {}
    for table_name, df in raw_dataframes.items():
        try:
            cleaned_df = clean_table(df, table_name, config)
            cleaned_dataframes[table_name] = cleaned_df
        except Exception as e:
            logger.error(f"❌ Failed to clean table '{table_name}': {e}", exc_info=True)
            raise
    return cleaned_dataframes
