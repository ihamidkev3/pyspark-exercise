"""Main entry point for brand data processing."""

from typing import Union, Iterable
from src.config.spark_session import get_spark_session
from src.etl.extract import extract_data_by_brand
from src.etl.transform import transform_data_by_brand
from src.etl.load import load_data_by_brand
from src.utils.logger import LOGGER
from logging import Logger

def get_data_by_brand(brands: Union[str, Iterable[str]], logger: Logger = LOGGER):
    """Process brand location data through ETL pipeline.

    Args:
        brand: allowed values are (clp, okay, spar, dats, cogo)
            - Brand name(s) in any of these formats:
               - Single brand name string ("clp")
               - Comma-separated string ("clp, dats")
               - List/Tuple/Set of brands (["clp", "dats"], ("clp", "dats"))
        logger: Logger object for logging

    Returns:
        The relevant dataframe
    """
    try:
        # Initialize Spark
        spark = get_spark_session()
        logger.info(f"Starting data processing for brands: {brands}")

        # Extract data - handles multiple input formats
        df = extract_data_by_brand(spark, brands, logger)

        # Transform data - works with the combined DataFrame
        df = transform_data_by_brand(df, logger)
    
        # Load data - saves the combined DataFrame
        df = load_data_by_brand(df, logger)

        logger.info(f"Completed processing for brands: {brands}")
        return df
        
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    get_data_by_brand("clp")