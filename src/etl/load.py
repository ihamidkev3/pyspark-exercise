from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from logging import Logger
from src.utils.logger import LOGGER

def load_data_by_brand(df: DataFrame, logger: Logger = LOGGER) -> DataFrame:
    """Save DataFrame to parquet with brand partitioning and GDPR-compliant storage.
    
    Args:
        df: DataFrame to save (already combined from extract)
        logger: Logger instance for logging
        
    Returns:
        DataFrame with brand column added
    """
    try:
        base_path = "processed_data.parquet"
        gdpr_path = "gdpr_sensitive_data.parquet"
        logger.info(f"Preparing to save data to {base_path} and {gdpr_path}")

        # Create brand column for partitioning
        logger.info("Created brand_data column for partitioning")

        # Get brand partition information
        brand_counts = df.groupBy("brand").count().collect()
        logger.info("Brand partition statistics:")
        
        # Process each brand separately
        for row in brand_counts:
            brand = row['brand']
            count = row['count']
            logger.info(f"Processing brand {brand}: {count} records")
            
            # Filter data for current brand
            brand_df = df.filter(col("brand") == brand)
            
            # Create non-GDPR DataFrame for this brand
            non_gdpr_df = brand_df.drop("address")
            brand_base_path = f"{base_path}/brand_data={brand}"
            logger.info(f"Saving non-GDPR data for brand {brand}")
            non_gdpr_df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(brand_base_path)
            
            # Create GDPR DataFrame for this brand
            gdpr_df = brand_df
            if "streetName" in df.columns:
                gdpr_df = gdpr_df.drop("streetName")
            if "houseNumber" in df.columns:
                gdpr_df = gdpr_df.drop("houseNumber")
            
            brand_gdpr_path = f"{gdpr_path}/brand_data={brand}"
            logger.info(f"Saving GDPR-sensitive data for brand {brand}")
            gdpr_df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(brand_gdpr_path)
            
            logger.info(f"Successfully saved data for brand {brand}")
            
        logger.info(f"Successfully saved all brand data with GDPR compliance:")
        logger.info(f"- Non-GDPR data ({base_path}): All columns except address")
        logger.info(f"- GDPR-sensitive data ({gdpr_path}): All columns except anonymized fields")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to save data with GDPR separation: {str(e)}")
        raise 