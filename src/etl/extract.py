from glob import glob
from typing import Union, Iterable
from logging import Logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, array, col, expr
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from src.utils.logger import LOGGER
from src.config.settings import VALID_BRANDS

class BrandExtractionError(Exception):
    """Custom exception for brand extraction errors."""
    pass

def _validate_brands(brands: Union[str, Iterable[str]], logger: Logger) -> list[str]:
    """
    Validate brand names against VALID_BRANDS.
    
    Args:
        brands: Brand names in any of these formats:
               - Single brand name string ("clp")
               - Comma-separated string ("clp, dats")
               - Iterable of brands (["clp", "dats"] or ("clp", "dats"))
        logger: Logger instance
        
    Returns:
        List of validated brand names
        
    Raises:
        BrandExtractionError: If any brand is invalid
    """
    if isinstance(brands, str):
        brand_list = [b.strip().strip("'\"") for b in brands.split(',')]
    elif hasattr(brands, '__iter__') and not isinstance(brands, (str, bytes, bytearray)):
        brand_list = [str(b).strip().strip("'\"") for b in brands]
    else:
        error_msg = f"Invalid input type for brands: {type(brands)}. Expected string or iterable."
        logger.error(error_msg)
        raise BrandExtractionError(error_msg)
    
    brand_list = [b.lower() for b in brand_list if b]
    
    if not brand_list:
        error_msg = "No valid brand names provided after processing input"
        logger.error(error_msg)
        raise BrandExtractionError(error_msg)
    
    brand_list = list(dict.fromkeys(brand_list))  # Remove duplicates while preserving order
    
    invalid_brands = [b for b in brand_list if b not in VALID_BRANDS]
    if invalid_brands:
        error_msg = f"Invalid brands: {invalid_brands}. Valid brands are {VALID_BRANDS}"
        logger.error(error_msg)
        raise BrandExtractionError(error_msg)
    
    logger.debug(f"Validated brands: {brand_list}")
    return brand_list

def _normalize_selling_partners(df: DataFrame, logger: Logger) -> DataFrame:
    """
    Normalize sellingPartners column to ensure consistent ARRAY<STRING> type.
    Automatically detects and resolves type mismatches dynamically.

    Args:
        df: Input DataFrame
        logger: Logger instance

    Returns:
        DataFrame with normalized sellingPartners column
    """
    try:
        target_type = ArrayType(StringType())

        # If column is missing, add an empty array of strings
        if "sellingPartners" not in df.columns:
            logger.debug("Adding missing sellingPartners column as empty ARRAY<STRING>")
            return df.withColumn("sellingPartners", lit(array()).cast(target_type))

        # Get current column type
        current_type = df.schema["sellingPartners"].dataType

        # If already correct type, return as is
        if current_type == target_type:
            return df

        # Convert any other type to ARRAY<STRING>
        logger.warning(f"Converting sellingPartners from {current_type} to ARRAY<STRING>")
        return df.withColumn("sellingPartners", col("sellingPartners").cast(target_type))

    except Exception as e:
        logger.error(f"Error normalizing sellingPartners: {str(e)}")
        raise

def _normalize_temporary_closures(df: DataFrame, logger: Logger) -> DataFrame:
    """
    Converts temporaryClosures to ARRAY<STRUCT<from: STRING, till: STRING>> if needed.

    Args:
        df: Input DataFrame
        logger: Logger instance

    Returns:
        DataFrame with standardized temporaryClosures column.
    """
    target_type = ArrayType(StructType([
        StructField("from", StringType(), True),
        StructField("till", StringType(), True)
    ]))

    if "temporaryClosures" not in df.columns:
        logger.debug("Adding missing temporaryClosures column as empty ARRAY<STRUCT>")
        return df.withColumn("temporaryClosures", lit([]).cast(target_type))

    current_type = df.schema["temporaryClosures"].dataType

    if current_type == target_type:
        return df

    # Convert ARRAY<STRING> to ARRAY<STRUCT<from: STRING, till: STRING>>
    if isinstance(current_type, ArrayType) and current_type.elementType == StringType():
        logger.debug("Converting temporaryClosures from ARRAY<STRING> to ARRAY<STRUCT>")
        return df.withColumn(
            "temporaryClosures",
            expr("transform(temporaryClosures, x -> struct(x as `from`, NULL as till))").cast(target_type)
        )

    logger.warning(f"Unexpected temporaryClosures type: {current_type}, converting to empty array")
    return df.withColumn("temporaryClosures", lit([]).cast(target_type))

def _add_brand_column(df: DataFrame, brand: str) -> DataFrame:
    """
    Add brand column to DataFrame.
    
    Args:
        df: Input DataFrame
        brand: Brand name to add
        
    Returns:
        DataFrame with brand column added
    """
    return df.withColumn("brand", lit(brand))

def extract_data_by_brand(
    spark: SparkSession, 
    brands: Union[str, Iterable[str]], 
    logger: Logger = LOGGER
) -> DataFrame:
    """
    Load JSON data for one or multiple brands.
    
    Args:
        spark: SparkSession instance
        brands: Brand names in any of these formats:
               - Single brand name string ("clp")
               - Comma-separated string ("clp, dats")
               - Iterable of brands (["clp", "dats"] or ("clp", "dats"))
        logger: Logger object for logging
        
    Returns:
        Spark DataFrame with combined brand data
        
    Raises:
        BrandExtractionError: If no valid data files are found
    """
    try:
        brand_list = _validate_brands(brands, logger)
        logger.info(f"Processing brands: {brand_list}")
        
        brand_dfs = {}
        
        # First, process all files for each brand
        for brand in brand_list:
            try:
                json_files = glob(f"{brand}-*.json")
                if not json_files:
                    logger.warning(f"No JSON files found for brand {brand}")
                    continue
                
                # Process all files for this brand
                brand_df = None
                for file_path in json_files:
                    logger.info(f"Processing file: {file_path}")
                    try:
                        current_df = spark.read.json(file_path)
                        current_df = _add_brand_column(current_df, brand)
                        
                        # Normalize schema components
                        current_df = _normalize_selling_partners(current_df, logger)
                        current_df = _normalize_temporary_closures(current_df, logger)
                        
                        if brand_df is None:
                            brand_df = current_df
                        else:
                            # Union files from the same brand
                            brand_df = brand_df.unionByName(current_df, allowMissingColumns=True)
                            
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {str(e)}")
                        continue
                
                if brand_df is not None:
                    brand_dfs[brand] = brand_df
                    logger.info(f"Successfully processed all files for brand: {brand}")
                
            except Exception as e:
                logger.error(f"Error processing brand {brand}: {str(e)}")
                continue
        
        # Check if we have any data
        if not brand_dfs:
            error_msg = f"Failed to load any data for brands: {brand_list}"
            logger.error(error_msg)
            raise BrandExtractionError(error_msg)
        
        # Now union all brand DataFrames together
        result_df = None
        for brand, df in brand_dfs.items():
            if result_df is None:
                result_df = df
            else:
                # Union across different brands
                result_df = result_df.unionByName(df, allowMissingColumns=True)
        
        # Log final statistics
        total_records = result_df.count()
        brand_counts = result_df.groupBy("brand").count().collect()
        logger.info(f"Total records processed: {total_records}")
        for row in brand_counts:
            logger.info(f"Brand {row['brand']}: {row['count']} records")
        
        return result_df
        
    except Exception as e:
        logger.error(f"An unexpected error occurred during data extraction: {str(e)}")
        raise