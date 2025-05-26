import re
from logging import Logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf, explode, array_contains
from pyspark.sql.types import StringType
from src.config.settings import PATTERNS
from src.utils.logger import LOGGER

def _find_matching_fields(fields, pattern):
    """Find fields matching a specific pattern."""
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    return [field for field in fields if compiled_pattern.search(field)]

def _drop_unwanted_columns(df: DataFrame) -> DataFrame:
    """Drop unwanted columns from DataFrame."""
    if "placeSearchOpeningHours" in df.columns:
        return df.drop("placeSearchOpeningHours")
    return df

def _process_postal_code(df: DataFrame, logger: Logger) -> DataFrame:
    """Process postal code from address structure."""
    if "address" not in df.columns:
        logger.error("No address column found in DataFrame")
        return df.withColumn("postal_code", lit(None).cast(StringType()))

    address_fields = [field.name for field in df.schema["address"].dataType.fields]
    matching_fields = _find_matching_fields(address_fields, PATTERNS['postal'])
    
    if not matching_fields:
        logger.error("No valid postal code field found!")
        df = df.withColumn("postal_code", lit(None).cast(StringType()))
    else:
        postal_field = matching_fields[0]
        df = df.withColumn("postal_code", col(f"address.{postal_field}").cast(StringType()))
        
        missing_count = df.filter(col("postal_code").isNull()).count()
        if missing_count > 0:
            logger.warning(f"Found {missing_count} records without postal code")
    
    return df

def _add_province_mapping(df: DataFrame, logger: Logger) -> DataFrame:
    """
    Add province mapping based on postal codes.
    
    Args:
        df: DataFrame with postal_code column
        logger: Logger instance
        
    Returns:
        DataFrame with added province column
    """
    if "postal_code" not in df.columns:
        logger.error("No postal_code column found, cannot map provinces")
        return df.withColumn("province", lit(None).cast(StringType()))
        
    # Province mapping UDF
    @udf(returnType=StringType())
    def get_province(postal_code):
        if not postal_code:
            return None
        try:
            code = int(postal_code)
            if 1000 <= code <= 1299:
                return "Brussel"
            elif 1300 <= code <= 1499:
                return "Waals-Brabant"
            elif (1500 <= code <= 1999) or (3000 <= code <= 3499):
                return "Vlaams-Brabant"
            elif 2000 <= code <= 2999:
                return "Antwerpen"
            elif 3500 <= code <= 3999:
                return "Limburg"
            elif 4000 <= code <= 4999:
                return "Luik"
            elif 5000 <= code <= 5999:
                return "Namen"
            elif (6000 <= code <= 6599) or (7000 <= code <= 7999):
                return "Henegouwen"
            elif 6600 <= code <= 6999:
                return "Luxemburg"
            elif 8000 <= code <= 8999:
                return "West-Vlaanderen"
            elif 9000 <= code <= 9999:
                return "Oost-Vlaanderen"
            return None
        except (ValueError, TypeError):
            return None
            
    # Create province column
    df = df.withColumn("province", get_province(col("postal_code")))
    
    # Log province distribution
    province_counts = df.groupBy("province").count().collect()
    logger.info(f"Province distribution:")
    for row in province_counts:
        province = row['province'] if row['province'] else 'Unknown'
        logger.info(f"Province {province}: {row['count']} locations")
    
    return df

def _process_coordinates(df: DataFrame, logger: Logger) -> DataFrame:
    """Process geographical coordinates."""
    if "geoCoordinates" not in df.columns:
        logger.warning("No geoCoordinates column found")
        return df.withColumn("lat", lit(None).cast("double")) \
                .withColumn("lon", lit(None).cast("double"))

    coord_fields = [field.name for field in df.schema["geoCoordinates"].dataType.fields]
    logger.info(f"Available coordinate fields: {coord_fields}")

    lat_fields = _find_matching_fields(coord_fields, PATTERNS['latitude'])
    lon_fields = _find_matching_fields(coord_fields, PATTERNS['longitude'])

    if lat_fields and lon_fields:
        df = df.withColumn("lat", col(f"geoCoordinates.{lat_fields[0]}").cast("double"))
        df = df.withColumn("lon", col(f"geoCoordinates.{lon_fields[0]}").cast("double"))
        
        # Log coordinates statistics
        null_coords = df.filter(col("lat").isNull() | col("lon").isNull()).count()
        if null_coords > 0:
            logger.warning(f"Found {null_coords} records with missing coordinates")
    else:
        logger.error("Could not find latitude/longitude fields")
        df = df.withColumn("lat", lit(None).cast("double"))
        df = df.withColumn("lon", lit(None).cast("double"))

    # Drop the original geoCoordinates column if it exists and was processed
    if "geoCoordinates" in df.columns and (lat_fields and lon_fields):
        df = df.drop("geoCoordinates")
        logger.info("Dropped original geoCoordinates column after processing")
    elif "geoCoordinates" in df.columns and not (lat_fields and lon_fields):
        logger.warning("Retaining geoCoordinates column as lat/lon fields were not found.")

    return df

def _one_hot_encode_services(df: DataFrame, logger: Logger) -> DataFrame:
    """
    One-hot encode handover services.
    
    Args:
        df: Input DataFrame
        logger: Logger instance
        
    Returns:
        DataFrame with one-hot encoded service columns
    """
    # Handle handoverServices
    if "handoverServices" in df.columns:
        # Get all unique services from the data using DataFrame operations
        services_df = (df
            .select(explode(col("handoverServices")).alias("services"))
            .distinct())
            
        # Convert to list for processing
        unique_services = [row.services for row in services_df.collect() if row.services]
        
        if unique_services:
            logger.info(f"Found services: {unique_services}")
            df = df.withColumn("has_handoverServices", lit(1).cast("int"))
            
            # Create one-hot encoded columns for each service
            for service in unique_services:
                column_name = f"has_{service.lower()}"
                df = df.withColumn(
                    column_name,
                    array_contains(col("handoverServices"), service).cast("int")
                ).fillna({column_name: 0})  # Fill null values with 0
            
            # Log services distribution
            for service in unique_services:
                count = df.filter(col(f"has_{service.lower()}") == 1).count()
                logger.info(f"Service {service}: {count} locations")
        else:
            logger.warning("No services found in handoverServices")
            # Set default to 0 to indicate no services are available
            df = df.withColumn("has_handoverServices", lit(0).cast("int"))
    else:
        logger.warning("No handoverServices column found")
        # Set default to 0 to indicate no services are available
        df = df.withColumn("has_handoverServices", lit(0).cast("int"))
    
    # Drop the original handoverServices column if it was processed or if it was absent initially
    if "handoverServices" in df.columns:
        df = df.drop("handoverServices")
        logger.info("Dropped original handoverServices column after processing")

    return df

def _anonymize_address(df: DataFrame, logger: Logger) -> DataFrame:
    """Anonymize sensitive address information."""
    if "address" not in df.columns:
        logger.warning("No address column found, skipping anonymization")
        return df

    anonymize = udf(lambda x: "***" if x else x, StringType())
    address_fields = [field.name for field in df.schema["address"].dataType.fields]

    # Find and anonymize house number fields
    house_fields = _find_matching_fields(address_fields, PATTERNS['house'])
    for field in house_fields:
        df = df.withColumn(f"{field}", anonymize(col(f"address.{field}")))
        logger.info(f"Anonymized house field: {field}")

    # Find and anonymize street name fields
    street_fields = _find_matching_fields(address_fields, PATTERNS['street'])
    for field in street_fields:
        df = df.withColumn(f"{field}", anonymize(col(f"address.{field}")))
        logger.info(f"Anonymized street field: {field}")

    return df

def transform_data_by_brand(df: DataFrame, logger: Logger = LOGGER) -> DataFrame:
    """Transform the input DataFrame by applying all necessary transformations.
    
    This function orchestrates all transformation steps in the correct order:
    1. Drop unwanted columns
    2. Process postal codes
    3. Add province mapping
    4. Process coordinates
    5. One-hot encode services
    6. Anonymize address data
    
    Args:
        df: Input DataFrame to transform (already combined from extract)
        logger: Logger instance for logging
        
    Returns:
        Transformed DataFrame with all processing steps applied
        
    Raises:
        Exception: If any transformation step fails
    """
    try:
        # Drop unwanted columns
        df = _drop_unwanted_columns(df)
        logger.info("Dropped unwanted columns")
        
        # Process postal code
        df = _process_postal_code(df, logger)
        logger.info("Processed postal codes")
        
        # Add province mapping
        df = _add_province_mapping(df, logger)
        logger.info("Added province mapping")
        
        # Process coordinates
        df = _process_coordinates(df, logger)
        logger.info("Processed coordinates")
        
        # One-hot encode services
        df = _one_hot_encode_services(df, logger)
        logger.info("One-hot encoded services")
        
        # Anonymize address
        df = _anonymize_address(df, logger)
        logger.info("Anonymized address data")
        
        logger.info("Data transformation completed successfully")
        return df
        
    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}")
        raise 