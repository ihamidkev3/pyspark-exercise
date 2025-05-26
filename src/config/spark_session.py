from pyspark.sql import SparkSession

def get_spark_session(app_name="Brand Processing"):
    """Initialize and return a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()