from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.dbutils import DBUtils
import math

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F5").getOrCreate()
dbutils = DBUtils(spark)

# Create widgets for table configuration
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("table", "table_output")

def calculate_probability():
    """
    SAS vs PySpark Implementation Differences:
    - SAS uses proc print for output display
    - PySpark uses DataFrame show() for display
    - SAS probnorm function is replaced with Python's math.erf
    - Results are saved to Unity Catalog table with configurable naming
    """
    # Calculate probability using normal distribution
    # Equivalent to SAS probnorm(-15/sqrt(325))
    pr = math.erf(-15/math.sqrt(325)/math.sqrt(2))/2 + 0.5
    
    # Create result DataFrame
    result_df = spark.createDataFrame([(pr,)], ["pr"])
    
    # Get table configuration from widgets
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    table = dbutils.widgets.get("table")
    
    # Save DataFrame to Unity Catalog
    result_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")
    
    # Display results in SAS format
    result_df.show(truncate=False, n=20, vertical=False)

if __name__ == "__main__":
    calculate_probability()
