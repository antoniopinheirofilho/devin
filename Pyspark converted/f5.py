from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import math

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F5").getOrCreate()

# Create widgets for table configuration
spark.sql("CREATE WIDGET TEXT catalog DEFAULT 'default'")
spark.sql("CREATE WIDGET TEXT schema DEFAULT 'default'")
spark.sql("CREATE WIDGET TEXT table DEFAULT 'probability'")

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
    catalog = spark.sql("GET WIDGET catalog").collect()[0][0]
    schema = spark.sql("GET WIDGET schema").collect()[0][0]
    table = spark.sql("GET WIDGET table").collect()[0][0]
    
    # Save DataFrame to Unity Catalog
    result_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")
    
    # Display results in SAS format
    result_df.show(truncate=False, n=20, vertical=False)

if __name__ == "__main__":
    calculate_probability()
