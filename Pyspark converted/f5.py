from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import math

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F5").getOrCreate()

def calculate_probability():
    # Create a DataFrame with the calculation
    data = [(1,)]  # dummy row
    df = spark.createDataFrame(data, ["dummy"])
    
    # Calculate probability using normal distribution
    # Equivalent to SAS probnorm(-15/sqrt(325))
    pr = math.erf(-15/math.sqrt(325)/math.sqrt(2))/2 + 0.5
    
    # Create result DataFrame
    result_df = spark.createDataFrame([(pr,)], ["pr"])
    result_df.show()

if __name__ == "__main__":
    calculate_probability()
