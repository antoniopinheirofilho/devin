from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F4").getOrCreate()

# Create widgets for table configuration
spark.sql("CREATE WIDGET TEXT catalog DEFAULT 'default'")
spark.sql("CREATE WIDGET TEXT schema DEFAULT 'default'")
spark.sql("CREATE WIDGET TEXT table DEFAULT 'greetings'")

class Greeting:
    """
    SAS vs PySpark Implementation Differences:
    - SAS uses package and data program scope
    - PySpark uses Python class with instance variables
    - SAS thread program is replaced with DataFrame operations
    - Table saving uses Unity Catalog with configurable naming
    """
    def __init__(self, message=None):
        self._message = message if message else "This is the default greeting."
    
    def greet(self):
        print(self._message)
    
    def set_message(self, message):
        self._message = message

def create_greetings_table():
    """
    Creates a DataFrame with greetings, equivalent to SAS data program #1
    """
    data = [
        ("Hello World!",),
        ("What's new?",),
        ("Good-bye World!",)
    ]
    schema = StructType([StructField("message", StringType(), True)])
    return spark.createDataFrame(data, schema)

def main():
    # Get table configuration from widgets
    catalog = spark.sql("GET WIDGET catalog").collect()[0][0]
    schema = spark.sql("GET WIDGET schema").collect()[0][0]
    table = spark.sql("GET WIDGET table").collect()[0][0]
    
    # Create the greetings table
    greetings_df = create_greetings_table()
    
    # Save DataFrame to Unity Catalog
    greetings_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")
    
    # Create a greeting instance with default message and process greetings
    g = Greeting()
    g.greet()
    
    # Process each greeting from the DataFrame
    for row in greetings_df.collect():
        g.set_message(row.message)
        g.greet()

if __name__ == "__main__":
    main()
