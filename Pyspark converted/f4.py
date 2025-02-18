from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F4").getOrCreate()

class Greeting:
    def __init__(self, message=None):
        self._message = message if message else "This is the default greeting."
    
    def greet(self):
        print(self._message)
    
    def set_message(self, message):
        self._message = message

def create_greetings_table():
    # Create a DataFrame with greetings
    data = [
        ("Hello World!",),
        ("What's new?",),
        ("Good-bye World!",)
    ]
    schema = StructType([StructField("message", StringType(), True)])
    return spark.createDataFrame(data, schema)

def main():
    # Create the greetings table
    greetings_df = create_greetings_table()
    
    # Create a greeting instance with default message
    g = Greeting()
    g.greet()
    
    # Process each greeting from the DataFrame
    for row in greetings_df.collect():
        g.set_message(row.message)
        g.greet()

if __name__ == "__main__":
    main()
