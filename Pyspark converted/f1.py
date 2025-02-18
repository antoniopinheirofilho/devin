from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F1").getOrCreate()

def hello_world():
    """
    SAS vs PySpark Implementation Differences:
    - SAS uses DS2 proc with data _null_ for standalone programs
    - PySpark uses simple Python function with print statement
    - SAS init() method is replaced with a standard Python function
    - Output format matches SAS put statement behavior
    """
    message = "Hello World!"
    print(message)

if __name__ == "__main__":
    hello_world()
