from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F1").getOrCreate()

def hello_world():
    # Equivalent to the SAS DS2 program that prints "Hello World!"
    message = "Hello World!"
    print(message)

if __name__ == "__main__":
    hello_world()
