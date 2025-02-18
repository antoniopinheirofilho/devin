from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F2").getOrCreate()

class Greeting:
    """
    SAS vs PySpark Implementation Differences:
    - SAS uses data program scope with dcl statement for variable declaration
    - PySpark uses Python class with instance variables
    - SAS greet() method is directly mapped to Python method
    - SAS init() method is mapped to Python __init__
    - Output format matches SAS put statement behavior
    """
    def __init__(self):
        self.message = None
    
    def greet(self):
        print(self.message)

def main():
    greeting = Greeting()
    greeting.message = "Hello World!"
    greeting.greet()

if __name__ == "__main__":
    main()
