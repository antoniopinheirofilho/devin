from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F3").getOrCreate()

class Greeting:
    """
    SAS vs PySpark Implementation Differences:
    - SAS uses package with forward declaration and method implementations
    - PySpark uses Python class with encapsulated methods
    - SAS package variables are mapped to Python instance variables
    - SAS setMessage method is implemented with Python naming conventions
    - Constructor behavior matches SAS package initialization
    - Output format matches SAS put statement behavior
    """
    def __init__(self, message):
        self._message = None
        self.set_message(message)
    
    def greet(self):
        print(self._message)
    
    def set_message(self, message):
        self._message = message

def main():
    # Create greeting instance with initial message
    g = Greeting("Hello World!")
    g.greet()
    
    # Change greeting and display again
    g.set_message("What's new?")
    g.greet()

if __name__ == "__main__":
    main()
