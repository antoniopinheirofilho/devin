from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F2").getOrCreate()

class Greeting:
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
