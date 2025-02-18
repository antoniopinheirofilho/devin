from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F7").getOrCreate()

def create_toxic_data():
    # Define schema
    schema = StructType([
        StructField("life", FloatType(), True),
        StructField("poison", StringType(), True),
        StructField("treatment", StringType(), True)
    ])
    
    # Create data
    data = [
        # Treatment A
        (0.31, "I", "A"), (0.45, "I", "A"), (0.46, "I", "A"), (0.43, "I", "A"),
        (0.36, "II", "A"), (0.29, "II", "A"), (0.40, "II", "A"), (0.23, "II", "A"),
        (0.22, "III", "A"), (0.21, "III", "A"), (0.18, "III", "A"), (0.23, "III", "A"),
        # Treatment B
        (0.82, "I", "B"), (1.10, "I", "B"), (0.88, "I", "B"), (0.72, "I", "B"),
        (0.92, "II", "B"), (0.61, "II", "B"), (0.49, "II", "B"), (1.24, "II", "B"),
        (0.30, "III", "B"), (0.37, "III", "B"), (0.38, "III", "B"), (0.29, "III", "B"),
        # Treatment C
        (0.43, "I", "C"), (0.45, "I", "C"), (0.63, "I", "C"), (0.76, "I", "C"),
        (0.44, "II", "C"), (0.35, "II", "C"), (0.31, "II", "C"), (0.40, "II", "C"),
        (0.23, "III", "C"), (0.25, "III", "C"), (0.24, "III", "C"), (0.22, "III", "C"),
        # Treatment D
        (0.45, "I", "D"), (0.71, "I", "D"), (0.66, "I", "D"), (0.62, "I", "D"),
        (0.56, "II", "D"), (1.02, "II", "D"), (0.71, "II", "D"), (0.38, "II", "D"),
        (0.30, "III", "D"), (0.36, "III", "D"), (0.31, "III", "D"), (0.33, "III", "D")
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    
    # Perform ANOVA-like analysis using Spark
    # Group by poison and treatment
    summary_stats = df.groupBy("poison", "treatment").agg({
        "life": "avg",
        "life": "variance"
    })
    
    # Show results
    df.show()
    summary_stats.show()

if __name__ == "__main__":
    create_toxic_data()
