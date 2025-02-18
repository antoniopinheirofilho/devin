from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from scipy import stats
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F6").getOrCreate()

# Create widgets for table configuration
spark.sql("CREATE WIDGET TEXT catalog DEFAULT 'default'")
spark.sql("CREATE WIDGET TEXT schema DEFAULT 'default'")
spark.sql("CREATE WIDGET TEXT table DEFAULT 'chi_ratios'")

def calculate_chi_ratios():
    """
    SAS vs PySpark Implementation Differences:
    - SAS uses proc print for data display and proc plot for visualization
    - PySpark uses DataFrame show() for display and matplotlib for plotting
    - SAS cinv function is replaced with scipy.stats.chi2.ppf
    - Results are saved to Unity Catalog table with configurable naming
    - Plot is saved as PNG file, similar to SAS plot output
    """
    # Create DataFrame with degrees of freedom
    df_values = list(range(20, 31))
    data = [(df,) for df in df_values]
    df = spark.createDataFrame(data, ["df"])
    
    # Calculate chi-square ratios
    chi_ratios = []
    for d in df_values:
        chi_ratio = stats.chi2.ppf(0.995, d) / stats.chi2.ppf(0.005, d)
        chi_ratios.append((d, chi_ratio))
    
    # Create result DataFrame
    result_df = spark.createDataFrame(chi_ratios, ["df", "chirat"])
    
    # Get table configuration from widgets
    catalog = spark.sql("GET WIDGET catalog").collect()[0][0]
    schema = spark.sql("GET WIDGET schema").collect()[0][0]
    table = spark.sql("GET WIDGET table").collect()[0][0]
    
    # Save DataFrame to Unity Catalog
    result_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")
    
    # Show results in SAS format
    result_df.show(truncate=False, n=20, vertical=False)
    
    # Create plot similar to SAS proc plot output
    plt.figure()
    plt.plot([x[0] for x in chi_ratios], [x[1] for x in chi_ratios])
    plt.xlabel('Degrees of Freedom')
    plt.ylabel('Chi-square Ratio')
    plt.title('Chi-square Ratio vs Degrees of Freedom')
    plt.grid(True)  # Add grid to match SAS plot style
    plt.savefig('chi_ratio_plot.png')

if __name__ == "__main__":
    calculate_chi_ratios()
