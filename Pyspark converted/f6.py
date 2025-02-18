from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.dbutils import DBUtils
from scipy import stats
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F6").getOrCreate()
dbutils = DBUtils(spark)

# Create widgets for table configuration
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("table", "table_output")

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
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    table = dbutils.widgets.get("table")
    
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
