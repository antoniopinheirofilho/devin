from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from scipy import stats
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("SAS_Conversion_F6").getOrCreate()

def calculate_chi_ratios():
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
    
    # Show results
    result_df.show()
    
    # Create plot
    plt.figure()
    plt.plot([x[0] for x in chi_ratios], [x[1] for x in chi_ratios])
    plt.xlabel('Degrees of Freedom')
    plt.ylabel('Chi-square Ratio')
    plt.title('Chi-square Ratio vs Degrees of Freedom')
    plt.savefig('chi_ratio_plot.png')

if __name__ == "__main__":
    calculate_chi_ratios()
