import logging
import warnings
from matplotlib import MatplotlibDeprecationWarning
from pyspark.sql import SparkSession
from ydata_profiling import ProfileReport
import argparse

logging.basicConfig(level=logging.INFO)

''' This Spark core script in python performs fdata profiling on the input csv taken from a
    hdfs path and generates a report file with statistics in html format '''

parser = argparse.ArgumentParser()
# input and output paths passed from the command line
parser.add_argument("--input_path", type=str, help="Input dataset path")
 
args = parser.parse_args()
dataset_filepath = args.input_path

spark_session = (
SparkSession.builder.appName("SparkProfiling").master("local[*]").config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").getOrCreate()
)

spark_df = spark_session.read.csv(dataset_filepath, header=True, inferSchema=True)

# Create and start the monitoring process
warnings.filterwarnings("ignore", category=MatplotlibDeprecationWarning)

# ydata_profiling tool
profile = ProfileReport(spark_df)

profile.to_file("report.html")