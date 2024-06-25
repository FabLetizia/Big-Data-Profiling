import logging
import warnings
import zipfile
import io

from matplotlib import MatplotlibDeprecationWarning
from pyspark.sql import SparkSession

from ydata_profiling import ProfileReport
from ydata_profiling.config import Settings

import argparse
import pyarrow.fs

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path in HDFS")
parser.add_argument("--file_type", type=str, default="csv", choices=["csv", "zip"], help="Type of input file (csv or zip)")
args = parser.parse_args()
dataset_filepath = args.input_path

# Initialize Spark session
spark_session = (
    SparkSession.builder
    .appName("SparkProfiling")
    .master("local[*]")  # Change to your cluster master if not running locally
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# Function to read CSV from ZIP file
def read_csv_from_zip(zip_path):
    hdfs = pyarrow.fs.HadoopFileSystem(host='ec2-52-91-127-17.compute-1.amazonaws.com', port=8020)
    with hdfs.open(zip_path) as f:
        with zipfile.ZipFile(io.BytesIO(f.read())) as z:
            filename = z.namelist()[0]
            with z.open(filename) as csv_file:
                return spark_session.read.csv(csv_file, header=True)

# Load dataset based on file type
if args.file_type == "csv":
    spark_df = spark_session.read.csv(dataset_filepath, header=True)
elif args.file_type == "zip":
    spark_df = read_csv_from_zip("hdfs:///input/Airline.csv.zip")

# Configure profiling settings
cfg = Settings()
cfg.infer_dtypes = False
cfg.correlations["auto"].calculate = False
cfg.correlations["pearson"].calculate = False
cfg.correlations["spearman"].calculate = False
cfg.interactions.continuous = False
cfg.missing_diagrams["bar"] = False
cfg.missing_diagrams["heatmap"] = False
cfg.missing_diagrams["matrix"] = False
cfg.samples.tail = 0
cfg.samples.random = 0

# Suppress matplotlib deprecation warnings
warnings.filterwarnings("ignore", category=MatplotlibDeprecationWarning)

# Create profile report
profile = ProfileReport(spark_df, config=cfg)

# Generate and save the profiling report
profile.to_file("airline_report.html")

# Stop Spark session
spark_session.stop()