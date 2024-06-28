import logging
import warnings
import argparse
import gzip  # Import gzip module for handling .gz files

import os
os.system('pip install ydata_profiling')
os.system('pip install matplotlib')
os.system('pip install pyspark')

from matplotlib import MatplotlibDeprecationWarning
from pyspark.sql import SparkSession
from ydata_profiling import ProfileReport
from ydata_profiling.config import Settings



logging.basicConfig(level=logging.INFO)

#parser = argparse.ArgumentParser()
# input and output paths passed from the command line
#parser.add_argument("--input_path", type=str, help="Input dataset path")
 
#args = parser.parse_args()
#dataset_filepath = args.input_path

spark_session = (
SparkSession.builder.appName("SparkProfiling").master("local[*]").config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").getOrCreate()
)


# Load the dataset from a gzip-compressed CSV file
''' with gzip.open("airline.csv.gz", 'rb') as f:
    spark_df = spark_session.read.csv(f, header=True) '''
# Load the dataset from a gzip-compressed CSV file
spark_df = spark_session.read.csv("/input/airline.csv.gz", header=True, inferSchema=True)



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

# Create and start the monitoring process
warnings.filterwarnings("ignore", category=MatplotlibDeprecationWarning)

profile = ProfileReport(spark_df, config=cfg)

# Genera e salva il report di profilazione
profile.to_file("airline_report_gz.html")