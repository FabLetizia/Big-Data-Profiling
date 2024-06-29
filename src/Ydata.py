import logging
import warnings

from matplotlib import MatplotlibDeprecationWarning
from pyspark.sql import SparkSession

from ydata_profiling import ProfileReport
from ydata_profiling.config import Settings

import argparse


logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
# input and output paths passed from the command line
parser.add_argument("--input_path", type=str, help="Input dataset path")
 
args = parser.parse_args()
dataset_filepath = args.input_path

spark_session = (
SparkSession.builder.appName("SparkProfiling").master("local[*]").config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").getOrCreate()
)

# Carica il dataset in un DataFrame Spark
spark_df = spark_session.read.csv(dataset_filepath, header=True, inferSchema=True)


# Create and start the monitoring process
warnings.filterwarnings("ignore", category=MatplotlibDeprecationWarning)

profile = ProfileReport(spark_df)

# Genera e salva il report di profilazione
profile.to_file("song_lyrics_report.html")