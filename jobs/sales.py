import json, os, re, sys, logging
from typing import Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{project_dir}\logs\job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"

logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('py4j')

sys.path.insert(1, project_dir)
from classes.class_pyspark import SparkApp


def openJson(filePath) -> dict:
    if isinstance(filePath, str) and os.path.exists(filePath):
        with open(filePath, "r") as f:
            data = json.load(f)
        return data


if __name__ == "__main__":
    config = openJson(f"{project_dir}\json\sales.json")
    app = SparkApp(config)
    spark = app.startSpark(config)
    df_1 = app.importDataFromPath(spark, r'C:\Users\ico\Desktop\projects\python\pyspark_etl\data')

    print(df_1.show())