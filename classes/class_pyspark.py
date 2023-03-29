import json, os, re, sys
from typing import Callable, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

class SparkApp :
    def __init__(self, config: dict):
        self.config = config

    def startSpark(self, kwargs) -> SparkSession:
        try:
            def createBuilder(master:str, appname:str, config:dict) -> SparkSession.Builder:
                """ create a spark session """
                builder = SparkSession\
                    .builder\
                    .appName(appname)\
                    .master(master)
                return builder
            
            def createSession(builder:SparkSession.Builder) -> SparkSession:
                if isinstance(builder, SparkSession.Builder):
                    return builder.getOrCreate()           

            def setLogging(spark:SparkSession, log_level:str) -> None:
                """ 
                set log level - ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN 
                this function will overide the configuration also set in log4j.properties
                for example, log4j.rootCategory=ERROR, console

                """
                if isinstance(spark, SparkSession):
                    spark.sparkContext.setLogLevel(log_level) if isinstance(log_level, str) else None 
                    
            
            MASTER = kwargs.get('spark_conf', {}).get('master', 'local[*]')
            APPNAME = kwargs.get('spark_conf', {}).get('appname', 'myapp')
            CONFIG = kwargs.get('config')
            LOG_LEVEL = kwargs.get('log', {}).get('level')
            
            builder = createBuilder(MASTER, APPNAME, CONFIG)
            spark = createSession(builder)
            setLogging(spark, LOG_LEVEL)
            return spark

        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e) 
    
    def importDataFromPath(self, spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
        try:
            def fileOrDirectory(spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> str:
                """ check if path is a directory or file """
                if isinstance(datapath, str) and os.path.exists(datapath):
                    if os.path.isdir(datapath):
                        return openDirectory(spark, datapath, pattern)
                    elif os.path.isfile(datapath):
                        return openFile(spark, datapath)

            def openDirectory(spark:SparkSession, datapath:str, pattern:Optional[str]=None) -> DataFrame:
                    filelist = self.listDirectory(datapath, pattern)
                    filetype = getUniqueFileExtentions(filelist)
                    if filetype == None: 
                        raise ValueError('Cannot create a single dataframe from varying file types or no files found') 
                    return self.createDataFrame(spark, filelist, filetype)

            def openFile(spark:SparkSession, datapath:str) -> DataFrame:
                    filelist = [datapath]
                    filetype = self.getFileExtension(datapath)
                    return self.createDataFrame(spark, filelist, filetype)
            
            def getUniqueFileExtentions(filelist:list) -> list:
                """ required if no search pattern is given and could return any file type """
                if isinstance(filelist, list) and len(filelist) > 0:
                    exts = set(os.path.splitext(f)[1] for f in filelist)
                    filetype = list(exts)
                    return filetype[0][1:] if len(filetype) == 1 else None

            return fileOrDirectory(spark, datapath, pattern)
    
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e) 
   

    # end::importData[]

    def getFileExtension(self, filepath:str) -> str:
        """ get extension from a single file """
        if isinstance(filepath, str):
            filename, file_extension = os.path.splitext(filepath)
            return file_extension[1:] if file_extension else None

    def listDirectory(self, directory:str, pattern:Optional[str]=None) -> list:
        """ recursively list the files of a directory  """
        def recursiveFilelist(directory):
            if os.path.exists(directory): 
                filelist = []
                for dirpath, dirnames, filenames in os.walk(directory):
                    for filename in filenames:
                        filelist.append(os.path.join(dirpath, filename))
                return filelist
        
        def filterFiles(filelist:list, pattern:str) -> list:
            """ if pattern is included then filter files """
            if isinstance(pattern, str):
                return [x for x in filelist if re.search(rf"{pattern}", x)]
            return filelist

        filelist = recursiveFilelist(directory)
        return filterFiles(filelist, pattern)

    def createDataFrame(self, spark:SparkSession, filelist:list, filetype:str) -> DataFrame:
        """
        create dataframe from list of files 
        add more functions for other filetypes for example, plain text files to create an RDD
        factor in text files without an extension

        """
        match filetype:
            case "csv":
                df = spark.read.format("csv") \
                    .option("header", "true")  \
                    .option("mode", "DROPMALFORMED") \
                    .load(filelist)
                return df
            case "json":
                df = spark.read.format("json") \
                    .option("mode", "PERMISSIVE") \
                    .load(filelist)
                return df
            case _:
                print("Invalid data format.")
