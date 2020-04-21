# Run: $SPARK_HOME/bin/spark-submit firstapp.py
# /usr/local/spark/bin/spark-submit firstapp.py firstapp.py
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd

logFile="file:///usr/local/spark/README.md"
sc=SparkContext("local[*]", "first app")
sc.setLogLevel('WARN')

logData=sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("\nLines with a: %i, lines with b: %i" % (numAs, numBs))

sql_sc = SQLContext(sc)
pandas_df = pd.read_csv("data/bike-data/201508_station_data.csv")
s_df = sql_sc.createDataFrame(pandas_df)
print(s_df.collect())
