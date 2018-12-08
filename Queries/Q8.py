from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib as plt
import matplotlib.pyplot as plt
import pandas as pd
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# spark is an existing SparkSession
df = spark.read.json("/home/siri/Downloads/project/finalPB3.json")
# Displays the content of the DataFrame to stdout
#df.show()
# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("Technology")
sqlDF = spark.sql("SELECT COUNT(*) AS NumberOfUsers, 'IOS' as OSPlatform FROM Technology where text LIKE '%IOS%'\
        UNION\
        SELECT COUNT(*) AS NumberOfUsers,'Android' as OSPlatform FROM Technology where text LIKE '%Android%'\
        UNION\
        SELECT COUNT(*) AS NumberOfUsers, 'windows' as OSPlatform FROM Technology where text LIKE '%windows%'")
sqlDF.show()
pd = sqlDF.toPandas()
pd.to_csv('eight.csv', index=False)
pd.plot(kind="bar",x="OSPlatform",y="NumberOfUsers")
plt.show()
#sqlDF.show()

