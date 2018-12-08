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
sqlDF = spark.sql("SELECT COUNT(*) AS NumberOfTweets, 'SQL' as Database FROM Technology where text LIKE '%SQL%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets,'NoSQL' as Database FROM Technology where text LIKE '%NoSQL%'")
pd = sqlDF.toPandas()
pd.to_csv('ten.csv', index=False)
pd.plot(kind="bar",x="Database",y="NumberOfTweets")

plt.show()
sqlDF.show()

