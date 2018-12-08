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
sqlDF = spark.sql("SELECT COUNT(*) AS NumberOfTweets, 'Python' as Language FROM Technology where text LIKE '%Python%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets,'Javascript' as Language FROM Technology where text LIKE '%javascript%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets, 'HTML' as Language FROM Technology where text LIKE '%HTML%'\
        UNION\
        SELECT COUNT(*) AS NumberOfTweets, 'CSS' as Language FROM Technology where text LIKE '%CSS%'\
         ")
pd = sqlDF.toPandas()
pd.to_csv('first.csv', index=False)
pd.plot.pie(y='NumberOfTweets',labels=['HTML','CSS','JavaScript','python'],figsize=(5,5))
plt.show()
sqlDF.show()

