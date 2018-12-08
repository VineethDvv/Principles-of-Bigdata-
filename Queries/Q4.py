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

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("Technology")
sqlDF = spark.sql("SELECT 'Microsoft' as Company, count(*) as Count from Technology where text like'%Microsoft%'\
        UNION\
        SELECT 'IBM' as Company, count(*) as Count from Technology where text like '%IBM%'\
        UNION\
        SELECT 'Cerner' as Company, count(*) as Count from Technology where text like '%cerner%'")
pd = sqlDF.toPandas()
pd.to_csv('fourth.csv', index=False)
pd.plot(kind="bar",x="Company",y="Count")
plt.show()
sqlDF.show()

