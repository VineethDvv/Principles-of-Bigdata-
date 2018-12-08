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
sqlDF = spark.sql("SELECT 'HomeAI' as Domain, count(*) as Count from Technology where text like '%home%' and text like '%AI%'\
        UNION\
        SELECT 'HealthAI' as Domain, count(*) as Count from Technology where text like '%health%' and text like '%AI%'\
        UNION\
        SELECT 'BusinessAI' as Domain, count(*) as Count from Technology where text like '%business%' and text like '%AI%'")
pd = sqlDF.toPandas()
pd.to_csv('fifth.csv', index=False)
pd.plot(kind="bar",x="Domain",y="Count")
plt.show()
sqlDF.show()

