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
sqlDF = spark.sql("SELECT 'high school' as SchoolLevel, count(*) as Count from Technology where text like '%high school%' and text like '%technology%' \
        UNION\
        SELECT 'college' as SchoolLevel, count(*) as Count from Technology where text like '%college%' and text like '%technology'")
pd = sqlDF.toPandas()
pd.to_csv('third.csv', index=False)
pd.plot(kind='hist',x='SchoolLevel',y='Count',bins=50,figsize=(12,6))
plt.show()
sqlDF.show()

