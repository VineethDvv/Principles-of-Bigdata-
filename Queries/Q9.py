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
sqlDF = spark.sql("SELECT COUNT(*) AS NumberOfUsers, 'Whatsapp' as SocialSite FROM Technology where text LIKE '%whatsapp%'\
        UNION\
        SELECT COUNT(*) AS NumberOfUsers,'Instagram' as SocialSite FROM Technology where text LIKE '%instgram%'\
        UNION\
        SELECT COUNT(*) AS NumberOfUsers, 'Snapchat' as SocialSite FROM Technology where text LIKE '%snapchat%'")
sqlDF.show()
pd = sqlDF.toPandas()
print(pd)
pd.to_csv('nine.csv', index=False)
pd.plot(kind="bar",x="SocialSite",y="NumberOfUsers")

plt.show()
#sqlDF.show()

