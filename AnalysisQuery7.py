from flask import jsonify
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd
import io
spark = SparkSession \
    .builder \
    .appName("Tweets Analysis using Python Saprk") \
    .getOrCreate()
# spark is an existing SparkSession
df = spark.read.json("importedtweetsdata.json")

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("MobileTweetsData")
subResultdf1 = spark.sql("SELECT distinct id, \
          CASE when user.verified LIKE '%true%' THEN 'VERIFIED ACCOUNT' \
          when user.verified LIKE '%false%' THEN 'NON-VERIFIED ACCOUNT' \
          END AS Verified from MobileTweetsData where text is not null")
subResultdf1.createOrReplaceTempView("AccountVerification")

resultdf = spark.sql("SELECT  Verified, Count(Verified) as count from AccountVerification w \
                     where id is NOT NULL and Verified is not null group by Verified order by Count DESC")

pd = resultdf.toPandas()
pd.to_csv('Query7Result.csv', index=False)

def query7_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query7_plot():
    pd.plot.line(x="Verified", y="count",
                 title="Account Verification tweets")
    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
