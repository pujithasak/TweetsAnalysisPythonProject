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
resultdf = spark.sql("SELECT SUBSTR(created_at, 0, 20) tweetsDate, COUNT(1) tweetsCount FROM MobileTweetsData \
                     where text is not null GROUP BY SUBSTR(created_at, 0, 20) ORDER BY COUNT(1) DESC LIMIT  5")
pd = resultdf.toPandas()

def query4_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})


def query4_plot():
    pd.plot(kind="bar", x="tweetsDate", y="tweetsCount",
            title="Number of tweets for date")
    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
