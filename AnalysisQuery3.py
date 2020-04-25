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
resultdf = spark.sql("SELECT lang as Language, count(*) AS TweetsCount FROM MobileTweetsData WHERE lang<>'null' GROUP BY lang ORDER BY TweetsCount DESC LIMIT 10")
pd = resultdf.toPandas()

def query3_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query3_plot():
    pd.plot.area(x="Language", y="TweetsCount", title="Top 10 languages with highest number of tweets")

    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
