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
resultdf = spark.sql("SELECT SUBSTR(retweeted_status.source, 17, 7) AS TweetSources, count(*) AS TweetsCount FROM MobileTweetsData \
                     where retweeted_status.source IS NOT null GROUP BY retweeted_status.source ORDER BY TweetsCount DESC limit 30")
pd = resultdf.toPandas()
pd.to_csv('Query11Result.csv', index=False)

def query11_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query11_plot():
    pd.plot.scatter(x='TweetSources', y='TweetsCount',
                    title='top Sources having highest number of tweets',
                    figsize=(5, 5))
    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
