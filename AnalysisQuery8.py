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
resultdf = spark.sql("SELECT SUBSTR(place.country, 0, 6) AS Country, SUM(user.followers_count) AS FollowersCount \
                     FROM MobileTweetsData WHERE place.country != 'null' GROUP BY place.country \
                     ORDER BY FollowersCount DESC LIMIT 10")
pd = resultdf.toPandas()

def query8_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query8_plot():
    pd.plot.line(x="Country", y="FollowersCount",
                 title="Top 10 countries having highest number of followers")
    pd.plot.area(x="Country", y="FollowersCount", title="Top 10 countries having highest number of followers")

    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image