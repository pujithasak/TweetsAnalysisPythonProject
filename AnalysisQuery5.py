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
resultdf = spark.sql("SELECT COUNT(*) AS TweetsCount, 'macOS' as OStype \
                     FROM MobileTweetsData where text LIKE '% macOS %' \
                     UNION SELECT COUNT(*) AS TweetsCount, 'ios' as OStype \
                     FROM MobileTweetsData where text LIKE '% ios %' \
                     UNION SELECT COUNT(*) AS TweetsCount, 'Android' as OStype \
                     FROM MobileTweetsData where text LIKE '% android %' \
                     UNION SELECT COUNT(*) AS TweetsCount, 'windows' as OStype \
                     FROM MobileTweetsData where text LIKE '% windows %' order by TweetsCount desc")
pd = resultdf.toPandas()
pd.to_csv('Query5Result.csv', index=False)

def query5_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query5_plot():
    pd.plot.line(x="OStype", y="TweetsCount",
                 title="Number of tweets based on Operating Systems")
    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
