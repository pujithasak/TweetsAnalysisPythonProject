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
#df = spark.read.json("importedtweetsdata.json")
df = spark.read.json("tweetsfile.json")

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("MobileTweetsData")
resultdf = spark.sql("SELECT distinct place.country as CountryName, count(*) as TweetsCount FROM MobileTweetsData \
                     where place.country is not null GROUP BY place.country ORDER BY TweetsCount DESC limit 30")
resultdf.show()
pd = resultdf.toPandas()

def query1_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})


def query1_plot():
    pd.plot(kind="barh", x="CountryName", y="TweetsCount",
            title="Tweets from different countries about phones",
            figsize=(7, 5))
    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
