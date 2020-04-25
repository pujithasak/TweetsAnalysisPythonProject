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
resultdf = spark.sql("SELECT user.geo_enabled as Location, count(*)  as UsersCount \
                     from MobileTweetsData group by user.geo_enabled")
pd = resultdf.toPandas()
pd.to_csv('Query9Result.csv', index=False)

def query9_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query9_plot():
    pd.plot.pie(y='UsersCount',
                labels=['Location On', 'Location OFF'],
                title="Users Location data",
                figsize=(5, 5))

    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
