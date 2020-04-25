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
resultdf = spark.sql("SELECT substring(user.created_at,27,4) as Year,count(*) as UsersCount from MobileTweetsData \
                     where user.created_at is not null group by substring(user.created_at,27,4) order by count(1) desc")
pd = resultdf.toPandas()

def query10_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query10_plot():
    pd.plot.area(x="Year", y="UsersCount", title="Tweeter users created per year")
    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
