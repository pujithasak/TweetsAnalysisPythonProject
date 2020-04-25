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
resultdf = spark.sql("select substring(user.created_at,0,4) as NameOfWeekDay, count(*) as TweetsCount \
                     from MobileTweetsData group by substring(user.created_at,0,4)")
pd = resultdf.toPandas()

def query12_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query12_plot():
    pd.plot.pie(y='TweetsCount',
                labels=['Mon', 'sun','Tue','Thu','Fri','Wed','Sat'],
                title="Number of Tweets on different weekdays",
                figsize=(5, 5))
    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
