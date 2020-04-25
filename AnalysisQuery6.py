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
resultdf = spark.sql("SELECT count(*) AS TweetsCount, 'iphone' as PhoneType FROM MobileTweetsData where text LIKE '%iphone%' \
                  UNION SELECT count(*) AS TweetsCount,'samsung' as PhoneType FROM MobileTweetsData where text LIKE '%samsung%' \
                  UNION SELECT count(*) AS TweetsCount, 'moto' as PhoneType FROM MobileTweetsData where text LIKE '%moto%' \
                  UNION SELECT count(*) AS TweetsCount, 'redmi' as PhoneType FROM MobileTweetsData where text LIKE '%redmi%' \
                  UNION SELECT count(*) AS TweetsCount,'oppo' as PhoneType FROM MobileTweetsData where text LIKE '%oppo%' \
                  UNION SELECT count(*) AS TweetsCount,'HTC' as PhoneType FROM MobileTweetsData where text LIKE '%HTC%' \
                  UNION SELECT count(*) AS TweetsCount,'Nokia' as PhoneType FROM MobileTweetsData where text LIKE '%nokia%' \
                  UNION SELECT count(*) AS TweetsCount,'lenovo' as PhoneType FROM MobileTweetsData where text LIKE '%Lenovo%' \
                  order by TweetsCount desc limit 10")
pd = resultdf.toPandas()

def query6_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query6_plot():
    pd.plot.pie(y='TweetsCount',
                labels=['iphone', 'moto', 'samsung', 'oppo','lenovo','Nokia','HTC','redmi'],
                title="Tweets count for different phone models",
                figsize=(5, 5))
    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
