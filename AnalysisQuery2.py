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
subResultdf1 = spark.sql("SELECT substring(user.created_at,1,3) as day from MobileTweetsData where text is not null")
subResultdf1.createOrReplaceTempView("dayDataResult")

subResultdf2 = spark.sql("SELECT Case \
            when day LIKE '%Mon%' then 'WEEKDAY' \
            when day LIKE '%Tue%' then 'WEEKDAY' \
            when day LIKE '%Wed%' then 'WEEKDAY' \
            when day LIKE '%Thu%' then 'WEEKDAY' \
            when day LIKE '%Fri%' then 'WEEKDAY' \
            when day LIKE '%Sat%' then 'WEEKEND' \
            when day LIKE '%Sun%' then 'WEEKEND' \
            else \
            null \
            end as day1 from dayDataResult where day is not null")
subResultdf2.createOrReplaceTempView("subresult")

resultdf = spark.sql("SELECT day1 as Day,Count(*) as DayCount from subresult \
                     where day1 is not null group by day1 order by count(*) desc")

pd = resultdf.toPandas()

def query2_output():
    #return pd.to_json(orient='records')
    return jsonify({"Results":pd.to_json(orient='records')})



def query2_plot():
    pd.plot.pie(y='DayCount',
                labels=['WEEKDAY', 'WEEKEND'],
                title="number of Tweets based on day ",
                figsize=(5, 5))

    image = io.BytesIO()
    plt.savefig(image, format='png')
    image.seek(0)
    return image
