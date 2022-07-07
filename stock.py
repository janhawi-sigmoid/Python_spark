from flask import Flask,jsonify,json
from pyspark.sql import SparkSession
import pandas as pd

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

spark = SparkSession.builder.appName('Read Multiple CSV Files').getOrCreate()

spark_df = spark.read.csv("/Users/janhawishresth/PycharmProjects/pythonSpark/Data/*", sep=',', header=True)
spark_df.groupBy("Stock_Name").count().show()

spark_df.createOrReplaceTempView("table")

@app.route('/Q1',methods=['GET'])
def get_question1():
    # print("Question - 1")
    sqlDF1 = spark.sql("WITH added_dense_rank AS (SELECT Date,Stock_Name,(High-Open)/Open , dense_rank() OVER ( partition by Date order by (High-Open)/Open desc ) as dense_rank FROM table) select * FROM added_dense_rank where dense_rank=1")
    #print(type(sqlDF1))
    results = sqlDF1.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/Q2',methods=['GET'])
def get_question2():
    # print("Question - 2")
    sqlDF2 = spark.sql("WITH added_dense_rank AS (SELECT Date,Stock_Name,Volume , dense_rank() OVER ( partition by Date order by Volume desc ) as dense_rank FROM table) select Date,Stock_Name,Volume FROM added_dense_rank where dense_rank=1")
    print(type(sqlDF2))
    results = sqlDF2.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/Q3',methods=['GET'])
def get_question3():
    # print("Question - 3")
    sqlDF3 = spark.sql("with added_previous_close as (select Stock_Name,Open,Date,Close,LAG(Close,1,35.724998) over(partition by Stock_Name order by Date) as previous_close from table ASC) select Stock_Name,ABS(previous_close-Open) as max_swing from added_previous_close order by max_swing DESC limit 1")
    print(type(sqlDF3))
    results = sqlDF3.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/Q4',methods=['GET'])
def get_question4():
    # print("Question - 4")
    sqlDF4 = spark.sql("Select stock_table.Stock_Name, stock_table.open, stock_table.high, stock_table.high-stock_table.open as max_diff from (Select Stock_Name, (Select open from table limit 1) as open, max(high) as high from table group by Stock_Name)stock_table order by max_diff desc limit 1")
    print(type(sqlDF4))
    results = sqlDF4.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/Q5',methods=['GET'])
def get_question5():
    # print("Question - 5")
    sqlDF5 = spark.sql("select Stock_Name, std(Volume) as Standard_Deviation from table group by Stock_Name")
    print(type(sqlDF5))
    results = sqlDF5.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/Q6',methods=['GET'])
def get_question6():
    # print("Question - 6")
    sqlDF6 = spark.sql("Select Stock_Name, avg(open) as Mean, percentile_approx(open,0.5) as Median from table group by Stock_Name")
    print(type(sqlDF6))
    results = sqlDF6.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/Q7',methods=['GET'])
def get_question7():
    # print("question - 7")
    sqlDF7 = spark.sql("SELECT Stock_Name, avg(Volume) as Average from table group by Stock_Name")
    print(type(sqlDF7))
    results = sqlDF7.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/Q8',methods=['GET'])
def get_question8():
    # print("Question - 8")
    sqlDF8 = spark.sql("select Stock_Name, avg(Volume) as Average from table group by Stock_Name order by Average DESC limit 1")
    print(type(sqlDF8))
    results = sqlDF8.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/Q9',methods=['GET'])
def get_question9():
    # print("Question - 9")
    sqlDF9 = spark.sql("select Stock_Name, min(Low) as Lowest_Price, max(High) as Highest_Price from table group by Stock_Name")
    print(type(sqlDF9))
    results = sqlDF9.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

# # df1 = spark_df.toPandas()
# # print(df1.head(20))

app.run(host='0.0.0.0',port=5008)

