package com.bigdata.csvPackage

import org.apache.spark.sql.SparkSession

/**
 * Author: Michael PK
 */
object StocksApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StocksApp").master("local[2]").getOrCreate()

    val stocksDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/xomtable.csv")

    //    stocksDF.show(false)
    //    stocksDF.printSchema()

    /**
     * TODO... 统计2016年股票周平均收盘价格
     * 1) 2016: Date
     * 2) 收盘价格 : Close
     * 3) 平均：agg(avg(""))
     * 4) 周   Time Window
     */


    val stocks2016DF = stocksDF.filter("year(Date)==2016")
    stocks2016DF.show(false)
    //
    import org.apache.spark.sql.functions._
    //
    //
    //    /**
    //     * 第一个字段：时间所在的列
    //     * 第二个字段：窗口的duration
    //     */
    stocks2016DF.groupBy(window(stocks2016DF.col("Date"), "1 week"))
      .agg(avg("Close").as("week_close_avgerage"))
      .sort("window.start")
      .select("window.start", "window.end", "week_close_avgerage")
      .show(false)


    spark.stop()
  }
}
