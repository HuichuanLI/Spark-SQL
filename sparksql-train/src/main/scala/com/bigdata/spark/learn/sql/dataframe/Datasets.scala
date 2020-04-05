package com.bigdata.spark.learn.sql.dataframe

import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object Datasets {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("SparkSessionApp").getOrCreate()
    import spark.implicits._
    // 读取csv
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/sales.csv")
    df.show(false)
    val ds = df.as[Sales]

    ds.map(line => line.itemId).show()
    spark.stop()
  }

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

}
