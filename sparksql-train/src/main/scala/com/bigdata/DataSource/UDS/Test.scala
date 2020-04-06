package com.bigdata.DataSource.UDS

import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/4/6
 **/
object Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName).master("local[8]")
      .getOrCreate()


    val df = spark
      .read
      .format("com.bigdata.DataSource.UDS.DefaultSource")
      .load("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/data.txt")

    df.show()
    df.printSchema()
    spark.stop()
  }
}
