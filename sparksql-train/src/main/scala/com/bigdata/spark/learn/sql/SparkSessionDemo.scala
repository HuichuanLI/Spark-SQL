package com.bigdata.spark.learn.sql

import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object SparkSessionDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("SparkSessionApp").getOrCreate()

    val people = spark.read.format("json").load("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.json")

    people.show()

    spark.stop()
  }
}
