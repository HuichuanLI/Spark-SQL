package com.bigdata.spark.learn.sql.datasorce

import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object paquetDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("SparkSessionApp").getOrCreate()
    // 标准写法
    val userDf = spark.read.format("parquet").load("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/users.parquet")

    userDf.printSchema()
    userDf.show()

    spark.stop()
  }
}
