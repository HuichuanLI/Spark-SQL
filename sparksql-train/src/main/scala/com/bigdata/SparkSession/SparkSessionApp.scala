package com.bigdata.SparkSession

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 认识SparkSession
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    // 就是DF/DS编程的入口点
    val spark: SparkSession = SparkSession.builder()
      .master("local").getOrCreate()

    // 读取文件的API
    val df: DataFrame = spark.read.text("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/input2.txt")

    // TODO... 业务逻辑处理，肯定是通过DF/DS提供的API来完成我们的业务
    df.printSchema()
    df.show()  // 展示出来  只有一个字段，string类型的value

    spark.stop()
  }
}
