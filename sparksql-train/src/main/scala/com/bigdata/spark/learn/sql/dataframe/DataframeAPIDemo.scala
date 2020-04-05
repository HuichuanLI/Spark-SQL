package com.bigdata.spark.learn.sql.dataframe

import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object DataframeAPIDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("SparkSessionApp").getOrCreate()

    // json to pd
    val people = spark.read.format("json").load("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.json")


    // 对应schema
    people.printSchema()

    // 输出20
    people.show(20)

    // 1.select
    people.select("name").show()
    //某几列
    people.select(people.col("name"), (people.col("age") + 10).as("age")).show()

    // filiter
    people.filter(people.col("age") > 19).show()

    // 分组
    people.groupBy(people.col("age")).count().show()


    spark.stop()

  }
}
