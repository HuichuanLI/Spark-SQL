package com.bigdata.project.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      //      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[*]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/access.log")

    //    accessRDD.take(10).foreach(println)
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)
    accessDF.printSchema()
    //    accessDF.show(20, false)
    accessDF.coalesce(1).write.mode(SaveMode.Overwrite).format("parquet").partitionBy("day").save("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/out")
    spark.stop()
  }
}
