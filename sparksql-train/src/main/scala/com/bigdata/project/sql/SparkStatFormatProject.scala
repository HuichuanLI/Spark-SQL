package com.bigdata.project.sql

import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object SparkStatFormatProject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkSessionApp").getOrCreate()

    val access = spark.sparkContext.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/access.20161111.log")
    //    access.take(10).foreach(println)
    access.map(line => {

      /**
       * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
       * [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
       */


      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)

      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).repartition(1).saveAsTextFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/out")
    spark.stop()
  }
}
