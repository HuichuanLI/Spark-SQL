package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Author : lihuichuan
 * Time   : 2020/4/4
 **/
object wordCountV2 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local").setAppName("SparkWordCountApp")
    val sc = new SparkContext(sparkconf)

    val file = sc.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/wc.txt")
    val wordCounts = file.flatMap(line => line.split(",")).map((word => (word, 1))).reduceByKey(_ + _)
    wordCounts.collect().foreach(println)

    println(wordCounts.count())

    println(wordCounts.take(5))

    wordCounts.map(tuple => tuple._1 + "\t" + tuple._2).collect().foreach(println)
  }
}
