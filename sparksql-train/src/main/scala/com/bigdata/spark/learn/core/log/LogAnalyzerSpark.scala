package com.bigdata.spark.learn.core.log

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object LogAnalyzerSpark {

  def main(args: Array[String]): Unit = {

    // Create SparkConf
    val sparkConf = new SparkConf()
      .setAppName("Log Analyzer Spark Application")
      .setMaster("local[2]")

    // Create SparkContext
    val sc = SparkContext.getOrCreate(sparkConf)
    // 设置日志级别

    /** ===================================================================*/

    // 从本地文件系统读取数据
    val accessLogsRDD = sc.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/access_log")
      .filter(log => ApacheAccessLog.isValidateLogLine(log))
      .map(log => ApacheAccessLog.parseLogLine(log))

    // 由于后面的需求都是针对accessLogsRDD进行分析的，所以进行缓存
    accessLogsRDD.cache()

    println(s"Count = ${accessLogsRDD.count()} \n ${accessLogsRDD.first()}")

    /**
     * 需求一：Content Size
     * The average, min, and max content size of responses returned from the server
     */

    val contentSizeRDD: RDD[Long] = accessLogsRDD.map(_.contentSize)
    // 此RDD同样使用很多次，需要cache
    contentSizeRDD.cache()
    // compute
    val avgContentSize = contentSizeRDD.reduce(_ + _) / contentSizeRDD.count()
    val minContentSize = contentSizeRDD.min()
    val maxContentSize = contentSizeRDD.max()

    // 释放内存
    contentSizeRDD.unpersist()
    //打印结果
    println(s"Content Size Avg: ${avgContentSize}, Min: ${minContentSize}, Max: ${maxContentSize}")

    /**
     * 需求二：Response Code
     * A count of response code's returned.
     */


    val responseCodeToCount = accessLogsRDD
      // WordCount
      .map(log => (log.responseCode, 1))
      // 聚合统计
      .reduceByKey(_ + _)
      // 返回数组
      .collect() // 由于Response Code 状态不多
    println(s"Response Code Count : ${responseCodeToCount.mkString("[", ",", "]")}")


    /**
     * 需求三：IP Address
     * All IP Addresses that have accessed this server more than N times
     */

    val ipAddresses: Array[(String, Int)] = accessLogsRDD
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 0) // 访问网站的次数大于某个值
      .take(1)
    println(s"IP Addresses: ${ipAddresses.mkString(",")}")

    /**
     * 需求四：Endpoint
     * The top endpoints requested by count.
     */

    val topEndpoints: Array[(String, Int)] = accessLogsRDD
      .map(log => (log.endpoint, 1))
      .reduceByKey(_ + _).top(5)(OrderingUtils.SecondValueOrdering)


    //      .map(tuple => (tuple._2, tuple._1))
    //      .sortByKey(ascending = false)
    //      .take(5).map(tuple => (tuple._2, tuple._1))

    println(s"Top Endpoints : ${topEndpoints.mkString("[", ",", "]")}")

    accessLogsRDD.unpersist()


    Thread.sleep(100000000)

    // SparkContext stop
    sc.stop()

  }
}
