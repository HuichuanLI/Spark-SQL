package com.bigdata.hpsk.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark Application 编程模板
 */
object WordCount {

  /**
   * 如果Spark Application运行在本地的话，Driver Program
   * JVM Process
   */
  def main(args: Array[String]): Unit = {

    /**
     * 从前面spark-shell命令行可知：
     * Spark 数据分析的程序入口SparkContext，用于读取数据
     */
    // 读取Spark Application的配置信息
    val sparkConf = new SparkConf()
      // 设置SparkApplication名称
      .setAppName("ModuleSpark Application")
      // 设置程序运行的环境，通常情况下，在IDE中开发的时候，设置为local mode，至少是两个Thread
      // 在实际部署的时候通过通过提交应用的命令悍进行设置
      .setMaster("local[2]")
    // 创建SparkContext上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)

    /** ===================================================================*/
    /**
     * Step 1: read data
     * SparkContext用于读取数据 -> RDD
     */
    val linesRdd = sc.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/wc.txt")

    val tupleRDD: RDD[(String, Int)] = linesRdd
      .flatMap(_.split(",").toList.filter(word => word.trim.length > 0).map(word => (word.trim.toLowerCase, 1)))

    val wordCountRDD: RDD[(String, Int)] = tupleRDD.reduceByKey((a, b) => a + b)
    wordCountRDD.cache()
    wordCountRDD.map(tuple => {
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@@")
      (tuple._1 + " - " + tuple._2)
    }).count()

    println("-------------------------------------------------")

    wordCountRDD.mapPartitions(iter => {
      println("##############################")
      iter.toList.map(tuple => (tuple._1 + " - " + tuple._2)).toIterator
    }).count()

    /**
     * Step 2: process data
     * RDD#transformation
     */
    //    wordCountRDD.sortBy(tuple => tuple._2, false).take(3).foreach(println)
    //    wordCountRDD.map(tuple => (tuple._2, tuple._1)).sortByKey(false).map(tuple => (tuple._2, tuple._1)).take(3).foreach(println)

    wordCountRDD.map(tuple => (tuple._2, tuple._1)).top(3).map(tuple => (tuple._2, tuple._1)).foreach(println)
    // 释放内存
    wordCountRDD.unpersist()


    /**
     * Step 3: write data
     * 将处理的结果数据存储
     * RDD#action
     */

    /** ===================================================================*/
    // 在开发测试的时候，为了在每个Application页面监控查看应用中Job的运行
    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
