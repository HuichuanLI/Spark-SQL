package com.bigdata.spark.learn.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author : lihuichuan
 * Time   : 2020/4/4
 **/
object TraitLog {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      // 设置SparkApplication名称
      .setAppName("ModuleSpark Application")
      // 设置程序运行的环境，通常情况下，在IDE中开发的时候，设置为local mode，至少是两个Thread
      // 在实际部署的时候通过通过提交应用的命令悍进行设置
      .setMaster("local[*]")
    // 创建SparkContext上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)

    val linesRdd = sc.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/access.log")

    val filiterRDD = linesRdd.filter(line => line.trim.length > 0)
      .map(line => {
        val splited = line.split("\t")
        (splited(0).split(" ")(0), splited(1), splited(2), splited(3))
      })
    filiterRDD.cache()

    val pvRDD = filiterRDD.map {
      case (date, url, time, guid) => (date, url)
    }.filter(tuple => tuple._2.trim.length > 0).map {
      case (date, url) => (date, 1)
    }.reduceByKey(_ + _).sortBy(tuple => tuple._2, false)
    pvRDD.take(2).foreach(println)

    //UV
    val uvRDD = filiterRDD.map {
      case (date, url, time, guid) => (date, guid)
    }.distinct().map {
      case (date, url) => (date, 1)
    }.reduceByKey(_ + _).sortBy(tuple => tuple._2, false)
    uvRDD.take(2).foreach(println)

    val joinRDD: RDD[(String, (Int, Int))] = pvRDD.join(uvRDD)
    joinRDD.coalesce(1).foreach {
      case (date, (pv, uv)) => println(s"${date} \t ${pv} \t ${uv}")
    }

    val unionRDD: RDD[(String, Int)] = pvRDD.union(uvRDD)
    unionRDD.coalesce(1).foreachPartition(iter => {
      iter.foreach(item => {
        println(item)
      })
    })

    // 判断RDD是否唯恐

    println(unionRDD.partitions.size)
    Thread.sleep(1000000)

    sc.stop()
  }
}
