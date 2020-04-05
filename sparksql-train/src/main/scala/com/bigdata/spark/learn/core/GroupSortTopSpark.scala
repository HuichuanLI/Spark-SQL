package com.bigdata.spark.learn.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object GroupSortTopSpark {
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

    val linesRDD = sc.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/groups.txt")
    //    println(s"count=${linesRDD.count()}")


    /**
     * 分析：
     * 每行数据先进行分割，将第一个字段作为key，使用groupByKey进行分组，再对组内数据进行排序
     */
    val sortTopRDD: RDD[(String, List[Int])] = linesRDD
      .map(line => {
        val split = line.split(" ")
        (split(0), split(1).toInt)
      }).groupByKey()
      .map {
        case (key, iter) => {
          // iter 迭代器中的元素进行排序
          val sortList: List[Int] = iter.toList.sorted.takeRight(3).reverse
          (key, sortList)
        }
      }

    // 输出：打印数据
    sortTopRDD.coalesce(1).sortByKey().foreach(println)

    println("--------------- 分阶段进行排序 -----------------------")
    linesRDD
      // 分割数据，返回(key, value)
      .map(line => {
        // 进行分割
        val splited = line.split(" ")
        // return
        (splited(0), splited(1).toInt)
      }) // 如何分阶段呢？打乱原始Key，通过加前缀
      /**
       * 尽心第一阶段分组排序
       */
      .map(tuple => {
        // 随机数实例
        val random = new Random(2) // 要么是0 ，要么是1
        //
        (random + "_" + tuple._1, tuple._2)
      }).groupByKey() //  RDD[(String, Iterable[Int])]
      .map {
        case (key, iter) => {
          // iter 迭代器中的元素进行排序
          val sortList: List[Int] = iter.toList.sorted.takeRight(3).reverse
          // return
          (key, sortList)
        }
      } // 去掉前缀，在此进行聚合操作
      /**
       * 进行第二阶段分组排序
       */
      .map(tuple => {
        val key = tuple._1.split("_")(1)
        //
        (key, tuple._2)
      })
      .groupByKey() // RDD[(String, List[Int])]
      .map {
        case (key, iter) => {
          val sortList: List[Int] = iter.toList.flatMap(_.toList).sorted.takeRight(3).reverse
          //
          (key, sortList)
        }
      }.foreach(println)

    println("--------------- 优化后....分阶段进行排序 -----------------------")

    val xx = linesRDD
      // 分割数据，返回(key, value)
      .map(line => {
        // 随机数实例
        val random = new Random(2) // 要么是0 ，要么是1
        // 进行分割
        val splited = line.split(" ")
        // return
        (random + "_" + splited(0), splited(1).toInt)
      })
      .groupByKey() //  RDD[(String, Iterable[Int])]
      // 对组内的数据进行排序
      .map {
        case (key, iter) => {
          // iter 迭代器中的元素进行排序
          val sortList: List[Int] = iter.toList.sorted.takeRight(3).reverse
          // return
          (key.split("_")(1), sortList)
        }
      }
      .groupByKey() // RDD[(String, Iterable[List[Int]])]
      .map {
        case (key, iter) => {
          val sortList: List[Int] = iter.toList.flatMap(_.toList).sorted.takeRight(3).reverse
          //
          (key, sortList)
        }
      }
      .foreach(println)

    /**
     * Step 2: process data
     * RDD#transformation
     */

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
