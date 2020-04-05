package com.bigdata

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object ModuleSQL {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      // 设置SparkApplication名称
      .setAppName("ModuleSpark Application")
      // 设置程序运行的环境，通常情况下，在IDE中开发的时候，设置为local mode，至少是两个Thread
      // 在实际部署的时候通过通过提交应用的命令悍进行设置
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(sparkConf)

    val sqlContext = new SQLContext(sc)

    //2)相关处理


    //3）关闭资源
    sc.stop()
  }
}
