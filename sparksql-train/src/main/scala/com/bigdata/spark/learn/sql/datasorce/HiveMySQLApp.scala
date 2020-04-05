package com.bigdata.spark.learn.sql.datasorce

import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object HiveMySQLApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").enableHiveSupport().appName("SparkSessionApp").getOrCreate()
    val hiveDF = spark.table("emp")

    hiveDF.show()
  }

}
