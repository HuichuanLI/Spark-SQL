package com.bigdata.SparkSession

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object SQLContextApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("SQLContextApp").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf) // 此处一定要把SparkConf传进来
    val sqlContext: SQLContext = new SQLContext(sc)

    val df: DataFrame = sqlContext.read.text("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/input2.txt")
    df.show()

    sc.stop()
  }

}
