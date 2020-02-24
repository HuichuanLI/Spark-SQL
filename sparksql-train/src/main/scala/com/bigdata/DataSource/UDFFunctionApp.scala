package com.bigdata.DataSource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author : lihuichuan
 * Time   : 2020/2/24
 **/
object UDFFunctionApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local").appName("HiveSourceApp")
      .getOrCreate()


    import spark.implicits._

    val infoRDD: RDD[String] = spark.sparkContext.textFile("file:/Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/hobbies.txt")
    val infoDF: DataFrame = infoRDD.map(_.split("###")).map(x => {
      Hobbies(x(0), x(1))
    }).toDF

    infoDF.show()
    // TODO... 定义函数 和 注册函数
    spark.udf.register("hobby_num", (s: String) => s.split(",").size)
    infoDF.createOrReplaceTempView("hobbies")
    spark.sql("select name, hobbies, hobby_num(hobbies) as hobby_count from hobbies").show(false)


    spark.stop()
  }

  case class Hobbies(name: String, hobbies: String)

}

