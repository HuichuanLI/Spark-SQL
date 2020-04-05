package com.bigdata.spark.learn.sql.dataframe

import com.bigdata.spark.learn.sql.dataframe.RDD2Dataframe.Info
import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object DataframeDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("SparkSessionApp").getOrCreate()
    val rdd = spark.sparkContext.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/student.data")
    import spark.implicits._
    val infoDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    infoDF.show(false)


    infoDF.take(10).foreach(println)

    infoDF.head(3)

    infoDF.select(infoDF.col("name"), infoDF.col("email")).show(false)
    infoDF.filter("name='' or name ='NULL'").show(false)
    infoDF.filter("SUBSTR(name,0,1)='M'").show()


    // 降序
    infoDF.sort(infoDF("name").desc).show()

    infoDF.sort("name", "id").show()

    infoDF.sort(infoDF("name").desc, infoDF("id").asc).show()


    val infoDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    infoDF.join(infoDF2, infoDF("id") === infoDF2("id")).show()
    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)

}
