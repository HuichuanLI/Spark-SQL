package com.bigdata.SparkSession

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DatasetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("DatasetApp").getOrCreate()
    import spark.implicits._

    val ds: Dataset[Person] = Seq(Person("PK", "30")).toDS()
    ds.show()

    val primitiveDS: Dataset[Int] = Seq(1, 2, 3).toDS()
    primitiveDS.map(x => x + 1).collect().foreach(println)

    val peopleDF: DataFrame = spark.read.json("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.json")
    val peopleDS: Dataset[Person] = peopleDF.as[Person]
    peopleDS.show(false)


    // 是在运行期报错
    //peopleDF.select("anme").show()
    //    peopleDS.map(x => x.name).show() //编译期报错

    spark.stop()
  }

  case class Person(name: String, age: String)

}
