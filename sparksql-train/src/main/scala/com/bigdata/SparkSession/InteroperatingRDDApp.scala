package com.bigdata.SparkSession

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InteroperatingRDDApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("DatasetApp").getOrCreate()
    //    runInferSchema(spark)

    runProgrammaticSchema(spark)
    spark.stop()
  }

  /**
   * 第二种方式：自定义编程
   */
  def runProgrammaticSchema(spark: SparkSession): Unit = {
    import spark.implicits._


    // step1
    val peopleRDD: RDD[String] = spark.sparkContext.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.txt")
    val peopleRowRDD: RDD[Row] = peopleRDD.map(_.split(",")) // RDD
      .map(x => Row(x(0), x(1).trim.toInt))

    // step2
    val struct =
      StructType(
        StructField("name", StringType, true) ::
          StructField("age", IntegerType, false) :: Nil)

    // step3
    val peopleDF: DataFrame = spark.createDataFrame(peopleRowRDD, struct)

    peopleDF.show()

    peopleRowRDD
  }

  /**
   * 第一种方式：反射
   * 1）定义case class
   * 2）RDD map，map中每一行数据转成case class
   */
  def runInferSchema(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleRDD: RDD[String] = spark.sparkContext.textFile("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.txt")

    //TODO... RDD => DF
    val peopleDF: DataFrame = peopleRDD.map(_.split(",")) //RDD
      .map(x => People(x(0), x(1).trim.toInt)) //RDD
      .toDF()
    //peopleDF.show(false)

    peopleDF.createOrReplaceTempView("people")
    val queryDF: DataFrame = spark.sql("select name,age from people where age between 19 and 29")
    //queryDF.show()

    //queryDF.map(x => "Name:" + x(0)).show()  // from index
    queryDF.map(x => "Name:" + x.getAs[String]("name")).show // from field
  }

  case class People(name: String, age: Int)

}
