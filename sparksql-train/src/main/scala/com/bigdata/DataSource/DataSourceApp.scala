package com.bigdata.DataSource

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Author : lihuichuan
 * Time   : 2020/2/23
 **/
object DataSourceApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    jdbc(spark)
    spark.stop()
  }

  def text(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val dataFrame = sparkSession.read.text("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.txt")
    val result = dataFrame.map(x => {
      val splits: Array[String] = x.getString(0).split(",")
      val builder = new StringBuilder()
      builder.append(splits(0).trim).append(",")
        .append(splits(1).trim)

      builder.toString()
    })
    //    result.write.mode(SaveMode.Overwrite).text("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/out")
    result.write.mode("overwrite").text("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/out")
  }

  // JSON
  def json(spark: SparkSession): Unit = {
    import spark.implicits._
    //    val jsonDF: DataFrame = spark.read.json("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.json")

    //    jsonDF.show()

    // TODO... 只要age>20的数据
    //    jsonDF.filter("age > 20").select("name").write.mode(SaveMode.Overwrite).json("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/out")

    val jsonDF2: DataFrame = spark.read.json("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people2.json")
    //    jsonDF2.show()
    jsonDF2.select($"name", $"age", $"info.work".as("work"), $"info.home".as("home")).show()
  }

  // 标准写法
  def common(spark: SparkSession): Unit = {
    import spark.implicits._

    // 源码面前 了无秘密
    //    val textDF: DataFrame = spark.read.format("text").load("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/people.txt")
    val jsonDF: DataFrame = spark.read.format("json").load("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/data/people.json")
    //
    //    textDF.show()
    //    println("~~~~~~~~")
    //    jsonDF.show()

    jsonDF.write.format("json").mode("overwrite").save("out")

  }

  // Parquet数据源
  def parquet(spark: SparkSession): Unit = {
    import spark.implicits._

    val parquetDF: DataFrame = spark.read.parquet("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/users.parquet")
    parquetDF.printSchema()
    parquetDF.show()

    //    parquetDF.select("name","favorite_numbers")
    //      .write.mode("overwrite")
    //        .option("compression","none")
    //      .parquet("out")

    //    spark.read.parquet("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/out").show()
  }

  // 存储类型转换：JSON==>Parquet
  def convert(spark: SparkSession): Unit = {
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.format("json").load("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.json")
    //    jsonDF.show()

    jsonDF.filter("age>20").write.format("parquet").mode(SaveMode.Overwrite).save("out")

    //    spark.read.parquet("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/out").show()

  }

  /**
   * 有些数据是在MySQL，如果使用Spark处理，肯定需要通过Spark读取出来MySQL的数据
   * 数据源是text/json，通过Spark处理完之后，我们要将统计结果写入到MySQL
   */
  def jdbc(spark: SparkSession): Unit = {
    import spark.implicits._

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "movie_cat.movie")
      .option("user", "root")
      .option("password", "root")
      .load()
    jdbcDF.show()
    //
    //    jdbcDF.filter($"cnt" > 100).show(100)

    // 死去活来法

    val url = "jdbc:mysql://localhost:3306"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")

    val jdbcDF1: DataFrame = spark.read.jdbc(url, "spark.browser_stat", connectionProperties)

    jdbcDF1.filter($"cnt" > 100).write.jdbc(url, "spark.browser_stat_2", connectionProperties)
  }
}
