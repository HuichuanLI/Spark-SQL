package com.bigdata.SparkSession

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameAPIApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("DataFrameAPIApp").getOrCreate()
    import spark.implicits._

    //
    //    val people: DataFrame = spark.read.json("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/people.json")
    //
    //    people.printSchema() // 查看DF的内部结构：列名、列的数据类型、是否可以为空
    //
    //    people.show() // 展示出DF内部的数据
    //
    //    // TODO... DF里面有两列，只要name列 ==> select name from people
    //    //    people.select("name").show()
    //    //    people.select($"name").show()
    //
    //    // TODO...  select * from people where age > 21
    //    //people.filter($"age" > 21).show()
    //    //    people.filter("age > 21").show()
    //
    //    // TODO... select age, count(1) from people group by age
    //    people.groupBy("age").count().show()
    //
    //    // TODO... select name,age+10 from people
    //    // people.select($"name", ($"age"+10).as("new_age")).show()
    //
    //
    //    // TODO... 使用SQL的方式操作
    //    people.createOrReplaceTempView("people")
    //    spark.sql("select name from people where age > 21").show()


    val zips: DataFrame = spark.read.json("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/zips.json")
    zips.printSchema() // 查看schema信息

    /**
     * 1）loc的信息没用展示全，超过一定长度就使用...来展示
     * 2）只显示了前20条
     * show() ==> show(20) ==> show(numRows, truncate = true)
     */
    zips.show(20, false)

    //zips.head(3).foreach(println)
    //zips.first()
    //zips.take(5)

    val count: Long = zips.count()
    println(s"Total Counts: $count")

    // 过滤出大于40000，字段重新命名
    //    zips.filter(zips.col("pop") > 40000).withColumnRenamed("_id", "new_id").show(10, false)


    import org.apache.spark.sql.functions._
    // 统计加州pop最多的10个城市名称和ID  desc是一个内置函数
//    zips.select("_id", "city", "pop", "state").filter(zips.col("state") === "CA").orderBy(desc("pop")).show(10, false)

    zips.createOrReplaceTempView("zips")
    spark.sql("select _id,city,pop,state from zips where state='CA' order by pop desc limit 10").show()


    spark.stop()
  }
}
