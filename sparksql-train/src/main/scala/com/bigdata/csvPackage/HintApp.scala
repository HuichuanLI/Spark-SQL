package com.bigdata.csvPackage

import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
  * Author: Michael PK
  */
object HintApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HintApp").master("local[2]").getOrCreate()

    val users = 1.to(10000).map(x => {
      User(x, UUID.randomUUID().toString, x%100)
    })

    import spark.implicits._
    val usersDF = spark.sparkContext.parallelize(users).toDF()
    usersDF.createOrReplaceTempView("users")
    spark.sql("create table pk_users_11 as select /*+ REPARTITION(3) */ age, count(1) " +
      "from users where age between 10 and 60 group by age").explain()








    //    val catalog = spark.catalog
    //    catalog.listDatabases().show(false)
    //    catalog.listDatabases().select("name").show(false)
    //
    //
    //    catalog.listTables("default").select("name","database").show(false)
    //
    //
    //    catalog.listFunctions().show(false)








    spark.stop()
  }


  case class User(id:Int, name:String, age:Int)

}
