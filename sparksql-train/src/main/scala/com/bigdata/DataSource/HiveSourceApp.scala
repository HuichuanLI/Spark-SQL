package com.bigdata.DataSource

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveSourceApp {

  def main(args: Array[String]): Unit = {


    // 如果你想使用Spark来访问Hive的时候，一定需要开启Hive的支持
    val spark: SparkSession = SparkSession.builder().master("local").appName("HiveSourceApp")
      .enableHiveSupport() //切记：一定要开启
      .getOrCreate()


    // 走的就是连接 default数据库中的pk表，如果你是其他数据库的，那么也采用类似的写法即可
    spark.table("pk").show()


    // input(Hive/MySQL/JSON...) ==> 处理 ==> output (Hive)


    import spark.implicits._
    //
    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val driver = config.getString("db.default.driver")
    val database = config.getString("db.default.database")
    val table = config.getString("db.default.table")
    val sinkTable = config.getString("db.default.sink.table")
    //
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    //
    //    val jdbcDF: DataFrame = spark.read
    //      .jdbc(url, s"$database.$table", connectionProperties).filter($"cnt" > 100)
    //
    //    //jdbcDF.show()
    //
    //    jdbcDF.write.saveAsTable("browser_stat_hive")
    //
    //    // TODO...  saveAsTable和insertInto的区别
    //    jdbcDF.write.insertInto("browser_stat_hive_1")

    spark.stop()

  }
}
