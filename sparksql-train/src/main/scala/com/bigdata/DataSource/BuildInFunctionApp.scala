package com.bigdata.DataSource

import org.apache.spark.sql.SparkSession

/**
 * Author : lihuichuan
 * Time   : 2020/2/24
 **/
object BuildInFunctionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("HiveserverApp")
      .getOrCreate()
    // 工作中，源码托管在gitlab，svn，cvs，你clone下来以后，千万不要手贱，做代码样式的格式化
    val userAccessLog = Array(
      "2016-10-01,1122", // day  userid
      "2016-10-01,1122",
      "2016-10-01,1123",
      "2016-10-01,1124",
      "2016-10-01,1124",
      "2016-10-02,1122",
      "2016-10-02,1121",
      "2016-10-02,1123",
      "2016-10-02,1123"
    )

    import spark.implicits._
    val userAceessRDD = spark.sparkContext.parallelize(userAccessLog)

    val logs = userAceessRDD.map(x => {
      val strings = x.split(",")
      Log(strings(0), strings(1).toInt)
    }).toDF

    import org.apache.spark.sql.functions._
    logs.groupBy("day").agg(count("userId").as("PV")).show()
    logs.groupBy("day").agg(countDistinct("userId").as("UV")).show()
  }

  case class Log(day: String, userId: Int)

}
