package com.bigdata.project.sql

import com.bigdata.project.sql.Bean.{DayCityVideoAccessStat, DayVideoAccessStat, DayVideoTrafficsStat}
import com.bigdata.project.sql.Dao.StatDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * Author : lihuichuan
 * Time   : 2020/4/6
 **/
object TopNStats {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("SparkSessionApp")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .getOrCreate()
    val accessDF = spark.read.format("parquet").load("file:///Users/hui/Desktop/Hadoop/Spark-SQL/Spark-SQL/Spark-SQL/sparksql-train/data/out/")
    accessDF.printSchema()
    accessDF.show(false)

    val day = "20170511"

    StatDao.deleteData(day)

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF, day)

    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark, accessDF, day)

    //按照流量进行统计
    videoTrafficsTopNStat(spark, accessDF, day)
    spark.stop()
  }

  /**
   * 按照流量进行统计
   */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame,day:String): Unit = {
    import spark.implicits._

    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
    cityAccessTopNDF.show(false)

    /**
     * 将统计结果写入到MySQL中
     */
    try {
      cityAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })

        StatDao.insertDayVideoTrafficsAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  //最受欢迎的TopN课程
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame,day:String): Unit = {
    import spark.implicits._
    /**
     * 使用DataFrame的方式进行统计
     */
    val videoTopN = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsID").agg(count("cmsId").as("times"))
      .orderBy($"times".desc)

    videoTopN.show(false)


    /**
     * 使用SQL的方式进行统计
     */
    //    accessDF.createOrReplaceTempView("access_log")
    //    spark.sql("select day,cmsId,count(1) as times from access_log where day = '20170511' and cmsType='video'" +
    //      "group by day,cmsId order by times desc ").show()

    /**
     * 将统计结果写入到MySQL中
     */
    try {
      videoTopN.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsID")
          val traffics = info.getAs[Long]("times")
          list.append(DayVideoAccessStat(day, cmsId, traffics))
        })

        StatDao.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
   * 按照地市进行统计TopN课程
   */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame,day:String): Unit = {
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))

    //Window函数在Spark SQL的使用

    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <=3") //.show(false)  //Top3

    top3DF.show(false)

    /**
     * 将统计结果写入到MySQL中
     */
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDao.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }


}
