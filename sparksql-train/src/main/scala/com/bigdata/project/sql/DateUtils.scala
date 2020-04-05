package com.bigdata.project.sql

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat



/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
object DateUtils {
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
   * 获取时间：yyyy-MM-dd HH:mm:ss
   */
  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
   * 获取输入日志时间：long类型
   *
   * time: [10/Nov/2016:00:01:02 +0800]
   */
  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,
        time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  def main(args: Array[String]) {
    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }
}
