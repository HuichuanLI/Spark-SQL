package com.bigdata.spark.learn.core.log

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/

case class ApacheAccessLog(
                            ipAddress: String,
                            clientIdented: String,
                            userId: String,
                            dateTime: String,
                            method: String,
                            endpoint: String,
                            protocol: String,
                            responseCode: Int,
                            contentSize: Long)


object ApacheAccessLog {
  // 正则表达式, 一般情况下以 ^ 开始，以 $ 结束
  // 1.1.1.1 - - [21/Jul/2014:10:00:00 -0800] "GET /chapter1/java/src/main/java/com/databricks/apps/logs/LogAnalyzer.java HTTP/1.1" 200 1234
  val PARTTERN: Regex = """^(\S+) (-|\S+) (-|\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  def parseLogLine(log: String): ApacheAccessLog = {
    // 使用正则表达式进行匹配
    // parse log info
    val res: Option[Match] = PARTTERN.findFirstMatchIn(log)

    // invalidate
    if (res.isEmpty) {
      throw new RuntimeException(s"Cannot parse log line: ${log}")
    }

    // get value
    val m: Match = res.get

    // return
    ApacheAccessLog(
      m.group(1),
      m.group(2),
      m.group(3),
      m.group(4),
      m.group(5),
      m.group(6),
      m.group(7),
      m.group(8).toInt,
      m.group(9).toLong
    )

  }

  def isValidateLogLine(log: String): Boolean = {
    // 使用正则表达式进行匹配
    val res: Option[Match] = PARTTERN.findFirstMatchIn(log)

    // return
    !res.isEmpty // 当匹配成功以后，res不为空，此时数据才保留
  }

}
