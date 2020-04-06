package com.bigdata.project.sql.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * Author : lihuichuan
 * Time   : 2020/4/6
 **/
object MySQLUtils {

  /**
   * 获取数据库连接
   */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sparksql_project?characterEncoding=UTF-8&user=root&password=root&useSSL=false")
  }

  /**
   * 释放数据库连接等资源
   *
   * @param connection
   * @param pstmt
   */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]) {
    println(getConnection())
  }
}
