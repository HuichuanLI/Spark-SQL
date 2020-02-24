package com.bigdata.DataSource

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object JDBCClientApp {

  def main(args: Array[String]): Unit = {

    // 加载驱动 访问THriftServer
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn: Connection = DriverManager.getConnection("jdbc:hive2://localhost:10000")
    val pstmt: PreparedStatement = conn.prepareStatement("select id,name from pk")
    val rs: ResultSet = pstmt.executeQuery()

    while (rs.next()) {
      println(rs.getObject(1) + " : " + rs.getObject(2))
    }


    // TODO... 自己根据try catch finally封装下可能出现的异常，以及资源的关闭
  }
}
