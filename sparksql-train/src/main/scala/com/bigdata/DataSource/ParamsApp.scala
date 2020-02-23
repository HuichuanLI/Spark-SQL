package com.bigdata.DataSource

import com.typesafe.config.{Config, ConfigFactory}

object ParamsApp {

  def main(args: Array[String]): Unit = {

    val config: Config = ConfigFactory.load()
    val url: String = config.getString("db.default.url")
    println(url)

  }

}
