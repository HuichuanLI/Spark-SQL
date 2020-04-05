package com.bigdata.spark.learn

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/

class signPen {
  def write(name: String) = println(s"name=${name}")
}

object signPen {
  implicit val signPen = new signPen()
}

object impliciteParamDemp {
  def sign(name: String)(implicit signPen: signPen): Unit = {
    signPen.write(name)
  }

  def main(args: Array[String]): Unit = {
    sign("huichuan")
  }
}
