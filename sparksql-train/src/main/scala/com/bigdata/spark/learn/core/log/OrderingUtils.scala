package com.bigdata.spark.learn.core.log

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/
/**
 * 自定义排序规则工具类
 */
object OrderingUtils {

  object SecondValueOrdering extends scala.math.Ordering[(String, Int)] {
    /**
     * 比较第二Value的大小
     *
     * @param x
     * @param y
     * @return
     */
    override def compare(x: (String, Int), y: (String, Int)): Int = {
      x._2.compare(y._2)
    }
  }


}