package com.bigdata.spark.learn

/**
 * Author : lihuichuan
 * Time   : 2020/4/5
 **/

class Man(val name: String)

object Man {
  implicit def man2SuperMan(man: Man): SuperMan = {
    val name = man.name
    new SuperMan(name)
  }
}

class SuperMan(val name: String) {
  def emitLaser = println("emit a laser....")
}


object implicite {
  def main(args: Array[String]): Unit = {
    val man = new Man("super")
    man.emitLaser
  }
}

//
//Implicit Conversions
//隐式转换
//偷龙转凤，在不知不觉中进行转换操作
//核心：
//定义隐式转换函数，将某个类型转换为另外一个类型。
//要点：
//以implicit开头，而且最好要定义函数返回类型。
//说明：
//隐式转换函数写在哪里比较好呢？？？
//- 推荐：
//让SCALA程序自动的找到函数，进行转换
//【函数放在元类型的伴生对象中】
//- 常用
//可以在程序中手动导入
//import .....

//  集合的列表的函数
//  def sorted[B >: A](implicit ord: Ordering[B])
//  使用implicit声明的函数的参数称为隐式参数
//
//  def sortBy[B](f: A => B)(implicit ord: Ordering[B])
//  有两个参数：
//  第一个参数：排序字段
//  第二个参数：隐式参数，指定排序升序还是降序，通常情况下，隐式参数放在另外的一个括号中，不与普通参数放在一起。
