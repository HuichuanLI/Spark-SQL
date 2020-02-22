package main.scala.com.bigdata

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 词频统计案例
  * 输入：文件
  * 需求：统计出文件中每个单词出现的次数
  * 1）读每一行数据
  * 2）按照分隔符把每一行的数据拆成单词
  * 3）每个单词赋上次数为1
  * 4）按照单词进行分发，然后统计单词出现的次数
  * 5）把结果输出到文件中
  * 输出：文件
  */
object SparkWordCountAppV2 {

  /**
    * master: 运行模式，local
    *
    * 奇怪了：老师，你本机安装spark了吗？为什么这里就能运行呢？
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)


    // Spark特性：提供了80+高阶API
    val rdd = sc.textFile(args(0))

    /**
      * 结果按照单词的出现的个数的降序排列
      */

    // 注意：scala的知识是一定要掌握的
    rdd.flatMap(_.split(",")).map(word => (word, 1))
      .reduceByKey(_+_).map(x => (x._2, x._1)).sortByKey(false)
        .map(x=> (x._2, x._1))
      .saveAsTextFile(args(1))
      //.collect().foreach(println)

      //.sortByKey().collect().foreach(println)
        //.saveAsTextFile("file:///Users/rocky/IdeaProjects/imooc-workspace/sparksql-train/out")
      //.collect().foreach(println)

    //rdd.collect().foreach(println)

    sc.stop()
  }
}


