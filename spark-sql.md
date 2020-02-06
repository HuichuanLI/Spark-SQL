---
description: 李汇川探索Spark 至Spark SQL
---

# Spark SQL

## MapReduce 槽点

### 吐槽点1
	需求：统计单词出现的个数（词频统计）
	file中每个单词出现的次数
		hello,hello,hello
		world,world
		pk
		1）读取file中每一行的数据
		2) 按照分隔符把每一行的内容进行拆分
		3）按照相同的key分发到同一个任务上去进行累加的操作
		
		
	这是一个简单的不能再简单的一个需求，我们需要开发很多的代码
		1）自定义Mapper
		2）自定义Reducer
		3）通过Driver把Mapper和Reducer串起来
		4）打包，上传到集群上去
		5）在集群上提交我们的wc程序		
	一句话：就是会花费非常多的时间在非业务逻辑改动的工作上	

### 吐槽点2

    Input => MapReduce ==> Output ==> MapReduce ==> Output
    	回顾下MapReduce执行流程：
    		MapTask或者ReduceTask都是进程级别
    		第一个MR的输出要先落地，然后第二个MR把第一个MR的输出当做输入
    		中间过程的数据是要落地

## Spark特性
    1）Speed
        both batch and streaming data
        批流一体 Spark Flink

        快：从哪些角度来知道快呢？
        1、消除了冗余的HDFS读写
        Hadoop每次shuffle操作后，必须写到磁盘，而Spark在shuffle后不一定落盘，可以cache到内存中，以便迭代时使用。如果操作复杂，很多的shufle操作，那么Hadoop的读写IO时间会大大增加。
        
        2、消除了冗余的MapReduce阶段
        Hadoop的shuffle操作一定连着完整的MapReduce操作，冗余繁琐。而Spark基于RDD提供了丰富的算子操作，且reduce操作产生shuffle数据，可以缓存在内存中。
        
        3、JVM的优化
        Hadoop每次MapReduce操作，启动一个Task便会启动一次JVM，基于进程的操作。而Spark每次MapReduce操作是基于线程的，只在启动Executor是启动一次JVM，内存的Task操作是在线程复用的。
        
        4、Spark是基于内存进行数据处理的，MapReduce是基于磁盘进行数据处理的
        
        5.spark 基于DAG模式的
    
    2）Ease of Use
    	high-level operators	
    
    3）Generality
    	stack  栈   生态
    
    4）Runs Everywhere
        It can access diverse data sources
        YARN/Local/Standalone Spark应用程序的代码需要改动吗？
        --master来指定你的Spark应用程序将要运行在什么模式下
     
    

